/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "job.h"
#include "log.h"
#include "sys.h"
#include "mysql_conn.h"
#include "pgsql_conn.h"
#include "http_client.h"
#include "node_info.h"
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>

Job* Job::m_inst = NULL;
int Job::do_exit = 0;

int64_t num_job_threads = 3;
extern int64_t http_server_port;
extern std::string http_upload_path;
extern std::string mysql_install_path;
extern std::string pgsql_install_path;
extern std::string cluster_install_path;
extern int64_t stmt_retries;
extern int64_t stmt_retry_interval_ms;
extern "C" void *thread_func_job_work(void*thrdarg);

Job::Job():
	path_id(0)
{
	
}

Job::~Job()
{

}

void Job::start_job_thread()
{
	int error = 0;
	pthread_mutex_init(&thread_mtx, NULL);
	pthread_cond_init(&thread_cond, NULL);

	//start job work thread
	for(int i=0; i<num_job_threads; i++)
	{
		pthread_t hdl;
		if ((error = pthread_create(&hdl,NULL, thread_func_job_work, m_inst)))
		{
			char errmsg_buf[256];
			syslog(Logger::ERROR, "Can not create http server work thread, error: %d, %s",
						error, errno, strerror_r(errno, errmsg_buf, sizeof(errmsg_buf)));
			do_exit = 1;
			return;
		}
		vec_pthread.push_back(hdl);
	}
}

void Job::join_all()
{
	do_exit = 1;
	pthread_mutex_lock(&thread_mtx);
	pthread_cond_broadcast(&thread_cond);
	pthread_mutex_unlock(&thread_mtx);
	
	for (auto &i:vec_pthread)
	{
		pthread_join(i, NULL);
	}
}

bool Job::get_job_type(char *str, Job_type &job_type)
{
	if(strcmp(str, "send")==0)
		job_type = JOB_SEND;
	else if(strcmp(str, "recv")==0)
		job_type = JOB_RECV;
	else if(strcmp(str, "delete")==0)
		job_type = JOB_DELETE;
	else if(strcmp(str, "peek")==0)
		job_type = JOB_PEEK;
	else if(strcmp(str, "update_node")==0)
		job_type = JOB_UPDATE_NODE;
	else if(strcmp(str, "mysql_cmd")==0)
		job_type = JOB_MYSQL_CMD;
	else if(strcmp(str, "pgsql_cmd")==0)
		job_type = JOB_PGSQL_CMD;
	else if(strcmp(str, "cluster_cmd")==0)
		job_type = JOB_CLUSTER_CMD;
	else if(strcmp(str, "get_node")==0)
		job_type = JOB_GET_NODE;
	else if(strcmp(str, "get_info")==0)
		job_type = JOB_GET_INFO;
	else
	{
		job_type = JOB_NONE;
		return false;
	}

	return true;
}

bool Job::get_file_type(char *str, File_type &file_type)
{
	if(strcmp(str, "table")==0)
		file_type = FILE_TABLE;
	else if(strcmp(str, "binlog")==0)
		file_type = FILE_BINLOG;
	else if(strcmp(str, "mysql")==0)
		file_type = FILE_MYSQL;
	else if(strcmp(str, "pgsql")==0)
		file_type = FILE_PGSQL;
	else if(strcmp(str, "cluster")==0)
		file_type = FILE_CLUSTER;
	else
		file_type = FILE_NONE;

	return true;
}

bool Job::get_table_path(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &db, std::string &tb, std::string &tb_path)
{
	syslog(Logger::INFO, "get_table_path ip=%s,port=%d,user=%s,psw=%s,db=%s,tb=%s", 
						ip.c_str(), port, user.c_str(), psw.c_str(), db.c_str(), tb.c_str());

	int retry = stmt_retries;
	MYSQL_CONN mysql_conn;

	while(retry--)
	{
		if(mysql_conn.connect(db.c_str(), ip.c_str(), port, user.c_str(), psw.c_str()))
		{
			syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s,db=%s", 
							ip.c_str(), port, user.c_str(), psw.c_str(), db.c_str());
			continue;
		}

		if(mysql_conn.send_stmt(SQLCOM_SELECT, "select @@datadir"))
			continue;

		MYSQL_ROW row;
		if ((row = mysql_fetch_row(mysql_conn.result)))
		{
			syslog(Logger::INFO, "row[]=%s",row[0]);
			tb_path = row[0];
		}
		else
		{
			continue;
		}

		break;
	}
	mysql_conn.free_mysql_result();
	mysql_conn.close_conn();

	if(retry<0)
		return false;

	size_t pos1 = db.find("_$$_");
	if(pos1 == size_t(-1))
	{
		syslog(Logger::ERROR, "database=%s error", db.c_str());
		return false;
	}

	tb_path = tb_path + db.substr(0,pos1) + "_@0024@0024_" + db.substr(pos1+4, db.length()-pos1+4);
	tb_path = tb_path + "/" +  tb + ".ibd";

	return true;
}

bool Job::get_binlog_path(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &binlog_path)
{
	syslog(Logger::INFO, "get_binlog_path ip=%s,port=%d,user=%s,psw=%s", 
						ip.c_str(), port, user.c_str(), psw.c_str());

	int retry = stmt_retries;
	MYSQL_CONN mysql_conn;

	while(retry--)
	{
		if(mysql_conn.connect(NULL, ip.c_str(), port, user.c_str(), psw.c_str()))
		{
			syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
							ip.c_str(), port, user.c_str(), psw.c_str());
			continue;
		}

		if(mysql_conn.send_stmt(SQLCOM_SELECT, "select @@log_error"))
			continue;

		MYSQL_ROW row;
		if ((row = mysql_fetch_row(mysql_conn.result)))
		{
			syslog(Logger::INFO, "row[]=%s",row[0]);
			binlog_path = row[0];
		}
		else
		{
			continue;
		}

		break;
	}
	mysql_conn.free_mysql_result();
	mysql_conn.close_conn();

	if(retry<0)
		return false;

	return true;
}

bool Job::delete_db_table(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &db, std::string &tb)
{
	syslog(Logger::INFO, "delete_db_table ip=%s,port=%d,user=%s,psw=%s,db=%s,tb=%s", 
						ip.c_str(), port, user.c_str(), psw.c_str(), db.c_str(), tb.c_str());
	
	int retry = stmt_retries;
	MYSQL_CONN mysql_conn;
	
	while(retry--)
	{
		if(mysql_conn.connect(db.c_str(), ip.c_str(), port, user.c_str(), psw.c_str()))
		{
			syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s,db=%s", 
							ip.c_str(), port, user.c_str(), psw.c_str(), db.c_str());
			continue;
		}

		std::string str_sql;
		str_sql = "drop table " + tb;
		if(mysql_conn.send_stmt(SQLCOM_DELETE, str_sql.c_str()))
			continue;

		break;
	}
	mysql_conn.free_mysql_result();
	mysql_conn.close_conn();
	
	if(retry<0)
		return false;
	
	return true;
}

bool Job::add_file_path(std::string &id, std::string &path)
{
	std::unique_lock<std::mutex> lock(mutex_);

	if(map_id_path.size()>=kMaxPathId)
		map_id_path.erase(std::to_string(path_id));

	id = std::to_string(path_id);
	
	map_id_path[std::to_string(path_id++)] = path;
	path_id %= kMaxPathId;

	return true;
}

bool Job::get_file_path(std::string &id, std::string &path)
{
	std::unique_lock<std::mutex> lock(mutex_);

	auto it = map_id_path.find(id);
	if(it == map_id_path.end())
		return false;
	else
		path = it->second;

	return true;
}

void Job::job_delete(cJSON *root)
{
	std::string job_id;
	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	File_type file_type;
	item = cJSON_GetObjectItem(root, "file_type");
	if(item == NULL || !get_file_type(item->valuestring, file_type))
	{
		syslog(Logger::ERROR, "get_file_type error");
		return;
	}
	
	std::string ip;
	int port;
	item = cJSON_GetObjectItem(root, "s_ip");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_ip error");
		return;
	}
	ip = item->valuestring;

	item = cJSON_GetObjectItem(root, "s_port");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_port error");
		return;
	}
	port = item->valueint;

	bool local = Node_info::get_instance()->check_local_ip(ip);
	if(!local)
	{
		syslog(Logger::ERROR, "ip %s is no local_ip", ip.c_str());
		return;
	}
	
	std::string user;
	item = cJSON_GetObjectItem(root, "s_user");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_user error");
		return;
	}
	user = item->valuestring;

	std::string pwd;
	item = cJSON_GetObjectItem(root, "s_pwd");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_pwd error");
		return;
	}
	pwd = item->valuestring;

	if(file_type == FILE_TABLE)
	{
		std::string db;
		item = cJSON_GetObjectItem(root, "s_dbname");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get s_dbname error");
			return;
		}
		db = item->valuestring;

		std::string tb;
		item = cJSON_GetObjectItem(root, "s_table_name");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get s_table_name error");
			return;
		}
		tb = item->valuestring;
	
		if(!delete_db_table(ip, port, user, pwd, db, tb))
		{
			syslog(Logger::ERROR, "delete_db_table error");
			return;
		}
	}
	else if(file_type == FILE_BINLOG)
	{
		std::string binlog_path;
		if(!get_binlog_path(ip, port, user, pwd, binlog_path))
		{
			syslog(Logger::ERROR, "get_binlog_path error");
			return;
		}

		if (remove(binlog_path.c_str()) == 0)
		{
			syslog(Logger::INFO, "delete file %s succeed!",binlog_path.c_str());
		}
		else
		{
			syslog(Logger::ERROR, "delete file %s fail!",binlog_path.c_str());
		}
	}
}

void Job::job_send(cJSON *root)
{
	std::string job_id;
	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	File_type file_type;
	item = cJSON_GetObjectItem(root, "file_type");
	if(item == NULL || !get_file_type(item->valuestring, file_type))
	{
		syslog(Logger::ERROR, "get_file_type error");
		return;
	}
	
	std::string ip;
	int port;
	item = cJSON_GetObjectItem(root, "s_ip");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_ip error");
		return;
	}
	ip = item->valuestring;

	item = cJSON_GetObjectItem(root, "s_port");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_port error");
		return;
	}
	port = item->valueint;

	bool local = Node_info::get_instance()->check_local_ip(ip);
	if(!local)
	{
		syslog(Logger::ERROR, "ip %s is no local_ip", ip.c_str());
		return;
	}
	
	std::string user;
	item = cJSON_GetObjectItem(root, "s_user");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_user error");
		return;
	}
	user = item->valuestring;

	std::string pwd;
	item = cJSON_GetObjectItem(root, "s_pwd");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get s_pwd error");
		return;
	}
	pwd = item->valuestring;

	if(file_type == FILE_TABLE)
	{
		std::string db;
		item = cJSON_GetObjectItem(root, "s_dbname");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get s_dbname error");
			return;
		}
		db = item->valuestring;

		std::string tb;
		item = cJSON_GetObjectItem(root, "s_table_name");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get s_table_name error");
			return;
		}
		tb = item->valuestring;
	
		std::string tb_path;
		if(!get_table_path(ip, port, user, pwd, db, tb, tb_path))
		{
			syslog(Logger::ERROR, "get_table_path error");
			return;
		}

		std::string remote_ip;
		int remote_port;

		item = cJSON_GetObjectItem(root, "d_ip");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get d_ip error");
			return;
		}
		remote_ip = item->valuestring;
		
		item = cJSON_GetObjectItem(root, "d_port");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get d_port error");
			return;
		}
		remote_port = item->valueint;

		//set a http get url
		std::string id;
		add_file_path(id, tb_path);

		std::string tb_name = tb + ".ibd";
		std::string post_url = "http://" + remote_ip + ":" + std::to_string(http_server_port);
		syslog(Logger::INFO, "post_url %s", post_url.c_str());

		cJSON_DeleteItemFromObject(root, "job_type");
		cJSON_AddStringToObject(root, "job_type", "RECV");

		std::string url = "http://" + ip + ":" + std::to_string(http_server_port) + "/" + id;
		cJSON_AddStringToObject(root, "url", url.c_str());

		char *cjson;
		cjson = cJSON_Print(root);
		if(cjson == NULL)
			return;

		//syslog(Logger::INFO, "cjson=%s", cjson);
		std::string result_str;
		Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
		free(cjson);
	}
	else if(file_type == FILE_BINLOG)
	{
	
	}
}

void Job::job_recv(cJSON *root)
{
	std::string job_id;
	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	File_type file_type;
	item = cJSON_GetObjectItem(root, "file_type");
	if(item == NULL || !get_file_type(item->valuestring, file_type))
	{
		syslog(Logger::ERROR, "get_file_type error");
		return;
	}

	std::string ip;
	int port;
	
	item = cJSON_GetObjectItem(root, "d_ip");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get d_ip error");
		return;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "d_port");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get d_port error");
		return;
	}
	port = item->valueint;

	bool local = Node_info::get_instance()->check_local_ip(ip);
	if(!local)
	{
		syslog(Logger::ERROR, "ip %s is no local_ip", ip.c_str());
		return;
	}

	if(file_type == FILE_TABLE)
	{
		std::string user;
		item = cJSON_GetObjectItem(root, "d_user");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get d_user error");
			return;
		}
		user = item->valuestring;
		
		std::string pwd;
		item = cJSON_GetObjectItem(root, "d_pwd");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get d_pwd error");
			return;
		}
		pwd = item->valuestring;

		std::string db;
		item = cJSON_GetObjectItem(root, "d_dbname");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get d_dbname error");
			return;
		}
		db = item->valuestring;

		std::string tb;
		item = cJSON_GetObjectItem(root, "d_table_name");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get d_table_name error");
			return;
		}
		tb = item->valuestring;
	
		std::string tb_path;
		if(!get_table_path(ip, port, user, pwd, db, tb, tb_path))
		{
			syslog(Logger::ERROR, "get_table_path error");
			return;
		}
		syslog(Logger::INFO, "tb_path: %s", tb_path.c_str());

		std::string http_url;
		item = cJSON_GetObjectItem(root, "url");
		if(item == NULL)
		{
			syslog(Logger::ERROR, "get url error");
			return;
		}
		http_url = item->valuestring;
		syslog(Logger::INFO, "http_url: %s", http_url.c_str());

		int pos = 0;
		if(Http_client::get_instance()->Http_client_get_file(http_url.c_str(), tb_path.c_str(), &pos)!=0)
		{
			int retry = 6;
			while(retry>0 && 
				Http_client::get_instance()->Http_client_get_file_range(http_url.c_str(), tb_path.c_str(), &pos, pos, 0)!=0)
				retry--;
		}
	}
	else if(file_type == FILE_BINLOG)
	{
	
	}
}

void Job::job_mysql_cmd(cJSON *root)
{
	cJSON *item;
	item = cJSON_GetObjectItem(root, "cmd");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get mysql cmd error");
		return;
	}

	std::string cmd = "cd " + mysql_install_path + ";" + item->valuestring;
	syslog(Logger::INFO, "start mysql cmd : %s",cmd.c_str());
	system(cmd.c_str());
}

void Job::job_pgsql_cmd(cJSON *root)
{
	cJSON *item;
	item = cJSON_GetObjectItem(root, "cmd");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get pgsql cmd error");
		return;
	}

	std::string cmd = "cd " + pgsql_install_path + ";" + item->valuestring;
	syslog(Logger::INFO, "start pgsql cmd : %s",cmd.c_str());
	system(cmd.c_str());
}

void Job::job_cluster_cmd(cJSON *root)
{
	cJSON *item;
	item = cJSON_GetObjectItem(root, "cmd");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get cluster cmd error");
		return;
	}

	std::string cmd = "cd " + cluster_install_path + ";" + item->valuestring;
	syslog(Logger::INFO, "start cluster cmd : %s",cmd.c_str());
	system(cmd.c_str());
}

void Job::add_job(std::string &str)
{
	pthread_mutex_lock(&thread_mtx);
	que_job.push(str);
	pthread_cond_signal(&thread_cond);
	pthread_mutex_unlock(&thread_mtx);
}

void Job::job_handle(std::string &job)
{
	syslog(Logger::INFO, "job_handle job=%s",job.c_str());

	cJSON *root;
	cJSON *item;
	Job_type job_type;

	root = cJSON_Parse(job.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "cJSON_Parse error");	
		return;
	}

	item = cJSON_GetObjectItem(root, "job_type");
	if(item == NULL || !get_job_type(item->valuestring, job_type))
	{
		syslog(Logger::ERROR, "get_job_type error");
		cJSON_Delete(root);
		return;
	}

	if(job_type == JOB_DELETE)
	{
		job_delete(root);
	}
	else if(job_type == JOB_SEND)
	{
		job_send(root);
	}
	else if(job_type == JOB_RECV)
	{
		job_recv(root);
	}
	else if(job_type == JOB_UPDATE_NODE)
	{
		Node_info::get_instance()->get_local_node(root);
	}
	else if(job_type == JOB_MYSQL_CMD)
	{
		job_mysql_cmd(root);
	}
	else if(job_type == JOB_PGSQL_CMD)
	{
		job_pgsql_cmd(root);
	}
	else if(job_type == JOB_CLUSTER_CMD)
	{
		job_cluster_cmd(root);
	}

	cJSON_Delete(root);
}

void Job::job_work()
{
	while (!Job::do_exit)  
    {  
		pthread_mutex_lock(&thread_mtx);

        while (que_job.size() == 0 && !Job::do_exit)
            pthread_cond_wait(&thread_cond, &thread_mtx);

		if(Job::do_exit)
		{
			pthread_mutex_unlock(&thread_mtx); 
			break;
		}

		auto job = que_job.front();
		que_job.pop();

		pthread_mutex_unlock(&thread_mtx);

		job_handle(job);
	}
}

extern "C" void *thread_func_job_work(void*thrdarg)
{
	Job* job = (Job*)thrdarg;
	Assert(job);

	signal(SIGPIPE, SIG_IGN);
	job->job_work();
	
	return NULL;
}

