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
#include "hdfs_client.h"
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

Job::Job()
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
	else if(strcmp(str, "get_status")==0)
		job_type = JOB_GET_STATUS;
	else if(strcmp(str, "coldbackup")==0)
		job_type = JOB_COLD_BACKUP;
	else if(strcmp(str, "coldrestore")==0)
		job_type = JOB_COLD_RESTORE;
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

bool Job::get_cnf_path(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &cnf_path)
{
	syslog(Logger::INFO, "get_cnf_path ip=%s,port=%d,user=%s,psw=%s", 
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
	
		if(mysql_conn.send_stmt(SQLCOM_SELECT, "select @@datadir"))
			continue;
	
		MYSQL_ROW row;
		if ((row = mysql_fetch_row(mysql_conn.result)))
		{
			//syslog(Logger::INFO, "row[]=%s",row[0]);
			cnf_path = row[0];
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
	
	cnf_path = cnf_path.substr(0, cnf_path.rfind("/"));
	cnf_path = cnf_path.substr(0, cnf_path.rfind("/"));
	cnf_path = cnf_path.substr(0, cnf_path.rfind("/"));

	cnf_path = cnf_path + "/my_" + std::to_string(port) + ".cnf";

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

bool Job::add_file_path(std::string &jobid, std::string &path)
{
	std::unique_lock<std::mutex> lock(mutex_path_);

	if(list_jobid_path.size()>=kMaxPath)
		list_jobid_path.pop_back();

	bool is_exist = false;
	for (auto it = list_jobid_path.begin(); it != list_jobid_path.end(); ++it)
	{
		if(it->first == jobid)
		{
			it->second = path;
			is_exist = true;
			break;
		}
	}

	if(!is_exist)
		list_jobid_path.push_front(std::make_pair(jobid, path));

	return true;
}

bool Job::get_file_path(std::string &jobid, std::string &path)
{
	std::unique_lock<std::mutex> lock(mutex_path_);

	bool ret = false;
	for (auto it = list_jobid_path.begin(); it != list_jobid_path.end(); ++it)
	{
		if(it->first == jobid)
		{
			path = it->second;
			ret = true;
			break;
		}
	}

	return ret;
}

bool Job::update_jobid_status(std::string &jobid, std::string &status)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);

	if(list_jobid_status.size()>=kMaxStatus)
		list_jobid_status.pop_back();

	bool is_exist = false;
	for (auto it = list_jobid_status.begin(); it != list_jobid_status.end(); ++it)
	{
		if(it->first == jobid)
		{
			it->second = status;
			is_exist = true;
			break;
		}
	}

	if(!is_exist)
		list_jobid_status.push_front(std::make_pair(jobid, status));

	return true;
}

bool Job::get_jobid_status(std::string &jobid, std::string &status)
{
	std::unique_lock<std::mutex> lock(mutex_stauts_);
	
	bool ret = false;
	for (auto it = list_jobid_status.begin(); it != list_jobid_status.end(); ++it)
	{
		if(it->first == jobid)
		{
			status = it->second;
			ret = true;
			break;
		}
	}

	return ret;
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
		add_file_path(job_id, tb_path);

		std::string tb_name = tb + ".ibd";
		std::string post_url = "http://" + remote_ip + ":" + std::to_string(http_server_port);
		syslog(Logger::INFO, "post_url %s", post_url.c_str());

		cJSON_DeleteItemFromObject(root, "job_type");
		cJSON_AddStringToObject(root, "job_type", "recv");

		std::string url = "http://" + ip + ":" + std::to_string(http_server_port) + "/" + job_id;
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
	//system(cmd.c_str());
	
	FILE* pfd;
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "job_mysql_cmd error %s" ,cmd.c_str());
		return;
	}
	pclose(pfd);
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
	//system(cmd.c_str());

	FILE* pfd;
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "job_pgsql_cmd error %s" ,cmd.c_str());
		return;
	}
	pclose(pfd);
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
	//system(cmd.c_str());

	FILE* pfd;
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "job_cluster_cmd error %s" ,cmd.c_str());
		return;
	}
	pclose(pfd);
}

void Job::job_cold_backup(cJSON *root)
{
	std::string job_id;
	std::string job_status;

	std::string ip;
	int port;
	std::string user;
	std::string pwd;
	std::string cnf_path;

	std::string date_time;
	std::string local_path;
	std::string hdfs_path;
	std::string record_path;
	bool is_node_find = false;

	std::string cmd;
	FILE* pfd;
	char buf[256];
	char *line, *p;
	
	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_status = "coldbackup start";
	update_jobid_status(job_id, job_status);

	//////////////////////////////////////////////////////////////////////
	//get mysql cnf path
	item = cJSON_GetObjectItem(root, "s_ip");
	if(item == NULL)
	{
		job_status = "get s_ip error";
		goto end;
	}
	ip = item->valuestring;

	item = cJSON_GetObjectItem(root, "s_port");
	if(item == NULL)
	{
		job_status = "get s_port error";
		goto end;
	}
	port = item->valueint;

	if(!Node_info::get_instance()->check_local_ip(ip))
	{
		job_status = ip + " is no local_ip";
		goto end;
	}
	
	item = cJSON_GetObjectItem(root, "s_user");
	if(item == NULL)
	{
		job_status = "get s_user error";
		goto end;
	}
	user = item->valuestring;

	item = cJSON_GetObjectItem(root, "s_pwd");
	if(item == NULL)
	{
		job_status = "get s_pwd error";
		goto end;
	}
	pwd = item->valuestring;

	if(!get_cnf_path(ip, port, user, pwd, cnf_path))
	{
		job_status = "get_cnf_path error";
		goto end;
	}

	job_status = "coldbackup working";
	update_jobid_status(job_id, job_status);

	//////////////////////////////////////////////////////////////////////
	//start backup
	cmd = "backup -clustername=cname -etcfile=" + cnf_path + " -storagetype=hdfs";
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "popen error " + cmd;
		goto end;
	}

	line = fgets(buf, 256, pfd);
	pclose(pfd);

	if(strstr(line, "coldback.tgz") == NULL)
	{
		job_status = "popen return error " + std::string(line);
		goto end;
	}

	p = strchr(line, '\n');
	if(p!= NULL)
		*p = '\0';

	job_status = "coldbackup copying";
	update_jobid_status(job_id, job_status);
	
	//////////////////////////////////////////////////////////////////////
	//set backup file name in hdfs
	System::get_instance()->get_date_time(date_time);
	local_path = std::string(line);
	
	for(auto &node: Node_info::get_instance()->vec_meta_node)
	{
		if(node->port == port)
		{
			hdfs_path = "/coldbackup/meta/coldback_" + date_time + ".tgz";
			record_path = "/coldbackup/meta.txt";
			is_node_find = true;
			break;
		}
	}

	if(!is_node_find)
	{
		for(auto &node: Node_info::get_instance()->vec_storage_node)
		{
			if(node->port == port)
			{
				hdfs_path = "/coldbackup/" + node->cluster + "/" + node->shard + 
							"/coldback_" + date_time + ".tgz";
				record_path = "/coldbackup/storage.txt";
				is_node_find = true;
				break;
			}
		}
	}

	if(!is_node_find)
	{
		job_status = "job_coldbackup get node info fail!";
		goto end;
	}

	//////////////////////////////////////////////////////////////////////
	// push file to hdfs and save to record file
	if(!Hdfs_client::get_instance()->hdfs_push_file(local_path, hdfs_path))
	{
		job_status = "job_cold_backup push to hdfs fail!";
		goto end;
	}

	hdfs_path += "\n";
	if(!Hdfs_client::get_instance()->hdfs_record_file(record_path, hdfs_path))
	{
		job_status = "job_cold_backup save to record fail!";
		goto end;
	}

	job_status = "coldbackup succeed";
	update_jobid_status(job_id, job_status);

	syslog(Logger::INFO, "job_cold_backup succeed!");
	return;

end:
	update_jobid_status(job_id, job_status);
	syslog(Logger::ERROR, "%s", job_status.c_str());
}

void Job::job_cold_restore(cJSON *root)
{
	std::string job_id;
	std::string job_status;

	std::string ip;
	int port;
	std::string user;
	std::string pwd;
	std::string cnf_path;

	std::string date_time;
	std::string local_path;
	std::string hdfs_path;
	std::string record_path;
	bool is_node_find = false;

	std::string cmd;
	FILE* pfd;
	char buf[256];
	char *line, *p;
	
	cJSON *item;
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_status = "coldrestore start";
	update_jobid_status(job_id, job_status);

	//////////////////////////////////////////////////////////////////////
	//get mysql cnf path
	item = cJSON_GetObjectItem(root, "s_ip");
	if(item == NULL)
	{
		job_status = "get s_ip error";
		goto end;
	}
	ip = item->valuestring;

	item = cJSON_GetObjectItem(root, "s_port");
	if(item == NULL)
	{
		job_status = "get s_port error";
		goto end;
	}
	port = item->valueint;

	if(!Node_info::get_instance()->check_local_ip(ip))
	{
		job_status = ip + " is no local_ip";
		goto end;
	}
	
	item = cJSON_GetObjectItem(root, "s_user");
	if(item == NULL)
	{
		job_status = "get s_user error";
		goto end;
	}
	user = item->valuestring;

	item = cJSON_GetObjectItem(root, "s_pwd");
	if(item == NULL)
	{
		job_status = "get s_pwd error";
		goto end;
	}
	pwd = item->valuestring;

	if(!get_cnf_path(ip, port, user, pwd, cnf_path))
	{
		job_status = "get_cnf_path error";
		goto end;
	}

	job_status = "coldrestore copying";
	update_jobid_status(job_id, job_status);

	//////////////////////////////////////////////////////////////////////
	// pull file from hdfs
	item = cJSON_GetObjectItem(root, "file");
	if(item == NULL)
	{
		job_status = "get file error";
		goto end;
	}
	hdfs_path = item->valuestring;
	
	local_path = hdfs_path.substr(hdfs_path.rfind("/"), hdfs_path.length());
	local_path = "." + local_path;
	syslog(Logger::ERROR, "%s", local_path.c_str());
	syslog(Logger::ERROR, "%s", hdfs_path.c_str());
	
	if(!Hdfs_client::get_instance()->hdfs_pull_file(local_path, hdfs_path))
	{
		job_status = "job_cold_restore poll from hdfs fail!";
		goto end;
	}

	job_status = "coldrestore working";
	update_jobid_status(job_id, job_status);
	
	//////////////////////////////////////////////////////////////////////
	//start restore
	cmd = "restore -backupfile-xtrabackup=" + local_path + " -etcfile-new-mysql=" + cnf_path;
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "popen error " + cmd;
		goto end;
	}

	line = fgets(buf, 256, pfd);
	pclose(pfd);

	if(strstr(line, "coldback.tgz") == NULL)
	{
		job_status = "popen return error " + std::string(line);
		goto end;
	}

	p = strchr(line, '\n');
	if(p!= NULL)
		*p = '\0';
	
	job_status = "coldrestore succeed";
	update_jobid_status(job_id, job_status);

	syslog(Logger::INFO, "job_cold_restore succeed!");
	return;

end:
	update_jobid_status(job_id, job_status);
	syslog(Logger::ERROR, "%s", job_status.c_str());
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
	//syslog(Logger::INFO, "job_handle job=%s",job.c_str());

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
		syslog(Logger::ERROR, "job_handle get_job_type error");
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
	else if(job_type == JOB_COLD_BACKUP)
	{
		job_cold_backup(root);
	}
	else if(job_type == JOB_COLD_RESTORE)
	{
		job_cold_restore(root);
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

