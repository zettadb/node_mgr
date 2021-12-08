/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "global.h"
#include "sys.h"
#include "log.h"
#include "cjson.h"
#include "job.h"
#include "instance_info.h"
#include "mysql_conn.h"
#include "pgsql_conn.h"
#include "http_client.h"
#include <sys/types.h>
#include <sys/stat.h>
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
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <iostream>

Instance_info* Instance_info::m_inst = NULL;

int64_t pullup_wait_const = 9;

int64_t stmt_retries = 3;
int64_t stmt_retry_interval_ms = 500;

std::string cluster_mgr_http_ip;
int64_t cluster_mgr_http_port = 0;

extern int64_t thread_work_interval;

Instance::Instance(Instance_type type_, std::string &ip_, int port_, std::string &user_, std::string &pwd_):
	type(type_),ip(ip_),port(port_),user(user_),pwd(pwd_),
	mysql_conn(NULL),pgsql_conn(NULL),pullup_wait(0)
{

}
		
Instance::~Instance()
{
	if(mysql_conn != NULL)
	{
		delete mysql_conn;
	}
	if(pgsql_conn != NULL)
	{
		delete pgsql_conn;
	}
}

Instance_info::Instance_info()
{

}

Instance_info::~Instance_info()
{
	for (auto &instance:vec_meta_instance)
		delete instance;
	vec_meta_instance.clear();
	for (auto &instance:vec_storage_instance)
		delete instance;
	vec_storage_instance.clear();
	for (auto &instance:vec_computer_instance)
		delete instance;
	vec_computer_instance.clear();
}

void Instance_info::get_local_instance()
{
	get_meta_instance();
	get_storage_instance();
	get_computer_instance();
}

void Instance_info::get_local_instance(cJSON *root)
{
	cJSON *item;
	
	item = cJSON_GetObjectItem(root, "node_type");
	if(item == NULL)
	{
		get_meta_instance();
		get_storage_instance();
		get_computer_instance();
		return;
	}

	if(strcmp(item->valuestring, "meta_instance"))
		get_meta_instance();
	else if(strcmp(item->valuestring, "storage_instance"))
		get_storage_instance();
	else if(strcmp(item->valuestring, "computer_instance"))
		get_computer_instance();
}

int Instance_info::get_meta_instance()
{
	std::unique_lock<std::mutex> lock(mutex_instance_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_instance");
	cJSON_AddStringToObject(root, "node_type", "meta_instance");
	int ip_count = 0;
	for(auto &local_ip: Job::get_instance()->vec_local_ip)
	{
		std::string node_ip = "node_ip" + std::to_string(ip_count++);
		cJSON_AddStringToObject(root, node_ip.c_str(), local_ip.c_str());
	}
	
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + cluster_mgr_http_ip + ":" + std::to_string(cluster_mgr_http_port);
	
	std::string result_str;
	int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
	free(cjson);
	
	if(ret == 0)
	{
		//syslog(Logger::INFO, "result_str=%s", result_str.c_str());
		cJSON *ret_root;
		cJSON *ret_item;

		ret_root = cJSON_Parse(result_str.c_str());
		if(ret_root == NULL)
			return -5;

		for (auto &instance:vec_meta_instance)
			delete instance;
		vec_meta_instance.clear();

		int node_count = 0;
		while(true)
		{
			std::string node_str = "meta_instance" + std::to_string(node_count++);
			item = cJSON_GetObjectItem(ret_root, node_str.c_str());
			if(item == NULL)
				break;
			
			std::string ip;
			int port;
			std::string user;
			std::string pwd;

			sub_item = cJSON_GetObjectItem(item, "ip");
			if(sub_item == NULL)
				break;
			ip = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "port");
			if(sub_item == NULL)
				break;
			port = sub_item->valueint;

			sub_item = cJSON_GetObjectItem(item, "user");
			if(sub_item == NULL)
				break;
			user = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "pwd");
			if(sub_item == NULL)
				break;
			pwd = sub_item->valuestring;

			Instance *instance = new Instance(Instance::META, ip, port, user, pwd);
			vec_meta_instance.push_back(instance);

			MYSQL_CONN *conn = new MYSQL_CONN();
			instance->mysql_conn = conn;
		}

	}

	//get the path of meta instance
	for (auto &instance:vec_meta_instance)
	{
		int retry = stmt_retries;
		auto conn = instance->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select @@datadir"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				instance->path = row[0];
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();
	}

	return ret;
}

int Instance_info::get_storage_instance()
{
	std::unique_lock<std::mutex> lock(mutex_instance_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_instance");
	cJSON_AddStringToObject(root, "node_type", "storage_instance");
	int ip_count = 0;
	for(auto &local_ip: Job::get_instance()->vec_local_ip)
	{
		std::string node_ip = "node_ip" + std::to_string(ip_count++);
		cJSON_AddStringToObject(root, node_ip.c_str(), local_ip.c_str());
	}
	
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + cluster_mgr_http_ip + ":" + std::to_string(cluster_mgr_http_port);
	
	std::string result_str;
	int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
	free(cjson);
	
	if(ret == 0)
	{
		//syslog(Logger::INFO, "result_str=%s", result_str.c_str());
		cJSON *ret_root;
		cJSON *ret_item;

		ret_root = cJSON_Parse(result_str.c_str());
		if(ret_root == NULL)
			return -5;

		for (auto &instance:vec_storage_instance)
			delete instance;
		vec_storage_instance.clear();

		int node_count = 0;
		while(true)
		{
			std::string node_str = "storage_instance" + std::to_string(node_count++);
			item = cJSON_GetObjectItem(ret_root, node_str.c_str());
			if(item == NULL)
				break;
		
			std::string ip;
			int port;
			std::string user;
			std::string pwd;

			sub_item = cJSON_GetObjectItem(item, "ip");
			if(sub_item == NULL)
				break;
			ip = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "port");
			if(sub_item == NULL)
				break;
			port = sub_item->valueint;

			sub_item = cJSON_GetObjectItem(item, "user");
			if(sub_item == NULL)
				break;
			user = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "pwd");
			if(sub_item == NULL)
				break;
			pwd = sub_item->valuestring;

			Instance *instance = new Instance(Instance::STORAGE, ip, port, user, pwd);
			vec_storage_instance.push_back(instance);

			sub_item = cJSON_GetObjectItem(item, "cluster");
			if(sub_item == NULL)
				break;
			instance->cluster = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "shard");
			if(sub_item == NULL)
				break;
			instance->shard = sub_item->valuestring;

			MYSQL_CONN *conn = new MYSQL_CONN();
			instance->mysql_conn = conn;
		}
	}

	//get the path of storage instance
	for (auto &instance:vec_storage_instance)
	{
		int retry = stmt_retries;
		auto conn = instance->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select @@datadir"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				instance->path = row[0];
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();
	}

	return ret;
}

int Instance_info::get_computer_instance()
{
	std::unique_lock<std::mutex> lock(mutex_instance_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_instance");
	cJSON_AddStringToObject(root, "node_type", "computer_instance");
	int ip_count = 0;
	for(auto &local_ip: Job::get_instance()->vec_local_ip)
	{
		std::string node_ip = "node_ip" + std::to_string(ip_count++);
		cJSON_AddStringToObject(root, node_ip.c_str(), local_ip.c_str());
	}
	
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + cluster_mgr_http_ip + ":" + std::to_string(cluster_mgr_http_port);
	
	std::string result_str;
	int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
	free(cjson);
	
	if(ret == 0)
	{
		//syslog(Logger::INFO, "result_str=%s", result_str.c_str());
		cJSON *ret_root;
		cJSON *ret_item;

		ret_root = cJSON_Parse(result_str.c_str());
		if(ret_root == NULL)
			return -5;

		for (auto &instance:vec_computer_instance)
			delete instance;
		vec_computer_instance.clear();

		int node_count = 0;
		while(true)
		{
			std::string node_str = "computer_instance" + std::to_string(node_count++);
			item = cJSON_GetObjectItem(ret_root, node_str.c_str());
			if(item == NULL)
				break;
		
			std::string ip;
			int port;
			std::string user;
			std::string pwd;

			sub_item = cJSON_GetObjectItem(item, "ip");
			if(sub_item == NULL)
				break;
			ip = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "port");
			if(sub_item == NULL)
				break;
			port = sub_item->valueint;

			sub_item = cJSON_GetObjectItem(item, "user");
			if(sub_item == NULL)
				break;
			user = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "pwd");
			if(sub_item == NULL)
				break;
			pwd = sub_item->valuestring;

			Instance *instance = new Instance(Instance::COMPUTER, ip, port, user, pwd);
			vec_computer_instance.push_back(instance);

			sub_item = cJSON_GetObjectItem(item, "cluster");
			if(sub_item == NULL)
				break;
			instance->cluster = sub_item->valuestring;

			PGSQL_CONN *conn = new PGSQL_CONN();
			instance->pgsql_conn = conn;
		}
	}

	//get the path of computer instance
	for (auto &instance:vec_computer_instance)
	{
		int retry = stmt_retries;
		auto conn = instance->pgsql_conn;
		
		while(retry--)
		{
			if(conn->connect("postgres", instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to pgsql error ip=%s,port=%d,user=%s,psw=%s", 
								instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str());
				continue;
			}
		
			if(conn->send_stmt(PG_COPYRES_TUPLES, "SELECT setting FROM pg_settings WHERE name='data_directory'"))
				continue;

			if(PQntuples(conn->result) == 1)
			{
				//syslog(Logger::INFO, "presult = %s", PQgetvalue(conn->result,0,0));
				instance->path = PQgetvalue(conn->result,0,0);
			}
			
			break;
		}
		conn->free_pgsql_result();
	}

	return ret;
}

void Instance_info::set_auto_pullup(int seconds, int port)
{
	std::unique_lock<std::mutex> lock(mutex_instance_);

	if(port<=0)		//done on all of the port
	{
		for (auto &instance:vec_meta_instance)
			instance->pullup_wait = seconds;

		for (auto &instance:vec_storage_instance)
			instance->pullup_wait = seconds;

		for (auto &instance:vec_computer_instance)
			instance->pullup_wait = seconds;
	}
	else	//find the instance compare by port
	{
		for (auto &instance:vec_meta_instance)
		{
			if(instance->port == port)
			{
				instance->pullup_wait = seconds;
				return;
			}
		}

		for (auto &instance:vec_storage_instance)
		{
			if(instance->port == port)
			{
				instance->pullup_wait = seconds;
				return;
			}
		}

		for (auto &instance:vec_computer_instance)
		{
			if(instance->port == port)
			{
				instance->pullup_wait = seconds;
				return;
			}
		}
	}
	
}

void Instance_info::keepalive_instance()
{
	std::unique_lock<std::mutex> lock(mutex_instance_);
#if 0
	/////////////////////////////////////////////////////////////
	// keep alive of meta
	for (auto &instance:vec_meta_instance)
	{
		if(instance->pullup_wait<0)
			continue;
		else if(instance->pullup_wait>0)
		{
			instance->pullup_wait -= thread_work_interval;
			if(instance->pullup_wait<0)
				instance->pullup_wait = 0;
			continue;
		}
	
		int retry = stmt_retries;
		auto conn = instance->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select version()"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				if (strcasestr(row[0], "kunlun-storage"))
					break;
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();

		if(retry<0)
		{
			syslog(Logger::ERROR, "meta_instance ip=%s, port=%d, retry=%d",instance->ip.c_str(),instance->port,retry);
			instance->pullup_wait = pullup_wait_const;

			std::string cmd = "cd " + mysql_install_path + ";./startmysql.sh " + std::to_string(instance->port);
			syslog(Logger::INFO, "start mysql cmd : %s",cmd.c_str());
			//system(cmd.c_str());
			
			FILE* pfd;
			pfd = popen(cmd.c_str(), "r");
			if(!pfd)
			{
				syslog(Logger::ERROR, "pullup instance error %s" ,cmd.c_str());
				return;
			}
			pclose(pfd);
		}
	}

	/////////////////////////////////////////////////////////////
	// keep alive of storage
	for (auto &instance:vec_storage_instance)
	{
		if(instance->pullup_wait<0)
			continue;
		else if(instance->pullup_wait>0)
		{
			instance->pullup_wait -= thread_work_interval;
			if(instance->pullup_wait<0)
				instance->pullup_wait = 0;
			continue;
		}
	
		int retry = stmt_retries;
		auto conn = instance->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select version()"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				if (strcasestr(row[0], "kunlun-storage"))
					break;
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();

		if(retry<0)
		{
			syslog(Logger::ERROR, "storage_instance ip=%s, port=%d, retry=%d",instance->ip.c_str(),instance->port,retry);
			instance->pullup_wait = pullup_wait_const;

			std::string cmd = "cd " + mysql_install_path + ";./startmysql.sh " + std::to_string(instance->port);
			syslog(Logger::INFO, "start mysql cmd : %s",cmd.c_str());
			//system(cmd.c_str());
			
			FILE* pfd;
			pfd = popen(cmd.c_str(), "r");
			if(!pfd)
			{
				syslog(Logger::ERROR, "pullup instance error %s" ,cmd.c_str());
				return;
			}
			pclose(pfd);
		}
	}

	/////////////////////////////////////////////////////////////
	// keep alive of computer
	for (auto &instance:vec_computer_instance)
	{
		if(instance->pullup_wait<0)
			continue;
		else if(instance->pullup_wait>0)
		{
			instance->pullup_wait -= thread_work_interval;
			if(instance->pullup_wait<0)
				instance->pullup_wait = 0;
			continue;
		}

		int retry = stmt_retries;
		auto conn = instance->pgsql_conn;
		
		while(retry--)
		{
			if(conn->connect("postgres", instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to pgsql error ip=%s,port=%d,user=%s,psw=%s", 
								instance->ip.c_str(), instance->port, instance->user.c_str(), instance->pwd.c_str());
				continue;
			}
		
			if(conn->send_stmt(PG_COPYRES_TUPLES, "select version()"))
				continue;

			if(PQntuples(conn->result) == 1)
			{
				//syslog(Logger::INFO, "presult = %s", PQgetvalue(conn->result,0,0));
				if (strcasestr(PQgetvalue(conn->result,0,0), "PostgreSQL"))
					break;
			}
			
			break;
		}
		conn->free_pgsql_result();

		if(retry<0)
		{
			syslog(Logger::ERROR, "computer_instance ip=%s, port=%d, retry=%d",instance->ip.c_str(),instance->port,retry);
			instance->pullup_wait = pullup_wait_const;

			std::string cmd = "cd " + pgsql_install_path + ";python2 start_pg.py port=" + std::to_string(instance->port);
			syslog(Logger::INFO, "start pgsql cmd : %s",cmd.c_str());
			//system(cmd.c_str());
			
			FILE* pfd;
			pfd = popen(cmd.c_str(), "r");
			if(!pfd)
			{
				syslog(Logger::ERROR, "pullup instance error %s" ,cmd.c_str());
				return;
			}
			pclose(pfd);
		}
	}
#endif
}

void Instance_info::trimString(std::string &str)
{
    int s = str.find_first_not_of(" ");
    int e = str.find_last_not_of(" ");
	if(s>=e)
		str = "";
	else
    	str = str.substr(s,e-s+1);
}

void Instance_info::set_path_space(char* paths)
{
	char *cStart, *cEnd;
	std::string path;

	vec_path_space.clear();

	cStart = paths;
	while(*cStart!='\0')
	{
		cEnd = strchr(cStart, ';');
		if(cEnd == NULL)
		{
			path = std::string(cStart, strlen(cStart));
			trimString(path);
			if(path.length())
				vec_path_space.push_back(std::make_pair(path, 0));
			break;
		}
		else
		{
			path = std::string(cStart, cEnd-cStart);
			trimString(path);
			if(path.length())
				vec_path_space.push_back(std::make_pair(path, 0));
			cStart = cEnd+1;
		}
	}

	for(auto it=vec_path_space.begin(); it!=vec_path_space.end(); )
	{
		uint64_t used,free;
		if(Job::get_instance()->get_path_size((*it).first, used, free))
		{
			(*it).second = free;
			it++;
		}
		else
		{
			it = vec_path_space.erase(it);
		}
	}
}

