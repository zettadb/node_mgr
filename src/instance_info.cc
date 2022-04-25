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

extern std::string cluster_mgr_http_ip;
extern int64_t cluster_mgr_http_port;
extern int64_t thread_work_interval;

extern std::string program_binaries_path;
extern std::string instance_binaries_path;
extern std::string storage_prog_package_name;
extern std::string computer_prog_package_name;

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
	
	item = cJSON_GetObjectItem(root, "instance_type");
	if(item == NULL)
	{
		get_meta_instance();
		get_storage_instance();
		get_computer_instance();
		return;
	}

	if(strcmp(item->valuestring, "meta_instance") == 0)
		get_meta_instance();
	else if(strcmp(item->valuestring, "storage_instance") == 0)
		get_storage_instance();
	else if(strcmp(item->valuestring, "computer_instance") == 0)
		get_computer_instance();
	else
		syslog(Logger::ERROR, "instance_type error %s", item->valuestring);
}

int Instance_info::get_meta_instance()
{
	std::lock_guard<std::mutex> lock(mutex_instance_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	int retry = 3;
	cJSON *ret_root;

	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_instance");
	cJSON_AddStringToObject(root, "instance_type", "meta_instance");
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
	while(retry-->0 && !Job::do_exit)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
			break;
	}
	free(cjson);

	if(retry<0)
	{
		syslog(Logger::ERROR, "get_meta_instance fail because http post");
		return 0;
	}
	
	//syslog(Logger::INFO, "result_str meta=%s", result_str.c_str());
	ret_root = cJSON_Parse(result_str.c_str());
	if(ret_root == NULL)
		return 0;

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
		vec_meta_instance.emplace_back(instance);

		MYSQL_CONN *conn = new MYSQL_CONN();
		instance->mysql_conn = conn;
	}

	syslog(Logger::INFO, "meta instance %d update", vec_meta_instance.size());

	return 1;
}

int Instance_info::get_storage_instance()
{
	std::lock_guard<std::mutex> lock(mutex_instance_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	int retry = 3;
	cJSON *ret_root;

	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_instance");
	cJSON_AddStringToObject(root, "instance_type", "storage_instance");
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
	while(retry-->0 && !Job::do_exit)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
			break;
	}
	free(cjson);
	
	if(retry<0)
	{
		syslog(Logger::ERROR, "get_storage_instance fail because http post");
		return 0;
	}

	//syslog(Logger::INFO, "result_str storage=%s", result_str.c_str());
	ret_root = cJSON_Parse(result_str.c_str());
	if(ret_root == NULL)
		return 0;

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
		vec_storage_instance.emplace_back(instance);

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

	syslog(Logger::INFO, "storage instance %d update", vec_storage_instance.size());

	return 1;
}

int Instance_info::get_computer_instance()
{
	std::lock_guard<std::mutex> lock(mutex_instance_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	int retry = 3;
	cJSON *ret_root;

	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_instance");
	cJSON_AddStringToObject(root, "instance_type", "computer_instance");
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
	while(retry-->0 && !Job::do_exit)
	{
		if(Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str)==0)
			break;
	}
	free(cjson);
	
	if(retry<0)
	{
		syslog(Logger::ERROR, "get_computer_instance fail because http post");
		return 0;
	}

	//syslog(Logger::INFO, "result_str computer=%s", result_str.c_str());
	ret_root = cJSON_Parse(result_str.c_str());
	if(ret_root == NULL)
		return 0;

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
		vec_computer_instance.emplace_back(instance);

		sub_item = cJSON_GetObjectItem(item, "cluster");
		if(sub_item == NULL)
			break;
		instance->cluster = sub_item->valuestring;

		sub_item = cJSON_GetObjectItem(item, "comp");
		if(sub_item == NULL)
			break;
		instance->comp = sub_item->valuestring;

		PGSQL_CONN *conn = new PGSQL_CONN();
		instance->pgsql_conn = conn;
	}

	syslog(Logger::INFO, "computer instance %d update", vec_computer_instance.size());

	return 1;
}

void Instance_info::remove_storage_instance(std::string &ip, int port)
{
	std::lock_guard<std::mutex> lock(mutex_instance_);

	for(auto it = vec_storage_instance.begin(); it != vec_storage_instance.end(); it++)
	{
		if((*it)->ip == ip && (*it)->port == port)
		{
			delete *it;
			vec_storage_instance.erase(it);
			return;
		}
	}

	for(auto it = vec_meta_instance.begin(); it != vec_meta_instance.end(); it++)
	{
		if((*it)->ip == ip && (*it)->port == port)
		{
			delete *it;
			vec_meta_instance.erase(it);
			return;
		}
	}
}

void Instance_info::remove_computer_instance(std::string &ip, int port)
{
	std::lock_guard<std::mutex> lock(mutex_instance_);

	for(auto it = vec_computer_instance.begin(); it != vec_computer_instance.end(); it++)
	{
		if((*it)->ip == ip && (*it)->port == port)
		{
			delete *it;
			vec_computer_instance.erase(it);
			return;
		}
	}
}

void Instance_info::set_auto_pullup(int seconds, int port)
{
	std::lock_guard<std::mutex> lock(mutex_instance_);

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

bool Instance_info::get_mysql_alive(MYSQL_CONN *mysql_conn, std::string &ip, int port, std::string &user, std::string &psw)
{
	//syslog(Logger::INFO, "get_mysql_alive ip=%s,port=%d,user=%s,psw=%s", ip.c_str(), port, user.c_str(), psw.c_str());

	int retry = stmt_retries;

	while(retry--)
	{
		if(mysql_conn->connect(NULL, ip.c_str(), port, user.c_str(), psw.c_str()))
		{
			syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
							ip.c_str(), port, user.c_str(), psw.c_str());
			continue;
		}

		if(mysql_conn->send_stmt(SQLCOM_SELECT, "select version()"))
			continue;

		MYSQL_ROW row;
		if ((row = mysql_fetch_row(mysql_conn->result)))
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
	mysql_conn->free_mysql_result();

	if(retry<0)
		return false;

	return true;
}

bool Instance_info::get_pgsql_alive(PGSQL_CONN *pgsql_conn, std::string &ip, int port, std::string &user, std::string &psw)
{
	//syslog(Logger::INFO, "get_pgsql_alive ip=%s,port=%d,user=%s,psw=%s", ip.c_str(), port, user.c_str(), psw.c_str());

	int retry = stmt_retries;

	while(retry--)
	{
		if(pgsql_conn->connect("postgres", ip.c_str(), port, user.c_str(), psw.c_str()))
		{
			syslog(Logger::ERROR, "connect to pgsql error ip=%s,port=%d,user=%s,psw=%s", 
							ip.c_str(), port, user.c_str(), psw.c_str());
			continue;
		}
	
		if(pgsql_conn->send_stmt(PG_COPYRES_TUPLES, "select version()"))
			continue;

		if(PQntuples(pgsql_conn->result) == 1)
		{
			//syslog(Logger::INFO, "presult = %s", PQgetvalue(pgsql_conn.result,0,0));
			if (strcasestr(PQgetvalue(pgsql_conn->result,0,0), "PostgreSQL"))
				break;
		}
		
		break;
	}
	pgsql_conn->free_pgsql_result();

	if(retry<0)
		return false;

	return true;
}

void Instance_info::keepalive_instance()
{
	std::lock_guard<std::mutex> lock(mutex_instance_);

	std::string install_path;
	/////////////////////////////////////////////////////////////
	// keep alive of meta
	for (auto &instance:vec_meta_instance)
	{
		if(instance->pullup_wait>0)
		{
			instance->pullup_wait -= thread_work_interval;
			if(instance->pullup_wait<0)
				instance->pullup_wait = 0;
			continue;
		}
	
		if(!get_mysql_alive(instance->mysql_conn, instance->ip, instance->port,instance->user,instance->pwd))
		{
			syslog(Logger::ERROR, "meta_instance no alive, ip=%s, port=%d",instance->ip.c_str(),instance->port);
			instance->pullup_wait = pullup_wait_const;
			Job::get_instance()->job_control_storage(instance->port, 2);
		}
	}

	/////////////////////////////////////////////////////////////
	// keep alive of storage
	for (auto &instance:vec_storage_instance)
	{
		if(instance->pullup_wait>0)
		{
			instance->pullup_wait -= thread_work_interval;
			if(instance->pullup_wait<0)
				instance->pullup_wait = 0;
			continue;
		}
	
		if(!get_mysql_alive(instance->mysql_conn, instance->ip, instance->port,instance->user,instance->pwd))
		{
			syslog(Logger::ERROR, "storage_instance no alive, ip=%s, port=%d",instance->ip.c_str(),instance->port);
			instance->pullup_wait = pullup_wait_const;
			Job::get_instance()->job_control_storage(instance->port, 2);
		}
	}

	/////////////////////////////////////////////////////////////
	// keep alive of computer
	for (auto &instance:vec_computer_instance)
	{
		if(instance->pullup_wait>0)
		{
			instance->pullup_wait -= thread_work_interval;
			if(instance->pullup_wait<0)
				instance->pullup_wait = 0;
			continue;
		}

		if(!get_pgsql_alive(instance->pgsql_conn, instance->ip, instance->port,instance->user,instance->pwd))
		{
			syslog(Logger::ERROR, "computer_instance no alive, ip=%s, port=%d",instance->ip.c_str(),instance->port);
			instance->pullup_wait = pullup_wait_const;
			Job::get_instance()->job_control_computer(instance->ip, instance->port, 2);
		}
	}
}

bool Instance_info::get_path_used(std::string &path, uint64_t &used)
{
	bool ret = false;
	FILE* pfd = NULL;

	char *p;
	char buf[256];
	std::string str_cmd;

	str_cmd = "du --max-depth=0 " + path;
	syslog(Logger::INFO, "get_path_used str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
    if(!pfd)
        goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	p = strchr(buf, 0x09);	//asii ht
	if(p == NULL)
		goto end;

	*p = '\0';
	used = atol(buf)>>10;		//Mbyte
	ret = true;

end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
}

bool Instance_info::get_path_free(std::string &path, uint64_t &free)
{
	bool ret = false;
	FILE* pfd = NULL;

	char *p, *q;
	char buf[256];
	std::string str_cmd;

	str_cmd = "df " + path;
	syslog(Logger::INFO, "get_path_free str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
    if(!pfd)
        goto end;

	//first line
	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	//second line
	if(fgets(buf, 256, pfd) == NULL)
		goto end;
	
	//first space
	p = strchr(buf, 0x20);
	if(p == NULL)
		goto end;

	while(*p == 0x20)
		p++;
	
	//second space
	p = strchr(p, 0x20);
	if(p == NULL)
		goto end;

	while(*p == 0x20)
		p++;

	//third space
	p = strchr(p, 0x20);
	if(p == NULL)
		goto end;

	while(*p == 0x20)
		p++;

	//fourth space
	q = strchr(p, 0x20);
	if(p == NULL)
		goto end;

	*q = '\0';
	free = atol(p)>>10;		//Mbyte
	ret = true;

end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
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

bool Instance_info::get_vec_path(std::vector<std::string> &vec_path, std::string &paths)
{
	const char *cStart, *cEnd;
	std::string path;

	cStart = paths.c_str();
	while(*cStart!='\0')
	{
		cEnd = strchr(cStart, ';');
		if(cEnd == NULL)
		{
			path = std::string(cStart, strlen(cStart));
			trimString(path);
			if(path.length())
				vec_path.emplace_back(path);
			break;
		}
		else
		{
			path = std::string(cStart, cEnd-cStart);
			trimString(path);
			if(path.length())
				vec_path.emplace_back(path);
			cStart = cEnd+1;
		}
	}

	return (vec_path.size() > 0);
}

bool Instance_info::set_path_space(std::vector<std::string> &vec_paths, std::string &result)
{
	bool ret = true;
	std::string info,path_used;
	uint64_t u_used,u_free;

	std::vector<std::string> vec_sub_path;
	vec_sub_path.emplace_back("/instance_data/data_dir_path");
	vec_sub_path.emplace_back("/instance_data/log_dir_path");
	vec_sub_path.emplace_back("/instance_data/innodb_log_dir_path");
	vec_sub_path.emplace_back("/instance_data/comp_datadir");

	std::vector<std::string> vec_path_index;
	vec_path_index.emplace_back("paths0");
	vec_path_index.emplace_back("paths1");
	vec_path_index.emplace_back("paths2");
	vec_path_index.emplace_back("paths3");

	vec_vec_path_used_free.clear();

	for(int i=0; i<4; i++)
	{
		std::vector<std::string> vec_path;
		if(!get_vec_path(vec_path, vec_paths[i]))
		{
			if(info.length()>0)
				info += ";" + vec_paths[i];
			else
				info = vec_paths[i];

			ret = false;
			continue;
		}
		
		std::vector<Tpye_Path_Used_Free> vec_path_used_free;
		for(auto &path:vec_path)
		{
			if(!get_path_free(path,u_free))
			{
				if(info.length()>0)
					info += ";" + path;
				else
					info = path;

				ret = false;
				continue;
			}
			else
			{
				path_used = path + vec_sub_path[i];
				if(!get_path_used(path_used,u_used))
				{
					u_used = 0;
				}
			}

			vec_path_used_free.emplace_back(std::make_tuple(path, u_used, u_free));
		}
		vec_vec_path_used_free.emplace_back(vec_path_used_free);
	}

	//json for return
	cJSON *root = cJSON_CreateObject();
	cJSON *item;
	cJSON *item_sub;
	cJSON *item_paths;
	char *cjson = NULL;

	if(ret)
	{
		cJSON_AddStringToObject(root, "result", "succeed");
		cJSON_AddStringToObject(root, "info", "");

		for(int i=0; i<4; i++)
		{
			item_paths = cJSON_CreateArray();
			cJSON_AddItemToObject(root, vec_path_index[i].c_str(), item_paths);
			for(auto &path_used_free: vec_vec_path_used_free[i])
			{
				item_sub = cJSON_CreateObject();
				cJSON_AddItemToArray(item_paths, item_sub);
				cJSON_AddStringToObject(item_sub, "path", std::get<0>(path_used_free).c_str());
				cJSON_AddNumberToObject(item_sub, "used", std::get<1>(path_used_free));
				cJSON_AddNumberToObject(item_sub, "free", std::get<2>(path_used_free));
			}
		}
	}
	else
	{
		info += " is error";
		cJSON_AddStringToObject(root, "result", "error");
		cJSON_AddStringToObject(root, "info", info.c_str());
	}

	cjson = cJSON_Print(root);
	result = cjson;
	cJSON_Delete(root);
	free(cjson);

	return ret;
}

