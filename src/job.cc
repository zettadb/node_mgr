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
#include "instance_info.h"
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h> 
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>

Job* Job::m_inst = NULL;
int Job::do_exit = 0;

int64_t num_job_threads = 3;
std::string http_cmd_version;

extern int64_t cluster_mgr_http_port;
extern int64_t node_mgr_http_port;
extern std::string http_upload_path;
extern int64_t stmt_retries;
extern int64_t stmt_retry_interval_ms;

std::string program_binaries_path;
std::string instance_binaries_path;
std::string storage_prog_package_name;
std::string computer_prog_package_name;

extern "C" void *thread_func_job_work(void*thrdarg);

Job::Job()
{
	
}

Job::~Job()
{

}

int Job::start_job_thread()
{
	int error = 0;
	pthread_mutex_init(&thread_mtx, NULL);
	pthread_cond_init(&thread_cond, NULL);
	get_user_name();
	get_local_ip();
	
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
			return -1;
		}
		vec_pthread.emplace_back(hdl);
	}

	return 0;
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

bool Job::get_node_instance(cJSON *root, std::string &str_ret)
{
	bool ret = false;
	int node_count = 0;
	cJSON *item;
	std::string disk_used, disk_free;
	
	item = cJSON_GetObjectItem(root, "instance_type");
	if(item == NULL)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();
		
		//meta node
		node_count = 0;
		for(auto &node: Instance_info::get_instance()->vec_meta_instance)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "meta_instance" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_path_size(node->path, disk_used, disk_free))
			{
				cJSON_AddStringToObject(ret_item, "disk_used", disk_used.c_str());
				cJSON_AddStringToObject(ret_item, "disk_free", disk_free.c_str());
			}
			else
			{
				cJSON_AddStringToObject(ret_item, "disk_used", "error");
				cJSON_AddStringToObject(ret_item, "disk_free", "error");
			}
		}

		//storage node
		node_count = 0;
		for(auto &node: Instance_info::get_instance()->vec_storage_instance)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "storage_instance" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", node->cluster.c_str());
			cJSON_AddStringToObject(ret_item, "shard", node->shard.c_str());
			if(get_path_size(node->path, disk_used, disk_free))
			{
				cJSON_AddStringToObject(ret_item, "disk_used", disk_used.c_str());
				cJSON_AddStringToObject(ret_item, "disk_free", disk_free.c_str());
			}
			else
			{
				cJSON_AddStringToObject(ret_item, "disk_used", "error");
				cJSON_AddStringToObject(ret_item, "disk_free", "error");
			}
		}

		//computer node
		node_count = 0;
		for(auto &node: Instance_info::get_instance()->vec_computer_instance)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "computer_instance" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", node->cluster.c_str());
			if(get_path_size(node->path, disk_used, disk_free))
			{
				cJSON_AddStringToObject(ret_item, "disk_used", disk_used.c_str());
				cJSON_AddStringToObject(ret_item, "disk_free", disk_free.c_str());
			}
			else
			{
				cJSON_AddStringToObject(ret_item, "disk_used", "error");
				cJSON_AddStringToObject(ret_item, "disk_free", "error");
			}
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);
	
		return true;
	}

	if(strcmp(item->valuestring, "meta_instance") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();
		
		for(auto &node: Instance_info::get_instance()->vec_meta_instance)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "meta_instance" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_path_size(node->path, disk_used, disk_free))
			{
				cJSON_AddStringToObject(ret_item, "disk_used", disk_used.c_str());
				cJSON_AddStringToObject(ret_item, "disk_free", disk_free.c_str());
			}
			else
			{
				cJSON_AddStringToObject(ret_item, "disk_used", "error");
				cJSON_AddStringToObject(ret_item, "disk_free", "error");
			}
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);

		ret = true;
	}
	else if(strcmp(item->valuestring, "storage_instance") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();

		for(auto &node: Instance_info::get_instance()->vec_storage_instance)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "storage_instance" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", node->cluster.c_str());
			cJSON_AddStringToObject(ret_item, "shard", node->shard.c_str());
			if(get_path_size(node->path, disk_used, disk_free))
			{
				cJSON_AddStringToObject(ret_item, "disk_used", disk_used.c_str());
				cJSON_AddStringToObject(ret_item, "disk_free", disk_free.c_str());
			}
			else
			{
				cJSON_AddStringToObject(ret_item, "disk_used", "error");
				cJSON_AddStringToObject(ret_item, "disk_free", "error");
			}
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);

		ret = true;
	}
	else if(strcmp(item->valuestring, "computer_instance") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();

		for(auto &node: Instance_info::get_instance()->vec_computer_instance)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "computer_instance" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			cJSON_AddStringToObject(ret_item, "cluster", node->cluster.c_str());
			if(get_path_size(node->path, disk_used, disk_free))
			{
				cJSON_AddStringToObject(ret_item, "disk_used", disk_used.c_str());
				cJSON_AddStringToObject(ret_item, "disk_free", disk_free.c_str());
			}
			else
			{
				cJSON_AddStringToObject(ret_item, "disk_used", "error");
				cJSON_AddStringToObject(ret_item, "disk_free", "error");
			}
		}

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);

		ret = true;
	}
	
	return ret;
}

bool Job::get_node_info(cJSON *root, std::string &str_ret)
{
	cJSON *ret_root;
	cJSON *ret_item;
	char *ret_cjson;
	ret_root = cJSON_CreateObject();

	std::string str1,str2;
	if(get_cpu_used(str1))
	{
		cJSON_AddStringToObject(ret_root, "cpu_used", str1.c_str());
	}
	if(get_mem_used(str1,str2))
	{
		cJSON_AddStringToObject(ret_root, "mem_used", str1.c_str());
		cJSON_AddStringToObject(ret_root, "mem_free", str2.c_str());
	}
	std::string path;
	get_user_path(path);
	if(get_path_size(path, str1,str2))
	{
		cJSON_AddStringToObject(ret_root, "disk_used", str1.c_str());
		cJSON_AddStringToObject(ret_root, "disk_free", str2.c_str());
	}

	cJSON_AddNumberToObject(ret_root, "meta_node", Instance_info::get_instance()->vec_meta_instance.size());
	cJSON_AddNumberToObject(ret_root, "storage_node", Instance_info::get_instance()->vec_storage_instance.size());
	cJSON_AddNumberToObject(ret_root, "computer_node", Instance_info::get_instance()->vec_computer_instance.size());
	
	ret_cjson = cJSON_Print(ret_root);
	str_ret = ret_cjson;
	
	if(ret_root != NULL)
		cJSON_Delete(ret_root);
	if(ret_cjson != NULL)
		free(ret_cjson);

	return true;
}

bool Job::set_auto_pullup(cJSON *root, std::string &str_ret)
{
	cJSON *item;
	int minutes = 0;
	int port = 0;

	item = cJSON_GetObjectItem(root, "minutes");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_auto_pullup get minutes error");
		return false;
	}
	minutes = item->valueint;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item != NULL)
	{
		port = item->valueint;
	}

	Instance_info::get_instance()->set_auto_pullup(minutes*60, port);

	str_ret = "{\"result\":\"succeed\"}";
	return true;
}

bool Job::get_disk_size(cJSON *root, std::string &str_ret)
{
	cJSON *item;
	std::string disk_used, disk_free;

	std::string path;
	item = cJSON_GetObjectItem(root, "path");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get path error");
		return false;
	}
	path = item->valuestring;

	if(get_path_size(path, disk_used, disk_free))
	{
		cJSON *ret_root;
		char *ret_cjson;

		ret_root = cJSON_CreateObject();
		cJSON_AddStringToObject(ret_root, "disk_used", disk_used.c_str());
		cJSON_AddStringToObject(ret_root, "disk_free", disk_free.c_str());

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;

		if(ret_root != NULL)
			cJSON_Delete(ret_root);
		if(ret_cjson != NULL)
			free(ret_cjson);

		return true;
	}

	return false;
}

bool Job::get_path_space(cJSON *root, std::string &str_ret)
{
	cJSON *item;
	std::vector<std::string> vec_paths; 
	std::string info;

	////////////////////////////////////////////////////////////
	//get vec_paths from json
	item = cJSON_GetObjectItem(root, "paths0");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get paths0 error");
		return false;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "paths1");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get paths1 error");
		return false;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "paths2");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get paths2 error");
		return false;
	}
	vec_paths.emplace_back(item->valuestring);

	item = cJSON_GetObjectItem(root, "paths3");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get paths3 error");
		return false;
	}
	vec_paths.emplace_back(item->valuestring);

	////////////////////////////////////////////////////////////
	//set path and get space
	Instance_info::get_instance()->set_path_space(vec_paths, str_ret);

	return true;
}

bool Job::get_path_size(std::string &path, std::string &used, std::string &free)
{
	bool ret = false;
	FILE* pfd = NULL;

	char *p, *q;
	char buf[256];
	std::string str_cmd;

	str_cmd = "du -h --max-depth=0 " + path;
	syslog(Logger::INFO, "get_path_size str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
    if(!pfd)
        goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	p = strchr(buf, 0x09);	//asii ht
	if(p != NULL)
		*p = '\0';
	else
	{
		char* p = strchr(buf, 0x20);	//space
		if(p != NULL)
			*p = '\0';
		else
			goto end;
	}

	used = buf;
		
	pclose(pfd);
	pfd = NULL;

	str_cmd = "df -h " + path;
	syslog(Logger::INFO, "get_path_size str_cmd : %s",str_cmd.c_str());

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

	free = p;

	ret = true;

end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
}

bool Job::get_path_size(std::string &path, uint64_t &used, uint64_t &free)
{
	bool ret = false;
	FILE* pfd = NULL;

	char *p, *q;
	char buf[256];
	std::string str_cmd;

	str_cmd = "du --max-depth=0 " + path;
	syslog(Logger::INFO, "get_path_size str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
    if(!pfd)
        goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	p = strchr(buf, 0x09);	//asii ht
	if(p != NULL)
		*p = '\0';
	else
	{
		char* p = strchr(buf, 0x20);	//space
		if(p != NULL)
			*p = '\0';
		else
			goto end;
	}

	used = atol(buf)>>20;
		
	pclose(pfd);
	pfd = NULL;

	str_cmd = "df " + path;
	syslog(Logger::INFO, "get_path_size str_cmd : %s",str_cmd.c_str());

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

	free = atol(p)>>20;

	ret = true;

end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
}

bool Job::get_cpu_used(std::string &cpu_used)
{
	bool ret = false;
	FILE* pfd;

	char *p, *q;
	char buf[256];
	float fcpu_use;
	std::string str_cmd;

	str_cmd = "top -bn 1 -i -c | grep %Cpu";
	syslog(Logger::INFO, "get_cpu_used str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
	if(!pfd)
		goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	p = strstr(buf, "ni,");
	if(p == NULL)
		goto end;

	p = strchr(p, 0x20);
	if(p == NULL)
		goto end;

	while(*p == 0x20)
		p++;

	q = strchr(p, 0x20);
	if(q == NULL)
		goto end;

	cpu_used = std::string(p, q - p);

	fcpu_use = atof(cpu_used.c_str());
	fcpu_use = 100 - fcpu_use;
	if(fcpu_use - 5 > 0)	// for top cmd use 5%
		fcpu_use = fcpu_use - 5;

	cpu_used = std::to_string(fcpu_use);
	cpu_used = cpu_used.substr(0, cpu_used.find(".")+3)+"%";

	ret = true;
end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
}

bool Job::get_mem_used(std::string &used, std::string &free)
{
	bool ret = false;
	FILE* pfd;

	char *p, *q;
	char buf[256];
	int mem_use;
	int mem_free;
	std::string str_cmd;

	str_cmd = "free -m | grep Mem";
	syslog(Logger::INFO, "get_mem_used str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
	if(!pfd)
		goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	p = strstr(buf, "Mem:");
	if(p == NULL)
		goto end;

	p = strchr(p, 0x20);
	if(p == NULL)
		goto end;

	while(*p == 0x20)
		p++;

	q = strchr(p, 0x20);
	if(q == NULL)
		goto end;

	free = std::string(p, q - p);
	mem_free = atoi(free.c_str());

	p = q;
	while(*p == 0x20)
		p++;

	q = strchr(p, 0x20);
	if(q == NULL)
		goto end;

	used = std::string(p, q - p);
	mem_use = atoi(used.c_str());

	mem_free = mem_free - mem_use;

	used = std::to_string(mem_use)+"M";
	free = std::to_string(mem_free)+"M";

	ret = true;
end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
}

bool Job::get_user_path(std::string &path)
{
	bool ret = false;
	FILE* pfd;

	char *p;
	char buf[256];
	std::string str_cmd;

	str_cmd = "who am i";
	syslog(Logger::INFO, "get_user_path str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
	if(!pfd)
		goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;
	
	p = strchr(buf, 0x20);
	if(p == NULL)
		goto end;

	path = "/home/" + std::string(buf, p-buf);

	ret = true;
end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
}

bool Job::check_local_ip(std::string &ip)
{
	for(auto &local_ip: vec_local_ip)
		if(ip == local_ip)
			return true;
	
	return false;
}

void Job::get_local_ip()
{
	int fd, num;
	struct ifreq ifq[16];
	struct ifconf ifc;

	fd = socket(AF_INET, SOCK_DGRAM, 0);
	if(fd < 0)
	{
		syslog(Logger::ERROR, "socket failed");
		return ;
	}
	
	ifc.ifc_len = sizeof(ifq);
	ifc.ifc_buf = (caddr_t)ifq;
	if(ioctl(fd, SIOCGIFCONF, (char *)&ifc))
	{
		syslog(Logger::ERROR, "ioctl failed\n");
		close(fd);
		return ;
	}
	num = ifc.ifc_len / sizeof(struct ifreq);
	if(ioctl(fd, SIOCGIFADDR, (char *)&ifq[num-1]))
	{
		syslog(Logger::ERROR, "ioctl failed\n");
		close(fd);
		return ;
	}
	close(fd);

	for(int i=0; i<num; i++)
	{
		char *tmp_ip = inet_ntoa(((struct sockaddr_in*)(&ifq[i].ifr_addr))-> sin_addr);
		//syslog(Logger::INFO, "tmp_ip=%s", tmp_ip);
		//if(strcmp(tmp_ip, "127.0.0.1") != 0)
		{
			vec_local_ip.emplace_back(tmp_ip);
		}
	}

	//for(auto &ip: vec_local_ip)
	//	syslog(Logger::INFO, "vec_local_ip=%s", ip.c_str());
}

void Job::get_user_name()
{
	FILE* pfd;

	char *p;
	char buf[256];
	std::string str_cmd;

	str_cmd = "who am i";
	//syslog(Logger::INFO, "get_user_name str_cmd : %s",str_cmd.c_str());

	pfd = popen(str_cmd.c_str(), "r");
	if(!pfd)
		goto end;

	if(fgets(buf, 256, pfd) == NULL)
		goto end;
	
	p = strchr(buf, 0x20);
	if(p == NULL)
		goto end;

	user_name = std::string(buf, p-buf);
	syslog(Logger::INFO, "current user=%s", user_name.c_str());

end:
	if(pfd != NULL)
		pclose(pfd);
}

bool Job::get_uuid(std::string &uuid)
{
	FILE *fp = fopen("/proc/sys/kernel/random/uuid", "rb");
	if (fp == NULL)
	{
		syslog(Logger::ERROR, "open file uuid error");
		return false;
	}

	char buf[60];
	memset(buf, 0, 60);
	size_t n = fread(buf, 1, 36, fp);
	fclose(fp);
	
	if(n != 36)
		return false;
	
	uuid = buf;
	return true;
}

bool Job::get_timestamp(std::string &timestamp)
{
	char sysTime[128];
	struct timespec ts = {0,0};
	clock_gettime(CLOCK_REALTIME, &ts);
		 
	snprintf(sysTime, 128, "%lu", ts.tv_sec); 

	timestamp = sysTime;

	return true;
}

bool Job::get_datatime(std::string &datatime)
{
	char sysTime[128];
	struct timespec ts = {0,0};
	clock_gettime(CLOCK_REALTIME, &ts);

	struct tm *tm;
	tm = localtime(&ts.tv_sec);
		 
	snprintf(sysTime, 128, "%04u-%02u-%02u %02u:%02u:%02u", 
		tm->tm_year+1900, tm->tm_mon+1,	tm->tm_mday, 
		tm->tm_hour, tm->tm_min, tm->tm_sec); 

	datatime = sysTime;

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

bool Job::update_variables(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &name, std::string &value)
{
	syslog(Logger::INFO, "update_variables ip=%s,port=%d,user=%s,psw=%s,name=%s,value=%s", 
						ip.c_str(), port, user.c_str(), psw.c_str(), name.c_str(), value.c_str());

	int retry = stmt_retries;
	MYSQL_CONN mysql_conn;
	std::string str_sql = "set global " + name + "='" + value + "'";
	
	while(retry--)
	{
		if(mysql_conn.connect(NULL, ip.c_str(), port, user.c_str(), psw.c_str()))
		{
			syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
							ip.c_str(), port, user.c_str(), psw.c_str());
			continue;
		}
	
		if(mysql_conn.send_stmt(SQLCOM_SET_OPTION, str_sql.c_str()))
			continue;
	
		break;
	}
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

bool Job::add_file_path(std::string &jobid, std::string &path)
{
	std::lock_guard<std::mutex> lock(mutex_path_);

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
	std::lock_guard<std::mutex> lock(mutex_path_);

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

	if(!check_local_ip(ip))
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

	if(!check_local_ip(ip))
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
		std::string post_url = "http://" + remote_ip + ":" + std::to_string(node_mgr_http_port);
		syslog(Logger::INFO, "post_url %s", post_url.c_str());

		cJSON_DeleteItemFromObject(root, "job_type");
		cJSON_AddStringToObject(root, "job_type", "recv");

		std::string url = "http://" + ip + ":" + std::to_string(node_mgr_http_port) + "/" + job_id;
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

	if(!check_local_ip(ip))
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

bool Job::job_system_cmd(std::string &cmd)
{
	FILE* pfd;
	char* line;
	char buf[256];

	syslog(Logger::INFO, "system cmd: %s" ,cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "system cmd error %s" ,cmd.c_str());
		return false;
	}
	memset(buf, 0, 256);
	line = fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		syslog(Logger::ERROR, "system cmd error %s, %s", cmd.c_str(), buf);
		return false;
	}

	return true;
}

bool Job::job_save_file(std::string &path, char* buf)
{
	FILE* pfd = fopen(path.c_str(), "wb");
	if(pfd == NULL)
	{
		syslog(Logger::ERROR, "Creat json file error %s", path.c_str());
		return false;
	}

	fwrite(buf,1,strlen(buf),pfd);
	fclose(pfd);
	
	return true;
}

bool Job::job_read_file(std::string &path, std::string &str)
{
	FILE* pfd = fopen(path.c_str(), "rb");
	if(pfd == NULL)
	{
		syslog(Logger::ERROR, "read json file error %s", path.c_str());
		return false;
	}

	int len = 0;
	char buf[1024];

	do
	{
		memset(buf, 0, 1024);
		len = fread(buf,1,1024-1,pfd);
		str += buf;
	} while (len > 0);

	fclose(pfd);
	
	return true;
}

void Job::job_stop_storage(int port)
{
	FILE* pfd;
	char* line;
	char buf[256];

	std::string cmd, process_id;

	cmd = "ps -ef | grep -v grep | grep -v mysqld_safe | grep /storage/" + std::to_string(port);
	syslog(Logger::INFO, "job_stop_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "job_stop_storage error %s", cmd.c_str());
		return;
	}
	line = fgets(buf, 256, pfd);
	pclose(pfd);

	if(line != NULL)
	{
		char *p, *q;
		p = strchr(line, 0x20);
		if(p!=NULL)
		{
			while(*p==0x20)
				p++;

			q = strchr(p, 0x20);
			if(q!=NULL)
			{
				while(*q==0x20)
					q++;

				q = strchr(q, 0x20);
				if(q!=NULL)
				{
					process_id = std::string(p, q-p);
					while(p!=q)
					{
						if(*p==0x20 || (*p>='0' && *p <= '9'))
							p++;
						else
							return;
					}

					cmd = "kill -9 " + process_id;
					syslog(Logger::INFO, "job_stop_storage cmd %s", cmd.c_str());

					pfd = popen(cmd.c_str(), "r");
					if(!pfd)
					{
						syslog(Logger::ERROR, "job_stop_storage error %s", cmd.c_str());
						return;
					}
					while(fgets(buf, 256, pfd)!=NULL)
					{
						//if(strcasestr(buf, "error") != NULL)
							syslog(Logger::INFO, "%s", buf);
					}
					pclose(pfd);
				}
			}
		}
	}
}

void Job::job_stop_computer(int port)
{
	FILE* pfd;
	char* line;
	char buf[256];

	std::string cmd, process_id;

	cmd = "ps -ef | grep -v grep | grep /computer/" + std::to_string(port);
	syslog(Logger::INFO, "job_stop_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		syslog(Logger::ERROR, "job_stop_computer error %s", cmd.c_str());
		return;
	}
	line = fgets(buf, 256, pfd);
	pclose(pfd);

	if(line != NULL)
	{
		char *p, *q;
		p = strchr(line, 0x20);
		if(p!=NULL)
		{
			while(*p==0x20)
				p++;

			q = strchr(p, 0x20);
			if(q!=NULL)
			{
				process_id = std::string(p, q-p);
				while(p!=q)
				{
					if(*p>='0' && *p <= '9')
						p++;
					else
						return;
				}

				cmd = "kill -9 " + process_id;
				syslog(Logger::INFO, "job_stop_computer cmd %s", cmd.c_str());

				pfd = popen(cmd.c_str(), "r");
				if(!pfd)
				{
					syslog(Logger::ERROR, "job_stop_computer error %s", cmd.c_str());
					return;
				}
				while(fgets(buf, 256, pfd)!=NULL)
				{
					//if(strcasestr(buf, "error") != NULL)
						syslog(Logger::INFO, "%s", buf);
				}
				pclose(pfd);
			}
		}
	}
}

void Job::job_install_storage(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;
	
	cJSON *item;
	cJSON *item_node;
	cJSON *item_sub;
	char *cjson;

	FILE* pfd;
	char buf[256];

	int retry;
	int install_id;
	int port;
	std::string cluster_name,shard_name,ha_mode,ip,user,pwd;
	std::string cmd, program_path, instance_path, jsonfile_path;
	MYSQL_CONN mysql_conn;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "install storage start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	item = cJSON_GetObjectItem(root, "ha_mode");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ha_mode error";
		goto end;
	}
	ha_mode = item->valuestring;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "shard_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get shard_name error";
		goto end;
	}
	shard_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "install_id");
	if(item == NULL)
	{
		job_info = "get install_id error";
		goto end;
	}
	install_id = item->valueint;

	item_node = cJSON_GetObjectItem(root, "nodes");
	if(item_node == NULL)
	{
		job_info = "get nodes error";
		goto end;
	}
	
	item_sub = cJSON_GetArrayItem(item_node,install_id);
	if(item_sub == NULL)
	{
		job_info = "get sub node error";
		goto end;
	}

	item = cJSON_GetObjectItem(item_sub, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get sub node ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(item_sub, "port");
	if(item == NULL)
	{
		job_info = "get sub node port error";
		goto end;
	}
	port = item->valueint;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	/////////////////////////////////////////////////////////////
	//upzip from program_binaries_path to instance_binaries_path
	program_path = program_binaries_path + "/" + storage_prog_package_name + ".tgz";
	instance_path = instance_binaries_path + "/storage/" + std::to_string(port);

	//////////////////////////////
	//check exist instance and kill
	if(access(instance_path.c_str(), F_OK) == 0)
	{
		cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
		cmd += "./stopmysql.sh " + std::to_string(port);
		syslog(Logger::INFO, "system stop_storage cmd %s", cmd.c_str());
		
		pfd = popen(cmd.c_str(), "r");
		if(!pfd)
		{
			syslog(Logger::ERROR, "job_stop_storage error %s", cmd.c_str());
			return;
		}
		while(fgets(buf, 256, pfd)!=NULL)
		{
			//if(strcasestr(buf, "error") != NULL)
				syslog(Logger::INFO, "%s", buf);
		}
		pclose(pfd);

		//job_stop_storage(port);
	}

	//////////////////////////////
	//mkdir instance_path
	cmd = "mkdir -p " + instance_path;
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//rm file in instance_path
	cmd = "rm -rf " + instance_path + "/*";
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//rm file in data_dir_path
	item = cJSON_GetObjectItem(item_sub, "data_dir_path");
	if(item == NULL)
	{
		job_info = "get sub node data_dir_path error";
		goto end;
	}
	cmd = "rm -rf " + std::string(item->valuestring);
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//rm file in innodb_log_dir_path
	item = cJSON_GetObjectItem(item_sub, "innodb_log_dir_path");
	if(item == NULL)
	{
		job_info = "get sub node innodb_log_dir_path error";
		goto end;
	}
	cmd = "rm -rf " + std::string(item->valuestring);
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//rm file in log_dir_path
	item = cJSON_GetObjectItem(item_sub, "log_dir_path");
	if(item == NULL)
	{
		job_info = "get sub node log_dir_path error";
		goto end;
	}
	cmd = "rm -rf " + std::string(item->valuestring);
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//tar to instance_path
	cmd = "tar zxf " + program_path + " -C " + instance_path;
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}
	
	/////////////////////////////////////////////////////////////
	// save json file to path/dba_tools
	jsonfile_path = instance_path + "/" + storage_prog_package_name + "/dba_tools/mysql_shard.json";

	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
	}
	
	/////////////////////////////////////////////////////////////
	// start install storage cmd
	cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
	cmd += "python2 install-mysql.py --config=./mysql_shard.json --target_node_index=" + std::to_string(install_id);
	cmd += " --cluster_id " + cluster_name + " --shard_id " + shard_name + " --ha_mode " + ha_mode;
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_info = "install error " + cmd;
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = 6;
	user = "pgx";
	pwd = "pgx_pwd";
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(Instance_info::get_instance()->get_mysql_alive(&mysql_conn, ip, port, user, pwd))
			break;
	}
	mysql_conn.close_conn();

	if(retry<0)
	{
		job_info = "connect storage instance error";
		goto end;
	}

	job_result = "succeed";
	job_info = "install storage succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
}

void Job::job_install_computer(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;
	
	cJSON *item;
	cJSON *item_node;
	cJSON *item_sub;
	char *cjson;

	FILE* pfd;
	char buf[256];

	int retry;
	int install_id;
	int port;
	std::string ip,user,pwd;
	std::string cmd, program_path, instance_path, jsonfile_path;
	PGSQL_CONN pgsql_conn;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "install computer start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	item = cJSON_GetObjectItem(root, "install_id");
	if(item == NULL)
	{
		job_info = "get install_id error";
		goto end;
	}
	install_id = item->valueint;

	item_node = cJSON_GetObjectItem(root, "nodes");
	if(item_node == NULL)
	{
		job_info = "get nodes error";
		goto end;
	}

	item_sub = cJSON_GetArrayItem(item_node,install_id);
	if(item_sub == NULL)
	{
		job_info = "get sub node error";
		goto end;
	}

	item = cJSON_GetObjectItem(item_sub, "id");
	if(item == NULL)
	{
		job_info = "get sub node id error";
		goto end;
	}
	install_id = item->valueint;

	item = cJSON_GetObjectItem(item_sub, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get sub node ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(item_sub, "port");
	if(item == NULL)
	{
		job_info = "get sub node port error";
		goto end;
	}
	port = item->valueint;

	item = cJSON_GetObjectItem(item_sub, "user");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get sub node user error";
		goto end;
	}
	user = item->valuestring;

	item = cJSON_GetObjectItem(item_sub, "password");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get sub node password error";
		goto end;
	}
	pwd = item->valuestring;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	/////////////////////////////////////////////////////////////
	//upzip from program_binaries_path to instance_binaries_path
	program_path = program_binaries_path + "/" + computer_prog_package_name + ".tgz";
	instance_path = instance_binaries_path + "/computer/" + std::to_string(port);

	//////////////////////////////
	//check exist instance and kill
	if(access(instance_path.c_str(), F_OK) == 0)
	{
		job_stop_computer(port);
	}

	//////////////////////////////
	//mkdir instance_path
	cmd = "mkdir -p " + instance_path;
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//rm file in instance_path
	cmd = "rm -rf " + instance_path + "/*";
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//rm file in datadir
	item = cJSON_GetObjectItem(item_sub, "datadir");
	if(item == NULL)
	{
		job_info = "get sub node datadir error";
		goto end;
	}
	cmd = "rm -rf " + std::string(item->valuestring);
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	//////////////////////////////
	//tar to instance_path
	cmd = "tar zxf " + program_path + " -C " + instance_path;
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}
	
	/////////////////////////////////////////////////////////////
	// save json file to path/scripts
	jsonfile_path = instance_path + "/" + computer_prog_package_name + "/scripts/pgsql_comp.json";

	cjson = cJSON_Print(item_node);
	if(cjson != NULL)
	{
		job_save_file(jsonfile_path, cjson);
		free(cjson);
	}

	/////////////////////////////////////////////////////////////
	// start install computer cmd
	cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/scripts;";
	cmd += "python2 install_pg.py --config=pgsql_comp.json --install_ids=" + std::to_string(install_id);
	syslog(Logger::INFO, "job_install_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_info = "install error " + cmd;
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = 6;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(Instance_info::get_instance()->get_pgsql_alive(&pgsql_conn, ip, port, user, pwd))
			break;
	}
	pgsql_conn.close_conn();

	if(retry<0)
	{
		job_info = "connect computer instance error";
		goto end;
	}

	job_result = "succeed";
	job_info = "install computer succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
}

void Job::job_delete_storage(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;
	
	FILE* pfd;
	char buf[256];

	cJSON *root_file = NULL;
	cJSON *item;
	cJSON *item_node;
	cJSON *item_sub;

	int retry;
	int nodes;
	int port;
	std::string ip;
	std::string cmd, pathdir, instance_path, jsonfile_path, jsonfile_buf;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "delete storage start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = item->valueint;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	System::get_instance()->set_auto_pullup_working(false);
	Instance_info::get_instance()->remove_storage_instance(ip, port);

	/////////////////////////////////////////////////////////////
	// stop storage cmd
	instance_path = instance_binaries_path + "/storage/" + std::to_string(port);
	cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
	cmd += "./stopmysql.sh " + std::to_string(port);
	syslog(Logger::INFO, "job_delete_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_info = "stop error " + cmd;
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	//force kill
	//job_stop_storage(port);

	/////////////////////////////////////////////////////////////
	// read json file from path/dba_tools
	jsonfile_path = instance_path + "/" + storage_prog_package_name + "/dba_tools/mysql_shard.json";

	if(!job_read_file(jsonfile_path, jsonfile_buf))
	{
		job_info = "job_read_file error";
		goto end;
	}

	root_file = cJSON_Parse(jsonfile_buf.c_str());
	if(root_file == NULL)
	{
		job_info = "file cJSON_Parse error";
		goto end;
	}

	item_node = cJSON_GetObjectItem(root_file, "nodes");
	if(item_node == NULL)
	{
		job_info = "get nodes error";
		goto end;
	}

	nodes = cJSON_GetArraySize(item_node);

	/////////////////////////////////////////////////////////////
	//find ip and port and delete pathdir
	for(int i=0; i<nodes; i++)
	{
		int port_sub;
		std::string ip_sub;

		item_sub = cJSON_GetArrayItem(item_node,i);
		if(item_sub == NULL)
		{
			job_info = "get sub node error";
			goto end;
		}

		item = cJSON_GetObjectItem(item_sub, "ip");
		if(item == NULL)
		{
			job_info = "get sub node ip error";
			goto end;
		}
		ip_sub = item->valuestring;
		
		item = cJSON_GetObjectItem(item_sub, "port");
		if(item == NULL)
		{
			job_info = "get sub node port error";
			goto end;
		}
		port_sub = item->valueint;

		if(ip_sub != ip || port_sub != port)
			continue;

		//////////////////////////////
		//rm file in data_dir_path
		item = cJSON_GetObjectItem(item_sub, "data_dir_path");
		if(item == NULL)
		{
			job_info = "get sub node data_dir_path error";
			goto end;
		}
		cmd = "rm -rf " + std::string(item->valuestring);
		if(!job_system_cmd(cmd))
		{
			job_info = "job_system_cmd error";
			goto end;
		}

		//////////////////////////////
		//rm file in innodb_log_dir_path
		item = cJSON_GetObjectItem(item_sub, "innodb_log_dir_path");
		if(item == NULL)
		{
			job_info = "get sub node innodb_log_dir_path error";
			goto end;
		}
		cmd = "rm -rf " + std::string(item->valuestring);
		if(!job_system_cmd(cmd))
		{
			job_info = "job_system_cmd error";
			goto end;
		}

		//////////////////////////////
		//rm file in log_dir_path
		item = cJSON_GetObjectItem(item_sub, "log_dir_path");
		if(item == NULL)
		{
			job_info = "get sub node log_dir_path error";
			goto end;
		}
		cmd = "rm -rf " + std::string(item->valuestring);
		if(!job_system_cmd(cmd))
		{
			job_info = "job_system_cmd error";
			goto end;
		}
		
		break;
	}
	cJSON_Delete(root_file);
	root_file = NULL;

	//////////////////////////////
	//rm instance_path
	cmd = "rm -rf " + instance_path;
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	job_result = "succeed";
	job_info = "delete storage succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	System::get_instance()->set_auto_pullup_working(true);
	return;

end:
	if(root_file!=NULL)
		cJSON_Delete(root_file);

	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	System::get_instance()->set_auto_pullup_working(true);
}

void Job::job_delete_computer(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;
	
	FILE* pfd;
	char buf[256];

	cJSON *root_file = NULL;
	cJSON *item;
	cJSON *item_sub;

	int nodes;
	int port;
	std::string ip;
	std::string cmd, pathdir, instance_path, jsonfile_path, jsonfile_buf;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "delete computer start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = item->valueint;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	System::get_instance()->set_auto_pullup_working(false);
	Instance_info::get_instance()->remove_computer_instance(ip, port);

	/////////////////////////////////////////////////////////////
	// read json file from path/dba_tools
	instance_path = instance_binaries_path + "/computer/" + std::to_string(port);
	jsonfile_path = instance_path + "/" + computer_prog_package_name + "/scripts/pgsql_comp.json";

	if(!job_read_file(jsonfile_path, jsonfile_buf))
	{
		job_info = "job_read_file error";
		goto end;
	}

	root_file = cJSON_Parse(jsonfile_buf.c_str());
	if(root_file == NULL)
	{
		job_info = "file cJSON_Parse error";
		goto end;
	}

	nodes = cJSON_GetArraySize(root_file);

	/////////////////////////////////////////////////////////////
	//find ip and port and delete pathdir
	for(int i=0; i<nodes; i++)
	{
		int port_sub;
		std::string ip_sub;

		item_sub = cJSON_GetArrayItem(root_file,i);
		if(item_sub == NULL)
		{
			job_info = "get sub node error";
			goto end;
		}

		item = cJSON_GetObjectItem(item_sub, "ip");
		if(item == NULL)
		{
			job_info = "get sub node ip error";
			goto end;
		}
		ip_sub = item->valuestring;
		
		item = cJSON_GetObjectItem(item_sub, "port");
		if(item == NULL)
		{
			job_info = "get sub node port error";
			goto end;
		}
		port_sub = item->valueint;

		if(ip_sub != ip || port_sub != port)
			continue;

		//////////////////////////////
		//get datadir
		item = cJSON_GetObjectItem(item_sub, "datadir");
		if(item == NULL)
		{
			job_info = "get sub node datadir error";
			goto end;
		}
		pathdir = item->valuestring;

		// stop computer cmd
		cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/bin;";
		cmd += "./pg_ctl -D " + pathdir + " stop";
		syslog(Logger::INFO, "job_delete_computer cmd %s", cmd.c_str());
		
		pfd = popen(cmd.c_str(), "r");
		if(!pfd)
		{
			job_info = "stop error " + cmd;
			goto end;
		}
		while(fgets(buf, 256, pfd)!=NULL)
		{
			//if(strcasestr(buf, "error") != NULL)
				syslog(Logger::INFO, "%s", buf);
		}
		pclose(pfd);
		syslog(Logger::INFO, "stop computer end");

		//force kill
		//job_stop_computer(port);

		//rm file in pathdir
		cmd = "rm -rf " + pathdir;
		if(!job_system_cmd(cmd))
		{
			job_info = "job_system_cmd error";
			goto end;
		}
		
		break;
	}
	cJSON_Delete(root_file);
	root_file = NULL;

	//////////////////////////////
	//rm instance_path
	cmd = "rm -rf " + instance_path;
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	job_result = "succeed";
	job_info = "delete computer succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	System::get_instance()->set_auto_pullup_working(true);
	return;

end:
	if(root_file!=NULL)
		cJSON_Delete(root_file);

	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	System::get_instance()->set_auto_pullup_working(true);
}

void Job::job_group_seeds(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;

	cJSON *item;
	int port;
	std::string ip,user,pwd,variables,group_seeds;
	std::string cnf_path, cnf_buf;
	size_t pos1,pos2;

	variables = "group_replication_group_seeds";
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "update group seeds start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = item->valueint;

	item = cJSON_GetObjectItem(root, "user");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get user error";
		goto end;
	}
	user = item->valuestring;

	item = cJSON_GetObjectItem(root, "password");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get password error";
		goto end;
	}
	pwd = item->valuestring;

	item = cJSON_GetObjectItem(root, "group_seeds");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get group_seeds error";
		goto end;
	}
	group_seeds = item->valuestring;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	////////////////////////////////////////////////////////
	// get cnf 
	if(!get_cnf_path(ip, port, user, pwd, cnf_path))
	{
		job_info = "get_cnf_path error";
		goto end;
	}

	////////////////////////////////////////////////////////
	// read buf 
	if(!job_read_file(cnf_path, cnf_buf))
	{
		job_info = "job_read_file error";
		goto end;
	}

	////////////////////////////////////////////////////////
	//update group seeds to cnf
	pos1 = cnf_buf.find(variables);
	if(pos1 == std::string::npos)
	{
		job_info = "find group_seeds0 error";
		goto end;
	}

	pos1 = cnf_buf.find("\"", pos1);
	if(pos1 == std::string::npos)
	{
		job_info = "find group_seeds1 error";
		goto end;
	}
	pos1 += 1;

	pos2 = cnf_buf.find("\"", pos1);
	if(pos2 == std::string::npos)
	{
		job_info = "find group_seeds2 error";
		goto end;
	}

	cnf_buf.replace(pos1, pos2-pos1, group_seeds);

	////////////////////////////////////////////////////////
	// save buf 
	if(!job_save_file(cnf_path, (char*)cnf_buf.c_str()))
	{
		job_info = "job_save_file error";
		goto end;
	}

	////////////////////////////////////////////////////////
	//update group seeds to storage instance variables
	if(!update_variables(ip, port, user, pwd, variables, group_seeds))
	{
		job_info = "update_variables error";
		goto end;
	}

	job_result = "succeed";
	job_info = "update group seeds succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
}

void Job::job_backup_shard(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;
	cJSON *item;
	
	FILE* pfd;
	char buf[512];

	std::string cmd, cluster_name,shard_name;
	int port,hdfs_port;
	std::string ip,hdfs_ip;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "backup shard start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = item->valueint;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "shard_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get shard_name error";
		goto end;
	}
	shard_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "hdfs_ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get hdfs_ip error";
		goto end;
	}
	hdfs_ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "hdfs_port");
	if(item == NULL)
	{
		job_info = "get hdfs_port error";
		goto end;
	}
	hdfs_port = item->valueint;

	job_info = "backup shard wroking";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	////////////////////////////////////////////////////////
	//start backup path
	cmd = "backup -backuptype=storage -port=" + std::to_string(port) + " -clustername=" + cluster_name + " -shardname=" + shard_name;
	cmd += " -HdfsNameNodeService=hdfs://" + hdfs_ip + ":" + std::to_string(hdfs_port);
	syslog(Logger::INFO, "job_backup_shard cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_info = "backup error " + cmd;
		goto end;
	}
	while(fgets(buf, 512, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	////////////////////////////////////////////////////////
	//check error, must be contain cluster_name & shard_name, and the tail like ".tgz\n\0"
	if(strstr(buf, cluster_name.c_str()) == NULL || strstr(buf, shard_name.c_str()) == NULL)
	{
		syslog(Logger::ERROR, "backup error: %s", buf);
		job_info = "backup cmd return error";
		goto end;
	}
	else
	{
		char *p = strstr(buf, ".tgz");
		if(p == NULL || *(p+4) != '\n' || *(p+5) != '\0')
		{
			syslog(Logger::ERROR, "backup error: %s", buf);
			job_info = "backup cmd return error";
			goto end;
		}
	}

	////////////////////////////////////////////////////////
	//rm backup path
	cmd = "rm -rf ./data";
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	job_result = "succeed";
	job_info = "backup succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
}

void Job::job_restore_storage(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;
	cJSON *item;
	
	FILE* pfd;
	char buf[512];

	std::string cmd, cluster_name, shard_name, timestamp;
	std::string ip,hdfs_ip,user,pwd;
	MYSQL_CONN mysql_conn;
	int port,hdfs_port;
	int retry;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "restore storage start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	System::get_instance()->set_auto_pullup_working(false);

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = item->valueint;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "shard_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get shard_name error";
		goto end;
	}
	shard_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "timestamp");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get timestamp error";
		goto end;
	}
	timestamp = item->valuestring;

	item = cJSON_GetObjectItem(root, "hdfs_ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get hdfs_ip error";
		goto end;
	}
	hdfs_ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "hdfs_port");
	if(item == NULL)
	{
		job_info = "get hdfs_port error";
		goto end;
	}
	hdfs_port = item->valueint;

	job_info = "restore storage wroking";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	////////////////////////////////////////////////////////
	//start backup path
	cmd = "restore -port=" + std::to_string(port) + " -origclustername=" + cluster_name + " -origshardname=" + shard_name;
	cmd += " -restoretime='" + timestamp + "' -HdfsNameNodeService=hdfs://" + hdfs_ip + ":" + std::to_string(hdfs_port);
	syslog(Logger::INFO, "job_restore_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_info = "restore error " + cmd;
		goto end;
	}
	while(fgets(buf, 512, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	////////////////////////////////////////////////////////
	//check error
	if(strstr(buf, "restore MySQL instance successfully") == NULL)
	{
		syslog(Logger::ERROR, "restore error: %s", buf);
		job_info = "restore cmd return error";
		goto end;
	}

	////////////////////////////////////////////////////////
	//rm restore path
	cmd = "rm -rf ./data";
	if(!job_system_cmd(cmd))
	{
		job_info = "job_system_cmd error";
		goto end;
	}

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = 6;
	user = "pgx";
	pwd = "pgx_pwd";
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(Instance_info::get_instance()->get_mysql_alive(&mysql_conn, ip, port, user, pwd))
			break;
	}
	mysql_conn.close_conn();

	if(retry<0)
	{
		job_info = "connect storage instance error";
		goto end;
	}

	job_result = "succeed";
	job_info = "restore storage succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	System::get_instance()->set_auto_pullup_working(true);
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
	System::get_instance()->set_auto_pullup_working(true);
}

void Job::job_restore_computer(cJSON *root)
{
	std::string job_id;
	std::string job_result;
	std::string job_info;
	cJSON *item;
	
	FILE* pfd;
	char buf[512];

	std::string cmd, strtmp, cluster_name, meta_str;
	std::string ip;
	int port;
	int retry;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_result = "busy";
	job_info = "restore computer start";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	item = cJSON_GetObjectItem(root, "ip");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		job_info = "get port error";
		goto end;
	}
	port = item->valueint;

	if(!check_local_ip(ip))
	{
		job_info = ip + " is not local ip";
		goto end;
	}

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item = cJSON_GetObjectItem(root, "meta_str");
	if(item == NULL || item->valuestring == NULL)
	{
		job_info = "get meta_str error";
		goto end;
	}
	meta_str = item->valuestring;

	job_info = "restore computer wroking";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());

	////////////////////////////////////////////////////////
	//restore meta to computer
	cmd = "restore -restoretype=compute -workdir=./data -port=" + std::to_string(port) + " -origclustername=";
	cmd += cluster_name + " -origmetaclusterconnstr=" + meta_str + " -metaclusterconnstr=" + meta_str;
	syslog(Logger::INFO, "job_restore_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_info = "restore error " + cmd;
		goto end;
	}
	while(fgets(buf, 512, pfd)!=NULL)
	{
		//if(strcasestr(buf, "error") != NULL)
			syslog(Logger::INFO, "%s", buf);
	}
	pclose(pfd);

	////////////////////////////////////////////////////////
	//rm restore path
	cmd = "rm -rf ./data";
	//if(!job_system_cmd(cmd))
	{
	//	job_info = "job_system_cmd error";
	//	goto end;
	}

	job_result = "succeed";
	job_info = "restore compouter succeed";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::INFO, "%s", job_info.c_str());
	return;

end:
	job_result = "error";
	update_jobid_status(job_id, job_result, job_info);
	syslog(Logger::ERROR, "%s", job_info.c_str());
}

bool Job::update_jobid_status(std::string &jobid, std::string &result, std::string &info)
{
	std::lock_guard<std::mutex> lock(mutex_stauts_);

	if(list_jobid_result_info.size()>=kMaxStatus)
		list_jobid_result_info.pop_back();

	bool is_exist = false;
	for (auto it = list_jobid_result_info.begin(); it != list_jobid_result_info.end(); ++it)
	{
		if(std::get<0>(*it) == jobid)
		{
			std::get<1>(*it) = result;
			std::get<2>(*it) = info;
			is_exist = true;
			break;
		}
	}

	if(!is_exist)
		list_jobid_result_info.push_front(std::make_tuple(jobid, result, info));

	return true;
}

bool Job::get_jobid_status(std::string &jobid, std::string &result, std::string &info)
{
	std::lock_guard<std::mutex> lock(mutex_stauts_);

	bool ret = false;
	for (auto it = list_jobid_result_info.begin(); it != list_jobid_result_info.end(); ++it)
	{
		if(std::get<0>(*it) == jobid)
		{
			result = std::get<1>(*it);
			info = std::get<2>(*it);
			ret = true;
			break;
		}
	}

	return ret;
}

bool Job::job_get_status(cJSON *root, std::string &str_ret)
{
	std::string job_id, result, info;

	cJSON *item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL || item->valuestring == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return false;
	}
	job_id = item->valuestring;
	
	if(!Job::get_instance()->get_jobid_status(job_id, result, info))
		str_ret = "{\"result\":\"busy\",\"info\":\"job id no find\"}";
	else
	{
		cJSON *ret_root;
		char *ret_cjson;

		ret_root = cJSON_CreateObject();
		cJSON_AddStringToObject(ret_root, "result", result.c_str());
		cJSON_AddStringToObject(ret_root, "info", info.c_str());

		ret_cjson = cJSON_Print(ret_root);
		str_ret = ret_cjson;
		cJSON_Delete(ret_root);
		free(ret_cjson);
	}

	return true;
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
	else if(strcmp(str, "update_instance")==0)
		job_type = JOB_UPDATE_INSTANCES;
	else if(strcmp(str, "get_instance")==0)
		job_type = JOB_GET_INSTANCES;
	else if(strcmp(str, "get_info")==0)
		job_type = JOB_GET_INFO;
	else if(strcmp(str, "get_status")==0)
		job_type = JOB_GET_STATUS;
	else if(strcmp(str, "get_disk_size")==0)
		job_type = JOB_GET_DISK_SIZE;
	else if(strcmp(str, "get_path_space")==0)
		job_type = JOB_GET_PATH_SPACE;
	else if(strcmp(str, "auto_pullup")==0)
		job_type = JOB_AUTO_PULLUP;
	else if(strcmp(str, "install_storage")==0)
		job_type = JOB_INSTALL_STORAGE;
	else if(strcmp(str, "install_computer")==0)
		job_type = JOB_INSTALL_COMPUTER;
	else if(strcmp(str, "delete_storage")==0)
		job_type = JOB_DELETE_STORAGE;
	else if(strcmp(str, "delete_computer")==0)
		job_type = JOB_DELETE_COMPUTER;
	else if(strcmp(str, "group_seeds")==0)
		job_type = JOB_GROUP_SEEDS;
	else if(strcmp(str, "backup_shard")==0)
		job_type = JOB_BACKUP_SHARD;
	else if(strcmp(str, "restore_storage")==0)
		job_type = JOB_RESTORE_STORAGE;
	else if(strcmp(str, "restore_computer")==0)
		job_type = JOB_RESTORE_COMPUTER;
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
	else
		file_type = FILE_NONE;

	return true;
}

bool Job::job_handle_ahead(const std::string &para, std::string &str_ret)
{
	bool ret = false;
	cJSON *root;
	cJSON *item;
	Job_type job_type;

	root = cJSON_Parse(para.c_str());
	if(root == NULL)
	{
		syslog(Logger::ERROR, "cJSON_Parse error");	
		goto end;
	}

	item = cJSON_GetObjectItem(root, "job_type");
	if(item == NULL || !Job::get_instance()->get_job_type(item->valuestring, job_type))
	{
		syslog(Logger::ERROR, "job_handle_ahead get_job_type error");
		goto end;
	}

	if(job_type == JOB_GET_STATUS)
	{
		ret = job_get_status(root, str_ret);
	}
	else if(job_type == JOB_GET_INSTANCES)
	{
		ret = get_node_instance(root, str_ret);
	}
	else if(job_type == JOB_GET_INFO)
	{
		ret = get_node_info(root, str_ret);
	}
	else if(job_type == JOB_GET_DISK_SIZE)
	{
		ret = get_disk_size(root, str_ret);
	}
	else if(job_type == JOB_GET_PATH_SPACE)
	{
		ret = get_path_space(root, str_ret);
	}
	else if(job_type == JOB_AUTO_PULLUP)
	{
		ret = set_auto_pullup(root, str_ret);
	}
	else
		ret = false;

end:
	if(root != NULL)
		cJSON_Delete(root);

	return ret;
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
	else if(job_type == JOB_UPDATE_INSTANCES)
	{
		Instance_info::get_instance()->get_local_instance(root);
	}
	else if(job_type == JOB_INSTALL_STORAGE)
	{
		job_install_storage(root);
	}
	else if(job_type == JOB_INSTALL_COMPUTER)
	{
		job_install_computer(root);
	}
	else if(job_type == JOB_DELETE_STORAGE)
	{
		job_delete_storage(root);
	}
	else if(job_type == JOB_DELETE_COMPUTER)
	{
		job_delete_computer(root);
	}
	else if(job_type == JOB_GROUP_SEEDS)
	{
		job_group_seeds(root);
	}
	else if(job_type == JOB_BACKUP_SHARD)
	{
		job_backup_shard(root);
	}
	else if(job_type == JOB_RESTORE_STORAGE)
	{
		job_restore_storage(root);
	}
	else if(job_type == JOB_RESTORE_COMPUTER)
	{
		job_restore_computer(root);
	}

	cJSON_Delete(root);
}

void Job::add_job(std::string &str)
{
	pthread_mutex_lock(&thread_mtx);
	que_job.push(str);
	pthread_cond_signal(&thread_cond);
	pthread_mutex_unlock(&thread_mtx);
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

