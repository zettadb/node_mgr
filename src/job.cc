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
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <dirent.h>

Job* Job::m_inst = NULL;
int Job::do_exit = 0;

int64_t num_job_threads = 3;

extern int64_t node_mgr_http_port;
extern std::string http_upload_path;
extern int64_t stmt_retries;
extern int64_t stmt_retry_interval_ms;

std::string program_binaries_path;
std::string instance_binaries_path;
std::string storage_prog_package_name;
std::string computer_prog_package_name;
int64_t storage_instance_port_start;
int64_t computer_instance_port_start;

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
		vec_pthread.push_back(hdl);
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
			vec_local_ip.push_back(tmp_ip);
		}
	}

	//for(auto &ip: vec_local_ip)
	//	syslog(Logger::INFO, "vec_local_ip=%s", ip.c_str());
}

bool Job::check_local_ip(std::string &ip)
{
	for(auto &local_ip: vec_local_ip)
		if(ip == local_ip)
			return true;
	
	return false;
}

bool Job::http_para_cmd(const std::string &para, std::string &str_ret)
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
		syslog(Logger::ERROR, "http_para_cmd get_job_type error");
		goto end;
	}

	if(job_type == JOB_GET_NODE)
	{
		ret = get_node_instance(root, str_ret);
	}
	else if(job_type == JOB_GET_INFO)
	{
		ret = get_node_info(root, str_ret);
	}
	else if(job_type == JOB_GET_STATUS)
	{
		ret = get_job_status(root, str_ret);
	}
	else if(job_type == JOB_GET_DISK_SIZE)
	{
		ret = get_disk_size(root, str_ret);
	}
	else if(job_type == JOB_GET_SPACE_PORT)
	{
		ret = get_space_port(root, str_ret);
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

	str_ret = "{\"result\":\"succeed\"}\r\n";
	return true;
}

bool Job::get_job_status(cJSON *root, std::string &str_ret)
{
	std::string job_id;
	std::string job_status;

	cJSON *item;
	cJSON *ret_root;
	cJSON *ret_item;
	char *ret_cjson;

	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return false;
	}
	job_id = item->valuestring;
	
	if(!Job::get_instance()->get_jobid_status(job_id, job_status))
		job_status = "none";

	ret_root = cJSON_CreateObject();
	cJSON_AddStringToObject(ret_root, "job_id", job_id.c_str());
	cJSON_AddStringToObject(ret_root, "job_status", job_status.c_str());

	ret_cjson = cJSON_Print(ret_root);
	str_ret = ret_cjson;
	
	if(ret_root != NULL)
		cJSON_Delete(ret_root);
	if(ret_cjson != NULL)
		free(ret_cjson);

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

bool Job::get_space_port(cJSON *root, std::string &str_ret)
{
	cJSON *item;
	int port_computer = 0;
	int port_storage = 0;
	int instance_computer = 0;
	int instance_storage = 0;
	std::string instance_binaries;

	item = cJSON_GetObjectItem(root, "paths");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get paths error");
		return false;
	}

	////////////////////////////////////////////////////////////
	//set path and get space
	Instance_info::get_instance()->set_path_space(item->valuestring);

	//syslog(Logger::INFO, "vec_path_space.size()=%d", Instance_info::get_instance()->vec_path_space.size());

	////////////////////////////////////////////////////////////
	//get port_computer
	instance_binaries = instance_binaries_path + "/computer";
	get_max_port_instance(instance_binaries, port_computer, instance_computer);
	if(port_computer < computer_instance_port_start)
		port_computer = computer_instance_port_start;
	else
		port_computer += 1;

	//syslog(Logger::INFO, "get port_computer %d", port_computer);
	
	////////////////////////////////////////////////////////////
	//get port_storage
	instance_binaries = instance_binaries_path + "/storage";
	get_max_port_instance(instance_binaries, port_storage, instance_storage);
	if(port_storage < storage_instance_port_start)
		port_storage = storage_instance_port_start;
	else
		port_storage += 3;

	//syslog(Logger::INFO, "get port_storage %d", port_storage);

	////////////////////////////////////////////////////////////
	cJSON *ret_root;
	char *ret_cjson;
	cJSON *ret_item_spaces;
	cJSON *ret_item_sub;
	
	ret_root = cJSON_CreateObject();
	cJSON_AddNumberToObject(ret_root, "port_computer", port_computer);
	cJSON_AddNumberToObject(ret_root, "port_storage", port_storage);
	cJSON_AddNumberToObject(ret_root, "instance_computer", instance_computer);
	cJSON_AddNumberToObject(ret_root, "instance_storage", instance_storage);

	ret_item_spaces = cJSON_CreateArray();
	cJSON_AddItemToObject(ret_root, "spaces", ret_item_spaces);

	for(auto &path_space: Instance_info::get_instance()->vec_path_space)
	{
		ret_item_sub = cJSON_CreateObject();
		cJSON_AddItemToArray(ret_item_spaces, ret_item_sub);
		cJSON_AddStringToObject(ret_item_sub, "path", path_space.first.c_str());
		cJSON_AddNumberToObject(ret_item_sub, "space", path_space.second);
	}

	ret_cjson = cJSON_Print(ret_root);
	str_ret = ret_cjson;

	if(ret_root != NULL)
		cJSON_Delete(ret_root);
	if(ret_cjson != NULL)
		free(ret_cjson);

	return true;
}

bool Job::get_max_port_instance(std::string &path, int &port, int &instance)
{
	bool ret = false;
	int port_dir = 0;
	
    DIR *dir;
    struct dirent *entry;

	dir = opendir(path.c_str());
    if(dir == NULL)
	{
        syslog(Logger::ERROR,"cannot open directory: %s", path.c_str());
        return false;
    }

    while((entry = readdir(dir)) != NULL)
	{
		if(entry->d_type == 4)
		{
			// skip system dir
			if(strcmp(".",entry->d_name) == 0 || strcmp("..",entry->d_name) == 0)
                continue;

			// must be digit
			bool is_digit = true;
			char *p = entry->d_name;
			while(*p != '\0')
			{
				if(*p < '0' || *p > '9')
				{
					is_digit = false;
					break;
				}
				p++;
			}

			// dir to port 
			if(is_digit)
			{
				instance++;
				port_dir = atoi(entry->d_name);

				if(port_dir > port)
					port = port_dir;
			}
		}
    }
    closedir(dir);

	return ret;
}

bool Job::get_path_size(std::string &path, std::string &used, std::string &free)
{
	bool ret = false;
	FILE* pfd;

	char *p, *q;
	char buf[256];
	std::string str_cmd;

	str_cmd = "df -h " + path;
	syslog(Logger::INFO, "get_path_size str_cmd : %s",str_cmd.c_str());
	
	pfd = popen(str_cmd.c_str(), "r");
    if(!pfd)
        goto end;

	//first line
	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	if(strstr(buf, "No such file or directory") != NULL)
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
	q = strchr(p, 0x20);
	if(q == NULL)
		goto end;

	*q = '\0';

	used = p;

	p = q+1;
	
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
	FILE* pfd;

	char *p, *q;
	char buf[256];
	std::string str_cmd;

	str_cmd = "df " + path;
	syslog(Logger::INFO, "get_path_size str_cmd : %s",str_cmd.c_str());
	
	pfd = popen(str_cmd.c_str(), "r");
	if(!pfd)
		goto end;

	//first line
	if(fgets(buf, 256, pfd) == NULL)
		goto end;

	if(strstr(buf, "No such file or directory") != NULL)
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
	q = strchr(p, 0x20);
	if(q == NULL)
		goto end;

	*q = '\0';

	used = atol(p)>>20;

	p = q+1;
	
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

bool Job::get_date_time(std::string &date_time)
{
	char szNow[36] = { 0 };
	time_t lt = time(NULL);
	struct tm* ptr = localtime(&lt);
	strftime(szNow, 36, "%Y%m%d_%H%M%S", ptr);
	date_time = szNow;
	return true;
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
	fread(buf, 1, 36, fp);
	fclose(fp);
	uuid = buf;
	
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
	else if(strcmp(str, "update_node")==0)
		job_type = JOB_UPDATE_NODE;
	else if(strcmp(str, "get_node")==0)
		job_type = JOB_GET_NODE;
	else if(strcmp(str, "get_info")==0)
		job_type = JOB_GET_INFO;
	else if(strcmp(str, "get_status")==0)
		job_type = JOB_GET_STATUS;
	else if(strcmp(str, "get_disk_size")==0)
		job_type = JOB_GET_DISK_SIZE;
	else if(strcmp(str, "get_space_port")==0)
		job_type = JOB_GET_SPACE_PORT;
	else if(strcmp(str, "auto_pullup")==0)
		job_type = JOB_AUTO_PULLUP;
	else if(strcmp(str, "coldbackup")==0)
		job_type = JOB_COLD_BACKUP;
	else if(strcmp(str, "coldrestore")==0)
		job_type = JOB_COLD_RESTORE;
	else if(strcmp(str, "install_storage")==0)
		job_type = JOB_INSTALL_STORAGE;
	else if(strcmp(str, "install_computer")==0)
		job_type = JOB_INSTALL_COMPUTER;
	else if(strcmp(str, "start_cluster")==0)
		job_type = JOB_START_CLUSTER;
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

bool Job::get_mysql_alive(std::string &ip, int port, std::string &user, std::string &psw)
{
	syslog(Logger::INFO, "get_mysql_alive ip=%s,port=%d,user=%s,psw=%s", 
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

		if(mysql_conn.send_stmt(SQLCOM_SELECT, "select version()"))
			continue;

		MYSQL_ROW row;
		if ((row = mysql_fetch_row(mysql_conn.result)))
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
	mysql_conn.free_mysql_result();
	mysql_conn.close_conn();

	if(retry<0)
		return false;

	return true;
}

bool Job::get_pgsql_alive(std::string &ip, int port, std::string &user, std::string &psw)
{
	syslog(Logger::INFO, "get_pgsql_alive ip=%s,port=%d,user=%s,psw=%s", 
						ip.c_str(), port, user.c_str(), psw.c_str());

	int retry = stmt_retries;
	PGSQL_CONN pgsql_conn;

	while(retry--)
	{
		if(pgsql_conn.connect("postgres", ip.c_str(), port, user.c_str(), psw.c_str()))
		{
			syslog(Logger::ERROR, "connect to pgsql error ip=%s,port=%d,user=%s,psw=%s", 
							ip.c_str(), port, user.c_str(), psw.c_str());
			continue;
		}
	
		if(pgsql_conn.send_stmt(PG_COPYRES_TUPLES, "select version()"))
			continue;

		if(PQntuples(pgsql_conn.result) == 1)
		{
			//syslog(Logger::INFO, "presult = %s", PQgetvalue(pgsql_conn.result,0,0));
			if (strcasestr(PQgetvalue(pgsql_conn.result,0,0), "PostgreSQL"))
				break;
		}
		
		break;
	}
	pgsql_conn.free_pgsql_result();
	pgsql_conn.close_conn();

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

	if(!check_local_ip(ip))
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
	syslog(Logger::INFO, "job_cold_backup cmd : %s",cmd.c_str());
	
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

	syslog(Logger::INFO, "job_cold_backup file %s", line);

	job_status = "coldbackup copying";
	update_jobid_status(job_id, job_status);
	
	//////////////////////////////////////////////////////////////////////
	//set backup file name in hdfs
	get_date_time(date_time);
	local_path = std::string(line);
	
	for(auto &node: Instance_info::get_instance()->vec_meta_instance)
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
		for(auto &node: Instance_info::get_instance()->vec_storage_instance)
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
#if 0
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
#endif
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

	if(!check_local_ip(ip))
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
#if 0
	if(!Hdfs_client::get_instance()->hdfs_pull_file(local_path, hdfs_path))
	{
		job_status = "job_cold_restore poll from hdfs fail!";
		goto end;
	}
#endif
	job_status = "coldrestore working";
	update_jobid_status(job_id, job_status);
	
	//////////////////////////////////////////////////////////////////////
	//start restore
	cmd = "restore -backupfile-xtrabackup=" + local_path + " -etcfile-new-mysql=" + cnf_path;
	syslog(Logger::INFO, "job_cold_restore cmd : %s",cmd.c_str());
	
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

void Job::job_install_storage(cJSON *root)
{
	std::string job_id;
	std::string job_status;
	
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
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_status = "install storage start";
	update_jobid_status(job_id, job_status);
	syslog(Logger::INFO, "%s", job_status.c_str());

	item = cJSON_GetObjectItem(root, "install_id");
	if(item == NULL)
	{
		job_status = "get install_id error";
		goto end;
	}
	install_id = item->valueint;

	item_node = cJSON_GetObjectItem(root, "nodes");
	if(item_node == NULL)
	{
		job_status = "get nodes error";
		goto end;
	}
	
	item_sub = cJSON_GetArrayItem(item_node,install_id);
	if(item_sub == NULL)
	{
		job_status = "get sub node error";
		goto end;
	}

	item = cJSON_GetObjectItem(item_sub, "ip");
	if(item == NULL)
	{
		job_status = "get sub node ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(item_sub, "port");
	if(item == NULL)
	{
		job_status = "get sub node port error";
		goto end;
	}
	port = item->valueint;

	if(!check_local_ip(ip))
	{
		job_status = ip + " is error";
		goto end;
	}

	/////////////////////////////////////////////////////////////
	//upzip from program_binaries_path to instance_binaries_path
	program_path = program_binaries_path + "/" + storage_prog_package_name + ".tgz";
	instance_path = instance_binaries_path + "/storage/" + std::to_string(port);

	//////////////////////////////
	//mkdir instance_path
	cmd = "mkdir -p " + instance_path;
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "mkdir error " + instance_path;
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "mkdir error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//rm file in instance_path
	cmd = "rm " + instance_path + "/* -rf";
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "rm file error " + instance_path;
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "rm file error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//rm file in data_dir_path
	item = cJSON_GetObjectItem(item_sub, "data_dir_path");
	if(item == NULL)
	{
		job_status = "get sub node data_dir_path error";
		goto end;
	}
	cmd = "rm " + std::string(item->valuestring) + " -rf";
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "rm file error " + std::string(item->valuestring);
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "rm file error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//rm file in innodb_log_dir_path
	item = cJSON_GetObjectItem(item_sub, "innodb_log_dir_path");
	if(item == NULL)
	{
		job_status = "get sub node innodb_log_dir_path error";
		goto end;
	}
	cmd = "rm " + std::string(item->valuestring) + " -rf";
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "rm file error " + std::string(item->valuestring);
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "rm file error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//rm file in log_dir_path
	item = cJSON_GetObjectItem(item_sub, "log_dir_path");
	if(item == NULL)
	{
		job_status = "get sub node log_dir_path error";
		goto end;
	}
	cmd = "rm " + std::string(item->valuestring) + " -rf";
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "rm file error " + std::string(item->valuestring);
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "rm file error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//tar to instance_path
	cmd = "tar zxf " + program_path + " -C " + instance_path;
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "tar error " + cmd;
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "tar error " + std::string(buf);
		goto end;
	}
	
	/////////////////////////////////////////////////////////////
	// save json file to path/dba_tools
	jsonfile_path = instance_path + "/" + storage_prog_package_name + "/dba_tools/mysql_shard.json";
	pfd = fopen(jsonfile_path.c_str(), "wb");
	if(pfd == NULL)
	{
		job_status = "Creat json file error " + jsonfile_path;
		goto end;
	}
	
	cjson = cJSON_Print(root);
	if(cjson != NULL)
	{
		fwrite(cjson,1,strlen(cjson),pfd);
		free(cjson);
	}
	fclose(pfd);

	/////////////////////////////////////////////////////////////
	// start install storage cmd
	cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
	cmd += "python2 install-mysql.py --config=./mysql_shard.json --target_node_index=" + std::to_string(install_id);
	syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "install error " + cmd;
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)
		;//syslog(Logger::INFO, "install %s", buf);
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = 3;
	user = "pgx";
	pwd = "pgx_pwd";
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(get_mysql_alive(ip, port, user, pwd))
			break;
	}

	if(retry<0)
	{
		job_status = "connect storage instance error";
		goto end;
	}

	job_status = "install succeed";
	update_jobid_status(job_id, job_status);
	syslog(Logger::INFO, "%s", job_status.c_str());
	return;

end:
	update_jobid_status(job_id, job_status);
	syslog(Logger::ERROR, "%s", job_status.c_str());
}

void Job::job_install_computer(cJSON *root)
{
	std::string job_id;
	std::string job_status;
	
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
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_status = "install computer start";
	update_jobid_status(job_id, job_status);
	syslog(Logger::INFO, "%s", job_status.c_str());

	item = cJSON_GetObjectItem(root, "install_id");
	if(item == NULL)
	{
		job_status = "get install_id error";
		goto end;
	}
	install_id = item->valueint;

	item_node = cJSON_GetObjectItem(root, "nodes");
	if(item_node == NULL)
	{
		job_status = "get nodes error";
		goto end;
	}

	item_sub = cJSON_GetArrayItem(item_node,install_id);
	if(item_sub == NULL)
	{
		job_status = "get sub node error";
		goto end;
	}

	item = cJSON_GetObjectItem(item_sub, "ip");
	if(item == NULL)
	{
		job_status = "get sub node ip error";
		goto end;
	}
	ip = item->valuestring;
	
	item = cJSON_GetObjectItem(item_sub, "port");
	if(item == NULL)
	{
		job_status = "get sub node port error";
		goto end;
	}
	port = item->valueint;

	item = cJSON_GetObjectItem(item_sub, "user");
	if(item == NULL)
	{
		job_status = "get sub node user error";
		goto end;
	}
	user = item->valuestring;

	item = cJSON_GetObjectItem(item_sub, "password");
	if(item == NULL)
	{
		job_status = "get sub node password error";
		goto end;
	}
	pwd = item->valuestring;

	if(!check_local_ip(ip))
	{
		job_status = ip + " is error";
		goto end;
	}

	/////////////////////////////////////////////////////////////
	//upzip from program_binaries_path to instance_binaries_path
	program_path = program_binaries_path + "/" + computer_prog_package_name + ".tgz";
	instance_path = instance_binaries_path + "/computer/" + std::to_string(port);

	//////////////////////////////
	//mkdir instance_path
	cmd = "mkdir -p " + instance_path;
	syslog(Logger::INFO, "job_install_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "mkdir error " + instance_path;
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "mkdir error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//rm file in instance_path
	cmd = "rm " + instance_path + "/* -rf";
	syslog(Logger::INFO, "job_install_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "rm file error " + instance_path;
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "rm file error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//rm file in datadir
	item = cJSON_GetObjectItem(item_sub, "datadir");
	if(item == NULL)
	{
		job_status = "get sub node datadir error";
		goto end;
	}
	cmd = "rm " + std::string(item->valuestring) + " -rf";
	syslog(Logger::INFO, "job_install_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "rm file error " + std::string(item->valuestring);
		goto end;
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "rm file error " + std::string(buf);
		goto end;
	}

	//////////////////////////////
	//tar to instance_path
	cmd = "tar zxf " + program_path + " -C " + instance_path;
	syslog(Logger::INFO, "job_install_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "tar error " + cmd;
		goto end;	
	}
	memset(buf, 0, 256);
	fgets(buf, 256, pfd);
	pclose(pfd);

	if(strlen(buf))
	{
		job_status = "tar error " + std::string(buf);
		goto end;
	}
	
	/////////////////////////////////////////////////////////////
	// save json file to path/scripts
	jsonfile_path = instance_path + "/" + computer_prog_package_name + "/scripts/pgsql_comp.json";
	pfd = fopen(jsonfile_path.c_str(), "wb");
	if(pfd == NULL)
	{
		job_status = "Creat json file error " + jsonfile_path;
		goto end;
	}
	
	cjson = cJSON_Print(item_node);
	if(cjson != NULL)
	{
		fwrite(cjson,1,strlen(cjson),pfd);
		free(cjson);
	}
	fclose(pfd);

	/////////////////////////////////////////////////////////////
	// start install computer cmd
	cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/scripts;";
	cmd += "python2 install_pg.py --config=pgsql_comp.json --install_ids=" + std::to_string(install_id+1);
	syslog(Logger::INFO, "job_install_computer cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "install error " + cmd;
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)
		;//syslog(Logger::INFO, "install %s", buf);
	pclose(pfd);

	/////////////////////////////////////////////////////////////
	// check instance succeed by connect to instance
	retry = 3;
	while(retry-->0 && !Job::do_exit)
	{
		sleep(1);
		if(get_pgsql_alive(ip, port, user, pwd))
			break;
	}

	if(retry<0)
	{
		job_status = "connect computer instance error";
		goto end;
	}

	job_status = "install succeed";
	update_jobid_status(job_id, job_status);
	syslog(Logger::INFO, "%s", job_status.c_str());
	return;

end:
	update_jobid_status(job_id, job_status);
	syslog(Logger::ERROR, "%s", job_status.c_str());
}

void Job::job_start_cluster(cJSON *root)
{
	std::string job_id;
	std::string job_status;
	
	cJSON *item;
	cJSON *item_shards;
	cJSON *item_meta;
	cJSON *item_sub;
	char *cjson;

	FILE* pfd;
	char buf[256];

	bool ret = false;
	int retry;
	int port;
	std::string cluster_name;
	std::string cmd, program_path, instance_path, jsonfile_path;
	
	item = cJSON_GetObjectItem(root, "job_id");
	if(item == NULL)
	{
		syslog(Logger::ERROR, "get_job_id error");
		return;
	}
	job_id = item->valuestring;

	job_status = "start cluster start";
	update_jobid_status(job_id, job_status);
	syslog(Logger::INFO, "%s", job_status.c_str());
	
	item = cJSON_GetObjectItem(root, "port");
	if(item == NULL)
	{
		job_status = "get port error";
		goto end;
	}
	port = item->valueint;

	item = cJSON_GetObjectItem(root, "cluster_name");
	if(item == NULL)
	{
		job_status = "get cluster_name error";
		goto end;
	}
	cluster_name = item->valuestring;

	item_shards = cJSON_GetObjectItem(root, "shards");
	if(item_shards == NULL)
	{
		job_status = "get shards error";
		goto end;
	}

	item_meta = cJSON_GetObjectItem(root, "meta");
	if(item_shards == NULL)
	{
		job_status = "get meta error";
		goto end;
	}

	/////////////////////////////////////////////////////////////
	//upzip from program_binaries_path to instance_binaries_path
	instance_path = instance_binaries_path + "/computer/" + std::to_string(port);

	/////////////////////////////////////////////////////////////
	// save shards json file to path/scripts
	jsonfile_path = instance_path + "/" + computer_prog_package_name + "/scripts/pgsql_shards.json";
	pfd = fopen(jsonfile_path.c_str(), "wb");
	if(pfd == NULL)
	{
		job_status = "Creat json file error " + jsonfile_path;
		goto end;
	}
	
	cjson = cJSON_Print(item_shards);
	if(cjson != NULL)
	{
		fwrite(cjson,1,strlen(cjson),pfd);
		free(cjson);
	}
	fclose(pfd);

	/////////////////////////////////////////////////////////////
	// save shards json file to path/scripts
	jsonfile_path = instance_path + "/" + computer_prog_package_name + "/scripts/pgsql_meta.json";
	pfd = fopen(jsonfile_path.c_str(), "wb");
	if(pfd == NULL)
	{
		job_status = "Creat json file error " + jsonfile_path;
		goto end;
	}
	
	cjson = cJSON_Print(item_meta);
	if(cjson != NULL)
	{
		fwrite(cjson,1,strlen(cjson),pfd);
		free(cjson);
	}
	fclose(pfd);

	/////////////////////////////////////////////////////////////
	// start cluster cmd
	cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/scripts;";
	cmd += "python2 create_cluster.py --shards_config ./pgsql_shards.json --comps_config ./pgsql_comp.json --meta_config ./pgsql_meta.json --cluster_name ";
	cmd += cluster_name + " --cluster_owner abc --cluster_biz kunlun";
	syslog(Logger::INFO, "job_start_cluster cmd %s", cmd.c_str());
	
	pfd = popen(cmd.c_str(), "r");
	if(!pfd)
	{
		job_status = "install error " + cmd;
		goto end;
	}
	while(fgets(buf, 256, pfd)!=NULL)
	{
		//syslog(Logger::INFO, "install %s", buf);
		if(strstr(buf, "Installation complete for cluster") != NULL)
			ret = true;
	}
	pclose(pfd);

	if(ret)
	{
		job_status = "install succeed";
		update_jobid_status(job_id, job_status);
		syslog(Logger::INFO, "%s", job_status.c_str());
		return;
	}
	else
	{
		job_status = "install error";
	}

end:
	update_jobid_status(job_id, job_status);
	syslog(Logger::ERROR, "%s", job_status.c_str());
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
	else if(job_type == JOB_UPDATE_NODE)
	{
		Instance_info::get_instance()->get_local_instance(root);
	}
	else if(job_type == JOB_COLD_BACKUP)
	{
		job_cold_backup(root);
	}
	else if(job_type == JOB_COLD_RESTORE)
	{
		job_cold_restore(root);
	}
	else if(job_type == JOB_INSTALL_STORAGE)
	{
		job_install_storage(root);
	}
	else if(job_type == JOB_INSTALL_COMPUTER)
	{
		job_install_computer(root);
	}
	else if(job_type == JOB_START_CLUSTER)
	{
		job_start_cluster(root);
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

