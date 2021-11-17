/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys_config.h"
#include "global.h"
#include "sys.h"
#include "log.h"
#include "job.h"
#include "node_info.h"
#include "config.h"
#include "thread_manager.h"
#include "http_server.h"
#include "http_client.h"
#include "node_info.h"
#include <utility>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <sys/file.h>
#include <sys/un.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>


System *System::m_global_instance = NULL;
extern std::string log_file_path;

System::~System()
{
	Http_server::get_instance()->do_exit = 1;
	Http_server::get_instance()->join_all();
	delete Http_server::get_instance();

	Job::get_instance()->do_exit = 1;
	Job::get_instance()->join_all();
	delete Job::get_instance();

	delete Http_client::get_instance();
	delete Node_info::get_instance();

	delete Configs::get_instance();
	delete Logger::get_instance();

	Thread_manager::do_exit = 1;
	Thread_manager::get_instance()->join_all();
	pthread_mutex_destroy(&mtx);
	pthread_mutexattr_destroy(&mtx_attr);
}

/*
  Read config file and initialize config settings;
  Connect to metadata shard and get the storage shards to work on, set up
  Shard objects and the connections to each shard node.
*/
int System::create_instance(const std::string&cfg_path)
{
	m_global_instance = new System(cfg_path);
	Configs *cfg = Configs::get_instance();
	int ret = 0;

	if ((ret = Logger::create_instance()))
		goto end;
	if ((ret = cfg->process_config_file(cfg_path)))
		goto end;
	if ((ret = Logger::get_instance()->init(log_file_path)) != 0)
		goto end;
	Thread_manager::get_instance();

	{
		Job *job_inst = Job::get_instance();
		job_inst->start_job_thread();
		
		Node_info *node_inst = Node_info::get_instance();
		node_inst->start_instance();
		
		Http_server *http_inst = Http_server::get_instance();
		http_inst->start_http_thread();
		
		Http_client::get_instance();

		std::string cpu;
		m_global_instance->get_cpu_used(cpu);

		std::string used,free;
		m_global_instance->get_mem_used(used, free);
	}
	
end:
	return ret;
}

bool System::http_para_cmd(const std::string &para, std::string &str_ret)
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
		syslog(Logger::ERROR, "get_job_type error");
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

end:
	if(root != NULL)
		cJSON_Delete(root);

	return ret;
}

bool System::get_node_instance(cJSON *root, std::string &str_ret)
{
	Scopped_mutex sm(mtx);
	
	bool ret = false;
	int node_count = 0;
	cJSON *item;
	std::string disk_used, disk_free;
	
	item = cJSON_GetObjectItem(root, "node_type");
	if(item == NULL)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();
		
		//meta node
		node_count = 0;
		for(auto &node: Node_info::get_instance()->vec_meta_node)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "meta_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_disk_size(node->path, disk_used, disk_free))
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
		for(auto &node: Node_info::get_instance()->vec_storage_node)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "storage_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_disk_size(node->path, disk_used, disk_free))
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
		for(auto &node: Node_info::get_instance()->vec_computer_node)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "computer_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_disk_size(node->path, disk_used, disk_free))
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

	if(strcmp(item->valuestring, "meta_node") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();
		
		for(auto &node: Node_info::get_instance()->vec_meta_node)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "meta_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_disk_size(node->path, disk_used, disk_free))
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
	else if(strcmp(item->valuestring, "storage_node") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();

		for(auto &node: Node_info::get_instance()->vec_storage_node)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "storage_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_disk_size(node->path, disk_used, disk_free))
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
	else if(strcmp(item->valuestring, "computer_node") == 0)
	{
		cJSON *ret_root;
		cJSON *ret_item;
		char *ret_cjson;
		ret_root = cJSON_CreateObject();

		for(auto &node: Node_info::get_instance()->vec_computer_node)
		{
			std::string str;
			ret_item = cJSON_CreateObject();
			str = "computer_node" + std::to_string(node_count++);
			cJSON_AddItemToObject(ret_root, str.c_str(), ret_item);
			
			cJSON_AddStringToObject(ret_item, "ip", node->ip.c_str());
			cJSON_AddNumberToObject(ret_item, "port", node->port);
			cJSON_AddStringToObject(ret_item, "user", node->user.c_str());
			cJSON_AddStringToObject(ret_item, "pwd", node->pwd.c_str());
			cJSON_AddStringToObject(ret_item, "path", node->path.c_str());
			if(get_disk_size(node->path, disk_used, disk_free))
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

bool System::get_node_info(cJSON *root, std::string &str_ret)
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
	if(get_disk_size(path, str1,str2))
	{
		cJSON_AddStringToObject(ret_root, "disk_used", str1.c_str());
		cJSON_AddStringToObject(ret_root, "disk_free", str2.c_str());
	}

	cJSON_AddNumberToObject(ret_root, "meta_node", Node_info::get_instance()->vec_meta_node.size());
	cJSON_AddNumberToObject(ret_root, "storage_node", Node_info::get_instance()->vec_storage_node.size());
	cJSON_AddNumberToObject(ret_root, "computer_node", Node_info::get_instance()->vec_computer_node.size());
	
	ret_cjson = cJSON_Print(ret_root);
	str_ret = ret_cjson;
	
	if(ret_root != NULL)
		cJSON_Delete(ret_root);
	if(ret_cjson != NULL)
		free(ret_cjson);

	return true;
}

bool System::get_disk_size(std::string &path, std::string &used, std::string &free)
{
	bool ret = false;
	FILE* pfd;

	char *p, *q;
	char buf[256];
	std::string str_cmd;

	str_cmd = "du -h --max-depth=0 " + path;
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

bool System::get_cpu_used(std::string &cpu_used)
{
	bool ret = false;
	FILE* pfd;

	char *p, *q;
	char buf[256];
	float fcpu_use;

	pfd = popen("top -bn 1 -i -c | grep %Cpu", "r");
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

bool System::get_mem_used(std::string &used, std::string &free)
{
	bool ret = false;
	FILE* pfd;

	char *p, *q;
	char buf[256];
	int men_use;
	int men_free;

	pfd = popen("free -m | grep Mem", "r");
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
	men_free = atoi(free.c_str());

	p = q;
	while(*p == 0x20)
		p++;

	q = strchr(p, 0x20);
	if(q == NULL)
		goto end;

	used = std::string(p, q - p);
	men_use = atoi(used.c_str());

	men_free = men_free - men_use;

	used = std::to_string(men_use)+"M";
	free = std::to_string(men_free)+"M";

	ret = true;
end:
	if(pfd != NULL)
		pclose(pfd);

	return ret;
}

bool System::get_user_path(std::string &path)
{
	bool ret = false;
	FILE* pfd;

	char *p;
	char buf[256];

	path = "/home";

	pfd = popen("who am i", "r");
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

