/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef SYS_H
#define SYS_H
#include "sys_config.h"
#include "global.h"
#include <vector>
#include <map>
#include "job.h"

class Thread;

/*
  Singleton class for global settings and functionality.
*/
class System
{
private:
	std::string config_path;

	mutable pthread_mutex_t mtx;
	mutable pthread_mutexattr_t mtx_attr;

	System(const std::string&cfg_path) :
		config_path(cfg_path)
	{
		pthread_mutexattr_init(&mtx_attr);
		pthread_mutexattr_settype(&mtx_attr, PTHREAD_MUTEX_RECURSIVE);
		pthread_mutex_init(&mtx, &mtx_attr);
	}

	static System *m_global_instance;
	System(const System&);
	System&operator=(const System&);
public:
	~System();
	static int create_instance(const std::string&cfg_path);
	static System* get_instance()
	{
		Assert(m_global_instance != NULL);
		return m_global_instance;
	}

	const std::string&get_config_path()const
	{
		Scopped_mutex sm(mtx);
		return config_path;
	}

	bool http_para_cmd(const std::string &para, std::string &str_ret);
	bool get_node_instance(cJSON *root, std::string &str_ret);
	bool get_node_info(cJSON *root, std::string &str_ret);
	bool get_job_status(cJSON *root, std::string &str_ret);
	bool get_disk_size(std::string &path, std::string &used, std::string &free);
	bool get_cpu_used(std::string &cpu_used);
	bool get_mem_used(std::string &used, std::string &free);
	bool get_user_path(std::string &path);
	bool get_date_time(std::string &date_time);
};
#endif // !SYS_H
