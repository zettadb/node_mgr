/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef INSTANCE_INFO_H
#define INSTANCE_INFO_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"
#include "job.h"
#include "mysql_conn.h"
#include "pgsql_conn.h"

#include <pthread.h>
#include <mutex>
#include <vector>
#include <string>
#include <algorithm>

class Instance
{
public:
	enum Instance_type {NONE, META, STORAGE, COMPUTER};
	Instance_type type;
	std::string ip;
	int port;
	std::string user;
	std::string pwd;
	std::string path;
	std::string cluster;
	std::string shard;
	MYSQL_CONN *mysql_conn;
	PGSQL_CONN *pgsql_conn;

	// pullup_wait<0 : stop keepalive,   ==0 ：start keepalive,   >0 : wait to 0
	int pullup_wait;
	Instance(Instance_type type_, std::string &ip_, int port_, std::string &user_, std::string &pwd_);
	~Instance();
};

class Instance_info
{
public:
	std::mutex mutex_instance_;
	std::vector<Instance*> vec_meta_instance;
	std::vector<Instance*> vec_storage_instance;
	std::vector<Instance*> vec_computer_instance;

	std::vector<std::pair<std::string, int>> vec_path_space;
	
private:
	static Instance_info *m_inst;
	Instance_info();
	
public:
	~Instance_info();
	static Instance_info *get_instance()
	{
		if (!m_inst) m_inst = new Instance_info();
		return m_inst;
	}

	void get_local_instance();
	void get_local_instance(cJSON *root);
	int get_meta_instance();
	int get_storage_instance();
	int get_computer_instance();
	
	void set_auto_pullup(int seconds, int port);
	void keepalive_instance();

	void trimString(std::string &str);
	void set_path_space(char *paths);
};

#endif // !INSTANCE_INFO_H
