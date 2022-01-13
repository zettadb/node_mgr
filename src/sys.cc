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
#include "config.h"
#include "instance_info.h"
#include "thread_manager.h"
#include "http_server.h"
#include "http_client.h"
#include "hdfs_client.h"
#include <utility>

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

	//delete Hdfs_client::get_instance();
	delete Http_client::get_instance();
	delete Instance_info::get_instance();

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
	if ((ret = (Thread_manager::get_instance()==NULL)) != 0)
		goto end;
	if ((ret = (Instance_info::get_instance()==NULL)) != 0)
		goto end;
	//if ((ret = (Hdfs_client::get_instance()==NULL)) != 0)
	//	goto end;
	if ((ret = (Http_client::get_instance()==NULL)) != 0)
		goto end;
	if ((ret = Job::get_instance()->start_job_thread()) != 0)
		goto end;
	if ((ret = Http_server::get_instance()->start_http_thread()) != 0)
		goto end;

	Instance_info::get_instance()->get_local_instance();

end:
	return ret;
}

