/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef JOB_H
#define JOB_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"
#include "cjson.h"

#include <pthread.h>
#include <mutex>
#include <map>
#include <queue>
#include <string>
#include <vector>
#include <algorithm>

enum Job_type {
JOB_NONE, 
JOB_SEND, 
JOB_RECV, 
JOB_DELETE, 
JOB_PEEK,
JOB_UPDATE_NODE,
JOB_SN_CMD,
JOB_CN_CMD,
};
enum File_type {
FILE_NONE, 
FILE_TABLE, 
FILE_BINLOG,
};

class Job
{
public:

	std::queue<std::string> que_job;
	volatile int do_exit;
	
private:
	static Job *m_inst;

	std::vector<pthread_t*> vec_pthread;
	pthread_mutex_t thread_mtx;
	pthread_cond_t thread_cond;

	static const int kMaxPathId = 30;
	int path_id;
	std::mutex mutex_;
	std::map<std::string, std::string> map_id_path;
	
public:
	Job();
	~Job();
	static Job *get_instance()
	{
		if (!m_inst) m_inst = new Job();
		return m_inst;
	}
	void start_job_thread();
	void join_all();
	
	bool get_job_type(char     *str, Job_type &job_type);
	bool get_file_type(char     *str, File_type &file_type);
	bool get_table_path(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &db, std::string &tb, std::string &tb_path);
	bool get_binlog_path(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &binlog_path);
	bool delete_db_table(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &db, std::string &tb);

	bool get_http_get_url(std::string &str, std::string &url);

	bool add_file_path(std::string &id, std::string &path);
	bool get_file_path(std::string &id, std::string &path);
	
	void job_delete(cJSON *root);
	void job_send(cJSON *root);
	void job_recv(cJSON *root);

	void job_sn_cmd(cJSON *root);
	void job_cn_cmd(cJSON *root);

	void add_job(std::string &str);
	void job_handle(std::string &job);
	void job_work();
};

#endif // !JOB_H
