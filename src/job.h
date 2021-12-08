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
#include <set>
#include <list>
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
JOB_GET_NODE,
JOB_GET_INFO,
JOB_GET_STATUS,
JOB_GET_DISK_SIZE,
JOB_GET_SPACE_PORT,
JOB_AUTO_PULLUP,
JOB_COLD_BACKUP,
JOB_COLD_RESTORE,
JOB_INSTALL_STORAGE, 
JOB_INSTALL_COMPUTER, 
JOB_START_CLUSTER, 
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
	static int do_exit;
	std::vector<std::string> vec_local_ip;

private:
	static Job *m_inst;

	std::vector<pthread_t> vec_pthread;
	pthread_mutex_t thread_mtx;
	pthread_cond_t thread_cond;

	static const int kMaxPath = 30;
	std::mutex mutex_path_;
	std::list<std::pair<std::string, std::string>> list_jobid_path;
	static const int kMaxStatus = 30;
	std::mutex mutex_stauts_;
	std::list<std::pair<std::string, std::string>> list_jobid_status;
	
public:
	Job();
	~Job();
	static Job *get_instance()
	{
		if (!m_inst) m_inst = new Job();
		return m_inst;
	}
	int start_job_thread();
	void join_all();

	void get_local_ip();
	bool check_local_ip(std::string &ip);

	bool http_para_cmd(const std::string &para, std::string &str_ret);
	bool get_node_instance(cJSON *root, std::string &str_ret);
	bool get_node_info(cJSON *root, std::string &str_ret);
	bool set_auto_pullup(cJSON *root, std::string &str_ret);
	bool get_job_status(cJSON *root, std::string &str_ret);
	bool get_disk_size(cJSON *root, std::string &str_ret);
	bool get_space_port(cJSON *root, std::string &str_ret);
	bool get_max_port_instance(std::string &path, int &port, int &instance);
	bool get_path_size(std::string &path, std::string &used, std::string &free);
	bool get_path_size(std::string &path, uint64_t &used, uint64_t &free);
	bool get_cpu_used(std::string &cpu_used);
	bool get_mem_used(std::string &used, std::string &free);
	bool get_user_path(std::string &path);
	bool get_date_time(std::string &date_time);

	bool get_uuid(std::string &uuid);
	bool get_job_type(char     *str, Job_type &job_type);
	bool get_file_type(char     *str, File_type &file_type);
	bool get_table_path(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &db, std::string &tb, std::string &tb_path);
	bool get_binlog_path(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &binlog_path);
	bool delete_db_table(std::string &ip, int port, std::string &user, std::string &psw, 
								std::string &db, std::string &tb);
	bool get_cnf_path(std::string &ip, int port, std::string &user, std::string &psw, 
									std::string &cnf_path);
	bool get_mysql_alive(std::string &ip, int port, std::string &user, std::string &psw);
	bool get_pgsql_alive(std::string &ip, int port, std::string &user, std::string &psw);

	bool add_file_path(std::string &jobid, std::string &path);
	bool get_file_path(std::string &jobid, std::string &path);
	
	bool update_jobid_status(std::string &jobid, std::string &status);
	bool get_jobid_status(std::string &jobid, std::string &status);

	void job_delete(cJSON *root);
	void job_send(cJSON *root);
	void job_recv(cJSON *root);

	void job_cold_backup(cJSON *root);
	void job_cold_restore(cJSON *root);

	void job_install_storage(cJSON *root);
	void job_install_computer(cJSON *root);
	void job_start_cluster(cJSON *root);

	void job_handle(std::string &job);
	void add_job(std::string &str);
	void job_work();
};

#endif // !JOB_H
