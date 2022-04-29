/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef JOB_H
#define JOB_H
#include "global.h"
#include "sys_config.h"
#include "json/json.h"
#include <errno.h>
#include "zettalib/biodirectpopen.h"
#include "zettalib/errorcup.h"
#include "util_func/meta_info.h"

#include <algorithm>
#include <list>
#include <map>
#include <mutex>
#include <pthread.h>
#include <queue>
#include <set>
#include <string>
#include <vector>

class Job {
private:
  static Job *m_inst;

public:
  Job();
  ~Job();
  static Job *get_instance() {
    if (!m_inst)
      m_inst = new Job();
    return m_inst;
  }

  bool job_system_cmd(std::string &cmd);
  bool job_save_file(std::string &path, const char *buf);
  bool job_read_file(std::string &path, std::string &str);
  bool job_create_program_path();
  bool job_control_storage(int port, int control);
  bool job_control_computer(std::string &ip, int port, int control);
  bool job_storage_add_lib(std::set<std::string> &set_lib);
  bool job_computer_add_lib(std::set<std::string> &set_lib);
  bool job_node_exporter(Json::Value &para, std::string &job_info);
  bool job_control_instance(Json::Value &para, std::string &job_info);
  bool job_install_storage(Json::Value &para, std::string &job_info);
  bool job_install_computer(Json::Value &para, std::string &job_info);
  bool job_delete_storage(Json::Value &para, std::string &job_info);
  bool job_delete_computer(Json::Value &para, std::string &job_info);
  bool job_backup_shard(Json::Value &para, std::string &job_info);
  bool job_restore_storage(Json::Value &para, std::string &job_info);
  bool job_restore_computer(Json::Value &para, std::string &job_info);
};

#endif // !JOB_H
