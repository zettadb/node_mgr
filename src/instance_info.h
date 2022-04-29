/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef INSTANCE_INFO_H
#define INSTANCE_INFO_H
#include "global.h"
#include "job.h"
#include "mysql_conn.h"
#include "pgsql_conn.h"
#include "sys_config.h"
#include <errno.h>

#include <algorithm>
#include <mutex>
#include <pthread.h>
#include <string>
#include <vector>

typedef std::tuple<std::string, int, int> Tpye_Path_Used_Free;

class Instance {
public:
  enum Instance_type { NONE, META, STORAGE, COMPUTER };
  Instance_type type;
  std::string ip;
  int port;
  std::string user;
  std::string pwd;
  std::string path;
  std::string cluster;
  std::string shard;
  std::string comp;
  MYSQL_CONN mysql_conn;
  PGSQL_CONN pgsql_conn;

  // pullup_wait==0 ：start keepalive,   pullup_wait>0 : wait to 0
  int pullup_wait;
  Instance(Instance_type type_, std::string &ip_, int port_, std::string &user_,
           std::string &pwd_);
  ~Instance();
};

class Instance_info {
public:
  std::mutex mutex_instance_;
  std::vector<Instance *> vec_meta_instance;
  std::vector<Instance *> vec_storage_instance;
  std::vector<Instance *> vec_computer_instance;

  std::vector<std::vector<Tpye_Path_Used_Free>> vec_vec_path_used_free;

private:
  static Instance_info *m_inst;
  Instance_info();

public:
  ~Instance_info();
  static Instance_info *get_instance() {
    if (!m_inst)
      m_inst = new Instance_info();
    return m_inst;
  }

  void get_local_instance();
  bool get_meta_instance();
  bool get_storage_instance();
  bool get_computer_instance();
  void remove_storage_instance(std::string &ip, int port);
  void remove_computer_instance(std::string &ip, int port);

  void set_auto_pullup(int seconds, int port);
  bool get_mysql_alive(MYSQL_CONN &mysql_conn, std::string &ip, int port,
                       std::string &user, std::string &psw);
  bool get_pgsql_alive(PGSQL_CONN &pgsql_conn, std::string &ip, int port,
                       std::string &user, std::string &psw);
  void keepalive_instance();

  bool get_path_used(std::string &path, uint64_t &used);
  bool get_path_free(std::string &path, uint64_t &free);
  void trimString(std::string &str);
  bool get_vec_path(std::vector<std::string> &vec_path, std::string &paths);
  bool get_path_space(std::vector<std::string> &vec_paths, std::string &result);
};

#endif // !INSTANCE_INFO_H
