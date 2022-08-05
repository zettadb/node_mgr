/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef INSTANCE_INFO_H
#define INSTANCE_INFO_H
#include "global.h"
#include "job.h"
#include "zettalib/op_mysql.h"
#include "zettalib/op_pg.h"
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
//#include "mysql_conn.h"
//#include "pgsql_conn.h"
#include "sys_config.h"
#include <errno.h>
#include <unordered_map>
#include <algorithm>
#include <mutex>
#include <pthread.h>
#include <string>
#include <vector>
#include <atomic>

using namespace kunlun;

typedef std::tuple<std::string, int, int> Tpye_Path_Used_Free;

class Instance : public ErrorCup
{
public:
  enum Instance_type
  {
    NONE,
    META,
    STORAGE,
    COMPUTER
  };
  Instance_type type;
  std::string sport;
  int port;
  std::string unix_sock;
  std::string user;
  std::string pwd;
  std::string path;
  std::string cluster;
  std::string shard;
  std::string comp;
  MysqlConnection *mysql_conn;
  PGConnection *pg_conn;
  // MYSQL_CONN *mysql_conn;
  // PGSQL_CONN *pgsql_conn;

  // pullup_wait==0 ：start keepalive,   pullup_wait>0 : wait to 0
  int pullup_wait;
  // add for rebuilding node for stop mysqld
  int manual_stop_pullup;
  Instance(Instance_type type_, const std::string &port_, const std::string &unix_sock_,
           const std::string &user_, const std::string &pwd_);
  ~Instance();
  const std::string &GetPort()
  {
    return sport;
  }
  bool Init();
  int send_mysql_stmt(const char *sql, MysqlResult *res);
  int send_pg_stmt(const char *sql, PgResult *res);

  void set_manual_stop_pollup(int stop_pollup)
  {
    manual_stop_pullup = stop_pollup;
  }

private:
  bool Init_PG();
  bool Init_Mysql();
};

class MetaConnection : public ErrorCup {
public:
  MetaConnection() : meta_master_conn_(nullptr) {}
  virtual ~MetaConnection() {}

  bool Init();
  bool send_stmt(const std::string& sql, MysqlResult* result);

private:
  MysqlConnection* ConnectMysql(const std::string& ip, const std::string& port);
  bool GetMetaHaMode();
  bool GetMetaMasterConn();
  bool GetMgrMetaMasterConn();
  bool GetRbrMetaMasterConn();

private:
  std::map<std::string, MysqlConnection*> meta_conns_;
  std::vector<std::string> meta_hosts_;
  MysqlConnection* meta_master_conn_;
  std::string meta_ha_mode_;
};

class exporter_stat {
public:
  exporter_stat(const std::string& port, const std::string& binName) : port_(port), 
        isdelete_(false), binName_(binName) {}
  virtual ~exporter_stat() {}

  bool IsDelete() {
    return isdelete_.load();
  } 
  void SetDelete(bool isdelete) {
    isdelete_.store(isdelete);
  }
  const std::string& GetPort() const {
    return port_;
  }
  const std::string& GetBinName() const {
    return binName_;
  }
private:
  std::string port_;
  std::atomic<bool> isdelete_;
  std::string binName_;
};

class KProcStat : public ErrorCup {
public:
   KProcStat():pid_(0) {}
   virtual ~KProcStat() {}
   bool Parse(const char * buf);

   pid_t GetPid() const {
      return pid_;
   }
   const std::string& GetBinName() const {
      return binName_;
   }
   const std::string& GetBinArgs() const {
        return binArgs_;
   }

private:
  pid_t pid_;
  std::string binName_;
  std::string binArgs_;
};

class Instance_info : public ErrorCup {
public:
  //std::mutex mutex_instance_;
  std::mutex meta_mux_;
  std::vector<Instance *> vec_meta_instance;
  std::mutex storage_mux_;
  std::vector<Instance *> vec_storage_instance;
  std::mutex computer_mux_;
  std::vector<Instance *> vec_computer_instance;

  std::mutex node_mux_;
  std::vector<exporter_stat*> node_exporters_;
  std::mutex mysqld_mux_;
  std::vector<exporter_stat*> mysqld_exporters_;
  std::mutex postgres_mux_;
  std::vector<exporter_stat*> postgres_exporters_;

  std::vector<std::vector<Tpye_Path_Used_Free>> vec_vec_path_used_free;

private:
  static Instance_info *m_inst;
  Instance_info();

public:
  ~Instance_info();
  static Instance_info *get_instance()
  {
    if (!m_inst)
      m_inst = new Instance_info();
    return m_inst;
  }

  void get_local_instance();
  bool connect_meta_db();
  //int send_stmt(const char *sql, MysqlResult *res);
  bool send_stmt(const std::string& sql, MysqlResult *res);
  bool get_meta_instance();
  bool get_storage_instance();
  bool get_computer_instance();
  void add_storage_instance(const std::string& logdir, 
                    const std::string& port);
  void add_computer_instance(const std::string& datadir, const std::string& port);
  void remove_storage_instance(std::string &ip, int port);
  void remove_computer_instance(std::string &ip, int port);

  void toggle_auto_pullup(bool start,int port);
  bool get_auto_pullup(int port);
  void set_auto_pullup(int seconds, int port);
  bool get_mysql_alive(Instance *instance);
  bool get_pgsql_alive(Instance *instance);
  bool get_mysql_alive_tcp(const std::string &ip, int port,
                           const std::string &user, const std::string &psw);
  bool get_pgsql_alive_tcp(const std::string &ip, int port,
                           const std::string &user, const std::string &psw);
  void keepalive_instance();
  void keepalive_exporter();
  void add_node_exporter(const std::string& exporter_port);
  void add_mysqld_exporter(const std::string& exporter_port);
  void add_postgres_exporter(const std::string& exporter_port);
  void remove_node_exporter(const std::string& exporter_port);
  void remove_mysqld_exporter(const std::string& exporter_port);
  void remove_postgres_exporter(const std::string& exporter_port);
  int check_exporter_process(exporter_stat* es, pid_t& pid);
  void start_exporter(exporter_stat* es);
  void stop_exporter(exporter_stat* es);
  bool CreatePsLog();

  bool get_path_used(std::string &path, uint64_t &used);
  bool get_path_free(std::string &path, uint64_t &free);
  void trimString(std::string &str);
  bool get_vec_path(std::vector<std::string> &vec_path, std::string &paths);
  bool get_path_space(Json::Value &para, std::string &result);
  bool check_port_idle(Json::Value &para, std::string &result);
  std::string get_mysql_unix_sock(const std::string& user, const std::string& passwd, 
            const std::string& port);
  
private:
  MetaConnection* meta_conn_;
  std::mutex sql_mux_;
};

#endif // !INSTANCE_INFO_H
