/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "instance_info.h"
#include "global.h"
#include "job.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "install_task/exporter_install_dealer.h"
#include "sys.h"
#include "json/json.h"
#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <net/if.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <fstream>

Instance_info *Instance_info::m_inst = NULL;

int64_t pullup_wait_const = 9;

int64_t stmt_retries = 3;
int64_t stmt_retry_interval_ms = 500;

int64_t mysql_connect_timeout = 3;
int64_t mysql_read_timeout = 3;
int64_t mysql_write_timeout = 3;
int64_t mysql_max_packet_size = 1024 * 1024 * 1024;

extern std::string cluster_mgr_http_ip;
extern int64_t cluster_mgr_http_port;
extern int64_t thread_work_interval;

extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string prometheus_path;
extern int64_t prometheus_port_start;

extern std::string local_ip;

Instance::Instance(Instance_type type_, const std::string &port_,
                   const std::string &unix_sock_, const std::string &user_,
                   const std::string &pwd_)
    : type(type_), sport(port_), unix_sock(unix_sock_), user(user_), pwd(pwd_),
      mysql_conn(nullptr), pg_conn(nullptr), pullup_wait(0),
      manual_stop_pullup(1) {
  port = atoi(sport.c_str());
}

Instance::~Instance() {
  if (mysql_conn) {
    delete mysql_conn;
  }
  if (pg_conn) {
    delete pg_conn;
  }
}

bool Instance::Init() {
  if (type == COMPUTER) {
    return Init_PG();
  } else if (type == STORAGE || type == META) {
    return Init_Mysql();
  }
  return false;
}

bool Instance::Init_PG() {
  PGConnectionOption option;
  option.connection_type = ENUM_SQL_CONNECT_TYPE::UNIX_DOMAIN_CONNECTION;
  option.sock_path = unix_sock;
  option.port_num = port;
  option.user = "agent";
  option.password = "agent_pwd";

  for(int i=0; i<3; i++) {
    pg_conn = new PGConnection(option);
    if (!pg_conn->Connect()) {
      setErr("connect pg unix failed: %s", pg_conn->getErr());
      KLOG_ERROR( "{}", getErr());
      delete pg_conn;
      pg_conn = nullptr;
      //return false;
    } else 
      break;

    sleep(5);
  }
  if(!pg_conn)
    return false;

  return true;
}

bool Instance::Init_Mysql() {
  MysqlConnectionOption option;
  option.connect_type = ENUM_SQL_CONNECT_TYPE::UNIX_DOMAIN_CONNECTION;
  option.user = "agent";
  option.password = "agent_pwd";
  option.file_path = unix_sock;
  option.ip = local_ip;
  option.port_str = sport;

  for(int i=0; i<stmt_retries; i++) {
    mysql_conn = new MysqlConnection(option);
    if (!mysql_conn->Connect()) {
      setErr("connect mysql unix failed: %s", mysql_conn->getErr());
      KLOG_ERROR( "{}", getErr());
      delete mysql_conn;
      mysql_conn = nullptr;
      //return false;
    } else 
      break;

    sleep(1);
  }

  if(!mysql_conn)
    return false;

  MysqlResult res;
  int ret = mysql_conn->ExcuteQuery("set session my_thread_handling=1", &res);
  if (ret == -1) {
    setErr("set session failed: %s", mysql_conn->getErr());
    KLOG_ERROR( "{}", getErr());
    delete mysql_conn;
    mysql_conn = nullptr;
    return false;
  }

  return true;
}

int Instance::send_mysql_stmt(const char *sql, MysqlResult *res) {
  if (!mysql_conn) {
    if (!Init_Mysql())
      return -1;
  }
  int ret = mysql_conn->ExcuteQuery(sql, res, false);
  if (ret == -1)
    setErr("%s", mysql_conn->getErr());

  return ret;
}

int Instance::send_pg_stmt(const char *sql, PgResult *res) {
  if (!pg_conn) {
    if (!Init_PG())
      return -1;
  }
  int ret = pg_conn->ExecuteQuery(sql, res);
  if (ret == -1) {
    setErr("%s", pg_conn->getErr());
    delete pg_conn;
    pg_conn = nullptr;
  }
  return ret;
}

MysqlConnection* MetaConnection::ConnectMysql(const std::string& ip, const std::string& port) {
  MysqlConnectionOption option;
  option.user = meta_svr_user;
  option.password = meta_svr_pwd;
  option.ip = ip;
  option.port_num = atoi(port.c_str());
  option.port_str = port;
  option.database = "kunlun_metadata_db";

  MysqlConnection* mysql_conn = nullptr;
  for(int i=0; i<stmt_retries; i++) {
    mysql_conn = new MysqlConnection(option);
    if (!mysql_conn->Connect()) {
      KLOG_ERROR( "connect host {}_{} mysql failed {}", ip, port, mysql_conn->getErr());
      delete mysql_conn;
      mysql_conn = nullptr;
    } else 
      break;

    sleep(1);
  }
  
  return mysql_conn;
}

bool MetaConnection::GetMetaHaMode() {
  if(meta_conns_.size() > 0) {
    auto it = meta_conns_.begin();
    MysqlConnection *mysql_conn = it->second;
    std::string sql = string_sprintf("select value from %s.global_configuration where name='meta_ha_mode'",
              KUNLUN_METADATA_DB_NAME);
    MysqlResult result;
    int ret = mysql_conn->ExcuteQuery(sql.c_str(), &result);
    if(ret == -1) {
      KLOG_ERROR("get meta ha_mode failed {}", mysql_conn->getErr());
      return false;
    }
    if (result.GetResultLinesNum() == 1) {
      meta_ha_mode_ = result[0]["value"];
      return true;
    }
  }
  return false;
}

bool MetaConnection::GetMgrMetaMasterConn() {
  if(meta_conns_.size() > 0) {
    auto it = meta_conns_.begin();
    MysqlConnection *mysql_conn = it->second;

    std::string sql = "select MEMBER_HOST,MEMBER_PORT from performance_schema.replication_group_members where MEMBER_ROLE='PRIMARY' and MEMBER_STATE = 'ONLINE'";
    MysqlResult result;
    int ret = mysql_conn->ExcuteQuery(sql.c_str(), &result);
    if(ret == -1) {
      KLOG_ERROR("get meta replication group members failed {}", mysql_conn->getErr());
      return false;
    }

    if(result.GetResultLinesNum() == 1) {
      std::string member_host = result[0]["MEMBER_HOST"];
      std::string member_port = result[0]["MEMBER_PORT"];
      std::string host = member_host+"_"+member_port;
      if(meta_conns_.find(host) == meta_conns_.end()) {
        MysqlConnection *mysql_conn = ConnectMysql(member_host, member_port);
        if(!mysql_conn) {
          return false;
        }
        meta_conns_[host] = mysql_conn;
        meta_master_conn_ = mysql_conn;
        return true;
      } else {
        meta_master_conn_ = meta_conns_[host];
        return true;
      }
    }
  }
  return false;
}

bool MetaConnection::GetRbrMetaMasterConn() {
  return false;
}

bool MetaConnection::GetMetaMasterConn() {
  bool ret = false;
  if(meta_ha_mode_ == "mgr") {
    ret = GetMgrMetaMasterConn();
  } else if(meta_ha_mode_ == "rbr") {
    ret = GetRbrMetaMasterConn();
  }
  return ret;
}

bool MetaConnection::Init() {
  meta_hosts_ = StringTokenize(meta_group_seeds, ",");
  if(meta_hosts_.size() <= 0)
    return false;

  for(size_t i=0; i< meta_hosts_.size(); i++) {
    std::string meta_ip = meta_hosts_[i].substr(0, meta_hosts_[i].rfind(":"));
    std::string meta_port = meta_hosts_[i].substr(meta_hosts_[i].rfind(":")+1);
    MysqlConnection* mysql_conn = ConnectMysql(meta_ip, meta_port);
    if(!mysql_conn) {
      KLOG_ERROR("connect host {} mysql error", meta_hosts_[i]);
      continue;
    }
    meta_conns_[meta_hosts_[i]] = mysql_conn;
  }

  if(!GetMetaHaMode()) 
    return false;
  
  if(!GetMetaMasterConn())
    return false;

  return true;
}

bool MetaConnection::send_stmt(const std::string& sql, MysqlResult* res) {
  if(!meta_master_conn_) {
    if(!GetMetaMasterConn())
      return false;
  }
  bool rets = true;
  for(int i=0; i < stmt_retries; i++) {
    int ret = meta_master_conn_->ExcuteQuery(sql.c_str(), res, false);
    if(ret == -1) {
      KLOG_ERROR("execute sql {} failed {}", sql, meta_master_conn_->getErr());
      rets = false;
    } else {
      rets = true;
      break;
    } 
    sleep(2);
  }

  if(!rets) {
    meta_master_conn_ = nullptr;
  }
  return rets;
}

Instance_info::Instance_info() { meta_conn_ = nullptr; }

Instance_info::~Instance_info() {
  for (auto &instance : vec_meta_instance)
    delete instance;
  vec_meta_instance.clear();
  for (auto &instance : vec_storage_instance)
    delete instance;
  vec_storage_instance.clear();
  for (auto &instance : vec_computer_instance)
    delete instance;
  vec_computer_instance.clear();

  if (meta_conn_) {
    delete meta_conn_;
    meta_conn_ = nullptr;
  }
}

void Instance_info::get_local_instance() {
  get_meta_instance();
  get_storage_instance();
  get_computer_instance();
}

bool Instance_info::send_stmt(const std::string& sql, MysqlResult *res) {
  std::lock_guard<std::mutex> lk(sql_mux_);
  if(!meta_conn_) {
    meta_conn_ = new MetaConnection();
    if(!meta_conn_->Init()) {
      setErr("%s", meta_conn_->getErr());
      return false;
    }
  }

  return (meta_conn_->send_stmt(sql, res));
}

std::string Instance_info::get_mysql_unix_sock(const std::string& user, const std::string& passwd, 
            const std::string& port) {
  std::string unix_sock;
  MysqlConnectionOption option;
  option.autocommit = true;
  option.ip = local_ip;
  option.port_num = atoi(port.c_str());
  option.user = user;
  option.password = passwd;

  MysqlConnection mysql_conn(option);
  
  char sql[1024] = {0};
  snprintf(sql, 1024, "show global variables like 'socket'");

  MysqlResult result;
  if(mysql_conn.ExcuteQuery(sql, &result, true) == -1) {
    KLOG_ERROR("get port: {} socket path failed {}", port, mysql_conn.getErr());
    return unix_sock;
  }
  if(result.GetResultLinesNum() != 1) {
    KLOG_ERROR("get global variable num failed");
    return unix_sock;
  }
  unix_sock = result[0]["Value"];
  return unix_sock;
}

bool Instance_info::get_meta_instance() {
  //std::lock_guard<std::mutex> lock(mutex_instance_);
  std::lock_guard<std::mutex> lk(meta_mux_);

  MysqlResult result_set;
  char sql[2048] = {0};
  sprintf(sql,
          "select port,user_name,passwd from meta_db_nodes where "
          "hostaddr='%s'", local_ip.c_str());

  bool ret = send_stmt(sql, &result_set);
  if (!ret) {
    KLOG_ERROR("metadata db query:[{}] failed: {}", sql, getErr());
    return false;
  }

  int lines = result_set.GetResultLinesNum();
  if (lines > 0) {
    for (int i = 0; i < lines; i++) {
      std::string port = result_set[i]["port"];
      int exist_flag = 0;
      for (auto it : vec_meta_instance) {
        if (it->GetPort() == port) {
          exist_flag = 1;
          break;
        }
      }

      if (exist_flag)
        continue;

      std::string user = result_set[i]["user_name"];
      std::string pwd = result_set[i]["passwd"];
      //std::string datadir = result_set[i]["datadir"];
      std::string unix_sock = get_mysql_unix_sock(user, pwd, port);//datadir +"/prod/mysql.sock";
    
      Instance *instance =
          new Instance(Instance::META, port, unix_sock, user, pwd);
      if (!instance->Init()) {
        KLOG_ERROR("instance init failed: {}", instance->getErr());
        //delete instance;
        //continue;
      }

      vec_meta_instance.emplace_back(instance);
    }
  }
  KLOG_INFO("meta instance {} update", vec_meta_instance.size());

  return true;
}

bool Instance_info::get_storage_instance() {
  //std::lock_guard<std::mutex> lock(mutex_instance_);
  std::lock_guard<std::mutex> lk(storage_mux_);

  MysqlResult result_set;
  char sql[2048] = {0};
  sprintf(sql,
          "select port,user_name,passwd from shard_nodes where hostaddr='%s'",
          local_ip.c_str());

  bool ret = send_stmt(sql, &result_set);
  if (!ret) {
    KLOG_ERROR( "metadata db query:[{}] failed: {}", sql, getErr());
    return false;
  }

  int lines = result_set.GetResultLinesNum();
  if (lines > 0) {
    for (int i = 0; i < lines; i++) {
      std::string port = result_set[i]["port"];
      int exist_flag = 0;
      for (auto it : vec_storage_instance) {
        if (it->GetPort() == port) {
          exist_flag = 1;
          break;
        }
      }

      if (exist_flag)
        continue;

      std::string user = result_set[i]["user_name"];
      std::string pwd = result_set[i]["passwd"];
      std::string unix_sock = get_mysql_unix_sock(user, pwd, port);//datadir + "/instance_data/data_dir_path/" + port +
                              //"/prod/mysql.sock";

      Instance *instance =
          new Instance(Instance::STORAGE, port, unix_sock, user, pwd);
      if (!instance->Init()) {
        KLOG_ERROR( "instance init failed: {}", instance->getErr());
      }

      vec_storage_instance.emplace_back(instance);
      int port_in = atoi(port.c_str()); 
      add_mysqld_exporter(std::to_string(port_in+1));
    }
  }
  KLOG_INFO("storage instance {} update",
         vec_storage_instance.size());

  return true;
}

bool Instance_info::get_computer_instance() {
  //std::lock_guard<std::mutex> lock(mutex_instance_);
  std::lock_guard<std::mutex> lk(computer_mux_);

  MysqlResult result_set;
  char sql[2048] = {0};
  sprintf(sql, "select comp_datadir from server_nodes where hostaddr='%s' and machine_type='computer'",
          local_ip.c_str());
  bool ret = send_stmt(sql, &result_set);
  if (!ret) {
    KLOG_ERROR("metadata db query:[{}] failed: {}", sql, getErr());
    return false;
  }

  int lines = result_set.GetResultLinesNum();
  if (lines != 1) {
    KLOG_ERROR("get host: {} comp_datadir failed", local_ip);
    return false;
  }

  std::string comp_datadir = result_set[0]["comp_datadir"];
  sprintf(sql,
          "select port,user_name,passwd from comp_nodes where hostaddr='%s'",
          local_ip.c_str());

  ret = send_stmt(sql, &result_set);
  if (!ret) {
    KLOG_ERROR( "metadata db query:[{}] failed: {}", sql, getErr());
    return false;
  }

  lines = result_set.GetResultLinesNum();
  if (lines > 0) {
    for (int i = 0; i < lines; i++) {
      std::string port = result_set[i]["port"];
      int exist_flag = 0;
      for (auto it : vec_computer_instance) {
        if (it->GetPort() == port) {
          exist_flag = 1;
          break;
        }
      }

      if (exist_flag)
        continue;

      std::string user = result_set[i]["user_name"];
      std::string pwd = result_set[i]["passwd"];
      std::string unix_sock =
          comp_datadir + "/" + port;
      Instance *instance =
          new Instance(Instance::COMPUTER, port, unix_sock, user, pwd);
      if (!instance->Init()) {
        KLOG_ERROR("instance init failed: {}", instance->getErr());
      }
      vec_computer_instance.emplace_back(instance);
      int port_in = atoi(port.c_str());
      add_postgres_exporter(std::to_string(port_in+2));
    }
  }
  KLOG_INFO( "computer instance {} update",
         vec_computer_instance.size());

  return true;
}

void Instance_info::add_storage_instance(const std::string& logdir, 
                    const std::string& port) {
  std::string user, pwd;
  std::string unix_sock = logdir+"/"+port+"/mysql.sock";
  Instance *instance =
          new Instance(Instance::STORAGE, port, unix_sock, user, pwd);
  if (!instance->Init()) {
    KLOG_ERROR( "instance init failed: {}", instance->getErr());
  }
  {
    std::lock_guard<std::mutex> lock(storage_mux_);
    vec_storage_instance.emplace_back(instance);
  }

  KLOG_INFO("report port {} into storage instance ", port);
}

void Instance_info::add_computer_instance(const std::string& datadir, const std::string& port) {
  std::string user, pwd;
  std::string unix_sock =
          datadir + "/" + port;
  Instance *instance =
        new Instance(Instance::COMPUTER, port, unix_sock, user, pwd);
  if (!instance->Init()) {
    KLOG_ERROR("instance init failed: {}", instance->getErr());
  }

  {
    std::lock_guard<std::mutex> lock(computer_mux_);
    vec_computer_instance.emplace_back(instance);
  }
  KLOG_INFO("report port {} into computer instance ", port);
}

void Instance_info::remove_storage_instance(std::string &ip, int port) {
  //std::lock_guard<std::mutex> lock(mutex_instance_);

  {
    std::lock_guard<std::mutex> lk(storage_mux_);
    for (auto it = vec_storage_instance.begin(); it != vec_storage_instance.end();
        it++) {
      if (local_ip == ip && (*it)->port == port) {
        delete *it;
        vec_storage_instance.erase(it);
        return;
      }
    }
  }

  {
    std::lock_guard<std::mutex> lk(computer_mux_);
    for(auto it = vec_meta_instance.begin(); it != vec_meta_instance.end(); 
        it++)	{
      if(local_ip == ip && (*it)->port == port) {
        delete *it;
        vec_meta_instance.erase(it);
        return;
      }
    }
  }
}

void Instance_info::remove_computer_instance(std::string &ip, int port) {
  //std::lock_guard<std::mutex> lock(mutex_instance_);
  std::lock_guard<std::mutex> lk(computer_mux_);

  for (auto it = vec_computer_instance.begin();
       it != vec_computer_instance.end(); it++) {
    if (local_ip == ip && (*it)->port == port) {
      delete *it;
      vec_computer_instance.erase(it);
      break;
    }
  }
}

bool Instance_info::get_auto_pullup(int port) {
  //std::lock_guard<std::mutex> lock(mutex_instance_);
  {
    std::lock_guard<std::mutex> lk(meta_mux_);
    for (auto &instance : vec_meta_instance) {
      if (instance->port == port) {
          return instance->manual_stop_pullup;
      }
    }
  }

  {
    std::lock_guard<std::mutex> lk(storage_mux_);
    for (auto &instance : vec_storage_instance) {
      if (instance->port == port) {
        return instance->manual_stop_pullup;
      }
    }
  }

  {
    std::lock_guard<std::mutex> lk(computer_mux_);
    for (auto &instance : vec_computer_instance) {
      if (instance->port == port) {
        return instance->manual_stop_pullup;
      }
    }
  }
  return true;
}

void Instance_info::toggle_auto_pullup(bool start, int port) {
  //std::lock_guard<std::mutex> lock(mutex_instance_);
  if (port <= 0) // done on all of the port
  {
    {
      std::lock_guard<std::mutex> lk(meta_mux_);
      for (auto &instance : vec_meta_instance)
        instance->manual_stop_pullup = start;
    }

    {
      std::lock_guard<std::mutex> lk(storage_mux_);
      for (auto &instance : vec_storage_instance)
        instance->manual_stop_pullup = start;
    }

    {
      std::lock_guard<std::mutex> lk(computer_mux_);
      for (auto &instance : vec_computer_instance)
        instance->manual_stop_pullup = start;
    }
  } else // find the instance compare by port
  {
    {
      std::lock_guard<std::mutex> lk(meta_mux_);
      for (auto &instance : vec_meta_instance) {
        if (instance->port == port) {
          instance->manual_stop_pullup = start;
          return;
        }
      }
    }

    {
      std::lock_guard<std::mutex> lk(storage_mux_);
      for (auto &instance : vec_storage_instance) {
        if (instance->port == port) {
          instance->manual_stop_pullup = start;
          return;
        }
      }
    }

    {
      std::lock_guard<std::mutex> lk(computer_mux_);
      for (auto &instance : vec_computer_instance) {
        if (instance->port == port) {
          instance->manual_stop_pullup = start;
          return;
        }
      }
    }
  }
}

void Instance_info::set_auto_pullup(int seconds, int port) {
  //std::lock_guard<std::mutex> lock(mutex_instance_);

  if (port <= 0) // done on all of the port
  {
    {
      std::lock_guard<std::mutex> lk(meta_mux_);
      for (auto &instance : vec_meta_instance)
        instance->pullup_wait = seconds;
    }
    
    {
      std::lock_guard<std::mutex> lk(storage_mux_);
      for (auto &instance : vec_storage_instance)
        instance->pullup_wait = seconds;
    }

    {
      std::lock_guard<std::mutex> lk(computer_mux_);
      for (auto &instance : vec_computer_instance)
        instance->pullup_wait = seconds;
    }
  } else // find the instance compare by port
  {
    {
      std::lock_guard<std::mutex> lk(meta_mux_);
      for (auto &instance : vec_meta_instance) {
        if (instance->port == port) {
          instance->pullup_wait = seconds;
          return;
        }
      }
    }

    {
      std::lock_guard<std::mutex> lk(storage_mux_);
      for (auto &instance : vec_storage_instance) {
        if (instance->port == port) {
          instance->pullup_wait = seconds;
          return;
        }
      }
    }

    {
      std::lock_guard<std::mutex> lk(computer_mux_);
      for (auto &instance : vec_computer_instance) {
        if (instance->port == port) {
          instance->pullup_wait = seconds;
          return;
        }
      }
    }
  }
}

bool Instance_info::get_mysql_alive(Instance *instance) {
  // syslog(Logger::INFO, "get_mysql_alive ip=%s,port=%d,user=%s,psw=%s",
  // ip.c_str(), port, user.c_str(), psw.c_str());
 
  int retry = stmt_retries;

  MysqlResult res;
  while (retry--) {
    if(instance->unix_sock.empty()) {
      instance->unix_sock = get_mysql_unix_sock(instance->user, instance->pwd,
                    instance->sport);
    }
    int ret = instance->send_mysql_stmt("select version()", &res);
    if (ret == -1) {
      KLOG_ERROR("mysql select version failed: {}",
             instance->getErr());
      continue;
    }

    if (res.GetResultLinesNum() != 1)
      continue;

    std::string version = res[0]["version()"];
    if (strcasestr(version.c_str(), "kunlun-storage"))
      break;
  }

  if (retry < 0)
    return false;

  return true;
}

bool Instance_info::get_pgsql_alive(Instance *instance) {
  // syslog(Logger::INFO, "get_pgsql_alive ip=%s,port=%d,user=%s,psw=%s",
  // ip.c_str(), port, user.c_str(), psw.c_str());

  int retry = stmt_retries;
  PgResult res;
  while (retry--) {
    int ret = instance->send_pg_stmt("select version()", &res);
    if (ret == -1) {
      KLOG_ERROR("pg select version failed: {}", instance->getErr());
      continue;
    }

    if (res.GetNumRows() != 1)
      continue;

    std::string version = res[0]["version"];
    if (strcasestr(version.c_str(), "PostgreSQL"))
      break;
  }

  if (retry < 0)
    return false;

  return true;
}

bool Instance_info::get_mysql_alive_tcp(const std::string &ip, int port,
                                        const std::string &user,
                                        const std::string &psw) {
  MysqlConnectionOption option;
  option.autocommit = true;
  option.ip = ip;
  option.port_num = port;
  option.user = user;
  option.password = psw;

  MysqlConnection mysql_conn(option);
  int retry = stmt_retries;

  MysqlResult res;
  while (retry--) {
    if (!mysql_conn.Connect()) {
      KLOG_ERROR( "mysql tcp connect failed: {}",
             mysql_conn.getErr());
      continue;
    }

    int ret = mysql_conn.ExcuteQuery("select version()", &res);
    if (ret == -1) {
      KLOG_ERROR("mysql tcp execute sql failed: {}",
             mysql_conn.getErr());
      continue;
    }

    if (res.GetResultLinesNum() != 1)
      continue;

    std::string version = res[0]["version()"];
    if (strcasestr(version.c_str(), "kunlun-storage"))
      break;
  }

  if (retry < 0)
    return false;

  return true;
}

bool Instance_info::get_pgsql_alive_tcp(const std::string &ip, int port,
                                        const std::string &user,
                                        const std::string &psw) {
  PGConnectionOption option;
  option.ip = ip;
  option.port_num = port;
  option.user = user;
  option.password = psw;

  PGConnection pg_conn(option);
  int retry = stmt_retries;

  PgResult res;
  while (retry--) {
    if (!pg_conn.Connect()) {
      KLOG_ERROR("pg tcp connect failed: {}", pg_conn.getErr());
      continue;
    }

    int ret = pg_conn.ExecuteQuery("select version()", &res);
    if (ret == -1) {
      KLOG_ERROR("pg tcp execute sql failed: {}", pg_conn.getErr());
      continue;
    }

    if (res.GetNumRows() != 1)
      continue;

    std::string version = res[0]["version"];
    if (strcasestr(version.c_str(), "PostgreSQL"))
      break;
  }

  if (retry < 0)
    return false;

  return true;
}

bool Instance_info::CreatePsLog() {
  std::string ps_cmd = "ps -eo pid,ppid,command > ./.ps_log";
  BiodirectPopen bi_open(ps_cmd.c_str());
  bool ret = bi_open.Launch("r");
  if(!ret || bi_open.get_chiled_status() != 0) {
    FILE *stderr_fp = bi_open.getReadStdErrFp();
    if(stderr_fp == nullptr){
      KLOG_ERROR("Biooppen launch failed: {}",bi_open.getErr());
      return false;
    }
        
    char buffer[8192];
    if (fgets(buffer, 8192, stderr_fp) != nullptr) {
      KLOG_ERROR("Biopopen execute failed and stderr: {}", buffer);
    }
    return false;
  }
  return true;
}

bool KProcStat::Parse(const char * buf) {
    std::vector<std::string> vec = StringTokenize(buf , " ");
    if (vec.size () < 3) {
        setErr("get buf is not valid ,size < 3,buf:%s" , buf);
        return false;
    }
    size_t len = vec.size();
    
    pid_ = strtoull(vec[0].c_str(), NULL, 10);
    binName_ = trim(vec[2]);
    if(len == 3)
        return true;

    for(size_t i=3; i<len - 1; i++)
        binArgs_ += vec[i]+" ";

    binArgs_ += vec[len-1];
    return true;
}

int Instance_info::check_exporter_process(exporter_stat* es, pid_t& pid) {
  std::ifstream fin("./.ps_log", std::ios::in);
  if(!fin.is_open()) {
    KLOG_ERROR("open ps_log file failed: {}.{}", errno, strerror(errno));
    return 1;
  }

  std::string sbuf;
  while(std::getline(fin, sbuf)) {
    trim(sbuf);
    KProcStat procStat;
    if(!procStat.Parse(sbuf.c_str())) {
      KLOG_ERROR("parse proc line: {} failed", sbuf);
      return 1;
    }
        
    std::string binName = "./"+es->GetBinName();
    std::string binArgs = "--web.listen-address=:"+es->GetPort();
    if(procStat.GetBinArgs() == binArgs && procStat.GetBinName() == binName) {
      pid = procStat.GetPid();
      return 0;
    }
  }
  return 2;
}

void Instance_info::keepalive_exporter() {
  /////////////////////////////////////////////////////////////
  // keep alive of node_exporter
  std::vector<std::string> del_ports;
  if(!CreatePsLog())
    return;

  pid_t pid;
  {
    std::lock_guard<std::mutex> lk(node_mux_);
    del_ports.clear();
    for (auto ne : node_exporters_) {
      if(ne->IsDelete()) {
        del_ports.push_back(ne->GetPort());
        continue;
      }
      
      if(check_exporter_process(ne, pid) == 2) {
        KLOG_ERROR("node_exporter port {} is not alive, restart again", ne->GetPort());
        start_exporter(ne);
      }
    }

    for(auto dp : del_ports) {
      for(std::vector<exporter_stat*>::iterator it = node_exporters_.begin(); it != node_exporters_.end();) {
        if((*it)->GetPort() == dp) {
          stop_exporter(*it);
          delete *it;
          node_exporters_.erase(it);
          break;
        }
        it++;
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // keep alive of storage
  {
    std::lock_guard<std::mutex> lk(mysqld_mux_);
    del_ports.clear();

    for (auto me : mysqld_exporters_) {
      if(me->IsDelete()) {
        del_ports.push_back(me->GetPort());
        continue;
      }

      if(check_exporter_process(me, pid) == 2) {
        KLOG_ERROR("mysql_exporter port {} is not alive, restart again", me->GetPort());
        start_exporter(me);
      }
    }

    for(auto dp : del_ports) {
      for(std::vector<exporter_stat*>::iterator it = mysqld_exporters_.begin(); it != mysqld_exporters_.end();) {
        if((*it)->GetPort() == dp) {
          stop_exporter(*it);
          delete *it;
          mysqld_exporters_.erase(it);
          break;
        }
        it++;
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // keep alive of computer
  {
    std::lock_guard<std::mutex> lk(postgres_mux_);
    del_ports.clear();

    for (auto pe : postgres_exporters_) {
      if(pe->IsDelete()) {
        del_ports.push_back(pe->GetPort());
        continue;
      }

      if(check_exporter_process(pe, pid) == 2) {
        KLOG_ERROR("postgres_exporter port {} is not alive, restart again", pe->GetPort());
        start_exporter(pe);
      }
    }

    for(auto dp : del_ports) {
      for(std::vector<exporter_stat*>::iterator it = postgres_exporters_.begin(); it != postgres_exporters_.end();) {
        if((*it)->GetPort() == dp) {
          stop_exporter(*it);
          delete *it;
          postgres_exporters_.erase(it);
          break;
        }
        it++;
      }
    }
  }
}

void Instance_info::start_exporter(exporter_stat* es) {
  timespec time;
  clock_gettime(CLOCK_REALTIME, &time);

  std::string cmd;
  if(es->GetBinName() == "node_exporter")
    cmd = string_sprintf("cd %s/%s; ./%s --web.listen-address=:%s > ./%s.log_%s_%ld 2>&1 &",
          prometheus_path.c_str(), es->GetBinName().c_str(), es->GetBinName().c_str(), 
          es->GetPort().c_str(), es->GetBinName().c_str(), es->GetPort().c_str(), time.tv_sec);
  else if(es->GetBinName() == "mysqld_exporter") {
    std::string sport = es->GetPort();
    int port = atoi(sport.c_str()) - 1;
    std::string unix_sock = get_mysql_unix_sock("clustmgr", "clustmgr_pwd", std::to_string(port));
    cmd = string_sprintf("export DATA_SOURCE_NAME=\"agent:agent_pwd@unix(%s)/?allowOldPasswords=true\"; cd %s/%s; ./%s --web.listen-address=:%s > ./%s.log_%s_%ld 2>&1 &",
              unix_sock.c_str(), prometheus_path.c_str(), es->GetBinName().c_str(), es->GetBinName().c_str(), 
          es->GetPort().c_str(), es->GetBinName().c_str(), es->GetPort().c_str(), time.tv_sec);
  } else if(es->GetBinName() == "postgres_exporter") {
    std::string sport = es->GetPort();
    int port = atoi(sport.c_str()) - 2;
    cmd = string_sprintf("export DATA_SOURCE_NAME=\"postgresql://agent:agent_pwd@localhost:%d/postgres?sslmode=disable\"; cd %s/%s; ./%s --web.listen-address=:%s > ./%s.log_%s_%ld 2>&1 &",
        port, prometheus_path.c_str(), es->GetBinName().c_str(), es->GetBinName().c_str(), 
          es->GetPort().c_str(), es->GetBinName().c_str(), es->GetPort().c_str(), time.tv_sec);
  }

  KLOG_INFO("Will execute cmd: {}", cmd);
  BiodirectPopen bi_open(cmd.c_str());
  bool ret = bi_open.Launch("r");
  if(!ret || bi_open.get_chiled_status() != 0) {
    FILE *stderr_fp = bi_open.getReadStdErrFp();
    if(stderr_fp == nullptr){
      KLOG_ERROR("Biooppen launch failed: {}",bi_open.getErr());
      return;
    }
        
    char buffer[8192];
    if (fgets(buffer, 8192, stderr_fp) != nullptr) {
      KLOG_ERROR("Biopopen execute failed and stderr: {}", buffer);
    }
    return;
  }
}

void Instance_info::stop_exporter(exporter_stat *es) {
  pid_t pid;
  if(check_exporter_process(es, pid))
    return;

  std::string cmd = string_sprintf("kill -9 %d", pid);
  KLOG_INFO("Will kill {} cmd: {}", es->GetBinName(), cmd);
  BiodirectPopen bi_open(cmd.c_str());
  bool ret = bi_open.Launch("r");
  if(!ret || bi_open.get_chiled_status() != 0) {
    FILE *stderr_fp = bi_open.getReadStdErrFp();
    if(stderr_fp == nullptr){
      KLOG_ERROR("Biooppen launch failed: {}",bi_open.getErr());
      return;
    }
        
    char buffer[8192];
    if (fgets(buffer, 8192, stderr_fp) != nullptr) {
      KLOG_ERROR("Biopopen execute failed and stderr: {}", buffer);
    }
    return;
  }
}

void Instance_info::add_node_exporter(const std::string& exporter_port) {
  std::lock_guard<std::mutex> lk(node_mux_);
  int exist_flag = 0;
  for(auto ne : node_exporters_) {
    if(ne->GetPort() == exporter_port) {
      exist_flag = 1;
      break;
    }
  }

  if(!exist_flag) {
    exporter_stat *es = new exporter_stat(exporter_port, "node_exporter");
    node_exporters_.emplace_back(es);
  }
}

void Instance_info::add_mysqld_exporter(const std::string& exporter_port) {
  std::lock_guard<std::mutex> lk(mysqld_mux_);
  int exist_flag = 0;
  for(auto me : mysqld_exporters_) {
    if(me->GetPort() == exporter_port) {
      exist_flag = 1;
      break;
    }
  }

  if(!exist_flag) {
    exporter_stat *es = new exporter_stat(exporter_port, "mysqld_exporter");
    mysqld_exporters_.emplace_back(es);
  }
}

void Instance_info::add_postgres_exporter(const std::string& exporter_port) {
  std::lock_guard<std::mutex> lk(postgres_mux_);
  int exist_flag = 0;
  for(auto pe : postgres_exporters_) {
    if(pe->GetPort() == exporter_port) {
      exist_flag = 1;
      break;
    }
  }

  if(!exist_flag) {
    exporter_stat *es = new exporter_stat(exporter_port, "postgres_exporter");
    postgres_exporters_.emplace_back(es);
  }
}

void Instance_info::remove_node_exporter(const std::string& exporter_port) {
  std::lock_guard<std::mutex> lk(node_mux_);
  for(auto ne : node_exporters_) {
    if(ne->GetPort() == exporter_port) {
      ne->SetDelete(true);
      break;
    }
  }
}

void Instance_info::remove_mysqld_exporter(const std::string& exporter_port) {
  std::lock_guard<std::mutex> lk(mysqld_mux_);
  for(auto me : mysqld_exporters_) {
    if(me->GetPort() == exporter_port) {
      me->SetDelete(true);
      break;
    }
  }
}

void Instance_info::remove_postgres_exporter(const std::string& exporter_port) {
  std::lock_guard<std::mutex> lk(postgres_mux_);
  for(auto pe : postgres_exporters_) {
    if(pe->GetPort() == exporter_port) {
      pe->SetDelete(true);
      break;
    }
  }
}

void Instance_info::keepalive_instance() {
  //std::lock_guard<std::mutex> lock(mutex_instance_);

  std::string install_path;
  /////////////////////////////////////////////////////////////
  // keep alive of meta
  {
    std::lock_guard<std::mutex> lk(meta_mux_);
    for (auto &instance : vec_meta_instance) {
      if (instance->pullup_wait > 0) {
        instance->pullup_wait -= thread_work_interval;
        if (instance->pullup_wait < 0)
          instance->pullup_wait = 0;
        continue;
      }
      if (!instance->manual_stop_pullup)
        continue;

      if (!get_mysql_alive(instance)) {
        KLOG_ERROR("meta_instance no alive, ip={}, port={}",
              local_ip, instance->port);
        instance->pullup_wait = pullup_wait_const;
        Job::get_instance()->job_control_storage(instance->port, 2);
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // keep alive of storage
  {
    std::lock_guard<std::mutex> lk(storage_mux_);
    for (auto &instance : vec_storage_instance) {
      if (instance->pullup_wait > 0) {
        instance->pullup_wait -= thread_work_interval;
        if (instance->pullup_wait < 0)
          instance->pullup_wait = 0;
        continue;
      }

      if (!instance->manual_stop_pullup)
        continue;

      if (!get_mysql_alive(instance)) {
        KLOG_ERROR("storage_instance no alive, ip={}, port={}",
              local_ip, instance->port);
        instance->pullup_wait = pullup_wait_const;
        Job::get_instance()->job_control_storage(instance->port, 2);
      }
    }
  }

  /////////////////////////////////////////////////////////////
  // keep alive of computer
  {
    std::lock_guard<std::mutex> lk(computer_mux_);
    for (auto &instance : vec_computer_instance) {
      if (instance->pullup_wait > 0) {
        instance->pullup_wait -= thread_work_interval;
        if (instance->pullup_wait < 0)
          instance->pullup_wait = 0;
        continue;
      }

      if (!instance->manual_stop_pullup)
        continue;

      if (!get_pgsql_alive(instance)) {
        KLOG_ERROR( "computer_instance no alive, ip={}, port={}",
              local_ip, instance->port);
        instance->pullup_wait = pullup_wait_const;
        Job::get_instance()->job_control_computer(local_ip, instance->port, 2);
      }
    }
  }
}

bool Instance_info::get_path_used(std::string &path, uint64_t &used) {
  bool ret = false;
  FILE *pfd = NULL;

  char *p;
  char buf[256];
  std::string str_cmd;

  str_cmd = "du --max-depth=0 " + path;
  KLOG_INFO("get_path_used str_cmd : {}", str_cmd);

  pfd = popen(str_cmd.c_str(), "r");
  if (!pfd)
    goto end;

  if (fgets(buf, 256, pfd) == NULL)
    goto end;

  p = strchr(buf, 0x09); // asii ht
  if (p == NULL)
    goto end;

  *p = '\0';
  used = atol(buf) >> 10; // Mbyte
  ret = true;

end:
  if (pfd != NULL)
    pclose(pfd);

  return ret;
}

bool Instance_info::get_path_free(std::string &path, uint64_t &free) {
  bool ret = false;
  FILE *pfd = NULL;

  char *p, *q;
  char buf[256];
  std::string str_cmd;

  str_cmd = "df " + path;
  KLOG_INFO("get_path_free str_cmd: {}", str_cmd);

  pfd = popen(str_cmd.c_str(), "r");
  if (!pfd)
    goto end;

  // first line
  if (fgets(buf, 256, pfd) == NULL)
    goto end;

  // second line
  if (fgets(buf, 256, pfd) == NULL)
    goto end;

  // first space
  p = strchr(buf, 0x20);
  if (p == NULL)
    goto end;

  while (*p == 0x20)
    p++;

  // second space
  p = strchr(p, 0x20);
  if (p == NULL)
    goto end;

  while (*p == 0x20)
    p++;

  // third space
  p = strchr(p, 0x20);
  if (p == NULL)
    goto end;

  while (*p == 0x20)
    p++;

  // fourth space
  q = strchr(p, 0x20);
  if (p == NULL)
    goto end;

  *q = '\0';
  free = atol(p) >> 10; // Mbyte
  ret = true;

end:
  if (pfd != NULL)
    pclose(pfd);

  return ret;
}

void Instance_info::trimString(std::string &str) {
  int s = str.find_first_not_of(" ");
  int e = str.find_last_not_of(" ");
  if (s >= e)
    str = "";
  else
    str = str.substr(s, e - s + 1);
}

bool Instance_info::get_vec_path(std::vector<std::string> &vec_path,
                                 std::string &paths) {
  const char *cStart, *cEnd;
  std::string path;

  cStart = paths.c_str();
  while (*cStart != '\0') {
    cEnd = strchr(cStart, ',');
    if (cEnd == NULL) {
      path = std::string(cStart, strlen(cStart));
      trimString(path);
      if (path.length())
        vec_path.emplace_back(path);
      break;
    } else {
      path = std::string(cStart, cEnd - cStart);
      trimString(path);
      if (path.length())
        vec_path.emplace_back(path);
      cStart = cEnd + 1;
    }
  }

  return (vec_path.size() > 0);
}

bool Instance_info::get_path_space(Json::Value &para, std::string &result) {
  bool ret = true;
  uint64_t u_used, u_free;
  std::string path_used;
  std::vector<std::string> vec_paths;

  vec_paths.emplace_back(para["path0"].asString());
  vec_paths.emplace_back(para["path1"].asString());
  vec_paths.emplace_back(para["path2"].asString());
  vec_paths.emplace_back(para["path3"].asString());

  std::vector<std::string> vec_sub_path;
  vec_sub_path.emplace_back("/instance_data/data_dir_path");
  vec_sub_path.emplace_back("/instance_data/log_dir_path");
  vec_sub_path.emplace_back("/instance_data/innodb_log_dir_path");
  vec_sub_path.emplace_back("/instance_data/comp_datadir");

  std::vector<std::string> vec_path_index;
  vec_path_index.emplace_back("path0");
  vec_path_index.emplace_back("path1");
  vec_path_index.emplace_back("path2");
  vec_path_index.emplace_back("path3");

  vec_vec_path_used_free.clear();

  for (int i = 0; i < 4; i++) {
    std::vector<std::string> vec_path;
    if (!get_vec_path(vec_path, vec_paths[i])) {
      if (result.length() > 0)
        result += ";" + vec_paths[i];
      else
        result = vec_paths[i];

      ret = false;
      continue;
    }

    std::vector<Tpye_Path_Used_Free> vec_path_used_free;
    for (auto &path : vec_path) {
      if (!get_path_free(path, u_free)) {
        if (result.length() > 0)
          result += ";" + path;
        else
          result = path;

        ret = false;
        continue;
      } else {
        path_used = path + vec_sub_path[i];
        if (!get_path_used(path_used, u_used)) {
          u_used = 0;
        }
      }

      vec_path_used_free.emplace_back(std::make_tuple(path, u_used, u_free));
    }
    vec_vec_path_used_free.emplace_back(vec_path_used_free);
  }

  // json for return
  Json::Value root;

  if (ret) {
    for (int i = 0; i < 4; i++) {
      Json::Value list;
      for (auto &path_used_free : vec_vec_path_used_free[i]) {
        Json::Value para_json_array;
        para_json_array["path"] = std::get<0>(path_used_free);
        para_json_array["used"] = std::get<1>(path_used_free);
        para_json_array["free"] = std::get<2>(path_used_free);
        list.append(para_json_array);
      }
      root[vec_path_index[i]] = list;
    }

    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    result = writer.write(root);
  }

  return ret;
}

bool Instance_info::check_port_idle(Json::Value &para, std::string &result){
  bool ret = true;
  int port, step;
	FILE* pfd = NULL;
	char buf[256];
	std::string str_cmd;
	std::string str_port;

  port = para["port"].asInt();
  step = para["step"].asInt();

  while(1){
    ret = true;
    for(int i=0; i<step; i++){
      str_port = std::to_string(port);
      str_cmd = "netstat -anp | grep " + str_port;
      KLOG_INFO("check_port_idle str_cmd: {}",str_cmd);

      pfd = popen(str_cmd.c_str(), "r");
      if(!pfd){
        ret = false;
        break;
      }

      while(fgets(buf, 256, pfd)) {
        KLOG_INFO("check_port_idle {}", buf);
        if(strstr(buf, str_port.c_str())) {
          ret = false;
          break;
        }
      }
        
      pclose(pfd);
      pfd = NULL;

      if(!ret)
        break;
    }

    if(ret)
      break;

    port += step;
  }

  if(ret){
    // json for return
    Json::Value root;
    root["port"] = port;
  
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    result = writer.write(root);
  }

  return ret;
}
