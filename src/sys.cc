/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys.h"
#include "config.h"
#include "global.h"
#include "instance_info.h"
#include "job.h"
#include "log.h"
#include "sys_config.h"
#include "thread_manager.h"
#include "zettalib/tool_func.h"
#include <utility>

System *System::m_global_instance = NULL;
extern std::string log_file_path;
extern int64_t node_mgr_brpc_http_port;
extern std::string node_mgr_tmp_data_path;

std::string meta_user;
std::string meta_pwd;
std::string meta_host;
int64_t meta_port;

std::string local_ip;
std::string dev_interface;

System::~System() {
  delete Job::get_instance();
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
int System::create_instance(const std::string &cfg_path) {
  m_global_instance = new System(cfg_path);
  Configs *cfg = Configs::get_instance();
  int ret = 0;

  if ((ret = Logger::create_instance()))
    goto end;
  if ((ret = cfg->process_config_file(cfg_path)))
    goto end;
  if ((ret = Logger::get_instance()->init(log_file_path)) != 0)
    goto end;
  if ((ret = (Thread_manager::get_instance() == NULL)) != 0)
    goto end;
  if ((ret = (Instance_info::get_instance() == NULL)) != 0)
    goto end;
  if ((ret = (Job::get_instance() == NULL)) != 0)
    goto end;
  if ((ret = regiest_to_meta()) == false)
    goto end;

  Instance_info::get_instance()->get_local_instance();
  return 0;

end:
  return ret;
}

bool System::regiest_to_meta() {
  kunlun::MysqlConnectionOption options;
  options.autocommit = true;
  options.ip = meta_host;
  options.port_num = meta_port;
  options.user = meta_user;
  options.password = meta_pwd;

  kunlun::MysqlConnection mysql_conn(options);
  if (!mysql_conn.Connect()) {
    syslog(Logger::ERROR, "connect to metadata db failed: %s",
           mysql_conn.getErr());
    return false;
  }
 // char addr[256] = {0};
 // if (kunlun::GetIpFromInterface(dev_interface.c_str(), addr) != 0) {
 //   return false;
 // }

 // local_ip = addr;
 // syslog(Logger::INFO, "Get addr string '%s' from interface: %s",
 //        local_ip.c_str(), dev_interface.c_str());

  kunlun::MysqlResult result_set;
  char sql[2048] = {0};
  sprintf(sql,
          "select nodemgr_port from kunlun_metadata_db.server_nodes where hostaddr = '%s'",
          local_ip.c_str());

  int ret = mysql_conn.ExcuteQuery(sql, &result_set);
  if (ret != 0) {
    syslog(Logger::ERROR, "metadata db query:[%s] failed: %s", sql,
           mysql_conn.getErr());
    return false;
  }
  if (result_set.GetResultLinesNum() > 0) {
    int lines = result_set.GetResultLinesNum();
    for (int i = 0; i < lines; i++) {
      if(atoi(result_set[i]["nodemgr_port"]) == node_mgr_brpc_http_port)
        continue;
      // update the conflict record in the kunlun_metadata_db.server_nodes;
      bzero(sql, sizeof(sql) / sizeof(sql[0]));
      sprintf(
          sql,
          "update kunlun_metadata_db.server_nodes "
          "set nodemgr_port=%d where hostaddr='%s'",
          node_mgr_brpc_http_port, local_ip.c_str());
      kunlun::MysqlResult rs;
      ret = mysql_conn.ExcuteQuery(sql, &rs);
      if (ret <= 0) {
        syslog(Logger::ERROR, "update:[%s] should affect at least one rows",
               sql);
      }
    }
  } else {
    bzero(sql, sizeof(sql) / sizeof(sql[0]));
    std::string abs_node_mgr_tmp_data_path =
        kunlun::ConvertToAbsolutePath(node_mgr_tmp_data_path.c_str());
    sprintf(sql,
            "insert into kunlun_metadata_db.server_nodes "
            "set hostaddr='%s',nodemgr_port=%d,total_cpu_cores=8,"
            "total_mem=16384,svc_since=current_timestamp(6);",
            local_ip.c_str(), node_mgr_brpc_http_port);
    ret = mysql_conn.ExcuteQuery(sql, &result_set);
    if (ret <= 0) {
      syslog(Logger::ERROR,
            "regiest current nodemanager to metadata db failed: %s",
            mysql_conn.getErr());
      return false;
    }
    sprintf(sql,
            "insert into kunlun_metadata_db.server_nodes_stats "
            "set id=(select id from kunlun_metadata_db.server_nodes where hostaddr='%s'),"
            "comp_datadir_used=0,comp_datadir_avail=0,"
            "datadir_used=0,datadir_avail=0,"
            "wal_log_dir_used=0,wal_log_dir_avail=0,"
            "log_dir_used=0,log_dir_avail=0,avg_network_usage_pct=0",
            local_ip.c_str());
    ret = mysql_conn.ExcuteQuery(sql, &result_set);
    if (ret <= 0) {
      syslog(Logger::ERROR,
            "regiest current nodemanager to metadata db failed: %s",
            mysql_conn.getErr());
      return false;
    }
  }
  syslog(Logger::INFO, "regiest current node_mgr to metadata db successfully");
  return true;
}
