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

std::string meta_group_seeds;
std::string meta_svr_user;
std::string meta_svr_pwd;
std::string meta_svr_ip;
int64_t meta_svr_port = 0;
std::vector<Tpye_Ip_Port> vec_meta_ip_port;

std::string comp_datadir;
std::string datadir;
std::string logdir;
std::string wal_log_dir;

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
  if ((ret = connet_to_meta_master()) == false)
    goto end;
  if ((ret = regiest_to_meta_master()) == false)
    goto end;

  Instance_info::get_instance()->get_local_instance();
  return 0;

end:
  return ret;
}

bool System::connet_to_meta_master() {
  int retry = 0;

retry_group_seeds:
  if (retry < vec_meta_ip_port.size()) {
    meta_svr_ip = vec_meta_ip_port[retry].first;
    meta_svr_port = vec_meta_ip_port[retry].second;
    retry++;
  } else
    return false;

  kunlun::MysqlConnectionOption options;
  options.autocommit = true;
  options.ip = meta_svr_ip;
  options.port_num = meta_svr_port;
  options.user = meta_svr_user;
  options.password = meta_svr_pwd;

  kunlun::MysqlConnection mysql_conn(options);
  if (!mysql_conn.Connect()) {
    syslog(Logger::ERROR, "connect to metadata db failed: %s",
           mysql_conn.getErr());
    goto retry_group_seeds;
  }

  kunlun::MysqlResult result_set;
  char sql[2048] = {0};
  sprintf(sql,
          "select MEMBER_HOST,MEMBER_PORT from performance_schema.replication_group_members where MEMBER_ROLE='PRIMARY'",
          local_ip.c_str());

  int ret = mysql_conn.ExcuteQuery(sql, &result_set);
  if (ret != 0) {
    syslog(Logger::ERROR, "metadata db query:[%s] failed: %s", sql,
           mysql_conn.getErr());
    goto retry_group_seeds;
  }
  if (result_set.GetResultLinesNum() == 1) {
    meta_svr_ip = result_set[0]["MEMBER_HOST"];
    meta_svr_port = atoi(result_set[0]["MEMBER_PORT"]);
    return true;
  }else
    goto retry_group_seeds;

  return false;
}

bool System::regiest_to_meta_master() {
  kunlun::MysqlConnectionOption options;
  options.autocommit = true;
  options.ip = meta_svr_ip;
  options.port_num = meta_svr_port;
  options.user = meta_svr_user;
  options.password = meta_svr_pwd;

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
          "select id from kunlun_metadata_db.server_nodes where hostaddr = '%s'",
          local_ip.c_str());

  int ret = mysql_conn.ExcuteQuery(sql, &result_set);
  if (ret != 0) {
    syslog(Logger::ERROR, "metadata db query:[%s] failed: %s", sql,
           mysql_conn.getErr());
    return false;
  }
  if (result_set.GetResultLinesNum() > 0) {
    // update the record in the kunlun_metadata_db.server_nodes;
    bzero(sql, sizeof(sql) / sizeof(sql[0]));
    sprintf(
        sql,
        "update kunlun_metadata_db.server_nodes "
        "set nodemgr_port=%d,comp_datadir='%s',"
        "datadir='%s',logdir='%s',wal_log_dir='%s'"
        " where hostaddr='%s'",
        node_mgr_brpc_http_port, comp_datadir.c_str(),
        datadir.c_str(),logdir.c_str(),
        wal_log_dir.c_str(),local_ip.c_str());
    kunlun::MysqlResult rs;
    ret = mysql_conn.ExcuteQuery(sql, &rs);
    if (ret <= 0) {
      syslog(Logger::ERROR, "update:[%s] should affect at least one rows",
              sql);
    }
  } else {
    bzero(sql, sizeof(sql) / sizeof(sql[0]));
    std::string abs_node_mgr_tmp_data_path =
        kunlun::ConvertToAbsolutePath(node_mgr_tmp_data_path.c_str());
    sprintf(sql,
            "insert into kunlun_metadata_db.server_nodes "
            "set hostaddr='%s',nodemgr_port=%d,total_cpu_cores=8,"
            "total_mem=16384,svc_since=current_timestamp(6),"
            "comp_datadir='%s',datadir='%s',logdir='%s',wal_log_dir='%s'",
            local_ip.c_str(),node_mgr_brpc_http_port,comp_datadir.c_str(),
            datadir.c_str(),logdir.c_str(),wal_log_dir.c_str());
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
