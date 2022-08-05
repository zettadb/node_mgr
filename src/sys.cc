/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "sys.h"
#include "unistd.h"
#include "config.h"
#include "global.h"
#include "instance_info.h"
#include "job.h"
//#include "log.h"
#include "zettalib/op_log.h"
#include "sys_config.h"
#include "thread_manager.h"
#include "zettalib/tool_func.h"
#include <utility>

System *System::m_global_instance = NULL;
extern int64_t node_mgr_brpc_http_port;
extern int64_t node_mgr_tcp_port;
extern int64_t prometheus_port_start;
extern std::string node_mgr_tmp_data_path;
std::string log_file_path;
int64_t max_log_file_size;

std::string meta_group_seeds;
std::string meta_svr_user;
std::string meta_svr_pwd;
std::string meta_svr_ip;
int64_t meta_svr_port = 0;
std::vector<Tpye_Ip_Port> vec_meta_ip_port;

std::string local_ip;
std::string dev_interface;

System::~System()
{
  delete Job::get_instance();
  delete Instance_info::get_instance();
  delete Configs::get_instance();
  //delete Logger::get_instance();

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
int System::create_instance(const std::string &cfg_path)
{
  m_global_instance = new System(cfg_path);
  Configs *cfg = Configs::get_instance();
  int ret = 0;
  
  if((ret = cfg->process_config_file(cfg_path))) {
    return ret;
  }

  std::string log_path = log_file_path.substr(0, log_file_path.rfind("/"));
  std::string log_prefix = log_file_path.substr(log_file_path.rfind("/")+1);
  Op_Log::getInstance()->init(log_path, log_prefix, max_log_file_size);
  //if ((ret = Logger::create_instance()))
  //  goto end;
  //if ((ret = Logger::get_instance()->init(log_file_path)) != 0)
  //  goto end;
  if ((ret = (Thread_manager::get_instance() == NULL)) != 0)
    goto end;
  if ((ret = (Instance_info::get_instance() == NULL)) != 0)
    goto end;
  if ((ret = (Job::get_instance() == NULL)) != 0)
    goto end;
  if ((ret = connet_to_meta_master()) == false){
    KLOG_ERROR("connect to metadata error");
    goto end;
  }
  if ((ret = regiest_to_meta_master()) == false){
    KLOG_ERROR( "regiest to metadata error");
    goto end;
  }

  return 0;

end:
  return ret;
}

bool System::connet_to_meta_master()
{
  int retry = 0;
  std::string meta_ha_mode;

  if (vec_meta_ip_port.size() == 0)
    return false;

  if (vec_meta_ip_port.size() == 1)
  {
    meta_svr_ip = vec_meta_ip_port[0].first;
    meta_svr_port = vec_meta_ip_port[0].second;
    return true;
  }

retry_group_seeds:
  if (retry < vec_meta_ip_port.size())
  {
    meta_svr_ip = vec_meta_ip_port[retry].first;
    meta_svr_port = vec_meta_ip_port[retry].second;
    retry++;
  }
  else
    return false;

  {
    kunlun::MysqlConnectionOption options;
    options.autocommit = true;
    options.ip = meta_svr_ip;
    options.port_num = meta_svr_port;
    options.user = meta_svr_user;
    options.password = meta_svr_pwd;

    kunlun::MysqlConnection mysql_conn(options);
    if (!mysql_conn.Connect())
    {
      KLOG_ERROR("connect to metadata db failed: {}",
             mysql_conn.getErr());
      goto retry_group_seeds;
    }

    kunlun::MysqlResult result_set;
    char sql[2048] = {0};
    sprintf(sql,
            "select value from kunlun_metadata_db.global_configuration where name='meta_ha_mode'");
    int ret = mysql_conn.ExcuteQuery(sql, &result_set);
    if (ret != 0)
    {
      KLOG_ERROR("metadata db query:[{}] failed: {}", sql,
             mysql_conn.getErr());
      goto retry_group_seeds;
    }
    if (result_set.GetResultLinesNum() == 1)
    {
      meta_ha_mode = result_set[0]["value"];
    }
    else
      goto retry_group_seeds;

    result_set.Clean();
    if (meta_ha_mode == "mgr")
    {
      sprintf(sql,
              "select MEMBER_HOST,MEMBER_PORT from performance_schema.replication_group_members where MEMBER_ROLE='PRIMARY'");

      ret = mysql_conn.ExcuteQuery(sql, &result_set);
      if (ret != 0)
      {
        KLOG_ERROR( "metadata db query:[{}] failed: {}", sql,
               mysql_conn.getErr());
        goto retry_group_seeds;
      }
      if (result_set.GetResultLinesNum() == 1)
      {
        meta_svr_ip = result_set[0]["MEMBER_HOST"];
        meta_svr_port = atoi(result_set[0]["MEMBER_PORT"]);
        return true;
      }
      else
        goto retry_group_seeds;
    }
    else if (meta_ha_mode == "rbr")
    {
      sprintf(sql,
              "select hostaddr,port from kunlun_metadata_db.meta_db_nodes where member_state='source'");

      ret = mysql_conn.ExcuteQuery(sql, &result_set);
      if (ret != 0)
      {
        KLOG_ERROR("metadata db query:[{}] failed: %s", sql,
               mysql_conn.getErr());
        goto retry_group_seeds;
      }
      if (result_set.GetResultLinesNum() == 1)
      {
        meta_svr_ip = result_set[0]["hostaddr"];
        meta_svr_port = atoi(result_set[0]["port"]);
        return true;
      }
      else
        goto retry_group_seeds;
    }
    else if (meta_ha_mode == "no_rep")
    {
      return true;
    }
    else
    {
      KLOG_ERROR("meta_ha_mode: {} no support!", meta_ha_mode);
    }
  }

  return false;
}

bool System::regiest_to_meta_master()
{
  kunlun::MysqlConnectionOption options;
  options.autocommit = true;
  options.ip = meta_svr_ip;
  options.port_num = meta_svr_port;
  options.user = meta_svr_user;
  options.password = meta_svr_pwd;

  kunlun::MysqlConnection mysql_conn(options);
  if (!mysql_conn.Connect())
  {
    KLOG_ERROR( "connect to metadata db failed: {}",
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
          "select nodemgr_port, machine_type from kunlun_metadata_db.server_nodes where hostaddr = '%s'",
          local_ip.c_str());

  int ret = mysql_conn.ExcuteQuery(sql, &result_set);
  if (ret != 0)
  {
    KLOG_ERROR( "metadata db query:[{}] failed: {}", sql,
           mysql_conn.getErr());
    return false;
  }
  if (result_set.GetResultLinesNum() > 0)
  {
    int nrows = result_set.GetResultLinesNum();
    int node_exporter_flag = 0;
    for(int i=0; i<nrows; i++) {
      std::string machine_type = result_set[i]["machine_type"];
      if(machine_type != "NULL") {
        node_exporter_flag = 1;
        break;
      }
    }
    if(node_exporter_flag)
      Instance_info::get_instance()->add_node_exporter(std::to_string(prometheus_port_start));

    // update the record in the kunlun_metadata_db.server_nodes;
    bzero(sql, sizeof(sql) / sizeof(sql[0]));
    sprintf(
        sql,
        "update kunlun_metadata_db.server_nodes "
        "set nodemgr_port=%ld, nodemgr_bin_path='%s', nodemgr_tcp_port=%ld, nodemgr_prometheus_port=%ld where hostaddr='%s'",
        node_mgr_brpc_http_port,(const char *)get_current_dir_name(), node_mgr_tcp_port, prometheus_port_start, local_ip.c_str());
    kunlun::MysqlResult rs;
    ret = mysql_conn.ExcuteQuery(sql, &rs);
    if (ret <= 0)
    {
      KLOG_ERROR( "update:[{}] should affect at least one rows, Errorinfo: {}",
             sql,mysql_conn.getErr());
    }
  }
  else
  {
    bzero(sql, sizeof(sql) / sizeof(sql[0]));
    std::string abs_node_mgr_tmp_data_path =
        kunlun::ConvertToAbsolutePath(node_mgr_tmp_data_path.c_str());
    sprintf(sql,
            "insert into kunlun_metadata_db.server_nodes "
            "set hostaddr='%s',nodemgr_port=%ld,nodemgr_bin_path='%s', nodemgr_tcp_port=%ld, nodemgr_prometheus_port=%ld",
            local_ip.c_str(), node_mgr_brpc_http_port,(const char *)get_current_dir_name(), node_mgr_tcp_port, prometheus_port_start);
    ret = mysql_conn.ExcuteQuery(sql, &result_set);
    if (ret <= 0)
    {
      KLOG_ERROR(
             "regiest current nodemanager to metadata db failed: {}",
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
    if (ret <= 0)
    {
      KLOG_ERROR(
             "regiest current nodemanager to metadata db failed: {}",
             mysql_conn.getErr());
      return false;
    }
  }
  KLOG_INFO( "regiest current node_mgr to metadata db successfully");
  return true;
}

bool System::GetClusterMgrFromMeta(std::string &cluster_mgr)
{
  kunlun::MysqlConnectionOption options;
  options.autocommit = true;
  options.ip = meta_svr_ip;
  options.port_num = meta_svr_port;
  options.user = meta_svr_user;
  options.password = meta_svr_pwd;
  options.database = "kunlun_metadata_db";

  kunlun::MysqlConnection mysql_conn(options);
  if (!mysql_conn.Connect())
  {
    KLOG_ERROR("connect to metadata db failed: {}",
           mysql_conn.getErr());
    return false;
  }

  kunlun::MysqlResult res;
  char sql[2048] = {0};
  sprintf(sql,
          "select hostaddr, port, member_state from cluster_mgr_nodes");

  int ret = mysql_conn.ExcuteQuery(sql, &res);
  if (ret != 0)
  {
    KLOG_ERROR("metadata db query:[{}] failed: {}", sql,
           mysql_conn.getErr());
    return false;
  }

  int num_rows = res.GetResultLinesNum();
  for (int i = 0; i < num_rows; i++)
  {
    std::string member_state = res[i]["member_state"];
    if (member_state == "source")
    {
      std::string hostaddr = res[i]["hostaddr"];
      std::string port = res[i]["port"];
      cluster_mgr = hostaddr + "_" + port;
      return true;
    }
  }
  return false;
}

void System::keepalive_exporter() {
  std::thread thd(System::loop_exporter);
  thd.detach();
}

void System::loop_exporter() {
  while(System::get_instance()->get_auto_pullup_working()) {
    Instance_info::get_instance()->keepalive_exporter();
    sleep(10);
  }
  return;
}