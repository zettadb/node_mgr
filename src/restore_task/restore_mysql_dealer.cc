/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "restore_mysql_dealer.h"
#include "instance_info.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"

using namespace kunlun;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string local_ip;
extern std::string log_file_path;
extern std::string program_binaries_path;
extern std::string instance_binaries_path;
extern std::string storage_prog_package_name;

bool MySQLRestoreDealer::make_metacluster_conn_str() {
  kunlun::StorageShardConnection shard_conn(meta_group_seeds, meta_svr_user,
                                            meta_svr_pwd);
  int ret = shard_conn.init();
  if (!ret) {
    KLOG_ERROR("{}", shard_conn.getErr());
    return false;
  }
  auto conn_master = shard_conn.get_master();
  if (conn_master == nullptr) {
    KLOG_ERROR("conn to metadata master is nullptr: {}", shard_conn.getErr());
    return false;
  }
  metacluster_conn_str_ = kunlun::string_sprintf(
      "%s:%s@tcp\\(%s:%u\\)/mysql", meta_svr_user.c_str(), meta_svr_pwd.c_str(),
      conn_master->get_ip().c_str(), conn_master->get_port_num());
  KLOG_INFO("metacluster master connection string: {}", metacluster_conn_str_);
  return true;
}

void MySQLRestoreDealer::AppendExtraToResponse(Json::Value &root) {
  Json::Value para_json = json_root_["paras"];
  std::string port = para_json["port"].asString();
  root["Extra"] = local_ip + "_" + port;
  return;
}

bool MySQLRestoreDealer::fetchMetaInfoFromMetadataCluster() { return true; }

bool MySQLRestoreDealer::constructCommand() {

  Json::Value para_json = json_root_["paras"];
  std::string command_name = para_json["command_name"].asString();
  std::string orig_cluster_name = para_json["orig_clustername"].asString();
  std::string orig_shard_name = para_json["orig_shardname"].asString();
  port_ = para_json["port"].asString();
  restore_time_str_ = para_json["restore_time"].asString();
  hdfs_addr_ = para_json["hdfs_addr"].asString();

  // concat the metacluster_conn_str;
  if (!make_metacluster_conn_str()) {
    return false;
  }

  execute_command_ = kunlun::string_sprintf(
      "./util/%s -restoretype=storage -HdfsNameNodeService=%s "
      "-metaclusterconnstr=%s "
      "-origclustername=%s -origshardname=%s -port=%s -restoretime=\'%s\' "
      "-workdir=../data ",
      command_name.c_str(), hdfs_addr_.c_str(), metacluster_conn_str_.c_str(),
      orig_cluster_name.c_str(), orig_shard_name.c_str(), port_.c_str(),
      restore_time_str_.c_str());

  return true;
}

bool MySQLRestoreDealer::Deal() {

  Instance_info::get_instance()->toggle_auto_pullup(false,
                                                    ::atoi(port_.c_str()));

  bool ret = this->executeCommand();

  Instance_info::get_instance()->toggle_auto_pullup(true,
                                                    ::atoi(port_.c_str()));

  return ret;
}
