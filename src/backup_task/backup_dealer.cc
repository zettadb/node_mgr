/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

//{"cluster_mgr_request_id":"77","job_type":"backup_shard","paras":{"backup_storage":"hdfs://192.168.0.135:9000","cluster_name":"cluster_1657247309_000006","ip":"192.168.0.135","port":55006,"shard_id":"4","shard_name":"shard_2"},"task_spec_info":"backup_mysql_192.168.0.135_55006}
//./util/backup -backuptype=storage -port=55006
//-clustername=cluster_1657247309_000006 -shardname=shard_2
//-HdfsNameNodeService=hdfs://192.168.0.135:9000
//-workdir=/home/summerxwu/kunlunbase/kunlun-node-manager-0.9.2/data
//-logdir=/home/summerxwu/kunlunbase/kunlun-node-manager-0.9.2/log

#include "backup_task/backup_dealer.h"
#include "instance_info.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"

using namespace kunlun;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string local_ip;
extern std::string node_mgr_tmp_data_path;

bool kunlun::BackUpDealer::constructCommand() {

  std::string type_string = json_root_["job_type"].asString();
  Json::Value para_json = json_root_["paras"];
  std::string command_name = para_json["command_name"].asString();
  std::string hdfs_addr = para_json["backup_storage"].asString();
  std::string cluster_name = para_json["cluster_name"].asString();
  std::string ip = para_json["ip"].asString();
  std::string port = para_json["port"].asString();
  std::string shard_id = para_json["shard_id"].asString();
  std::string shard_name = para_json["shard_name"].asString();
  std::string data_dir = kunlun::GetBasePath(get_current_dir_name()) + "/data";
  std::string log_dir = kunlun::GetBasePath(get_current_dir_name()) + "/log/backup";

  if (request_type_ == kunlun::kBackupShardType) {
    execute_command_ = kunlun::string_sprintf(
        "./util/backup -backuptype=storage -port=%s -clustername=%s "
        "-shardname=%s "
        "-HdfsNameNodeService=%s -workdir=%s -logdir=%s",
        port.c_str(), cluster_name.c_str(), shard_name.c_str(),
        hdfs_addr.c_str(), data_dir.c_str(), log_dir.c_str());
  } else {
    execute_command_ = kunlun::string_sprintf(
        "./util/backup -backuptype=compute -port=%s -clustername=%s "
        "-HdfsNameNodeService=%s -workdir=%s -logdir=%s",
        port.c_str(), cluster_name.c_str(), hdfs_addr.c_str(), data_dir.c_str(),
        log_dir.c_str());
  }
  return true;
}

bool BackUpDealer::Deal() {
  bool ret = executeCommand();
  return ret;
}

void BackUpDealer::AppendExtraToResponse(Json::Value &root) {
  // root["Extra"] = local_ip + "_" + json_root_["port"].asString();
  Json::Value para_json = json_root_["paras"];
  std::string port = para_json["port"].asString();
  root["Extra"] = local_ip + "_" + port;
  return;
}
