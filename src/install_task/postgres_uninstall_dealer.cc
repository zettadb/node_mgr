/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "postgres_uninstall_dealer.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include "instance_info.h"
#include "exporter_uninstall_dealer.h"

using namespace kunlun;
extern std::string meta_group_seeds;
extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string local_ip;
extern std::string log_file_path;
extern std::string program_binaries_path;
extern std::string instance_binaries_path;
extern std::string computer_prog_package_name;

bool PostgresUninstallDealer::fetchMetaInfoFromMetadataCluster() {
  // fetch install_prefix; data_prefix; log_prefix; wal_prefix

  kunlun::StorageShardConnection conn(meta_group_seeds, meta_svr_user,
                                      meta_svr_pwd);
  bool ret = conn.init();
  if (!ret) {
    KLOG_ERROR("connect to metadatacluster failed: {}", conn.getErr());
    setErr("connect to metadatacluster failed: %s", conn.getErr());
    return false;
  }
  kunlun::MysqlConnection *cp = conn.get_master();
  char sql[2048] = {'\0'};
  snprintf(sql, 2048,
           "select * from kunlun_metadata_db.server_nodes where hostaddr='%s' "
           "and machine_type='computer'",
           local_ip.c_str());
  kunlun::MysqlResult result;
  int ret0 = cp->ExcuteQuery(sql, &result);
  if (ret0 < 0) {
    KLOG_ERROR("fetch metainfo from metadatacluster failed: {}", cp->getErr());
    setErr("fetch metainfo from metadatacluster failed: %s", cp->getErr());
    return false;
  }
  if (result.GetResultLinesNum() != 1) {
    KLOG_ERROR(
        "Can not get unique result from server_nodes where hostaddr is {}",
        local_ip);
    setErr("Can not get unique result from server_nodes where hostaddr is %s",
           local_ip.c_str());
    return false;
  }

  data_prefix_ = result[0]["comp_datadir"];
  KLOG_INFO("Get {} metainfo: comp_data_prefix({})", local_ip, data_prefix_);

  return true;
}
bool PostgresUninstallDealer::constructCommand() {

  bool ret = fetchMetaInfoFromMetadataCluster();
  if (!ret) {
    return false;
  }
  Json::Value para_json = json_root_["paras"];
  std::string command_name = para_json["command_name"].asString();
  std::string pg_port = para_json["pg_protocal_port"].asString();
  std::string mysql_port = para_json["mysql_protocal_port"].asString();
  std::string comp_node_id = para_json["compute_node_id"].asString();
  std::string init_user = para_json["user"].asString();
  std::string init_pass = para_json["password"].asString();
  std::string bind_address = para_json["bind_address"].asString();

  // fetch related directory prefix from Metadata cluster
  std::string install_log = kunlun::string_sprintf(
      "%s/kunlun_install.log",
      kunlun::GetBasePath(program_binaries_path).c_str());
  execute_command_ = kunlun::string_sprintf(
      "%s/%s/scripts/%s --pg_protocal_port=%s --mysql_protocal_port=%s "
      "--compute_node_id=%s --datadir_prefix=%s --install_prefix=%s/computer "
      "--user=%s "
      "--password=%s --bind_address=%s --prog_name=%s >> %s ",
      program_binaries_path.c_str(), computer_prog_package_name.c_str(),
      command_name.c_str(), pg_port.c_str(), mysql_port.c_str(),
      comp_node_id.c_str(), data_prefix_.c_str(),
      instance_binaries_path.c_str(), init_user.c_str(), init_pass.c_str(),
      bind_address.c_str(), computer_prog_package_name.c_str(),
      install_log.c_str());
  return true;
}
bool PostgresUninstallDealer::Deal() {
  Json::Value para_json = json_root_["paras"];
  int pg_port = atoi(para_json["pg_protocal_port"].asCString());
  exporter_port_ = para_json["exporter_port"].asString();

  Instance_info::get_instance()->remove_postgres_exporter(exporter_port_);
  PostgresExporterUninstallDealer postgres_exporter(exporter_port_);
  bool ret = postgres_exporter.Deal();
  if(!ret) {
    deal_info_ = postgres_exporter.getErr();
    deal_success_ = ret;
    return ret;
  }

  Instance_info::get_instance()->remove_computer_instance(local_ip, pg_port);

  ret = executeCommand();
  if (!ret) {
    KLOG_ERROR("execute cmd failed: {}", getErr());
  } else {
    KLOG_INFO("execute cmd success: {}", execute_command_);
  }
  return ret;
}

void PostgresUninstallDealer::AppendExtraToResponse(Json::Value &root) {
  Json::Value para_json = json_root_["paras"];
  std::string pg_port = para_json["pg_protocal_port"].asString();
  std::string bind_address = para_json["bind_address"].asString();
  root["Extra"] = bind_address + "_" + pg_port;
  return;
}
