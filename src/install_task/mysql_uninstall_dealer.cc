/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "mysql_uninstall_dealer.h"
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
extern std::string storage_prog_package_name;

bool MySQLUninstallDealer::fetchMetaInfoFromMetadataCluster() {
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
           "and machine_type='storage'",
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

  data_prefix_ = result[0]["datadir"];
  log_prefix_ = result[0]["logdir"];
  wal_prefix_ = result[0]["wal_log_dir"];
  KLOG_INFO("Get {} metainfo: data_prefix({}), log_prefix({}), wal_prefix({})",
            local_ip, data_prefix_, log_prefix_, wal_prefix_);

  return true;
}
bool MySQLUninstallDealer::constructCommand() {

  bool ret = fetchMetaInfoFromMetadataCluster();
  if (!ret) {
    return false;
  }
  Json::Value para_json = json_root_["paras"];
  std::string command_name = para_json["command_name"].asString();
  std::string port = para_json["port"].asString();
  std::string innodb_buffer_size_M =
      para_json["innodb_buffer_size_M"].asString();
  // fetch related directory prefix from Metadata cluster
  std::string install_log = kunlun::string_sprintf(
      "%s/kunlun_install.log",
      kunlun::GetBasePath(program_binaries_path).c_str());
  execute_command_ = kunlun::string_sprintf(
      "%s/%s/dba_tools/%s --port=%s --innodb_buffer_poll_size_M=%s "
      "--datadir_prefix=%s --logdir_prefix=%s --waldir_prefix=%s "
      "--install_prefix=%s/storage --user=%s --bind_address=%s --prog_name=%s "
      ">> %s ",
      program_binaries_path.c_str(), storage_prog_package_name.c_str(),
      command_name.c_str(), port.c_str(), innodb_buffer_size_M.c_str(),
      data_prefix_.c_str(), log_prefix_.c_str(), wal_prefix_.c_str(),
      instance_binaries_path.c_str(),
      kunlun::getCurrentProcessOwnerName().c_str(), local_ip.c_str(),
      storage_prog_package_name.c_str(), install_log.c_str());
  return true;
}
void MySQLUninstallDealer::AppendExtraToResponse(Json::Value &root) {
  Json::Value para_json = json_root_["paras"];
  std::string port = para_json["port"].asString();
  root["Extra"] = local_ip + "_" + port;
  return;
}
bool MySQLUninstallDealer::Deal() {
  Json::Value para_json = json_root_["paras"];
  int port = atoi(para_json["port"].asCString());
  exporter_port_ = para_json["exporter_port"].asString();

  Instance_info::get_instance()->remove_mysqld_exporter(exporter_port_);
  MysqldExporterUninstallDealer mysqld_exporter(exporter_port_);
  bool ret = mysqld_exporter.Deal();
  if(!ret) {
    deal_info_ = mysqld_exporter.getErr();
    deal_success_ = ret;
    return ret;
  }

  Instance_info::get_instance()->remove_storage_instance(local_ip, port);
  
  ret = executeCommand();
  return ret;
}
