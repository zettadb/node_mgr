/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "error.h"
#include "gflags/gflags.h"
#include "stdio.h"
#include "stdlib.h"
#include "string.h"
#include "zettalib/biodirectpopen.h"
#include "zettalib/config_deal.h"
#include "zettalib/op_log.h"
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include "json/json.h"
#include <string>

#define RUNTIME_BUFF_SIZE_BYTES 2048

DEFINE_string(exclude_gtids, "",
              "The GTID where flashback operation not process");
DEFINE_string(etc_file, "", "MySQL instance config file path");
DEFINE_string(node_mgr_cnf_file, "", "Node_mgr config file path");
DEFINE_string(output_file, "", "mysqlbinlog flashback script output file");
DEFINE_string(general_log_file, "", "kunlun flashback general log file");
DEFINE_string(meta_host, "", "metadata cluster hostaddr(ip)");
DEFINE_uint32(meta_port, 0, "metadata cluster port");
DEFINE_string(meta_user, "", "metadata cluster username");
DEFINE_string(meta_pass, "", "metadata cluster password");
DEFINE_string(previous_master_uuid, "", "previous master uuid");

std::string node_mgr_cnf = "";

using namespace kunlun;

static std::string get_last_gtid(std::string &gtid_set, std::string &uuid) {
  auto container = kunlun::StringTokenize(gtid_set, ",");
  auto iter = container.begin();
  std::string set = "";
  for (; iter != container.end(); iter++) {
    if (kunlun::contains(*iter, uuid)) {
      set = *iter;
      break;
    }
  }
  if (set.empty()) {
    fprintf(stderr, "miss uuid %s in gtid_set %s", uuid.c_str(),
            gtid_set.c_str());
    KLOG_ERROR("miss uuid {} in gtid_set {}", uuid, gtid_set);
    _exit(1);
  }
  auto c1 = kunlun::StringTokenize(set, ":");
  if (c1.size() != 2) {
    fprintf(stderr, "misformat gtid %s", set.c_str());
    KLOG_ERROR("misformat gtid {}", set);
    _exit(1);
  }
  auto c2 = kunlun::StringTokenize(c1[1], "-");
  if (c2.size() == 2) {
    return c1[0] + ":" + c2[1];
  } else if (c2.size() == 1) {
    return c1[0] + ":" + c2[0];
  } else {
    fprintf(stderr, "misformat gtid %s", set.c_str());
    KLOG_ERROR("misformat gtid {}", set);
  }
  _exit(1);
}

static std::string get_uuid_from_meta(MysqlConnection *conn,
                                      std::string ip_port) {
  if (!FLAGS_previous_master_uuid.empty()) {
    return FLAGS_previous_master_uuid;
  }
  char sql[1024] = {'\0'};
  snprintf(sql, 1024,
           "select master_uuid from kunlun_metadata_db.node_map_master where "
           "node_host = '%s' limit 1",
           ip_port.c_str());
  MysqlResult result;
  int ret = conn->ExcuteQuery(sql, &result);
  if (ret != 0) {
    fprintf(stderr, "get uuid from metadata failed: %s\n", conn->getErr());
    KLOG_ERROR("get uuid from metadata failed: {}\n", conn->getErr());
    _exit(1);
  }
  if (result.GetResultLinesNum() != 1) {
    fprintf(stderr,
            "get uuid from metadata failed: result size is not 1,sql:%s\n",
            sql);
    KLOG_ERROR("get uuid from metadata failed: result size is not 1,sql:{}\n",
               sql);
    _exit(1);
  }
  return result[0]["master_uuid"];
}

int main(int argc, char *argv[]) {

  google::ParseCommandLineFlags(&argc, &argv, false);
  if (argc < 6) {
    fprintf(
        stderr,
        "Usage: %s --exclude_gtids='gtid-set' "
        "--etc_file='path/to/etcfile' --node_mgr_cnf='path/to/nodemgr/conf' "
        "--output_file='path/to/result/file' "
        "--general_log_file='path/of/log/file'\n"
        "Optional parameter:\n"
        "\t--meta_host='ip'\n \t--meta_port=port_num\n "
        "\t--meta_user='user_name'\n "
        "\t--meta_pass='pwd'\n"
        "\t--previous_master_uuid='uuid'\n"
        "%s will parse the node_mgr config file to fetch metadata cluster \n"
        "parameter for connection, if node_mgr config file is invalid, --meta* "
        "parameter is needed\n",
        argv[0], argv[0]);
    exit(1);
  }
  node_mgr_cnf = FLAGS_node_mgr_cnf_file;

  std::string general_log_path = kunlun::ConvertToAbsolutePath(
      kunlun::GetBasePath(FLAGS_general_log_file.c_str()).c_str());
  auto tokevec = kunlun::StringTokenize(FLAGS_general_log_file, "/");
  size_t size = tokevec.size();
  kunlun::Op_Log::getInstance()->init(general_log_path, tokevec[size - 1], 500);

  std::string meta_user, meta_pass, meta_host;
  // parse the etc_file
  kunlun::MysqlCfgFileParser etc_parser(FLAGS_etc_file.c_str());
  bool ret = etc_parser.Parse();
  if (!ret) {
    fprintf(stderr, "etc_file parse failed, %s", etc_parser.getErr());
    KLOG_ERROR("etc_file parse failed: {}", etc_parser.getErr());
    _exit(1);
  }

  // parse the node_mgr config file
  kunlun::KConfig cnf_parser(node_mgr_cnf);
  ret = cnf_parser.parse();
  if (ret) {
    meta_user = cnf_parser.get_config("meta_user");
    meta_pass = cnf_parser.get_config("meta_pwd");
    meta_host = cnf_parser.get_config("meta_group_seeds");
  } else {
    meta_host = FLAGS_meta_host + "_" + std::to_string(FLAGS_meta_port);
    meta_user = FLAGS_meta_user;
    meta_pass = FLAGS_meta_pass;
  }

  std::string binlog_dir = etc_parser.get_cfg_value("log-bin");
  binlog_dir = kunlun::GetBasePath(binlog_dir);
  if (binlog_dir.empty()) {
    fprintf(stderr, "can't get binlog dir from etc file %s",
            FLAGS_etc_file.c_str());
    KLOG_ERROR("can't get binlog dir from etc file {}", FLAGS_etc_file);
    _exit(1);
  }
  // get mysql instance base_dir from plugin-dir config
  std::string plugin_dir = etc_parser.get_cfg_value("plugin-dir");
  if (plugin_dir.empty()) {
    fprintf(stderr,
            "can't get plugin-dir from etc file %s, we need this value "
            "to evaluate the instance base dir",
            FLAGS_etc_file.c_str());
    KLOG_ERROR("can't get plugin-dir from etc file {}, we need this value "
               "to evaluate the instance base dir",
               FLAGS_etc_file);
    _exit(1);
  }
  // plugin-dir=base_dir/lib/plugin
  // so we get basepath twice
  plugin_dir = kunlun::GetBasePath(plugin_dir);
  std::string mysql_base_dir = kunlun::GetBasePath(plugin_dir);

  std::string mysqld_socket_file = etc_parser.get_cfg_value("socket");
  if (mysqld_socket_file.empty()) {
    fprintf(stderr, "can't get socket file from etch file %s",
            FLAGS_etc_file.c_str());
    KLOG_ERROR("can't get socket file from etch file {}", FLAGS_etc_file);
    _exit(1);
  }
  std::string bind_address = etc_parser.get_cfg_value("bind_address");
  std::string mysqld_port = etc_parser.get_cfg_value("port");

  std::string binlog_index_file = binlog_dir + "/binlog.index";
  std::string mysql_client = mysql_base_dir + "/bin/mysql";
  std::string mysql_binlog_util = mysql_base_dir + "/bin/mysqlbinlog";

  // get previous master uuid from metacluster
  kunlun::StorageShardConnection meta_conn(meta_host, meta_user, meta_pass);
  ret = meta_conn.init();
  if (!ret) {
    fprintf(stderr, "Init connection to metadatacluster failed: %s",
            meta_conn.getErr());
    KLOG_ERROR("Init connection to metadatacluster failed: {}",
               meta_conn.getErr());
    _exit(-1);
  }
  std::string uuid = get_uuid_from_meta(meta_conn.get_master(),
                                        bind_address + "_" + mysqld_port);
  if (uuid.empty()) {
    fprintf(stderr, "get empty uuid from metadata");
    KLOG_ERROR("get empty uuid from metadata");
    _exit(1);
  }
  std::string last_gtid = get_last_gtid(FLAGS_exclude_gtids, uuid);
  KLOG_INFO("last_gtid fetched: {}", last_gtid);

  // 1. get the filepos based on the given gtid string.
  char cmd_buff[RUNTIME_BUFF_SIZE_BYTES] = {'\0'};
  char output_buff[RUNTIME_BUFF_SIZE_BYTES] = {'\0'};
  snprintf(cmd_buff, RUNTIME_BUFF_SIZE_BYTES,
           "%s --binlog-index-file=%s --gtid-to-filepos='%s'",
           mysql_binlog_util.c_str(), binlog_index_file.c_str(),
           last_gtid.c_str());
  kunlun::BiodirectPopen *biopopen_1 = new kunlun::BiodirectPopen(cmd_buff);
  ret = biopopen_1->Launch("r");
  if (!ret) {
    fprintf(stderr,
            "Error occur during gtid-to-filepos, Cmd: %s, error-info: %s\n",
            cmd_buff, biopopen_1->getErr());
    KLOG_ERROR("Error occur during gtid-to-filepos, Cmd: {}, error-info: {}\n",
               cmd_buff, biopopen_1->getErr());
    _exit(1);
  }

  FILE *fp = biopopen_1->getReadStdOutFp();
  if (!fp) {
    fprintf(stderr, "Read output result from mysqlbinlog gtid-to-filepos "
                    "failed, STDOUT file pointer is invalid\n");
    KLOG_ERROR("Read output result from mysqlbinlog gtid-to-filepos "
               "failed, STDOUT file pointer is invalid\n");
    _exit(1);
  }

  // only one line
  if (!fgets(output_buff, RUNTIME_BUFF_SIZE_BYTES, fp)) {
    fprintf(stderr,
            "Read output result from mysqlbinlog gtid-to-filepos "
            "failed, fgets() failed: %s\n",
            strerror(errno));
    KLOG_ERROR("Read output result from mysqlbinlog gtid-to-filepos "
               "failed, fgets() failed: {}\n",
               strerror(errno));
  }
  KLOG_INFO("Get File-pos info: {}", output_buff);
  Json::Reader reader;
  Json::Value root;
  ret = reader.parse(std::string(output_buff), root);
  if (!ret) {
    fprintf(stderr, "%s\n", reader.getFormattedErrorMessages().c_str());
    KLOG_ERROR("{}", reader.getFormattedErrorMessages());
    _exit(1);
  }

  std::string filename = root["filename"].asString();
  std::string pos = root["pos"].asString();
  // mysqlbinlog: [Warning] option 'start-position': unsigned value 0 adjusted
  // to 4.
  if (pos == "0") {
    pos = "4";
  }

  bzero((void *)cmd_buff, RUNTIME_BUFF_SIZE_BYTES);
  bzero((void *)output_buff, RUNTIME_BUFF_SIZE_BYTES);
  delete biopopen_1;

  // 2. start flashback operation
  char turn_off_read_only[RUNTIME_BUFF_SIZE_BYTES] = {'\0'};
  snprintf(turn_off_read_only, RUNTIME_BUFF_SIZE_BYTES,
           "%s -S %s -uroot -proot -e \"set global super_read_only=off\" 2>&1 | grep -v \"Warning\" ",
           mysql_client.c_str(), mysqld_socket_file.c_str());
  kunlun::BiodirectPopen *biopopen_2_3 = new kunlun::BiodirectPopen(turn_off_read_only);
  KLOG_INFO("Exec: {}",turn_off_read_only);
  ret = biopopen_2_3->Launch("r");
  if (!ret || biopopen_2_3->HasErrorOutput()) {
    fprintf(stderr, "Error occur during flashback, Cmd: %s, error-info: %s\n",
            cmd_buff, biopopen_2_3->getErr());
    KLOG_ERROR("Error occur during flashback, Cmd: {}, error-info: {}\n",
               cmd_buff, biopopen_2_3->getErr());
    _exit(1);
  }

  char mysql_client_buff[RUNTIME_BUFF_SIZE_BYTES] = {'\0'};
  snprintf(mysql_client_buff, RUNTIME_BUFF_SIZE_BYTES,
           "%s -S %s -uroot -proot < %s 2>&1", mysql_client.c_str(),
           mysqld_socket_file.c_str(), FLAGS_output_file.c_str());
  snprintf(cmd_buff, RUNTIME_BUFF_SIZE_BYTES,
           "%s --start-position=%s --flashback --skip-gtids -vv %s > %s",
           mysql_binlog_util.c_str(), pos.c_str(), filename.c_str(),
           FLAGS_output_file.c_str());
  kunlun::BiodirectPopen *biopopen_2_1 = new kunlun::BiodirectPopen(cmd_buff);
  KLOG_INFO("Exec: {}", cmd_buff);
  ret = biopopen_2_1->Launch("r");
  if (!ret || biopopen_2_1->HasErrorOutput()) {
    fprintf(stderr, "Error occur during flashback, Cmd: %s, error-info: %s\n",
            cmd_buff, biopopen_2_1->getErr());
    KLOG_ERROR("Error occur during flashback, Cmd: {}, error-info: {}\n",
               cmd_buff, biopopen_2_1->getErr());
    _exit(1);
  }

  kunlun::BiodirectPopen *biopopen_2_2 =
      new kunlun::BiodirectPopen(mysql_client_buff);
  KLOG_INFO("Exec: {}", mysql_client_buff);
  ret = biopopen_2_2->Launch("r");
  if (!ret || biopopen_2_2->HasErrorOutput()) {
    fprintf(stderr,
            "Error occur during applying flashback SQL script, Cmd: %s, "
            "error-info: %s\n",
            mysql_client_buff, biopopen_2_2->getErr());
    KLOG_ERROR("Error occur during applying flashback SQL script, Cmd: {}, "
               "error-info: {}\n",
               mysql_client_buff, biopopen_2_2->getErr());
    //_exit(1);
  }

  char turn_on_read_only[RUNTIME_BUFF_SIZE_BYTES] = {'\0'};
  snprintf(turn_on_read_only, RUNTIME_BUFF_SIZE_BYTES,
           "%s -S %s -uroot -proot -e \"set global super_read_only=on\" 2>&1 | grep -v \"Warning\" ",
           mysql_client.c_str(),mysqld_socket_file.c_str());
  kunlun::BiodirectPopen *biopopen_2_4 = new kunlun::BiodirectPopen(turn_on_read_only);
  KLOG_INFO("Exec: {}", turn_on_read_only);
  ret = biopopen_2_4->Launch("r");
  if (!ret || biopopen_2_4->HasErrorOutput()) {
    fprintf(stderr, "Error occur during flashback, Cmd: %s, error-info: %s\n",
            cmd_buff, biopopen_2_4->getErr());
    KLOG_ERROR("Error occur during flashback, Cmd: {}, error-info: {}\n",
               cmd_buff, biopopen_2_4->getErr());
    _exit(1);
  }
  delete biopopen_2_1;
  delete biopopen_2_2;
  delete biopopen_2_3;
  delete biopopen_2_4;

  // 3. truncate binlog file
  char binlog_truncate_cmd_buff[RUNTIME_BUFF_SIZE_BYTES];
  snprintf(binlog_truncate_cmd_buff, RUNTIME_BUFF_SIZE_BYTES,
           "%s --binlog-index-file=%s --truncate-file-by-gtid='%s'",
           mysql_binlog_util.c_str(), binlog_index_file.c_str(),
           last_gtid.c_str());
  kunlun::BiodirectPopen *biopopen_3 =
      new kunlun::BiodirectPopen(binlog_truncate_cmd_buff);
  KLOG_INFO("Exec: {}", binlog_truncate_cmd_buff);
  ret = biopopen_3->Launch("r");
  // if (!ret || biopopen_3->HasErrorOutput()) {
  if (!ret) {
    fprintf(
        stderr,
        "Error occur during truncate binlog file, Cmd: %s, error-info: %s\n",
        binlog_truncate_cmd_buff, biopopen_3->getErr());
    KLOG_ERROR(
        "Error occur during truncate binlog file, Cmd: {}, error-info: {}\n",
        binlog_truncate_cmd_buff, biopopen_3->getErr());
    _exit(1);
  }
  KLOG_INFO("Finish !");
  _exit(0);
}
