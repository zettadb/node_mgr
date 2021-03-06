/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "meta_info.h"
#include "arpa/inet.h"
#include <string>
#include <time.h>
#include <unistd.h>

namespace kunlun {

typedef std::uint64_t hash_t;
constexpr hash_t prime = 0x100000001B3ull;
constexpr hash_t basis = 0xCBF29CE484222325ull;

static hash_t hash_(char const *str) {
  hash_t ret{basis};
  while (*str) {
    ret ^= *str;
    ret *= prime;
    str++;
  }
  return ret;
}

constexpr hash_t hash_compile_time(char const *str, hash_t last_value = basis) {
  return *str ? hash_compile_time(str + 1, (*str ^ last_value) * prime)
              : last_value;
}

constexpr unsigned long long operator"" _hash(char const *p, size_t) {
  return hash_compile_time(p);
}

ClusterRequestTypes GetReqTypeEnumByStr(const char *type_str) {
  ClusterRequestTypes type_enum = kRequestTypeUndefined;
  switch (hash_(type_str)) {
  case "ping_pong"_hash:
    type_enum = kPingPongType;
    break;
  case "execute_command"_hash:
    type_enum = kExecuteCommandType;
    break;
  case "get_paths_space"_hash:
    type_enum = kGetPathsSpaceType;
    break;
  case "check_port_idle"_hash:
    type_enum = kCheckPortIdleType;
    break;
  case "install_storage"_hash:
    type_enum = kInstallStorageType;
    break;
  case "install_computer"_hash:
    type_enum = kInstallComputerType;
    break;
  case "delete_storage"_hash:
    type_enum = kDeleteStorageType;
    break;
  case "delete_computer"_hash:
    type_enum = kDeleteComputerType;
    break;
  case "backup_shard"_hash:
    type_enum = kBackupShardType;
    break;
  case "restore_storage"_hash:
    type_enum = kRestoreStorageType;
    break;
  case "restore_computer"_hash:
    type_enum = kRestoreComputerType;
    break;

  case "control_instance"_hash:
    type_enum = kControlInstanceType;
    break;
  case "update_instance"_hash:
    type_enum = kUpdateInstanceType;
    break;
  case "node_exporter"_hash:
    type_enum = kNodeExporterType;
    break;

  // addtional type convert should add above
  default:
    type_enum = kRequestTypeMax;
  }
  return type_enum;
}

bool RecognizedRequestType(ClusterRequestTypes type) {
  return type < kRequestTypeMax && type > kRequestTypeUndefined;
}

bool RecognizedJobTypeStr(std::string &job_type) {
  ClusterRequestTypes type = GetReqTypeEnumByStr(job_type.c_str());
  return RecognizedRequestType(type);
}

std::string GenerateNewClusterIdStr(MysqlConnection *conn) { return ""; }

bool ValidNetWorkAddr(const char *addr) {
  char *dst[512];
  int ret = inet_pton(AF_INET, addr, dst);
  return ret == 1 ? true : false;
}

std::string FetchNodemgrTmpDataPath(MysqlConnection *meta, const char *ip) {
  char sql[2048] = {'\0'};
  sprintf(sql,
          "select nodemgr_tmp_data_abs_path as path"
          " from kunlun_metadata_db.server_nodes"
          " where hostaddr='%s'",
          ip);
  kunlun::MysqlResult result;
  int ret = meta->ExcuteQuery(sql, &result);
  if (ret < 0) {
    return "";
  }
  return result[0]["path"];
}

int64_t FetchNodeMgrListenPort(MysqlConnection *meta, const char *ip) {
  char sql[2048] = {'\0'};
  sprintf(sql,
          "select nodemgr_port as port from kunlun_metadata_db.server_nodes "
          "where hostaddr='%s'",
          ip);

  kunlun::MysqlResult result;
  int ret = meta->ExcuteQuery(sql, &result);
  if (ret < 0) {
    return -1;
  }
  return ::atoi(result[0]["port"]);
}

}; // namespace kunlun
