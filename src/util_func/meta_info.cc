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
  case "install_mysql"_hash:
    type_enum = kInstallMySQLType;
    break;
  case "uninstall_mysql"_hash:
    type_enum = kUninstallMySQLType;
    break;
  case "install_postgres"_hash:
    type_enum = kInstallPostgresType;
    break;
  case "uninstall_postgres"_hash:
    type_enum = kUninstallPostgresType;
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
  case "backup_compute"_hash:
    type_enum = kBackupComputeType;
    break;
  case "restore_mysql"_hash:
    type_enum = kRestoreMySQLType;
    break;
  case "restore_postgres"_hash:
    type_enum = kRestorePostGresType;
    break;

  case "control_instance"_hash:
    type_enum = kControlInstanceType;
    break;
  case "update_instance"_hash:
    type_enum = kUpdateInstanceType;
    break;
  //case "node_exporter"_hash:
  //  type_enum = kNodeExporterType;
  //  break;
  case "install_node_exporter"_hash:
    type_enum = kInstallNodeExporterType;
    break;
  case "uninstall_node_exporter"_hash:
    type_enum = kUninstallNodeExporterType;
    break;
  case "rebuild_node"_hash:
    type_enum = kRebuildNodeType;
    break;

  case "kill_mysql"_hash:
    type_enum = kKillMysqlType;
    break;
    
#ifndef NDEBUG
  case "node_debug"_hash:
    type_enum = kNodeDebugType;
    break;
#endif

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

bool CheckPidStatus(pid_t pid, int& retcode, char* errinfo) {
  int status = 0;
	if(pid <= 0) {
		snprintf(errinfo, 4096, "pid: %d is not Positive", pid);
		retcode = -1;
		return true;
	}
	
    pid_t check_pid = waitpid (pid , &status , WNOHANG);
    if (check_pid == 0) { //child pid alive
        return false;
    }
    else if (check_pid < 0) {
        if (errno != EINTR) {
            snprintf(errinfo, 4096, "waitpid %d failed: %d,%s" , pid, errno, strerror(errno));
            retcode = -1 * errno;
            return true;
        }
        else { //interrupt
            return false;
        }
    }
    else {  //>0 child pid exit
        if (WIFEXITED(status)) {
            retcode = WEXITSTATUS(status);
        }
        else if (WIFSIGNALED(status)) {
            retcode = WTERMSIG(status);
        }
        else {
            retcode = status;
        }
        return true;
    }
}

std::string getCurrentProcessOwnerName(){
  char buff[1024] = {'\0'};
  getlogin_r(buff,1024);
  return std::string(const_cast<const char *>(buff));
}


std::string TrimResponseInfo(std::string origInfo) {
  std::string result;
  result = trim(origInfo);
  result = StringReplace(result, "'", " ");
  result = StringReplace(result, "\"", " ");
  result = StringReplace(result, "\n", " ");
  result = StringReplace(result, "\t", " ");
  result = StringReplace(result, "\r", " ");
  result = StringReplace(result, "\f", " ");
  result = StringReplace(result, "\v", " ");
  return result;
}

}; // namespace kunlun
