/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "instance_info.h"
#include "global.h"
#include "job.h"
#include "log.h"
#include "mysql_conn.h"
#include "pgsql_conn.h"
#include "json/json.h"
#include "sys.h"
#include <arpa/inet.h>
#include <errno.h>
#include <iostream>
#include <net/if.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

Instance_info *Instance_info::m_inst = NULL;

int64_t pullup_wait_const = 9;

int64_t stmt_retries = 3;
int64_t stmt_retry_interval_ms = 500;

extern std::string cluster_mgr_http_ip;
extern int64_t cluster_mgr_http_port;
extern int64_t thread_work_interval;

extern std::string program_binaries_path;
extern std::string instance_binaries_path;
extern std::string storage_prog_package_name;
extern std::string computer_prog_package_name;

extern std::string meta_svr_user;
extern std::string meta_svr_pwd;
extern std::string meta_svr_ip;
extern int64_t meta_svr_port;

Instance::Instance(Instance_type type_, std::string &ip_, int port_,
                   std::string &user_, std::string &pwd_)
    : type(type_), ip(ip_), port(port_), user(user_), pwd(pwd_), pullup_wait(0) {}

Instance::~Instance() {
}

Instance_info::Instance_info() {}

Instance_info::~Instance_info() {
  for (auto &instance : vec_meta_instance)
    delete instance;
  vec_meta_instance.clear();
  for (auto &instance : vec_storage_instance)
    delete instance;
  vec_storage_instance.clear();
  for (auto &instance : vec_computer_instance)
    delete instance;
  vec_computer_instance.clear();
}

void Instance_info::get_local_instance() {
  get_meta_instance();
  get_storage_instance();
  get_computer_instance();
}


bool Instance_info::get_meta_instance() {
  std::lock_guard<std::mutex> lock(mutex_instance_);

retry_group_seeds:
  {
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
      if(System::get_instance()->connet_to_meta_master())
        goto retry_group_seeds;
      else
        return false;
    }

    kunlun::MysqlResult result_set;
    char sql[2048] = {0};
    sprintf(sql, "select hostaddr,port,user_name,passwd from kunlun_metadata_db.meta_db_nodes");

    int ret = mysql_conn.ExcuteQuery(sql, &result_set);
    if (ret != 0) {
      syslog(Logger::ERROR, "metadata db query:[%s] failed: %s", sql,
            mysql_conn.getErr());
      return false;
    }

    for (auto &instance:vec_meta_instance)
      delete instance;
    vec_meta_instance.clear();
    if (result_set.GetResultLinesNum() > 0) {
      int lines = result_set.GetResultLinesNum();
      for (int i = 0; i < lines; i++) {
        std::string ip;
        int port;
        std::string user;
        std::string pwd;

        ip = result_set[i]["hostaddr"];
        port = stoi(result_set[i]["port"]);
        user = result_set[i]["user_name"];
        pwd = result_set[i]["passwd"];

        Instance *instance = new Instance(Instance::META, ip, port, user, pwd);
        vec_meta_instance.emplace_back(instance);
      }
    }
  }

  syslog(Logger::INFO, "meta instance %d update", vec_meta_instance.size());

  return true;
}

bool Instance_info::get_storage_instance() {
  std::lock_guard<std::mutex> lock(mutex_instance_);

retry_meta_master:
  {
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
      if(System::get_instance()->connet_to_meta_master())
        goto retry_meta_master;
      else
        return false;
    }

    kunlun::MysqlResult result_set;
    char sql[2048] = {0};
    sprintf(sql, "select hostaddr,port,user_name,passwd from kunlun_metadata_db.shard_nodes");

    int ret = mysql_conn.ExcuteQuery(sql, &result_set);
    if (ret != 0) {
      syslog(Logger::ERROR, "metadata db query:[%s] failed: %s", sql,
            mysql_conn.getErr());
      return false;
    }

    for (auto &instance:vec_storage_instance)
      delete instance;
    vec_storage_instance.clear();
    if (result_set.GetResultLinesNum() > 0) {
      int lines = result_set.GetResultLinesNum();
      for (int i = 0; i < lines; i++) {
        std::string ip;
        int port;
        std::string user;
        std::string pwd;

        ip = result_set[i]["hostaddr"];
        port = stoi(result_set[i]["port"]);
        user = result_set[i]["user_name"];
        pwd = result_set[i]["passwd"];

        Instance *instance = new Instance(Instance::STORAGE, ip, port, user, pwd);
        vec_storage_instance.emplace_back(instance);
      }
    }
  }

  syslog(Logger::INFO, "storage instance %d update",
         vec_storage_instance.size());

  return true;
}

bool Instance_info::get_computer_instance() {
  std::lock_guard<std::mutex> lock(mutex_instance_);
  
retry_meta_master:
  {
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
      if(System::get_instance()->connet_to_meta_master())
        goto retry_meta_master;
      else
        return false;
    }

    kunlun::MysqlResult result_set;
    char sql[2048] = {0};
    sprintf(sql, "select hostaddr,port,user_name,passwd from kunlun_metadata_db.comp_nodes");

    int ret = mysql_conn.ExcuteQuery(sql, &result_set);
    if (ret != 0) {
      syslog(Logger::ERROR, "metadata db query:[%s] failed: %s", sql,
            mysql_conn.getErr());
      return false;
    }

    for (auto &instance:vec_computer_instance)
      delete instance;
    vec_computer_instance.clear();
    if (result_set.GetResultLinesNum() > 0) {
      int lines = result_set.GetResultLinesNum();
      for (int i = 0; i < lines; i++) {
        std::string ip;
        int port;
        std::string user;
        std::string pwd;

        ip = result_set[i]["hostaddr"];
        port = stoi(result_set[i]["port"]);
        user = result_set[i]["user_name"];
        pwd = result_set[i]["passwd"];

        Instance *instance = new Instance(Instance::COMPUTER, ip, port, user, pwd);
        vec_computer_instance.emplace_back(instance);
      }
    }
  }

  syslog(Logger::INFO, "computer instance %d update",
         vec_computer_instance.size());

  return true;
}

void Instance_info::remove_storage_instance(std::string &ip, int port) {
  std::lock_guard<std::mutex> lock(mutex_instance_);

  for (auto it = vec_storage_instance.begin(); it != vec_storage_instance.end();
       it++) {
    if ((*it)->ip == ip && (*it)->port == port) {
      delete *it;
      vec_storage_instance.erase(it);
      return;
    }
  }

	for(auto it = vec_meta_instance.begin(); it != vec_meta_instance.end(); 
      it++)	{
		if((*it)->ip == ip && (*it)->port == port) {
			delete *it;
			vec_meta_instance.erase(it);
			return;
		}
	}
}

void Instance_info::remove_computer_instance(std::string &ip, int port) {
  std::lock_guard<std::mutex> lock(mutex_instance_);

  for (auto it = vec_computer_instance.begin();
       it != vec_computer_instance.end(); it++) {
    if ((*it)->ip == ip && (*it)->port == port) {
      delete *it;
      vec_computer_instance.erase(it);
      return;
    }
  }
}

void Instance_info::set_auto_pullup(int seconds, int port) {
  std::lock_guard<std::mutex> lock(mutex_instance_);

  if (port <= 0) // done on all of the port
  {
    for (auto &instance : vec_meta_instance)
      instance->pullup_wait = seconds;

    for (auto &instance : vec_storage_instance)
      instance->pullup_wait = seconds;

    for (auto &instance : vec_computer_instance)
      instance->pullup_wait = seconds;
  } else // find the instance compare by port
  {
    for (auto &instance : vec_meta_instance) {
      if (instance->port == port) {
        instance->pullup_wait = seconds;
        return;
      }
    }

    for (auto &instance : vec_storage_instance) {
      if (instance->port == port) {
        instance->pullup_wait = seconds;
        return;
      }
    }

    for (auto &instance : vec_computer_instance) {
      if (instance->port == port) {
        instance->pullup_wait = seconds;
        return;
      }
    }
  }
}

bool Instance_info::get_mysql_alive(MYSQL_CONN &mysql_conn, std::string &ip,
                                    int port, std::string &user,
                                    std::string &psw) {
  // syslog(Logger::INFO, "get_mysql_alive ip=%s,port=%d,user=%s,psw=%s",
  // ip.c_str(), port, user.c_str(), psw.c_str());

  int retry = stmt_retries;

  while (retry--) {
    if (mysql_conn.connect(NULL, ip.c_str(), port, user.c_str(),
                            psw.c_str())) {
      syslog(Logger::ERROR,
             "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", ip.c_str(),
             port, user.c_str(), psw.c_str());
      continue;
    }

    if (mysql_conn.send_stmt(SQLCOM_SELECT, "select version()"))
      continue;

    MYSQL_ROW row;
    if ((row = mysql_fetch_row(mysql_conn.result))) {
      // syslog(Logger::INFO, "row[]=%s",row[0]);
      if (strcasestr(row[0], "kunlun-storage"))
        break;
    } else {
      continue;
    }

    break;
  }
  mysql_conn.free_mysql_result();

  if (retry < 0)
    return false;

  return true;
}

bool Instance_info::get_pgsql_alive(PGSQL_CONN &pgsql_conn, std::string &ip,
                                    int port, std::string &user,
                                    std::string &psw) {
  // syslog(Logger::INFO, "get_pgsql_alive ip=%s,port=%d,user=%s,psw=%s",
  // ip.c_str(), port, user.c_str(), psw.c_str());

  int retry = stmt_retries;

  while (retry--) {
    if (pgsql_conn.connect("postgres", ip.c_str(), port, user.c_str(),
                            psw.c_str())) {
      syslog(Logger::ERROR,
             "connect to pgsql error ip=%s,port=%d,user=%s,psw=%s", ip.c_str(),
             port, user.c_str(), psw.c_str());
      continue;
    }

    if (pgsql_conn.send_stmt(PG_COPYRES_TUPLES, "select version()"))
      continue;

    if (PQntuples(pgsql_conn.result) == 1) {
      // syslog(Logger::INFO, "presult = %s",
      // PQgetvalue(pgsql_conn.result,0,0));
      if (strcasestr(PQgetvalue(pgsql_conn.result, 0, 0), "PostgreSQL"))
        break;
    }

    break;
  }
  pgsql_conn.free_pgsql_result();

  if (retry < 0)
    return false;

  return true;
}

void Instance_info::keepalive_instance() {
  std::lock_guard<std::mutex> lock(mutex_instance_);

  /////////////////////////////////////////////////////////////
  // keep alive of meta
  for (auto &instance : vec_meta_instance) {
    if (instance->pullup_wait > 0) {
      instance->pullup_wait -= thread_work_interval;
      if (instance->pullup_wait < 0)
        instance->pullup_wait = 0;
      continue;
    }

    if (!get_mysql_alive(instance->mysql_conn, instance->ip, instance->port,
                         instance->user, instance->pwd)) {
      syslog(Logger::ERROR, "meta_instance no alive, ip=%s, port=%d",
             instance->ip.c_str(), instance->port);
      instance->pullup_wait = pullup_wait_const;
      Job::get_instance()->job_control_storage(instance->port, 2);
    }
  }

  /////////////////////////////////////////////////////////////
  // keep alive of storage
  for (auto &instance : vec_storage_instance) {
    if (instance->pullup_wait > 0) {
      instance->pullup_wait -= thread_work_interval;
      if (instance->pullup_wait < 0)
        instance->pullup_wait = 0;
      continue;
    }

    if (!get_mysql_alive(instance->mysql_conn, instance->ip, instance->port,
                         instance->user, instance->pwd)) {
      syslog(Logger::ERROR, "storage_instance no alive, ip=%s, port=%d",
             instance->ip.c_str(), instance->port);
      instance->pullup_wait = pullup_wait_const;
      Job::get_instance()->job_control_storage(instance->port, 2);
    }
  }

  /////////////////////////////////////////////////////////////
  // keep alive of computer
  for (auto &instance : vec_computer_instance) {
    if (instance->pullup_wait > 0) {
      instance->pullup_wait -= thread_work_interval;
      if (instance->pullup_wait < 0)
        instance->pullup_wait = 0;
      continue;
    }

    if (!get_pgsql_alive(instance->pgsql_conn, instance->ip, instance->port,
                         instance->user, instance->pwd)) {
      syslog(Logger::ERROR, "computer_instance no alive, ip=%s, port=%d",
             instance->ip.c_str(), instance->port);
      instance->pullup_wait = pullup_wait_const;
      Job::get_instance()->job_control_computer(instance->ip, instance->port, 2);
    }
  }
}

bool Instance_info::get_path_used(std::string &path, uint64_t &used) {
  bool ret = false;
  FILE *pfd = NULL;

  char *p;
  char buf[256];
  std::string str_cmd;

  str_cmd = "du --max-depth=0 " + path;
  syslog(Logger::INFO, "get_path_used str_cmd : %s", str_cmd.c_str());

  pfd = popen(str_cmd.c_str(), "r");
  if (!pfd)
    goto end;

  if (fgets(buf, 256, pfd) == NULL)
    goto end;

  p = strchr(buf, 0x09); // asii ht
  if (p == NULL)
    goto end;

  *p = '\0';
  used = atol(buf) >> 10; // Mbyte
  ret = true;

end:
  if (pfd != NULL)
    pclose(pfd);

  return ret;
}

bool Instance_info::get_path_free(std::string &path, uint64_t &free) {
  bool ret = false;
  FILE *pfd = NULL;

  char *p, *q;
  char buf[256];
  std::string str_cmd;

  str_cmd = "df " + path;
  syslog(Logger::INFO, "get_path_free str_cmd : %s", str_cmd.c_str());

  pfd = popen(str_cmd.c_str(), "r");
  if (!pfd)
    goto end;

  // first line
  if (fgets(buf, 256, pfd) == NULL)
    goto end;

  // second line
  if (fgets(buf, 256, pfd) == NULL)
    goto end;

  // first space
  p = strchr(buf, 0x20);
  if (p == NULL)
    goto end;

  while (*p == 0x20)
    p++;

  // second space
  p = strchr(p, 0x20);
  if (p == NULL)
    goto end;

  while (*p == 0x20)
    p++;

  // third space
  p = strchr(p, 0x20);
  if (p == NULL)
    goto end;

  while (*p == 0x20)
    p++;

  // fourth space
  q = strchr(p, 0x20);
  if (p == NULL)
    goto end;

  *q = '\0';
  free = atol(p) >> 10; // Mbyte
  ret = true;

end:
  if (pfd != NULL)
    pclose(pfd);

  return ret;
}

void Instance_info::trimString(std::string &str) {
  int s = str.find_first_not_of(" ");
  int e = str.find_last_not_of(" ");
  if (s >= e)
    str = "";
  else
    str = str.substr(s, e - s + 1);
}

bool Instance_info::get_vec_path(std::vector<std::string> &vec_path,
                                 std::string &paths) {
  const char *cStart, *cEnd;
  std::string path;

  cStart = paths.c_str();
  while (*cStart != '\0') {
    cEnd = strchr(cStart, ',');
    if (cEnd == NULL) {
      path = std::string(cStart, strlen(cStart));
      trimString(path);
      if (path.length())
        vec_path.emplace_back(path);
      break;
    } else {
      path = std::string(cStart, cEnd - cStart);
      trimString(path);
      if (path.length())
        vec_path.emplace_back(path);
      cStart = cEnd + 1;
    }
  }

  return (vec_path.size() > 0);
}

bool Instance_info::get_path_space(Json::Value &para, std::string &result) {
  bool ret = true;
  uint64_t u_used, u_free;
  std::string path_used;
  std::vector<std::string> vec_paths;

  vec_paths.emplace_back(para["path0"].asString());
  vec_paths.emplace_back(para["path1"].asString());
  vec_paths.emplace_back(para["path2"].asString());
  vec_paths.emplace_back(para["path3"].asString());

  std::vector<std::string> vec_sub_path;
  vec_sub_path.emplace_back("/instance_data/data_dir_path");
  vec_sub_path.emplace_back("/instance_data/log_dir_path");
  vec_sub_path.emplace_back("/instance_data/innodb_log_dir_path");
  vec_sub_path.emplace_back("/instance_data/comp_datadir");

  std::vector<std::string> vec_path_index;
  vec_path_index.emplace_back("path0");
  vec_path_index.emplace_back("path1");
  vec_path_index.emplace_back("path2");
  vec_path_index.emplace_back("path3");

  vec_vec_path_used_free.clear();

  for (int i = 0; i < 4; i++) {
    std::vector<std::string> vec_path;
    if (!get_vec_path(vec_path, vec_paths[i])) {
      if (result.length() > 0)
        result += ";" + vec_paths[i];
      else
        result = vec_paths[i];

      ret = false;
      continue;
    }

    std::vector<Tpye_Path_Used_Free> vec_path_used_free;
    for (auto &path : vec_path) {
      if (!get_path_free(path, u_free)) {
        if (result.length() > 0)
          result += ";" + path;
        else
          result = path;

        ret = false;
        continue;
      } else {
        path_used = path + vec_sub_path[i];
        if (!get_path_used(path_used, u_used)) {
          u_used = 0;
        }
      }

      vec_path_used_free.emplace_back(std::make_tuple(path, u_used, u_free));
    }
    vec_vec_path_used_free.emplace_back(vec_path_used_free);
  }

  // json for return
  Json::Value root;
  
  if (ret) {
    for (int i = 0; i < 4; i++) {
      Json::Value list;
      for (auto &path_used_free : vec_vec_path_used_free[i]) {
        Json::Value para_json_array;
        para_json_array["path"] = std::get<0>(path_used_free);
        para_json_array["used"] = std::get<1>(path_used_free);
        para_json_array["free"] = std::get<2>(path_used_free);
        list.append(para_json_array);
      }
      root[vec_path_index[i]] = list;
    }

    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    result = writer.write(root);
  }

  return ret;
}

bool Instance_info::check_port_idle(Json::Value &para, std::string &result){
  bool ret = true;
  int port, step;
	FILE* pfd = NULL;
	char buf[256];
	std::string str_cmd;
	std::string str_port;

  port = para["port"].asInt();
  step = para["step"].asInt();

  while(1){
    ret = true;
    for(int i=0; i<step; i++){
      str_port = std::to_string(port);
      str_cmd = "netstat -anp | grep " + str_port;
      syslog(Logger::INFO, "check_port_idle str_cmd : %s",str_cmd.c_str());

      pfd = popen(str_cmd.c_str(), "r");
      if(!pfd){
        ret = false;
        break;
      }

      while(fgets(buf, 256, pfd)) {
        syslog(Logger::INFO, "check_port_idle %s",buf);
        if(strstr(buf, str_port.c_str())) {
          ret = false;
          break;
        }
      }
        
      pclose(pfd);
      pfd = NULL;

      if(!ret)
        break;
    }

    if(ret)
      break;

    port += step;
  }

  if(ret){
    // json for return
    Json::Value root;
    root["port"] = port;
  
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    result = writer.write(root);
  }

  return ret;
}
