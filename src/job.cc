/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "job.h"
#include "global.h"
#include "instance_info.h"
//#include "log.h"
#include "zettalib/op_log.h"
#include "sys.h"
#include "sys_config.h"
#include <arpa/inet.h>
#include <net/if.h>
#include <netdb.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include "zettalib/tool_func.h"

Job *Job::m_inst = NULL;

extern int64_t stmt_retries;
extern int64_t stmt_retry_interval_ms;

std::string program_binaries_path;
std::string instance_binaries_path;
std::string storage_prog_package_name;
std::string computer_prog_package_name;

std::string data_dir = kunlun::GetBasePath(get_current_dir_name()) + "/data";
std::string log_dir = kunlun::GetBasePath(get_current_dir_name()) + "/log";

std::string prometheus_path;
int64_t prometheus_port_start;

Job::Job() {}

Job::~Job() {}

bool Job::job_system_cmd(std::string &cmd) {
  kunlun::BiodirectPopen *popen_p = new kunlun::BiodirectPopen(cmd.c_str());
  FILE *stderr_fp;

  if (!popen_p->Launch("rw")) {
    goto end;
  }
  stderr_fp = popen_p->getReadStdErrFp();
  char buf[256];
  if (fgets(buf, 256, stderr_fp) != nullptr) {
    KLOG_ERROR("Biopopen stderr: {}", buf);
    goto end;
  }

end:
  if (popen_p != nullptr)
    delete popen_p;

  return true;
}

bool Job::job_save_file(std::string &path, const char *buf) {
  FILE *pfd = fopen(path.c_str(), "wb");
  if (pfd == NULL) {
    KLOG_ERROR( "Creat json file error {}", path);
    return false;
  }

  fwrite(buf, 1, strlen(buf), pfd);
  fclose(pfd);

  return true;
}

bool Job::job_read_file(std::string &path, std::string &str) {
  FILE *pfd = fopen(path.c_str(), "rb");
  if (pfd == NULL) {
    KLOG_ERROR("read json file error {}", path);
    return false;
  }

  int len = 0;
  char buf[1024];

  do {
    memset(buf, 0, 1024);
    len = fread(buf, 1, 1024 - 1, pfd);
    str += buf;
  } while (len > 0);

  fclose(pfd);

  return true;
}

bool Job::job_create_program_path() {
  std::string cmd, cmd_path, program_path;

  // unzip to program_binaries_path for install cmd
  // storage
  cmd_path =
      program_binaries_path + "/" + storage_prog_package_name + "/dba_tools";
  if (access(cmd_path.c_str(), F_OK) != 0) {
    KLOG_INFO("unzip {}.tgz", storage_prog_package_name);
    program_path =
        program_binaries_path + "/" + storage_prog_package_name + ".tgz";

    cmd = "tar zxf " + program_path + " -C " + program_binaries_path;
    if (!job_system_cmd(cmd))
      return false;
  }

  // computer
  cmd_path =
      program_binaries_path + "/" + computer_prog_package_name + "/scripts";
  if (access(cmd_path.c_str(), F_OK) != 0) {
    KLOG_INFO("unzip {}.tgz", computer_prog_package_name);
    program_path =
        program_binaries_path + "/" + computer_prog_package_name + ".tgz";

    cmd = "tar zxf " + program_path + " -C " + program_binaries_path;
    if (!job_system_cmd(cmd))
      return false;
  }

  return true;
}

bool Job::job_control_storage(int port, int control) {
  bool ret = false;
  FILE *pfd;
  char buf[256];

  std::string cmd, instance_path;

  /////////////////////////////////////////////////////////////
  instance_path = instance_binaries_path + "/storage/" + std::to_string(port);
  cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";

  if (control == 1) {
    // stop
    cmd =
        "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
    cmd += "./stopmysql.sh " + std::to_string(port);
    KLOG_INFO("job_control_storage cmd {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR( "stop error {}", cmd);
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      KLOG_INFO( "{}", buf);
    }
    pclose(pfd);
    return true;
  } else if (control == 2) {
    // start
    cmd =
        "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
    cmd += "./startmysql.sh " + std::to_string(port);
    KLOG_INFO( "job_control_storage cmd {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR( "start error {}", cmd);
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      KLOG_INFO( "{}", buf);
    }
    pclose(pfd);
    return true;
  } else if (control == 3) {
    // stop
    cmd =
        "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
    cmd += "./stopmysql.sh " + std::to_string(port);
    KLOG_INFO("job_control_storage cmd {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR( "stop error {}", cmd);
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      KLOG_INFO( "{}", buf);
    }
    pclose(pfd);

    sleep(1);

    // start
    cmd =
        "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
    cmd += "./startmysql.sh " + std::to_string(port);
    KLOG_INFO( "job_control_storage cmd {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR( "start error {}", cmd);
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      KLOG_INFO( "{}", buf);
    }
    pclose(pfd);
    return true;
  }

end:
  return ret;
}

bool Job::job_control_computer(std::string &ip, int port, int control) {
  bool ret = false;
  FILE *pfd;
  char buf[256];

  int nodes;
  std::string cmd, pathdir, instance_path, jsonfile_path, jsonfile_buf;
  Json::Value root;
  Json::Reader reader;

  /////////////////////////////////////////////////////////////
  // read json file from path/dba_tools
  instance_path = instance_binaries_path + "/computer/" + std::to_string(port);
  jsonfile_path = instance_path + "/" + computer_prog_package_name +
                  "/scripts/pgsql_comp.json";

  if (control != 2) {
    if (!job_read_file(jsonfile_path, jsonfile_buf)) {
      KLOG_ERROR( "job_read_file error");
      goto end;
    }

    if (!reader.parse(jsonfile_buf.c_str(), root)) {
      KLOG_ERROR( "json file parse error");
      goto end;
    }

    nodes = root.size();

    /////////////////////////////////////////////////////////////
    // find ip and port and delete pathdir
    for (int i = 0; i < nodes; i++) {
      Json::Value nodes_sub;
      int port_sub;
      std::string ip_sub;

      nodes_sub = root[i];
      ip_sub = nodes_sub["ip"].asString();
      port_sub = nodes_sub["port"].asInt();

      if (ip_sub != ip || port_sub != port)
        continue;

      //////////////////////////////
      // get datadir
      pathdir = nodes_sub["datadir"].asString();

      break;
    }
  }

  /////////////////////////////////////////////////////////////
  if (control == 1) {
    // stop computer cmd
    cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/bin;";
    cmd += "./pg_ctl -D " + pathdir + " stop";
    KLOG_INFO( "stop_computer cmd {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR("stop error {}", cmd);
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      KLOG_INFO( "{}", buf);
    }
    pclose(pfd);
    return true;
  } else if (control == 2) {
    // start
    cmd = "cd " + instance_path + "/" + computer_prog_package_name +
          "/scripts; python2 start_pg.py --port=" + std::to_string(port);
    KLOG_INFO( "start pgsql cmd : {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR("start error {}", cmd);
      goto end;
    }
    pclose(pfd);
    return true;
  } else if (control == 3) {
    // stop computer cmd
    cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/bin;";
    cmd += "./pg_ctl -D " + pathdir + " stop";
    KLOG_INFO("stop_computer cmd {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR( "stop error {}", cmd);
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      KLOG_INFO( "{}", buf);
    }
    pclose(pfd);

    sleep(1);

    // start
    cmd = "cd " + instance_path + "/" + computer_prog_package_name +
          "/scripts; python2 start_pg.py --port=" + std::to_string(port);
    KLOG_INFO("start pgsql cmd : {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      KLOG_ERROR("start error {}", cmd);
      goto end;
    }
    pclose(pfd);

    return true;
  }

end:
  return ret;
}

bool Job::job_storage_add_lib(std::set<std::string> &set_lib) {
  std::string cmd, program_path;

  program_path =
      program_binaries_path + "/" + storage_prog_package_name + "/lib";

  for (auto &lib : set_lib) {
    cmd = "cd program_path;cp deps/" + lib + " .";
    job_system_cmd(cmd);
  }

  return true;
}

bool Job::job_computer_add_lib(std::set<std::string> &set_lib) {
  std::string cmd, program_path;

  program_path =
      program_binaries_path + "/" + computer_prog_package_name + "/lib";

  for (auto &lib : set_lib) {
    cmd = "cd program_path;cp deps/" + lib + " .";
    job_system_cmd(cmd);
  }

  return true;
}

bool Job::job_node_exporter(Json::Value &para, std::string &job_info) {

  FILE* pfd;
	char buf[256];

	std::string cmd, process_id;

  job_info = "node exporter start";
  KLOG_INFO( "{}", job_info);

	/////////////////////////////////////////////////////////
	// get process_id of node_exporter
	cmd = "netstat -tnpl | grep tcp6 | grep " + std::to_string(prometheus_port_start+1);
	KLOG_INFO( "start_node_exporter cmd {}", cmd);

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		KLOG_ERROR("get error {}", cmd);
		goto end;
	}
	if(fgets(buf, 256, pfd)!=NULL) {
		char *p, *q;
		p = strstr(buf, "LISTEN");
		if(p != NULL) {
			p = strchr(p, 0x20);
			if(p != NULL) {
				while(*p == 0x20)
					p++;

				q = strchr(p, '/');

				if(p != NULL)
					process_id = std::string(p, q - p);
			}
		}
	}
	pclose(pfd);

	/////////////////////////////////////////////////////////
	// start prometheus
	if(process_id.length() == 0) {
		cmd = "cd " + prometheus_path + "/node_exporter;";
		cmd += "./node_exporter --web.listen-address=:" + std::to_string(prometheus_port_start+1) + " &";
		KLOG_INFO( "job_restart_prometheus cmd {}", cmd);

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			KLOG_ERROR("start error {}", cmd);
			goto end;
		}
		pclose(pfd);
	}

  job_info = "node exporter successfully";
  KLOG_INFO("{}", job_info);
  return true;

end:
  job_info = "node exporter failed";
  KLOG_ERROR("{}", job_info);
  return false;
}

bool Job::job_control_instance(Json::Value &para, std::string &job_info) {
  int port;
  std::string ip, type, control;
  bool ret;

  type = para["type"].asString();
  control = para["control"].asString();
  ip = para["ip"].asString();
  port = para["port"].asInt();

  job_info = "control instance start";
  KLOG_INFO( "{}", job_info);

  //System::get_instance()->set_auto_pullup_working(false);
  Instance_info *info_handler = Instance_info::get_instance();
  info_handler->toggle_auto_pullup(false,port);

  /////////////////////////////////////////////////////////////
  if (type == "storage") {
    if (control == "stop") {
      Instance_info::get_instance()->remove_storage_instance(ip, port);
      ret = job_control_storage(port, 1);
    } else if (control == "start")
      ret = job_control_storage(port, 2);
    else if (control == "restart")
      ret = job_control_storage(port, 3);
  } else if (type == "computer") {
    if (control == "stop") {
      Instance_info::get_instance()->remove_storage_instance(ip, port);
      ret = job_control_computer(ip, port, 1);
    } else if (control == "start")
      ret = job_control_computer(ip, port, 2);
    else if (control == "restart")
      ret = job_control_computer(ip, port, 3);
  } else {
    job_info = "type item error";
    goto end;
  }

  if (!ret) {
    job_info = "control instance error";
    goto end;
  }

  job_info = "control instance successfully";
  KLOG_INFO("{}", job_info);
  //System::get_instance()->set_auto_pullup_working(true);
  info_handler->toggle_auto_pullup(true,port);
  return true;

end:
  KLOG_ERROR("{}", job_info);
  //System::get_instance()->set_auto_pullup_working(true);
  info_handler->toggle_auto_pullup(true,port);
  return false;
}

bool Job::job_install_storage(Json::Value &para, std::string &job_info) {

  kunlun::BiodirectPopen *popen_p = nullptr;
  FILE *stderr_fp;
  char buffer[8192];

  int retry = 9;
  int install_id, dbcfg=0;
  int port;
  std::string cluster_name, shard_name, ha_mode, ip, user, pwd;
  std::string cmd, program_path, instance_path, file_path;
  Json::FastWriter writer;

  job_info = "install storage start";
  KLOG_INFO( "{}", job_info);

  cluster_name = para["cluster_name"].asString();
  ha_mode = para["ha_mode"].asString();
  shard_name = para["shard_name"].asString();
  install_id = para["install_id"].asInt();

  dbcfg = 0;
  if (para.isMember("dbcfg")) {
    dbcfg = para["dbcfg"].asInt();
  }

  Json::Value nodes = para["nodes"];
  Json::Value sub_node = nodes[install_id];
  ip = sub_node["ip"].asString();
  port = sub_node["port"].asInt();

  /////////////////////////////////////////////////////////////
  // cp from program_binaries_path to instance_binaries_path
  program_path =
      program_binaries_path + "/" + storage_prog_package_name;
  instance_path = instance_binaries_path + "/storage/" + std::to_string(port);

  //////////////////////////////
  // check program_path
  if (access(program_path.c_str(), F_OK) != 0) {
    job_info = "error, " + program_path + " no exist";
    goto end;
  }

  //////////////////////////////
  // check exist instance and kill
  if (access(instance_path.c_str(), F_OK) == 0){
    job_control_storage(port, 1);
    
    //////////////////////////////
    // rm file in instance_path
    cmd = "rm -rf " + instance_path + "/*";
    if (!job_system_cmd(cmd)) {
      job_info = "job_system_cmd error";
      goto end;
    }
  }

  //////////////////////////////
  // mkdir instance_path
  cmd = "mkdir -p " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  //////////////////////////////
  // rm file in data_dir_path
  file_path = sub_node["data_dir_path"].asString();
  cmd = "rm -rf " + file_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  //////////////////////////////
  // rm file in innodb_log_dir_path
  file_path = sub_node["innodb_log_dir_path"].asString();
  cmd = "rm -rf " + file_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  //////////////////////////////
  // rm file in log_dir_path
  file_path = sub_node["log_dir_path"].asString();
  cmd = "rm -rf " + file_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  //////////////////////////////
  // cp to instance_path
  cmd = "cp -rf " + program_path + " " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  /////////////////////////////////////////////////////////////
  // save json file to path/dba_tools
  file_path = instance_path + "/" + storage_prog_package_name +
                  "/dba_tools/mysql_shard.json";

  writer.omitEndingLineFeed();
  cmd = writer.write(para);
  job_save_file(file_path, cmd.c_str());

  /////////////////////////////////////////////////////////////
  // start install storage cmd
  cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
  cmd += "python2 install-mysql.py --config=./mysql_shard.json "
         "--target_node_index=" +
         std::to_string(install_id);
  cmd += " --cluster_id " + cluster_name + " --shard_id " + shard_name +
         " --ha_mode " + ha_mode;
  if(dbcfg)
    cmd += " --dbcfg=./template-small.cnf";

  KLOG_INFO("job_install_storage cmd {}", cmd);

  popen_p = new kunlun::BiodirectPopen(cmd.c_str());
  if (!popen_p->Launch("rw")) {
    job_info = "BiodirectPopen error";
    goto end;
  }
  stderr_fp = popen_p->getReadStdErrFp();
  while (fgets(buffer, 8192, stderr_fp) != nullptr) {
    KLOG_INFO("Biopopen: {}", buffer);
  }
  delete popen_p;
  popen_p = nullptr;

  /////////////////////////////////////////////////////////////
  // check instance succeed by connect to instance
  retry = 9;
  user = "pgx";
  pwd = "pgx_pwd";
  while (retry-- > 0) {
    sleep(1);
    if (Instance_info::get_instance()->get_mysql_alive_tcp(ip, port,
                                                       user, pwd))
      break;
  }

  if (retry < 0) {
    job_info = "connect storage instance error";
    goto end;
  }

  job_info = "install storage successfully";
  KLOG_INFO( "{}", job_info);
  return true;

end:
  if(popen_p != nullptr)
    delete popen_p;

  KLOG_INFO( "{}", job_info);
  return false;
}

bool Job::job_install_computer(Json::Value &para, std::string &job_info) {
 
  kunlun::BiodirectPopen *popen_p = nullptr;
  FILE *stderr_fp;
  char buffer[8192];

  int retry = 9;
  int install_id;
  int port;
  std::string ip, user, pwd;
  std::string cmd, program_path, instance_path, file_path;
  Json::FastWriter writer;

  job_info = "install computer start";
  KLOG_INFO("{}", job_info);

  install_id = para["install_id"].asInt();
  Json::Value nodes = para["nodes"];
  Json::Value sub_node = nodes[install_id];
  install_id = sub_node["id"].asInt();
  ip = sub_node["ip"].asString();
  port = sub_node["port"].asInt();
  user = sub_node["user"].asString();
  pwd = sub_node["password"].asString();

  /////////////////////////////////////////////////////////////
  // cp from program_binaries_path to instance_binaries_path
  program_path =
      program_binaries_path + "/" + computer_prog_package_name;
  instance_path = instance_binaries_path + "/computer/" + std::to_string(port);

  //////////////////////////////
  // check exist instance and kill
  if (access(instance_path.c_str(), F_OK) == 0){
    job_control_computer(ip, port, 1);

    //////////////////////////////
    // rm file in instance_path
    cmd = "rm -rf " + instance_path + "/*";
    if (!job_system_cmd(cmd)) {
      job_info = "job_system_cmd error";
      goto end;
    }
  }

  //////////////////////////////
  // mkdir instance_path
  cmd = "mkdir -p " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  //////////////////////////////
  // rm file in datadir
  file_path = sub_node["datadir"].asString();
  cmd = "rm -rf " + file_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  //////////////////////////////
  // cp to instance_path
  cmd = "cp -rf " + program_path + " " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  /////////////////////////////////////////////////////////////
  // save json file to path/scripts
  file_path = instance_path + "/" + computer_prog_package_name +
                  "/scripts/pgsql_comp.json";

  writer.omitEndingLineFeed();
  cmd = writer.write(nodes);
  job_save_file(file_path, cmd.c_str());

  /////////////////////////////////////////////////////////////
  // start install computer cmd
  cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/scripts;";
  cmd += "python2 install_pg.py --config=pgsql_comp.json --install_ids=" +
         std::to_string(install_id);
  KLOG_INFO("job_install_computer cmd {}", cmd);

  popen_p = new kunlun::BiodirectPopen(cmd.c_str());
  if (!popen_p->Launch("rw")) {
    job_info = "BiodirectPopen error";
    goto end;
  }
  stderr_fp = popen_p->getReadStdErrFp();
  while (fgets(buffer, 8192, stderr_fp) != nullptr) {
    KLOG_INFO( "Biopopen: {}", buffer);
  }
  delete popen_p;
  popen_p = nullptr;

  /////////////////////////////////////////////////////////////
  // check instance succeed by connect to instance
  retry = 9;
  while (retry-- > 0) {
    sleep(1);
    if (Instance_info::get_instance()->get_pgsql_alive_tcp(ip, port,
                                                       user, pwd))
      break;
  }

  if (retry < 0) {
    job_info = "connect computer instance error";
    goto end;
  }

  job_info = "install computer successfully";
  KLOG_INFO( "{}", job_info);
  return true;

end:
  if(popen_p != nullptr)
    delete popen_p;
  KLOG_ERROR( "{}", job_info);
  return false;
}

bool Job::job_delete_storage(Json::Value &para, std::string &job_info) {

  FILE *pfd;
  char buf[256];

  int nodes;
  int port;
  std::string ip;
  std::string cmd, pathdir, instance_path, jsonfile_path, jsonfile_buf;
  Json::Value root;
  Json::Reader reader;

  job_info = "delete storage start";
  KLOG_INFO("{}", job_info);

  ip = para["ip"].asString();
  port = para["port"].asInt();

  //System::get_instance()->set_auto_pullup_working(false);
  Instance_info *info_handler = Instance_info::get_instance();
  info_handler->toggle_auto_pullup(false,port);
  Instance_info::get_instance()->remove_storage_instance(ip, port);

  /////////////////////////////////////////////////////////////
  // stop storage cmd
  instance_path = instance_binaries_path + "/storage/" + std::to_string(port);
  cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
  cmd += "./stopmysql.sh " + std::to_string(port);
  KLOG_INFO( "job_delete_storage cmd {}", cmd);

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "stop error " + cmd;
    goto end;
  }
  while (fgets(buf, 256, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    KLOG_INFO("{}", buf);
  }
  pclose(pfd);

  /////////////////////////////////////////////////////////////
  // read json file from path/dba_tools
  jsonfile_path = instance_path + "/" + storage_prog_package_name +
                  "/dba_tools/mysql_shard.json";

  if (!job_read_file(jsonfile_path, jsonfile_buf)) {
    job_info = "job_read_file error";
    goto end;
  }

  if (!reader.parse(jsonfile_buf.c_str(), root)) {
    job_info = "json file parse error";
    goto end;
  }

  nodes = root["nodes"].size();

  /////////////////////////////////////////////////////////////
  // find ip and port and delete pathdir
  for (int i = 0; i < nodes; i++) {
    Json::Value nodes_sub;
    int port_sub;
    std::string ip_sub;

    nodes_sub = root["nodes"][i];
    ip_sub = nodes_sub["ip"].asString();;
    port_sub = nodes_sub["port"].asInt();;

    if (ip_sub != ip || port_sub != port)
      continue;

    //////////////////////////////
    // rm file in data_dir_path
    pathdir = nodes_sub["data_dir_path"].asString();;
    cmd = "rm -rf " + pathdir;
    if (!job_system_cmd(cmd)) {
      job_info = "job_system_cmd error";
      goto end;
    }

    //////////////////////////////
    // rm file in innodb_log_dir_path
    pathdir = nodes_sub["innodb_log_dir_path"].asString();;
    cmd = "rm -rf " + pathdir;
    if (!job_system_cmd(cmd)) {
      job_info = "job_system_cmd error";
      goto end;
    }

    //////////////////////////////
    // rm file in log_dir_path
    pathdir = nodes_sub["log_dir_path"].asString();;
    cmd = "rm -rf " + pathdir;
    if (!job_system_cmd(cmd)) {
      job_info = "job_system_cmd error";
      goto end;
    }

    break;
  }

  //////////////////////////////
  // rm instance_path
  cmd = "rm -rf " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  job_info = "delete storage successfully";
  KLOG_INFO("{}", job_info);
  //System::get_instance()->set_auto_pullup_working(true);
  //info_handler->toggle_auto_pullup(true,port);
  return true;

end:
  KLOG_ERROR( "{}", job_info);
  //System::get_instance()->set_auto_pullup_working(true);
  //info_handler->toggle_auto_pullup(true,port);
  return false;
}

bool Job::job_delete_computer(Json::Value &para, std::string &job_info) {

  FILE *pfd;
  char buf[256];

  int nodes;
  int port;
  std::string ip;
  std::string cmd, pathdir, instance_path, jsonfile_path, jsonfile_buf;
  Json::Value root;
  Json::Reader reader;

  job_info = "delete computer start";
  KLOG_INFO("{}", job_info);

  ip = para["ip"].asString();
  port = para["port"].asInt();

  //System::get_instance()->set_auto_pullup_working(false);
  Instance_info *info_handler = Instance_info::get_instance();
  info_handler->toggle_auto_pullup(false,port);
  Instance_info::get_instance()->remove_computer_instance(ip, port);

  /////////////////////////////////////////////////////////////
  // read json file from path/dba_tools
  instance_path = instance_binaries_path + "/computer/" + std::to_string(port);
  jsonfile_path = instance_path + "/" + computer_prog_package_name +
                  "/scripts/pgsql_comp.json";

  if (!job_read_file(jsonfile_path, jsonfile_buf)) {
    job_info = "job_read_file error";
    goto end;
  }

  if (!reader.parse(jsonfile_buf.c_str(), root)) {
    job_info = "json file parse error";
    goto end;
  }

  nodes = root.size();

  /////////////////////////////////////////////////////////////
  // find ip and port and delete pathdir
  for (int i = 0; i < nodes; i++) {
    Json::Value nodes_sub;
    int port_sub;
    std::string ip_sub;

    nodes_sub = root[i];
    ip_sub = nodes_sub["ip"].asString();;
    port_sub = nodes_sub["port"].asInt();;

    if (ip_sub != ip || port_sub != port)
      continue;

    //////////////////////////////
    // get datadir
    pathdir = nodes_sub["datadir"].asString();

    // stop computer cmd
    cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/bin;";
    cmd += "./pg_ctl -D " + pathdir + " stop";
    KLOG_INFO( "job_delete_computer cmd {}", cmd);

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      job_info = "stop error " + cmd;
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      KLOG_INFO("{}", buf);
    }
    pclose(pfd);
    KLOG_INFO("stop computer end");

    // rm file in pathdir
    cmd = "rm -rf " + pathdir;
    if (!job_system_cmd(cmd)) {
      job_info = "job_system_cmd error";
      goto end;
    }

    break;
  }

  //////////////////////////////
  // rm instance_path
  //cmd = "rm -rf " + instance_path;
  //if (!job_system_cmd(cmd)) {
  //  job_info = "job_system_cmd error";
  //  goto end;
  //}

  job_info = "delete computer successfully";
  KLOG_INFO( "{}", job_info);
//  System::get_instance()->set_auto_pullup_working(true);
  //info_handler->toggle_auto_pullup(true,port);
  return true;

end:
  KLOG_ERROR( "{}", job_info);
  //System::get_instance()->set_auto_pullup_working(true);
  //info_handler->toggle_auto_pullup(true,port);

  return false;
}

bool Job::job_backup_compute(Json::Value &para, std::string &job_info) {

  FILE *pfd;
  char buf[512];

  char *p = nullptr;
  std::string cmd, backup_storage;
  int port;
  std::string ip;
  Json::Value root_ret;
  Json::FastWriter writer;

  job_info = "backup compute start";
  KLOG_INFO("{}", job_info);

  ip = para["ip"].asString();
  port = para["port"].asInt();
  backup_storage = para["backup_storage"].asString();

  ////////////////////////////////////////////////////////
  // start backup path
  cmd = "./util/backup -backuptype=compute -port=" + std::to_string(port) ;
  cmd += " -HdfsNameNodeService=" + backup_storage;
  cmd += " -workdir=" + data_dir;
  cmd += " -logdir=" + log_dir; 
  KLOG_INFO("job_backup_compute cmd {}", cmd);

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "backup error " + cmd;
    goto end;
  }
  while (fgets(buf, 512, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    KLOG_INFO( "{}", buf);
  }
  pclose(pfd);

  ////////////////////////////////////////////////////////
  // check error, must be contain cluster_name & shard_name, and the tail like
  // ".tgz\n\0"
  p = strstr(buf, ".tgz");
  if (p == NULL || *(p + 4) != '\n' || *(p + 5) != '\0') {
    KLOG_ERROR("backup compute error: {}", buf);
    job_info = "backup compute cmd return error";
    goto end;
  }
 
    job_info = buf;

  ////////////////////////////////////////////////////////
  // rm backup path
//  cmd = "rm -rf ./data";
//  if (!job_system_cmd(cmd)) {
//    job_info = "job_system_cmd error";
//    goto end;
//  }
//
  root_ret["path"] = job_info;
  writer.omitEndingLineFeed();
  job_info = writer.write(root_ret);

  // job_info = "backup successfully"; //for shard_backup_path
  KLOG_INFO("backup successfully");
  return true;

end:
  KLOG_ERROR("{}", job_info);
  return false;
}


bool Job::job_backup_shard(Json::Value &para, std::string &job_info) {

  FILE *pfd;
  char buf[512];

  std::string cmd, cluster_name, shard_name, shard_id, backup_storage;
  int port;
  std::string ip;
  Json::Value root_ret;
  Json::FastWriter writer;

  job_info = "backup shard start";
  KLOG_INFO("{}", job_info);

  ip = para["ip"].asString();
  port = para["port"].asInt();
  cluster_name = para["cluster_name"].asString();
  shard_name = para["shard_name"].asString();
  shard_id = para["shard_id"].asString();
  backup_storage = para["backup_storage"].asString();

  ////////////////////////////////////////////////////////
  // start backup path
  cmd = "./util/backup -backuptype=storage -port=" + std::to_string(port) +
        " -clustername=" + cluster_name + " -shardname=" + shard_name;
  cmd += " -HdfsNameNodeService=" + backup_storage;
  cmd += " -workdir=" + data_dir;
  cmd += " -logdir=" + log_dir; 
  KLOG_INFO("job_backup_shard cmd {}", cmd);

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "backup error " + cmd;
    goto end;
  }
  while (fgets(buf, 512, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    KLOG_INFO( "{}", buf);
  }
  pclose(pfd);

  ////////////////////////////////////////////////////////
  // check error, must be contain cluster_name & shard_name, and the tail like
  // ".tgz\n\0"
  if (strstr(buf, cluster_name.c_str()) == NULL ||
      strstr(buf, shard_name.c_str()) == NULL) {
    KLOG_ERROR("backup error: {}", buf);
    job_info = "backup cmd return error";
    goto end;
  } else {
    char *p = strstr(buf, ".tgz");
    if (p == NULL || *(p + 4) != '\n' || *(p + 5) != '\0') {
      KLOG_ERROR("backup error: {}", buf);
      job_info = "backup cmd return error";
      goto end;
    }

    job_info = buf;
  }

  ////////////////////////////////////////////////////////
  // rm backup path
  cmd = "rm -rf ./data";
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  root_ret["shard_id"] = shard_id;
  root_ret["path"] = job_info;
  writer.omitEndingLineFeed();
  job_info = writer.write(root_ret);

  // job_info = "backup successfully"; //for shard_backup_path
  KLOG_INFO( "backup successfully");
  return true;

end:
  KLOG_ERROR("{}", job_info);
  return false;
}

bool Job::job_restore_storage(Json::Value &para, std::string &job_info) {

  FILE *pfd;
  char buf[512];

  std::string cmd, cluster_name, shard_name, timestamp, backup_storage;
  std::string ip, user, pwd;
  int port;
  int retry;

  job_info = "restore storage start";
  KLOG_INFO( "{}", job_info);
  //System::get_instance()->set_auto_pullup_working(false);
  Instance_info *info_handler = Instance_info::get_instance();
  info_handler->toggle_auto_pullup(false,port);

  ip = para["ip"].asString();
  port = para["port"].asInt();
  cluster_name = para["cluster_name"].asString();
  shard_name = para["shard_name"].asString();
  timestamp = para["timestamp"].asString();
  backup_storage = para["backup_storage"].asString();

  ////////////////////////////////////////////////////////
  // start backup path
  cmd = "restore -port=" + std::to_string(port) +
        " -origclustername=" + cluster_name + " -origshardname=" + shard_name;
  cmd += " -restoretime='" + timestamp +
         "' -HdfsNameNodeService=" + backup_storage;
  KLOG_INFO("job_restore_storage cmd {}", cmd);

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "restore error " + cmd;
    goto end;
  }
  while (fgets(buf, 512, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    KLOG_INFO( "{}", buf);
  }
  pclose(pfd);

  ////////////////////////////////////////////////////////
  // check error
  if (strstr(buf, "restore MySQL instance successfully") == NULL) {
    KLOG_ERROR( "restore storage error: {}", buf);
    job_info = "restore cmd return error";
    goto end;
  }

  ////////////////////////////////////////////////////////
  // rm restore path
  cmd = "rm -rf ./data";
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  /////////////////////////////////////////////////////////////
  // check instance succeed by connect to instance
  retry = 6;
  user = "pgx";
  pwd = "pgx_pwd";
  while (retry-- > 0) {
    sleep(1);
    if (Instance_info::get_instance()->get_mysql_alive_tcp(ip, port,
                                                       user, pwd))
      break;
  }

  if (retry < 0) {
    job_info = "connect storage instance error";
    goto end;
  }

  job_info = "restore storage successfully";
  KLOG_INFO("{}", job_info);
  //System::get_instance()->set_auto_pullup_working(true);
  info_handler->toggle_auto_pullup(true,port);
  return true;

end:
  KLOG_ERROR( "{}", job_info);
  //System::get_instance()->set_auto_pullup_working(true);
  info_handler->toggle_auto_pullup(true,port);
  return false;
}

bool Job::job_restore_computer(Json::Value &para, std::string &job_info) {

  FILE *pfd;
  char buf[512];

  std::string cmd, strtmp, cluster_name, meta_str, shard_map;
  std::string ip;
  int port;
  int retry;

  job_info = "restore computer start";
  KLOG_INFO( "{}", job_info);

  ip = para["ip"].asString();
  port = para["port"].asInt();
  cluster_name = para["cluster_name"].asString();
  meta_str = para["meta_str"].asString();
  shard_map = para["shard_map"].asString();

  ////////////////////////////////////////////////////////
  // restore meta to computer
  cmd = "restore -restoretype=compute -workdir=./data -port=" +
        std::to_string(port) + " -origclustername=" + cluster_name;
  cmd += " -origmetaclusterconnstr=" + meta_str +
         " -metaclusterconnstr=" + meta_str + " -shard_map=\"" + shard_map + "\"";
  KLOG_INFO( "job_restore_computer cmd {}", cmd);

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "restore error " + cmd;
    goto end;
  }
  while (fgets(buf, 512, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    KLOG_INFO( "{}", buf);
  }
  pclose(pfd);

  ////////////////////////////////////////////////////////
  // check error
  if (strstr(buf, "restore Compute successfully") == NULL) {
    KLOG_ERROR("restore computer error: {}", buf);
    job_info = "restore cmd return error";
    goto end;
  }

  ////////////////////////////////////////////////////////
  // rm restore path
  cmd = "rm -rf ./data";
  // if(!job_system_cmd(cmd))
  {
    //	job_info = "job_system_cmd error";
    //	goto end;
  }

  job_info = "restore compouter successfully";
  KLOG_INFO("{}", job_info);
  return true;

end:
  KLOG_ERROR("{}", job_info);
  return false;
}