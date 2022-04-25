/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "job.h"
#include "global.h"
#include "instance_info.h"
#include "log.h"
#include "mysql_conn.h"
#include "pgsql_conn.h"
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

Job *Job::m_inst = NULL;

extern int64_t stmt_retries;
extern int64_t stmt_retry_interval_ms;

std::string program_binaries_path;
std::string instance_binaries_path;
std::string storage_prog_package_name;
std::string computer_prog_package_name;

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
    syslog(Logger::ERROR, "Biopopen stderr: %s", buf);
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
    syslog(Logger::ERROR, "Creat json file error %s", path.c_str());
    return false;
  }

  fwrite(buf, 1, strlen(buf), pfd);
  fclose(pfd);

  return true;
}

bool Job::job_read_file(std::string &path, std::string &str) {
  FILE *pfd = fopen(path.c_str(), "rb");
  if (pfd == NULL) {
    syslog(Logger::ERROR, "read json file error %s", path.c_str());
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
    syslog(Logger::INFO, "unzip %s.tgz", storage_prog_package_name.c_str());
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
    syslog(Logger::INFO, "unzip %s.tgz", computer_prog_package_name.c_str());
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
    syslog(Logger::INFO, "job_control_storage cmd %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "stop error %s", cmd.c_str());
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      syslog(Logger::INFO, "%s", buf);
    }
    pclose(pfd);
    return true;
  } else if (control == 2) {
    // start
    cmd =
        "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
    cmd += "./startmysql.sh " + std::to_string(port);
    syslog(Logger::INFO, "job_control_storage cmd %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "start error %s", cmd.c_str());
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      syslog(Logger::INFO, "%s", buf);
    }
    pclose(pfd);
    return true;
  } else if (control == 3) {
    // stop
    cmd =
        "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
    cmd += "./stopmysql.sh " + std::to_string(port);
    syslog(Logger::INFO, "job_control_storage cmd %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "stop error %s", cmd.c_str());
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      syslog(Logger::INFO, "%s", buf);
    }
    pclose(pfd);

    sleep(1);

    // start
    cmd =
        "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
    cmd += "./startmysql.sh " + std::to_string(port);
    syslog(Logger::INFO, "job_control_storage cmd %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "start error %s", cmd.c_str());
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      syslog(Logger::INFO, "%s", buf);
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
      syslog(Logger::ERROR, "job_read_file error");
      goto end;
    }

    if (!reader.parse(jsonfile_buf.c_str(), root)) {
      syslog(Logger::ERROR, "json file parse error");
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
    syslog(Logger::INFO, "stop_computer cmd %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "stop error %s", cmd.c_str());
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      syslog(Logger::INFO, "%s", buf);
    }
    pclose(pfd);
    return true;
  } else if (control == 2) {
    // start
    cmd = "cd " + instance_path + "/" + computer_prog_package_name +
          "/scripts; python2 start_pg.py --port=" + std::to_string(port);
    syslog(Logger::INFO, "start pgsql cmd : %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "start error %s", cmd.c_str());
      goto end;
    }
    pclose(pfd);
    return true;
  } else if (control == 3) {
    // stop computer cmd
    cmd = "cd " + instance_path + "/" + computer_prog_package_name + "/bin;";
    cmd += "./pg_ctl -D " + pathdir + " stop";
    syslog(Logger::INFO, "stop_computer cmd %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "stop error %s", cmd.c_str());
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      syslog(Logger::INFO, "%s", buf);
    }
    pclose(pfd);

    sleep(1);

    // start
    cmd = "cd " + instance_path + "/" + computer_prog_package_name +
          "/scripts; python2 start_pg.py --port=" + std::to_string(port);
    syslog(Logger::INFO, "start pgsql cmd : %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      syslog(Logger::ERROR, "start error %s", cmd.c_str());
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

#if 0
void Job::job_backup_shard(cJSON *root) {
  std::string job_id;
  std::string job_result;
  std::string job_info;
  cJSON *item;

  FILE *pfd;
  char buf[512];

  std::string cmd, cluster_name, shard_name, backup_storage;
  int port;
  std::string ip;

  item = cJSON_GetObjectItem(root, "job_id");
  if (item == NULL || item->valuestring == NULL) {
    syslog(Logger::ERROR, "get_job_id error");
    return;
  }
  job_id = item->valuestring;

  job_result = "busy";
  job_info = "backup shard start";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());

  item = cJSON_GetObjectItem(root, "ip");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get ip error";
    goto end;
  }
  ip = item->valuestring;

  item = cJSON_GetObjectItem(root, "port");
  if (item == NULL) {
    job_info = "get port error";
    goto end;
  }
  port = item->valueint;

  if (!check_local_ip(ip)) {
    job_info = ip + " is not local ip";
    goto end;
  }

  item = cJSON_GetObjectItem(root, "cluster_name");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get cluster_name error";
    goto end;
  }
  cluster_name = item->valuestring;

  item = cJSON_GetObjectItem(root, "shard_name");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get shard_name error";
    goto end;
  }
  shard_name = item->valuestring;

  item = cJSON_GetObjectItem(root, "backup_storage");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get backup_storage error";
    goto end;
  }
  backup_storage = item->valuestring;

  job_info = "backup shard wroking";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());

  ////////////////////////////////////////////////////////
  // start backup path
  cmd = "backup -backuptype=storage -port=" + std::to_string(port) +
        " -clustername=" + cluster_name + " -shardname=" + shard_name;
  cmd += " -HdfsNameNodeService=" + backup_storage;
  syslog(Logger::INFO, "job_backup_shard cmd %s", cmd.c_str());

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "backup error " + cmd;
    goto end;
  }
  while (fgets(buf, 512, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    syslog(Logger::INFO, "%s", buf);
  }
  pclose(pfd);

  ////////////////////////////////////////////////////////
  // check error, must be contain cluster_name & shard_name, and the tail like
  // ".tgz\n\0"
  if (strstr(buf, cluster_name.c_str()) == NULL ||
      strstr(buf, shard_name.c_str()) == NULL) {
    syslog(Logger::ERROR, "backup error: %s", buf);
    job_info = "backup cmd return error";
    goto end;
  } else {
    char *p = strstr(buf, ".tgz");
    if (p == NULL || *(p + 4) != '\n' || *(p + 5) != '\0') {
      syslog(Logger::ERROR, "backup error: %s", buf);
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

  job_result = "succeed";
  // job_info = "backup succeed"; //for shard_backup_path
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());
  return;

end:
  job_result = "error";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::ERROR, "%s", job_info.c_str());
}

void Job::job_restore_storage(cJSON *root) {
  std::string job_id;
  std::string job_result;
  std::string job_info;
  cJSON *item;

  FILE *pfd;
  char buf[512];

  std::string cmd, cluster_name, shard_name, timestamp, backup_storage;
  std::string ip, user, pwd;
  MYSQL_CONN mysql_conn;
  int port;
  int retry;

  item = cJSON_GetObjectItem(root, "job_id");
  if (item == NULL || item->valuestring == NULL) {
    syslog(Logger::ERROR, "get_job_id error");
    return;
  }
  job_id = item->valuestring;

  job_result = "busy";
  job_info = "restore storage start";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(false);

  item = cJSON_GetObjectItem(root, "ip");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get ip error";
    goto end;
  }
  ip = item->valuestring;

  item = cJSON_GetObjectItem(root, "port");
  if (item == NULL) {
    job_info = "get port error";
    goto end;
  }
  port = item->valueint;

  if (!check_local_ip(ip)) {
    job_info = ip + " is not local ip";
    goto end;
  }

  item = cJSON_GetObjectItem(root, "cluster_name");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get cluster_name error";
    goto end;
  }
  cluster_name = item->valuestring;

  item = cJSON_GetObjectItem(root, "shard_name");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get shard_name error";
    goto end;
  }
  shard_name = item->valuestring;

  item = cJSON_GetObjectItem(root, "timestamp");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get timestamp error";
    goto end;
  }
  timestamp = item->valuestring;

  item = cJSON_GetObjectItem(root, "backup_storage");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get backup_storage error";
    goto end;
  }
  backup_storage = item->valuestring;

  job_info = "restore storage wroking";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());

  ////////////////////////////////////////////////////////
  // start backup path
  cmd = "restore -port=" + std::to_string(port) +
        " -origclustername=" + cluster_name + " -origshardname=" + shard_name;
  cmd += " -restoretime='" + timestamp +
         "' -HdfsNameNodeService=" + backup_storage;
  syslog(Logger::INFO, "job_restore_storage cmd %s", cmd.c_str());

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "restore error " + cmd;
    goto end;
  }
  while (fgets(buf, 512, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    syslog(Logger::INFO, "%s", buf);
  }
  pclose(pfd);

  ////////////////////////////////////////////////////////
  // check error
  if (strstr(buf, "restore MySQL instance successfully") == NULL) {
    syslog(Logger::ERROR, "restore storage error: %s", buf);
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
  while (retry-- > 0 && !Job::do_exit) {
    sleep(1);
    if (Instance_info::get_instance()->get_mysql_alive(&mysql_conn, ip, port,
                                                       user, pwd))
      break;
  }
  mysql_conn.close_conn();

  if (retry < 0) {
    job_info = "connect storage instance error";
    goto end;
  }

  job_result = "succeed";
  job_info = "restore storage succeed";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
  return;

end:
  job_result = "error";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::ERROR, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
}

void Job::job_restore_computer(cJSON *root) {
  std::string job_id;
  std::string job_result;
  std::string job_info;
  cJSON *item;

  FILE *pfd;
  char buf[512];

  std::string cmd, strtmp, cluster_name, meta_str, shard_map;
  std::string ip;
  int port;
  int retry;

  item = cJSON_GetObjectItem(root, "job_id");
  if (item == NULL || item->valuestring == NULL) {
    syslog(Logger::ERROR, "get_job_id error");
    return;
  }
  job_id = item->valuestring;

  job_result = "busy";
  job_info = "restore computer start";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());

  item = cJSON_GetObjectItem(root, "ip");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get ip error";
    goto end;
  }
  ip = item->valuestring;

  item = cJSON_GetObjectItem(root, "port");
  if (item == NULL) {
    job_info = "get port error";
    goto end;
  }
  port = item->valueint;

  if (!check_local_ip(ip)) {
    job_info = ip + " is not local ip";
    goto end;
  }

  item = cJSON_GetObjectItem(root, "cluster_name");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get cluster_name error";
    goto end;
  }
  cluster_name = item->valuestring;

  item = cJSON_GetObjectItem(root, "meta_str");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get meta_str error";
    goto end;
  }
  meta_str = item->valuestring;

  item = cJSON_GetObjectItem(root, "shard_map");
  if (item == NULL || item->valuestring == NULL) {
    job_info = "get shard_map error";
    goto end;
  }
  shard_map = item->valuestring;

  job_info = "restore computer wroking";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());

  ////////////////////////////////////////////////////////
  // restore meta to computer
  cmd = "restore -restoretype=compute -workdir=./data -port=" +
        std::to_string(port) + " -origclustername=" + cluster_name;
  cmd += " -origmetaclusterconnstr=" + meta_str +
         " -metaclusterconnstr=" + meta_str + " -shard_map=" + shard_map;
  syslog(Logger::INFO, "job_restore_computer cmd %s", cmd.c_str());

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "restore error " + cmd;
    goto end;
  }
  while (fgets(buf, 512, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    syslog(Logger::INFO, "%s", buf);
  }
  pclose(pfd);

  ////////////////////////////////////////////////////////
  // check error
  if (strstr(buf, "restore Compute successfully") == NULL) {
    syslog(Logger::ERROR, "restore computer error: %s", buf);
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

  job_result = "succeed";
  job_info = "restore compouter succeed";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::INFO, "%s", job_info.c_str());
  return;

end:
  job_result = "error";
  update_jobid_status(job_id, job_result, job_info);
  syslog(Logger::ERROR, "%s", job_info.c_str());
}
#endif

bool Job::job_node_exporter(Json::Value &para, std::string &job_info) {

  FILE* pfd;
	char buf[256];

	std::string cmd, process_id;

  job_info = "node exporter start";
  syslog(Logger::INFO, "%s", job_info.c_str());

	/////////////////////////////////////////////////////////
	// get process_id of node_exporter
	cmd = "netstat -tnpl | grep tcp6 | grep " + std::to_string(prometheus_port_start+1);
	syslog(Logger::INFO, "start_node_exporter cmd %s", cmd.c_str());

	pfd = popen(cmd.c_str(), "r");
	if(!pfd) {
		syslog(Logger::ERROR, "get error %s", cmd.c_str());
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
		syslog(Logger::INFO, "job_restart_prometheus cmd %s", cmd.c_str());

		pfd = popen(cmd.c_str(), "r");
		if(!pfd) {
			syslog(Logger::ERROR, "start error %s", cmd.c_str());
			goto end;
		}
		pclose(pfd);
	}

  job_info = "node exporter succeed";
  syslog(Logger::INFO, "%s", job_info.c_str());
  return true;

end:
  job_info = "node exporter failed";
  syslog(Logger::ERROR, "%s", job_info.c_str());
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
  syslog(Logger::INFO, "%s", job_info.c_str());

  System::get_instance()->set_auto_pullup_working(false);

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

  job_info = "control instance succeed";
  syslog(Logger::INFO, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
  return true;

end:
  syslog(Logger::ERROR, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
  return false;
}

bool Job::job_install_storage(Json::Value &para, std::string &job_info) {

  kunlun::BiodirectPopen *popen_p;
  FILE *stderr_fp;
  char buffer[8192];

  int retry = 9;
  int install_id;
  int port;
  std::string cluster_name, shard_name, ha_mode, ip, user, pwd;
  std::string cmd, program_path, instance_path, file_path;
  MYSQL_CONN mysql_conn;
  std::set<std::string> set_lib;
  Json::FastWriter writer;

  job_info = "install storage start";
  syslog(Logger::INFO, "%s", job_info.c_str());

  cluster_name = para["cluster_name"].asString();
  ha_mode = para["ha_mode"].asString();
  shard_name = para["shard_name"].asString();
  install_id = para["install_id"].asInt();

  Json::Value nodes = para["nodes"];
  Json::Value sub_node = nodes[install_id];
  ip = sub_node["ip"].asString();
  port = sub_node["port"].asInt();

  /////////////////////////////////////////////////////////
  // for install cluster
  if (!job_create_program_path()) {
    job_info = "create_cmd_path error";
    goto end;
  }

  /////////////////////////////////////////////////////////////
  // unzip from program_binaries_path to instance_binaries_path
  program_path =
      program_binaries_path + "/" + storage_prog_package_name + ".tgz";
  instance_path = instance_binaries_path + "/storage/" + std::to_string(port);

  //////////////////////////////
  // check exist instance and kill
  if (access(instance_path.c_str(), F_OK) == 0)
    job_control_storage(port, 1);

  //////////////////////////////
  // mkdir instance_path
  cmd = "mkdir -p " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

start:
  //////////////////////////////
  // rm file in instance_path
  cmd = "rm -rf " + instance_path + "/*";
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
  // tar to instance_path
  cmd = "tar zxf " + program_path + " -C " + instance_path;
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
  syslog(Logger::INFO, "job_install_storage cmd %s", cmd.c_str());

  popen_p = new kunlun::BiodirectPopen(cmd.c_str());
  if (!popen_p->Launch("rw")) {
    job_info = "BiodirectPopen error";
    goto end;
  }
  stderr_fp = popen_p->getReadStdErrFp();
  while (fgets(buffer, 8192, stderr_fp) != nullptr) {
    syslog(Logger::ERROR, "Biopopen: %s", buffer);

    char *p,*q;
		p = strstr(buffer, "error while loading shared libraries");
		if(p == NULL)
			continue;

		p = strstr(p, ":");
		if(p == NULL)
			continue;

		p++;
		while(*p == 0x20)
			p++;

		q = strstr(p, ":");
		if(q == NULL)
			continue;

		set_lib.insert(std::string(p, q-p));
  }
  delete popen_p;
  popen_p = nullptr;

  if (set_lib.size() > 0 && retry-- > 0) {
    job_storage_add_lib(set_lib);
    job_control_storage(port, 1);
    goto start;
  }

  /////////////////////////////////////////////////////////////
  // check instance succeed by connect to instance
  retry = 9;
  user = "pgx";
  pwd = "pgx_pwd";
  while (retry-- > 0) {
    sleep(1);
    if (Instance_info::get_instance()->get_mysql_alive(mysql_conn, ip, port,
                                                       user, pwd))
      break;
  }
  mysql_conn.close_conn();

  if (retry < 0) {
    job_info = "connect storage instance error";
    goto end;
  }

  job_info = "install storage succeed";
  syslog(Logger::INFO, "%s", job_info.c_str());
  return true;

end:
  if(popen_p != nullptr)
    delete popen_p;

  syslog(Logger::INFO, "%s", job_info.c_str());
  return false;
}

bool Job::job_install_computer(Json::Value &para, std::string &job_info) {
 
  kunlun::BiodirectPopen *popen_p;
  FILE *stderr_fp;
  char buffer[8192];

  int retry = 9;
  int install_id;
  int port;
  std::string ip, user, pwd;
  std::string cmd, program_path, instance_path, file_path;
  PGSQL_CONN pgsql_conn;
  std::set<std::string> set_lib;
  Json::FastWriter writer;

  job_info = "install computer start";
  syslog(Logger::INFO, "%s", job_info.c_str());

  install_id = para["install_id"].asInt();
  Json::Value nodes = para["nodes"];
  Json::Value sub_node = nodes[install_id];
  install_id = sub_node["id"].asInt();
  ip = sub_node["ip"].asString();
  port = sub_node["port"].asInt();
  user = sub_node["user"].asString();
  pwd = sub_node["password"].asString();

  /////////////////////////////////////////////////////////
  // for install cluster
  if (!job_create_program_path()) {
    job_info = "create_cmd_path error";
    goto end;
  }

  /////////////////////////////////////////////////////////////
  // unzip from program_binaries_path to instance_binaries_path
  program_path =
      program_binaries_path + "/" + computer_prog_package_name + ".tgz";
  instance_path = instance_binaries_path + "/computer/" + std::to_string(port);

  //////////////////////////////
  // check exist instance and kill
  if (access(instance_path.c_str(), F_OK) == 0)
    job_control_computer(ip, port, 1);

  //////////////////////////////
  // mkdir instance_path
  cmd = "mkdir -p " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

start:
  //////////////////////////////
  // rm file in instance_path
  cmd = "rm -rf " + instance_path + "/*";
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
  // tar to instance_path
  cmd = "tar zxf " + program_path + " -C " + instance_path;
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
  syslog(Logger::INFO, "job_install_computer cmd %s", cmd.c_str());

  popen_p = new kunlun::BiodirectPopen(cmd.c_str());
  if (!popen_p->Launch("rw")) {
    job_info = "BiodirectPopen error";
    goto end;
  }
  stderr_fp = popen_p->getReadStdErrFp();
  while (fgets(buffer, 8192, stderr_fp) != nullptr) {
    syslog(Logger::ERROR, "Biopopen: %s", buffer);

    char *p,*q;
		p = strstr(buffer, "error while loading shared libraries");
		if(p == NULL)
			continue;

		p = strstr(p, ":");
		if(p == NULL)
			continue;

		p++;
		while(*p == 0x20)
			p++;

		q = strstr(p, ":");
		if(q == NULL)
			continue;

		set_lib.insert(std::string(p, q-p));
  }
  delete popen_p;
  popen_p = nullptr;

  if (set_lib.size() > 0 && retry-- > 0) {
    job_computer_add_lib(set_lib);
    job_control_computer(ip, port, 1);
    goto start;
  }

  /////////////////////////////////////////////////////////////
  // check instance succeed by connect to instance
  retry = 6;
  while (retry-- > 0) {
    sleep(1);
    if (Instance_info::get_instance()->get_pgsql_alive(pgsql_conn, ip, port,
                                                       user, pwd))
      break;
  }
  pgsql_conn.close_conn();

  if (retry < 0) {
    job_info = "connect computer instance error";
    goto end;
  }

  job_info = "install computer succeed";
  syslog(Logger::INFO, "%s", job_info.c_str());
  return true;

end:
  if(popen_p != nullptr)
    delete popen_p;
  syslog(Logger::ERROR, "%s", job_info.c_str());
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
  syslog(Logger::INFO, "%s", job_info.c_str());

  ip = para["ip"].asString();
  port = para["port"].asInt();

  System::get_instance()->set_auto_pullup_working(false);
  Instance_info::get_instance()->remove_storage_instance(ip, port);

  /////////////////////////////////////////////////////////////
  // stop storage cmd
  instance_path = instance_binaries_path + "/storage/" + std::to_string(port);
  cmd = "cd " + instance_path + "/" + storage_prog_package_name + "/dba_tools;";
  cmd += "./stopmysql.sh " + std::to_string(port);
  syslog(Logger::INFO, "job_delete_storage cmd %s", cmd.c_str());

  pfd = popen(cmd.c_str(), "r");
  if (!pfd) {
    job_info = "stop error " + cmd;
    goto end;
  }
  while (fgets(buf, 256, pfd) != NULL) {
    // if(strcasestr(buf, "error") != NULL)
    syslog(Logger::INFO, "%s", buf);
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

  job_info = "delete storage succeed";
  syslog(Logger::INFO, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
  return true;

end:
  syslog(Logger::INFO, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
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
  syslog(Logger::INFO, "%s", job_info.c_str());

  ip = para["ip"].asString();
  port = para["port"].asInt();

  System::get_instance()->set_auto_pullup_working(false);
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
    syslog(Logger::INFO, "job_delete_computer cmd %s", cmd.c_str());

    pfd = popen(cmd.c_str(), "r");
    if (!pfd) {
      job_info = "stop error " + cmd;
      goto end;
    }
    while (fgets(buf, 256, pfd) != NULL) {
      // if(strcasestr(buf, "error") != NULL)
      syslog(Logger::INFO, "%s", buf);
    }
    pclose(pfd);
    syslog(Logger::INFO, "stop computer end");

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
  cmd = "rm -rf " + instance_path;
  if (!job_system_cmd(cmd)) {
    job_info = "job_system_cmd error";
    goto end;
  }

  job_info = "delete computer succeed";
  syslog(Logger::INFO, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
  return true;

end:
  syslog(Logger::ERROR, "%s", job_info.c_str());
  System::get_instance()->set_auto_pullup_working(true);
  return false;
}
