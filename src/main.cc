/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "config.h"
#include "global.h"
#include "job.h"
//#include "log.h"
#include "zettalib/op_log.h"
#include "mysql/mysql.h"
#include "os.h"
#include "server_http/server_http.h"
#include "sys.h"
#include "sys_config.h"
#include "thread_manager.h"
#include "zettalib/proc_env.h"
#include "zettalib/tool_func.h"
#include <signal.h>
#include <unistd.h>
#include <fstream>
#include <sys/resource.h>
#include "gflags/gflags.h"
#include "util_func/meta_info.h"

extern int g_exit_signal;
extern int64_t thread_work_interval;
std::string tcp_server_file;
int64_t node_mgr_tcp_port;
extern std::string local_ip;
std::string pid_file = "./.kl_server.pid";

void WriteChildPid(int pid) {
  char buf[8] = {0};
  int fd = open(pid_file.c_str(), O_RDWR|O_CREAT, S_IRUSR | S_IWUSR);
  if(fd < 0) {
    KLOG_ERROR("can't open kl_server.pid file {} failed. {}.{}", pid_file, errno, strerror(errno));
    return;
  }

  ftruncate(fd, 0);
  snprintf(buf, 8, "%d", pid);
  write(fd, buf, strlen(buf)+1);
  close(fd);
  return;
}

std::string GetFileContent(const std::string& path) {
  std::string cont;
  std::ifstream fin(path.c_str(), std::ios::in);
  if(!fin.is_open()) {
    KLOG_ERROR("open file: {} failed: {}.{}", path, errno, strerror(errno));
    return cont;
  }

  std::string sbuf;
  while(std::getline(fin, sbuf)) {
    trim(sbuf);
    cont = cont+sbuf;
  }
  return cont;
}

void WaitKlServerPid(int pid) {
  int status;
  char errinfo[1024] = {0};
  for(int i=0; i<50; i++) {
    if(kunlun::CheckPidStatus(pid, status, errinfo))
      break;
    sleep(1);
  }
}

bool CheckChildPidExist(int& pid) {
  std::string spid = GetFileContent(pid_file);
  if(spid.empty())
    return false;
  pid = atoi(spid.c_str());
  
  std::string proc_file = string_sprintf("/proc/%s/cmdline", spid.c_str());
  std::string sbuf = GetFileContent(proc_file);
  if(sbuf.empty()) {
    WaitKlServerPid(pid);
    return false;
  }
  
  if(sbuf.find("./kl_server") == std::string::npos)
    return false;

  return true;
}

void start_kl_server() {
  std::string cmd;
  pid_t kl_pid;
  int rc = 0;
  char errinfo[1024] = {0};
  char *buffer;
	if((buffer = getcwd(NULL, 0)) == NULL) {
		KLOG_ERROR("get current work direct failed");
		return;
	}

  while(true) {
    if(!CheckChildPidExist(kl_pid)) {
      KLOG_ERROR("kl server pid {} is unalive, so restart again", kl_pid);
      cmd = string_sprintf("export LD_PRELOAD=%s/../lib/libjemalloc.so.3.6.0; ./kl_server --base_path=%s --localip=%s --listen_port=%ld --conf_file=%s",
              buffer, buffer, local_ip.c_str(), node_mgr_tcp_port, tcp_server_file.c_str());
      
      kl_pid = fork();
      if(kl_pid < 0) {
        KLOG_ERROR("fork process for kl server failed\n");
        return;
      }

      if(kl_pid == 0) {
        const char *argvs[4];
        argvs[0] = "sh";
        argvs[1] = "-c";
        argvs[2] = cmd.c_str();
        argvs[3] = nullptr;

        /*execv*/
        execv("/bin/sh", (char *const *)(argvs));
      }

      WriteChildPid(kl_pid);
    } 

    sleep(10);
  }
}

int main(int argc, char **argv) {
  if (argc != 2) {
    printf("\nUsage: node_mgr node_mgr.cnf\n");
    return 1;
  }

  kunlun::procDaemonize();
  kunlun::procInvokeKeepalive();

  google::ParseCommandLineFlags(&argc, &argv, true);
  google::SetCommandLineOption("defer_close_second", "3600");
  google::SetCommandLineOption("idle_timeout_second", "3600");
  google::SetCommandLineOption("log_connection_close", "true");
  google::SetCommandLineOption("log_idle_connection_close", "true");
  
  if (System::create_instance(argv[1])) {
    return 1;
  }

  std::thread kserver_th(start_kl_server);
  kserver_th.detach();

  Thread main_thd;
  brpc::Server *httpServer = NewHttpServer();
  if (httpServer == nullptr) {
    fprintf(stderr, "node manager start faild");
    return 1;
  }

  // httpServer->RunUntilAskedToQuit();

  // while (!Thread_manager::do_exit)
  //{
  //   if (Job::get_instance()->check_timestamp())
  //   {
  //     Instance_info::get_instance()->get_local_instance();
  //     break;
  //   }

  //  syslog(Logger::ERROR, "The time of node_mgr is different to
  //  cluster_mgr!"); Thread_manager::get_instance()->sleep_wait(&main_thd,
  //                                             thread_work_interval * 1000);
  //}

  Instance_info::get_instance()->get_local_instance();
  //int retcode = 0;
  //char errmsg[4096] = {0};

  System::get_instance()->keepalive_exporter();

  while (!Thread_manager::do_exit) {
    if (System::get_instance()->get_auto_pullup_working()) {
      System::get_instance()->keepalive_instance();
    }

    Thread_manager::get_instance()->sleep_wait(&main_thd,
                                               thread_work_interval * 1000);
  }

  if (g_exit_signal)
    KLOG_INFO("Instructed to exit by signal {}.", g_exit_signal);
  else
    KLOG_INFO("Exiting because of internal error.");

  delete System::get_instance();
}


