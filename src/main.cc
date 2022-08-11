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


