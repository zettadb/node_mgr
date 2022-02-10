/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "config.h"
#include "global.h"
#include "job.h"
#include "log.h"
#include "os.h"
#include "server_http/server_http.h"
#include "sys.h"
#include "sys_config.h"
#include "thread_manager.h"
#include "zettalib/proc_env.h"
#include "mysql/mysql.h"
#include <signal.h>
#include <unistd.h>

extern int g_exit_signal;
extern int64_t thread_work_interval;

int main(int argc, char **argv)
{
  if (argc != 2)
  {
    printf("\nUsage: node_mgr node_mgr.cnf\n");
    return 1;
  }

  // kunlun::procDaemonize();
  // kunlun::procInvokeKeepalive();
  mysql_server_init(0, NULL, NULL);

  if (System::create_instance(argv[1]))
  {
    return 1;
  }

  Thread main_thd;
  brpc::Server *httpServer = NewHttpServer();
  if (httpServer == nullptr)
  {
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

  //  syslog(Logger::ERROR, "The time of node_mgr is different to cluster_mgr!");
  //  Thread_manager::get_instance()->sleep_wait(&main_thd,
  //                                             thread_work_interval * 1000);
  //}

  while (!Thread_manager::do_exit)
  {
    if (System::get_instance()->get_auto_pullup_working())
      System::get_instance()->keepalive_instance();

    Thread_manager::get_instance()->sleep_wait(&main_thd,
                                               thread_work_interval * 1000);
  }

  if (g_exit_signal)
    syslog(Logger::INFO, "Instructed to exit by signal %d.", g_exit_signal);
  else
    syslog(Logger::INFO, "Exiting because of internal error.");

  delete System::get_instance();
}
