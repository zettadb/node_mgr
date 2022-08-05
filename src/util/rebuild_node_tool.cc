/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include <stdio.h>
#include <string>
#include <fstream>
#include <unistd.h>
#include <sys/stat.h>
#include "zettalib/op_mysql.h"
#include "zettalib/tool_func.h"
#include "zettalib/op_mysql.h"
#include "json/json.h"
#include "util_func/error_code.h"
#include "util_func/meta_info.h"

using namespace kunlun;

void Clear_Temp_Data(const std::string& path) {
    std::string buff = "rm -rf "+path+"/*";
    fprintf(stdout, "buff: %s\n", buff.c_str());
    system(buff.c_str());
}

std::string Get_Mysql_Global_Variables(const std::string& host, const std::string& key_name) {
    std::string unix_sock;
    std::string ip = host.substr(0, host.rfind("_"));
    std::string port = host.substr(host.rfind("_")+1);

    kunlun::MysqlConnectionOption option;
    option.ip = ip;
    option.port_num = atoi(port.c_str());
    option.user = "clustmgr";
    option.password = "clustmgr_pwd";
    //option.database = "kunlun_metadata_db";

    kunlun::MysqlConnection mysql_conn(option);
    if (!mysql_conn.Connect()) {
      fprintf(stderr, "connect mysql failed: %s\n", mysql_conn.getErr());
      return unix_sock;
    }

    kunlun::MysqlResult res;
    std::string sql = string_sprintf("show global variables like '%s'", key_name.c_str());
    if(mysql_conn.ExcuteQuery(sql.c_str(), &res) == -1) {
        fprintf(stderr, "execute sql failed: %s\n", mysql_conn.getErr());
        return unix_sock;
    }

    if(res.GetResultLinesNum() != 1) {
        fprintf(stderr, "get mysql global variable failed\n");
        return unix_sock;
    }

    unix_sock = res[0]["Value"];
    return unix_sock;
}

bool Execute_Cmd(const char* buff) {
    fprintf(stdout, "buff: %s\n", buff);

    int rc = 0;
    pid_t kl_pid = fork();
    if(kl_pid < 0) {
        fprintf(stderr, "fork process for execute command failed\n");
        return 1;
    }

    if(kl_pid == 0) {
        const char *argvs[4];
        argvs[0] = "sh";
        argvs[1] = "-c";
        argvs[2] = buff;
        argvs[3] = nullptr;

        /*execv*/
        execv("/bin/sh", (char *const *)(argvs));
    } 
        
    char errinfo[1024] = {0};
    while(true) {
        if(!kunlun::CheckPidStatus(kl_pid, rc, errinfo)) 
            sleep(1);
        else
            break;
    }
    
    if(rc) {
        fprintf(stderr, "%s\n", errinfo);
    }
    return rc;
}

std::string GetXtrabackBinlogInfo(const std::string& base_path) {
    std::string gtid_purged;

    std::string binlog_info = base_path+"/xtrabackup_binlog_info";
    std::ifstream fin(binlog_info.c_str(), std::ios::in);
    if(!fin.is_open()) {
        fprintf(stderr, "open file: %s failed: %d.%s", binlog_info.c_str(), errno, strerror(errno));
        return gtid_purged;
    }

    std::string sbuf, conts;
    while(std::getline(fin, sbuf)) {
        trim(sbuf);
        conts = conts+sbuf;
    }
    
    std::vector<std::string> vec = kunlun::StringTokenize(conts, "\t");
    if(vec.size() <=0)
        return gtid_purged;
    
    int len = vec.size()-1;
    gtid_purged = vec[len];
    return gtid_purged;
}

int main(int argc, char** argv) {
    if(argc < 9) {
        fprintf(stderr, "Usage: %s src_host src_my_conf node_mgr_path pv_limit rb_host rb_datadir rb_logdir rb_waldir pull_host master_host need_backup hdfs_host cluster_name shard_name\n", argv[0]);
        return -1;
    }

    std::string src_host = argv[1];
    std::string src_my_conf = argv[2];
    std::string node_mgr_path=argv[3];
    std::string pvlimit = argv[4];
    std::string rb_host = argv[5];
    std::string datadir = argv[6];
    std::string logdir = argv[7];
    std::string waldir = argv[8];
    std::string pull_host = argv[9];
    std::string master_host = argv[10];
    int need_backup = 0;
    std::string hdfs_host, cluster_name, shard_name;
    if(argc > 11) {
        need_backup = atoi(argv[11]);
        hdfs_host = argv[12];
        cluster_name = argv[13];
        shard_name=argv[14];
    }

    std::string src_unixsock = Get_Mysql_Global_Variables(pull_host, "socket");
    if(src_unixsock.empty()) {
        fprintf(stderr, "get src mysql unix socket failed\n");
        return 1;
    }

    std::string dst_unixsock = Get_Mysql_Global_Variables(rb_host, "socket");
    if(dst_unixsock.empty()) {
        fprintf(stderr, "get dst mysql unix socket failed\n");
        return 1;
    }

    std::string dst_port = rb_host.substr(rb_host.rfind("_")+1);
    std::string xtrabackup_tmp = datadir+"/"+dst_port+"/xtrabackup_tmp";
    if(access(xtrabackup_tmp.c_str(), F_OK) == -1) {
        mkdir(xtrabackup_tmp.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
    std::string backup_tmp = datadir+"/"+dst_port+"/backup_tmp";
    if(access(backup_tmp.c_str(), F_OK) == -1) {
        mkdir(backup_tmp.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
    std::string plugindir = Get_Mysql_Global_Variables(rb_host, "plugin_dir");
    std::string sub_path = plugindir.substr(0, plugindir.rfind("/"));
    plugindir = sub_path;
    sub_path = plugindir.substr(0, plugindir.rfind("/"));
    std::string tool_dir = sub_path.substr(0, sub_path.rfind("/"))+"/dba_tools";

    Clear_Temp_Data(xtrabackup_tmp);

    //2. xtrabackup data
    char buff[10240] = {0};
    snprintf(buff, 10240, "./util/kl_tool --host=%s --command=\"cd %s; ./util/xtrabackup --defaults-file=%s --user=agent --socket=%s -pagent_pwd --kill-long-queries-timeout=20 --stream=xbstream --parallel=4 --compress-threads=4 --backup --no-backup-locks=1 | ./util/lz4 -B4 | ./util/pv --rate-limit=%s\" | ./util/lz4 -d | ./util/xbstream -x -C %s",
                    src_host.c_str(), node_mgr_path.c_str(), src_my_conf.c_str(), src_unixsock.c_str(), pvlimit.c_str(), xtrabackup_tmp.c_str());
    if(Execute_Cmd(buff)) {
        fprintf(stderr, "xtrabackup data failed\n");
        //Clear_Temp_Data(xtrabackup_tmp)
        return 1;
    }

    //3. check xtrabackup data ok
    memset(buff, 0, 10240);
    snprintf(buff, 10240, "./util/xtrabackup --prepare --apply-log-only --target-dir=%s ", xtrabackup_tmp.c_str());
    if(Execute_Cmd(buff)) {
        fprintf(stderr, "xtrabackup data check failed\n");
        //Clear_Temp_Data(xtrabackup_tmp);
        return 1;
    }

    //4. need_backup
    if(need_backup) {
        Clear_Temp_Data(backup_tmp);
        memset(buff, 0, 10240);
        snprintf(buff, 10240, "./util/backup -HdfsNameNodeService=\"hdfs://\"%s -backuptype=storage -clustername=%s -coldstoragetype=hdfs -port=%s -shardname=%s -workdir=%s",
                        hdfs_host.c_str(), cluster_name.c_str(), dst_port.c_str(), shard_name.c_str(), backup_tmp.c_str());
        if(!Execute_Cmd(buff)) {
            fprintf(stderr, "backup mysql data failed\n");
            //Clear_Temp_Data(xtrabackup_tmp);
            //Clear_Temp_Data(backup_tmp);
            return 1;
        }
        Clear_Temp_Data(backup_tmp);
    }

    //5.stop mysqld and check data
    // stop node_mgr auto_pullup
    memset(buff, 0, 10240);
    snprintf(buff, 10240, "./util/safe_killmysql %s %s", dst_port.c_str(), datadir.c_str());
    if(Execute_Cmd(buff)) {
        fprintf(stderr, "kill mysql process failed\n");
        return 1;
    }

    std::string dst_my_conf = datadir+"/"+dst_port+"/data/"+dst_port+".cnf";
    std::string back_cnf_cmd = string_sprintf("cp %s %s", dst_my_conf.c_str(), 
                        (datadir+"/"+dst_port).c_str());
    system(back_cnf_cmd.c_str());
    std::string back_auto_cmd = string_sprintf("cp %s %s", (datadir+"/"+dst_port+"/data/mysqld-auto.cnf").c_str(), (datadir+"/"+dst_port).c_str());
    system(back_auto_cmd.c_str());

    //std::string rm_path = dsub_path+dst_port+"/dbdata_raw/data";
    //Clear_Temp_Data(datadir);
    std::string rm_path = datadir+"/"+dst_port+"/data";
    Clear_Temp_Data(rm_path);
    rm_path = waldir+"/"+dst_port+"/redolog";
    Clear_Temp_Data(rm_path);
    rm_path = logdir+"/"+dst_port+"/relay";
    Clear_Temp_Data(rm_path);
    rm_path = logdir+"/"+dst_port+"/binlog";
    Clear_Temp_Data(rm_path);

    std::string tmp_dst_conf = datadir+"/"+dst_port+"/"+dst_port+".cnf";
    //6.recover data
    memset(buff, 0, 10240);
    snprintf(buff, 10240, "./util/xtrabackup --defaults-file=%s --user=agent --pagent_pwd --copy-back --target-dir=%s",
                    tmp_dst_conf.c_str(), xtrabackup_tmp.c_str());
    if(Execute_Cmd(buff)) {
        fprintf(stderr, "recover xtrabackup data failed\n");
        return 1;
    }

    std::string gtid_purged = GetXtrabackBinlogInfo(xtrabackup_tmp);
    if(gtid_purged.empty()) {
        fprintf(stderr, "get xtrabackup gtid position failed\n");
        return 1;
    } else
        Clear_Temp_Data(xtrabackup_tmp);

    back_cnf_cmd = string_sprintf("mv %s %s", tmp_dst_conf.c_str(),
                            (datadir+"/"+dst_port+"/data/").c_str());
    system(back_cnf_cmd.c_str());

    back_auto_cmd = string_sprintf("mv %s %s", (datadir+"/"+dst_port+"/mysqld-auto.cnf").c_str(),
                (datadir+"/"+dst_port+"/data").c_str());
    system(back_auto_cmd.c_str());
    //7. start mysql
    memset(buff, 0, 10240);
    snprintf(buff, 10240, "cd %s; ./startmysql.sh %s", tool_dir.c_str(), dst_port.c_str());
    if(Execute_Cmd(buff)) {
        fprintf(stderr, "start mysql failed\n");
        return 1;
    }
    sleep(2);

    //8. build sync
    MysqlConnectionOption option;
    option.connect_type = ENUM_SQL_CONNECT_TYPE::UNIX_DOMAIN_CONNECTION;
    option.user = "agent";
    option.password = "agent_pwd";
    option.file_path = dst_unixsock;

    MysqlConnection *mysql_conn = nullptr;
    for(int i=0; i<10; i++) {
        mysql_conn = new MysqlConnection(option);
        if(!mysql_conn->Connect()) {
            fprintf(stderr, "connect mysql unix failed: %s", mysql_conn->getErr());
            delete mysql_conn;
            mysql_conn = nullptr;
            sleep(2);
        } else 
            break;
    }

    if(!mysql_conn) {
        fprintf(stderr, "connect mysql unix socket failed\n");
        return -1;
    }

    std::string master_ip = master_host.substr(0, master_host.rfind("_"));
    std::string master_port = master_host.substr(master_host.rfind("_")+1);
    int change_flag = 0;
    for(int i=0; i < 10; i++) {
        memset(buff, 0, 10240);
        snprintf(buff, 10240, "reset master; set global gtid_purged='%s'; stop slave; reset slave all; change master to  MASTER_AUTO_POSITION = 1,  MASTER_HOST='%s' , MASTER_PORT=%s, MASTER_USER='repl' , MASTER_PASSWORD='repl_pwd',MASTER_CONNECT_RETRY=1 ,MASTER_RETRY_COUNT=1000 , MASTER_HEARTBEAT_PERIOD=10 for CHANNEL 'kunlun_repl'; start slave for CHANNEL 'kunlun_repl';",
                    gtid_purged.c_str(), master_ip.c_str(), master_port.c_str());

        fprintf(stdout, "rebuild sync sql: %s\n", buff);
        MysqlResult res;
        int ret = mysql_conn->ExcuteQuery(buff, &res);
        if(ret == -1) {
            sleep(2);
        } else {
            change_flag = 1;
            break;
        }
    }

    if(!change_flag) {
        fprintf(stderr, "connect mysql execut sql failed: %s\n", mysql_conn->getErr());
        return -1;
    }

    return 0;
}