/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "rebuild_node.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include "zettalib/op_mysql.h"
#include "instance_info.h"
#include "util_func/error_code.h"
#include "util_func/meta_info.h"
#include "json/json.h"
#include <stdio.h>
#include <string>
#include <fstream>
#include <unistd.h>
#include <sys/stat.h>

namespace kunlun
{

bool CRbNode::Run() {
    bool ret = true;
    KLOG_INFO("start rebuild host {} node job_id {}", rb_host_, job_id_);
    KLOG_INFO("rb host step: check params");
    if(!PrepareParams()) {
        ret = false;
        UpdateStatRecord();
        return ret;
    }
    KLOG_INFO("rb host step: xtraback data");
    UpdateStatRecord();
    if(!XtrabackData()) {
        ret = false;
        UpdateStatRecord();
        return ret;
    }
    KLOG_INFO("rb host step: check xtraback data");
    UpdateStatRecord();
    if(!CheckXtrabackData()) {
        ret = false;
        UpdateStatRecord();
        return ret;
    }
    KLOG_INFO("rb host step: backup old data");
    UpdateStatRecord();
    if(!BackupOldData()) {
        ret = false;
        UpdateStatRecord();
        return ret;
    }
    KLOG_INFO("rb host step: clear old data");
    UpdateStatRecord();
    if(!ClearOldData()) {
        ret = false;
        UpdateStatRecord();
        return ret;
    }
    KLOG_INFO("rb host step: recover data");
    UpdateStatRecord();
    if(!RecoverXtrabackData()) {
        ret = false;
        UpdateStatRecord();
        return ret;
    }
    KLOG_INFO("rb host step: rebuild sync");
    UpdateStatRecord();
    if(!RebuildSync()){
        ret = false;
        UpdateStatRecord();
        return ret;
    }
    step_ = "done";
    KLOG_INFO("rb host ok");
    UpdateStatRecord();
    return ret;
}   

bool CRbNode::PrepareParams() {
    step_ = "check_param";
    std::string hostaddr = pull_host_.substr(0, pull_host_.rfind("_"));
    std::string port = pull_host_.substr(pull_host_.rfind("_")+1);
    MysqlResult result;
    std::string sql = string_sprintf("select datadir, nodemgr_tcp_port, nodemgr_bin_path from %s.server_nodes where hostaddr='%s' and machine_type='storage'",
                KUNLUN_METADATA_DB_NAME, hostaddr.c_str());
    KLOG_INFO("get hostaddr: {} datadir and nodemgr bin path sql: {}", hostaddr, sql);
    bool ret = Instance_info::get_instance()->send_stmt(sql.c_str(), &result);
    if(!ret) {
        KLOG_ERROR("meta conn execute sql failed: {}", Instance_info::get_instance()->getErr());
        error_code_ = RB_GET_PULLHOST_PARAM_FROM_METADB_ERROR;
        return false;
    }

    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get host: {} record too many in server_nodes", hostaddr);
        error_code_ = RB_GET_PULLHOST_PARAM_FROM_METADB_ERROR;
        return false;
    }

    nodemgr_tcp_port_ = result[0]["nodemgr_tcp_port"];
    nodemgr_bin_path_ = result[0]["nodemgr_bin_path"];
    std::string datadir = result[0]["datadir"];
    pull_etcfile_ = datadir+"/"+port+"/data/"+port+".cnf";

    pull_unixsock_ = GetMysqlGlobalVariables(pull_host_, "socket");
    if(pull_unixsock_.empty()) {
        KLOG_ERROR("get pull_host {} unix socket failed", pull_host_);
        error_code_ = RB_NODE_PULL_MYSQL_UNIX_SOCK_ERR;
        return false;
    }

    std::string rb_hostaddr = rb_host_.substr(0, rb_host_.rfind("_"));
    std::string rb_port = rb_host_.substr(rb_host_.rfind("_")+1);
    sql = string_sprintf("select datadir, logdir, wal_log_dir from %s.server_nodes where hostaddr='%s' and machine_type='storage'",
                KUNLUN_METADATA_DB_NAME, rb_hostaddr.c_str());
    KLOG_INFO("get hostaddr: {} datadir, logdir, wal_logdir sql: {}", rb_hostaddr, sql);
    ret = Instance_info::get_instance()->send_stmt(sql.c_str(), &result);
    if(!ret) {
        KLOG_ERROR("meta conn execute sql failed: {}", Instance_info::get_instance()->getErr());
        error_code_ = RB_GET_RBHOST_PARAM_FROM_METADB_ERROR;
        return false;
    }

    if(result.GetResultLinesNum() != 1) {
        KLOG_ERROR("get host: {} record too many in server_nodes", rb_hostaddr);
        error_code_ = RB_GET_RBHOST_PARAM_FROM_METADB_ERROR;
        return false;
    }
    rb_datadir_ = result[0]["datadir"];
    rb_logdir_ = result[0]["logdir"];
    rb_wallogdir_ = result[0]["wal_log_dir"];
    rb_etcfile_ = rb_datadir_+"/"+rb_port+"/data/"+rb_port+".cnf";
    tmp_etcfile_ = rb_datadir_+"/"+rb_port+"/"+rb_port+".cnf";

    rb_unixsock_ = GetMysqlGlobalVariables(rb_host_, "socket");
    if(rb_unixsock_.empty()) {
        KLOG_ERROR("get rb_host {} unix socket failed", rb_host_);
        error_code_ = RB_NODE_RBUILD_MYSQL_UNIX_SOCK_ERR;
        return false;
    }

    std::string plugindir = GetMysqlGlobalVariables(rb_host_, "plugin_dir");
    std::string sub_path = plugindir.substr(0, plugindir.rfind("/"));
    plugindir = sub_path;
    sub_path = plugindir.substr(0, plugindir.rfind("/"));
    tool_dir_ = sub_path.substr(0, sub_path.rfind("/"))+"/dba_tools";

    xtrabackup_tmp_ = rb_datadir_+"/"+rb_port+"/xtrabackup_tmp";
    if(access(xtrabackup_tmp_.c_str(), F_OK) == -1) {
        mkdir(xtrabackup_tmp_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }

    backup_tmp_ = rb_datadir_+"/"+rb_port+"/backup_tmp";
    if(access(backup_tmp_.c_str(), F_OK) == -1) {
        mkdir(backup_tmp_.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
    }
    return true;
}

bool CRbNode::XtrabackData() {
    step_ = "xtracback_data";
    ClearTempData(xtrabackup_tmp_);
    std::string hostaddr = pull_host_.substr(0, pull_host_.rfind("_"));
    std::string kl_host = hostaddr+":"+nodemgr_tcp_port_;
    std::string xtra_cmd = string_sprintf("./util/kl_tool --host=%s --command=\"cd %s; ./util/xtrabackup --defaults-file=%s --user=agent --socket=%s -pagent_pwd --kill-long-queries-timeout=20 --stream=xbstream --parallel=4 --compress-threads=4 --backup --no-backup-locks=1 | ./util/lz4 -B4 | ./util/pv --rate-limit=%d\" | ./util/lz4 -d | ./util/xbstream -x -C %s > ../log/rebuild_node_tool_%s.log 2>&1",
            kl_host.c_str(), nodemgr_bin_path_.c_str(), pull_etcfile_.c_str(), pull_unixsock_.c_str(),
            pvlimit_, xtrabackup_tmp_.c_str(), job_id_.c_str());
    KLOG_INFO("xtraback cmd: {}", xtra_cmd);
    if(ExecuteCmd(xtra_cmd.c_str())) {
        error_code_ = RB_XTRACBACK_DATA_FROM_PULL_HOST_ERR;
        return false;
    }
    return true;
}

bool CRbNode::CheckXtrabackData() {
    step_ = "checksum_data";
    std::string cmd = string_sprintf("./util/xtrabackup --prepare --apply-log-only --target-dir=%s >> ../log/rebuild_node_tool_%s.log 2>&1",
                            xtrabackup_tmp_.c_str(), job_id_.c_str());
    KLOG_INFO("checksum data cmd: {}", cmd);
    if(ExecuteCmd(cmd.c_str())) {
        error_code_ = RB_CHECKSUM_DATA_FROM_PULL_HOST_ERR;
        return false;
    }
    return true;
}

bool CRbNode::BackupOldData() {
    step_ = "backup_old_data";
    if(need_backup_) {
        ClearTempData(backup_tmp_);
        std::string port = rb_host_.substr(rb_host_.rfind("_")+1);
        std::string cmd = string_sprintf("./util/backup -HdfsNameNodeService=\"hdfs://\"%s -backuptype=storage -clustername=%s -coldstoragetype=hdfs -port=%s -shardname=%s -workdir=%s >> ../log/rebuild_node_tool_%s.log 2>&1",
                            hdfs_host_.c_str(), cluster_name_.c_str(), port.c_str(), shard_name_.c_str(), backup_tmp_.c_str(),
                            job_id_.c_str());
        KLOG_INFO("backup data cmd: {}", cmd);
        if(ExecuteCmd(cmd.c_str())) {
            error_code_ = RB_BACKUP_REBUILD_NODE_DATA_ERR;
            return false;
        }
        ClearTempData(backup_tmp_);
    }
    return true;
}

bool CRbNode::ClearOldData() {
    step_ = "clear_old_data";
    std::string rb_port = rb_host_.substr(rb_host_.rfind("_")+1);
    std::string cmd = string_sprintf("./util/safe_killmysql %s %s >> ../log/rebuild_node_tool_%s.log 2>&1",
            rb_port.c_str(), rb_datadir_.c_str(), job_id_.c_str());
    KLOG_INFO("stop mysql cmd: {}", cmd);
    if(ExecuteCmd(cmd.c_str())) {
        KLOG_ERROR("stop mysql failed");
        error_code_ = RB_CLEARUP_REBUILD_NODE_DATA_ERR;
        return false;
    }
    std::string back_cnf_cmd = string_sprintf("cp %s %s", rb_etcfile_.c_str(), (rb_datadir_+"/"+rb_port).c_str());
    system(back_cnf_cmd.c_str());
    std::string back_auto_cmd = string_sprintf("cp %s %s", (rb_datadir_+"/"+rb_port+"/data/mysqld-auto.cnf").c_str(),
                            (rb_datadir_+"/"+rb_port).c_str());
    system(back_auto_cmd.c_str());

    std::string rm_path = rb_datadir_+"/"+rb_port+"/data";
    ClearTempData(rm_path);
    rm_path = rb_wallogdir_+"/"+rb_port+"/redolog";
    ClearTempData(rm_path);
    rm_path = rb_logdir_+"/"+rb_port+"/relay";
    ClearTempData(rm_path);
    rm_path = rb_logdir_+"/"+rb_port+"/binlog";
    ClearTempData(rm_path);
    return true;
}

bool CRbNode::RecoverXtrabackData() {
    step_ = "recover_data";
    std::string rb_port = rb_host_.substr(rb_host_.rfind("_")+1);
    std::string cmd = string_sprintf("./util/xtrabackup --defaults-file=%s --user=agent --pagent_pwd --copy-back --target-dir=%s >> ../log/rebuild_node_tool_%s.log 2>&1",
                    tmp_etcfile_.c_str(), xtrabackup_tmp_.c_str(), job_id_.c_str());
    KLOG_INFO("recover data cmd: {}", cmd);
    if(ExecuteCmd(cmd.c_str())) {
        error_code_ = RB_XTRACBACKUP_RECOVER_DATA_ERR;
        return false;
    }

    gtid_purged_ = GetXtrabackBinlogInfo();
    if(gtid_purged_.empty()) {
        error_code_ = RB_GET_XTRABACKUP_GIT_POSITION_ERR;
        return false;
    }

    ClearTempData(xtrabackup_tmp_);
    std::string back_cnf_cmd = string_sprintf("mv %s %s", tmp_etcfile_.c_str(),
                            (rb_datadir_+"/"+rb_port+"/data/").c_str());
    system(back_cnf_cmd.c_str());
    std::string back_auto_cmd = string_sprintf("mv %s %s", (rb_datadir_+"/"+rb_port+"/mysqld-auto.cnf").c_str(),
                            (rb_datadir_+"/"+rb_port+"/data/").c_str());
    system(back_auto_cmd.c_str());
    return true;
}

bool CRbNode::RebuildSync() {
    step_ = "rebuild_sync";
    std::string rb_port = rb_host_.substr(rb_host_.rfind("_")+1);
    std::string cmd = string_sprintf("cd %s; ./startmysql.sh %s", 
                    tool_dir_.c_str(), rb_port.c_str());
    KLOG_INFO("start mysql cmd: {}", cmd);
    if(ExecuteCmd(cmd.c_str())) {
        KLOG_ERROR("start mysql failed");
        error_code_ = RB_START_MYSQL_ERR;
        return false;
    }
    sleep(2);

    //8. build sync
    MysqlConnectionOption option;
    option.connect_type = ENUM_SQL_CONNECT_TYPE::UNIX_DOMAIN_CONNECTION;
    option.user = "agent";
    option.password = "agent_pwd";
    option.file_path = rb_unixsock_;

    MysqlConnection *mysql_conn = nullptr;
    for(int i=0; i<10; i++) {
        mysql_conn = new MysqlConnection(option);
        if(!mysql_conn->Connect()) {
            KLOG_ERROR("connect mysql unix failed: {}", mysql_conn->getErr());
            delete mysql_conn;
            mysql_conn = nullptr;
            sleep(2);
        } else 
            break;
    }

    if(!mysql_conn) {
        KLOG_ERROR("connect mysql unix socket failed");
        error_code_ = RB_CONNECT_RECOVER_MYSQL_UNIX_ERR;
        return false;
    }

    std::string master_ip = master_host_.substr(0, master_host_.rfind("_"));
    std::string master_port = master_host_.substr(master_host_.rfind("_")+1);
    int change_flag = 0;
    for(int i=0; i < 10; i++) {
        std::string sql = string_sprintf("reset master; set global gtid_purged='%s'; stop slave; reset slave all; change master to  MASTER_AUTO_POSITION = 1,  MASTER_HOST='%s' , MASTER_PORT=%s, MASTER_USER='repl' , MASTER_PASSWORD='repl_pwd',MASTER_CONNECT_RETRY=1 ,MASTER_RETRY_COUNT=1000 , MASTER_HEARTBEAT_PERIOD=10 for CHANNEL 'kunlun_repl'; start slave for CHANNEL 'kunlun_repl'",
                    gtid_purged_.c_str(), master_ip.c_str(), master_port.c_str());
        KLOG_INFO("rebuild sync sql: {}", sql);
        MysqlResult res;
        int ret = mysql_conn->ExcuteQuery(sql.c_str(), &res);
        if(ret == -1) {
            KLOG_ERROR("rebuild sync sql {} failed {}", sql, mysql_conn->getErr());
            sleep(2);
        } else {
            change_flag = 1;
            break;
        }
    }

    if(!change_flag) {
        KLOG_ERROR("connect mysql execut sql failed: {}", mysql_conn->getErr());
        error_code_ = RB_CHANGE_MASTER_SQL_EXECUTE_ERR;
        return false;
    }
    return true;
}

void CRbNode::UpdateStatRecord() {
    Json::Value doc;
    doc["error_code"] = GlobalErrorNum(error_code_).EintToStr();
    doc["error_info"] = GlobalErrorNum(error_code_).get_err_num_str();
    doc["step"] = step_;

    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string memo = writer.write(doc);

    std::string status = "ongoing";
    if(error_code_)
        status = "failed";
    else {
        if(step_ == "done")
            status = "done";
    }
    
    std::string sql = string_sprintf("update kunlun_metadata_db.cluster_general_job_log set status = '%s', when_ended = CURRENT_TIMESTAMP(6) , memo = '%s' where id = %s",
            status.c_str(), memo.c_str(), job_id_.c_str());

    MysqlResult result;
    bool ret = Instance_info::get_instance()->send_stmt(sql.c_str(), &result);
    if(!ret) {
        KLOG_ERROR("update cluster_general_job_log sql: {} failed {}", sql, 
                    Instance_info::get_instance()->getErr());
        return;
    }
    KLOG_INFO("update cluster_general_job_log sql: {} success", sql); 
}

int CRbNode::ExecuteCmd(const char* buff) {
    int rc = 0;
    pid_t kl_pid = fork();
    if(kl_pid < 0) {
        KLOG_ERROR("fork process for execute command failed, {}.{}", errno, strerror(errno));
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
        if(!CheckPidStatus(kl_pid, rc, errinfo)) 
            sleep(1);
        else
            break;
    }
    
    if(rc) {
        //error_info_ = string_sprintf("%s", errinfo);
        KLOG_ERROR("execute cmd failed {}", errinfo);
    }
    return rc;
}

void CRbNode::ClearTempData(const std::string& path) {
    std::string buff = "rm -rf "+path+"/*";
    KLOG_INFO(" clear dir: {} data", buff);
    system(buff.c_str());
}

std::string CRbNode::GetMysqlGlobalVariables(const std::string& host, const std::string& key_name) {
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
      KLOG_ERROR("connect mysql failed: {}", mysql_conn.getErr());
      return unix_sock;
    }

    MysqlResult res;
    std::string sql = string_sprintf("show global variables like '%s'", key_name.c_str());
    if(mysql_conn.ExcuteQuery(sql.c_str(), &res) == -1) {
        KLOG_ERROR("execute sql failed: {}", mysql_conn.getErr());
        return unix_sock;
    }

    if(res.GetResultLinesNum() != 1) {
        KLOG_ERROR("get mysql global variable failed");
        return unix_sock;
    }

    unix_sock = res[0]["Value"];
    return unix_sock;
}

std::string CRbNode::GetXtrabackBinlogInfo() {
    std::string gtid_purged;

    std::string binlog_info = xtrabackup_tmp_+"/xtrabackup_binlog_info";
    std::ifstream fin(binlog_info.c_str(), std::ios::in);
    if(!fin.is_open()) {
        KLOG_ERROR("open file: {} failed: {}.{}", binlog_info, errno, strerror(errno));
        return gtid_purged;
    }

    std::string sbuf, conts;
    while(std::getline(fin, sbuf)) {
        trim(sbuf);
        conts = conts+sbuf;
    }
    
    KLOG_INFO("xtrabackup_binlog_info content: {}", conts);
    std::vector<std::string> vec = kunlun::StringTokenize(conts, "\t");
    if(vec.size() <=0)
        return gtid_purged;
    
    int len = vec.size()-1;
    gtid_purged = vec[len];
    return gtid_purged;
}

} // namespace kunlun
