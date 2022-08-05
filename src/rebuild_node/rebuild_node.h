/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _NODE_MGR_REBUILD_NODE_H_
#define _NODE_MGR_REBUILD_NODE_H_
#include "zettalib/errorcup.h"
#include <string>

namespace kunlun
{

class CRbNode : public ErrorCup {
public:
    CRbNode(const std::string& rb_host, int pvlimit, const std::string& pull_host, const std::string& master_host,
            int need_backup, const std::string& hdfs_host, const std::string& cluster_name, const std::string& shard_name,
            const std::string& job_id) : rb_host_(rb_host), pvlimit_(pvlimit), pull_host_(pull_host), 
            master_host_(master_host), need_backup_(need_backup), hdfs_host_(hdfs_host), 
            cluster_name_(cluster_name), shard_name_(shard_name), job_id_(job_id), error_code_(0) {}

    virtual ~CRbNode() {}
    bool Run();
    
private:
    bool PrepareParams();
    bool XtrabackData();
    bool CheckXtrabackData();
    bool BackupOldData();
    bool ClearOldData();
    bool RecoverXtrabackData();
    bool RebuildSync();
    void UpdateStatRecord();

    int ExecuteCmd(const char* buff);
    void ClearTempData(const std::string& path);
    std::string GetMysqlGlobalVariables(const std::string& host, const std::string& key_name);
    std::string GetXtrabackBinlogInfo();
private:
    std::string rb_host_;
    int pvlimit_;
    std::string pull_host_;
    std::string master_host_;
    int need_backup_;
    std::string hdfs_host_;
    std::string cluster_name_;
    std::string shard_name_;
    std::string job_id_;
    int error_code_;
    int error_info_;
    std::string step_;

    std::string nodemgr_tcp_port_;
    std::string nodemgr_bin_path_;
    std::string pull_etcfile_;
    std::string pull_unixsock_;
    std::string rb_unixsock_;
    std::string xtrabackup_tmp_;
    std::string backup_tmp_;
    std::string rb_datadir_;
    std::string rb_logdir_;
    std::string rb_wallogdir_;
    std::string rb_etcfile_;
    std::string tmp_etcfile_;
    std::string gtid_purged_;
    std::string tool_dir_;
};

}

#endif