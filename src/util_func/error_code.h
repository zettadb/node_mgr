/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _NODE_MGR_ERROR_CODE_H_
#define _NODE_MGR_ERROR_CODE_H_
#include <string>

namespace kunlun 
{

#define EERROR_NUM_START (60000)

#define EOFFSET(offset) (EERROR_NUM_START) + (offset)

#define EOK 0
#define EFAIL EOFFSET(1)
#define EIVALID_REQUEST_PROTOCAL EOFFSET(1)
#define EIVALID_RESPONSE_PROTOCAL EOFFSET(2)

#define RB_NODE_PULL_MYSQL_UNIX_SOCK_ERR   EOFFSET(3)
#define RB_GET_REBUILD_NODE_DIR_ERR   EOFFSET(4)
#define RB_NODE_RBUILD_MYSQL_UNIX_SOCK_ERR   EOFFSET(5)
#define RB_XTRACBACK_DATA_FROM_PULL_HOST_ERR   EOFFSET(6)
#define RB_CHECKSUM_DATA_FROM_PULL_HOST_ERR   EOFFSET(7)
#define RB_BACKUP_REBUILD_NODE_DATA_ERR   EOFFSET(8)
#define RB_CLEARUP_REBUILD_NODE_DATA_ERR   EOFFSET(9)
#define RB_XTRACBACKUP_RECOVER_DATA_ERR   EOFFSET(10)
#define RB_START_MYSQL_ERR   EOFFSET(11)
#define RB_CONNECT_RECOVER_MYSQL_UNIX_ERR   EOFFSET(12)
#define RB_CHANGE_MASTER_SQL_EXECUTE_ERR   EOFFSET(13)
#define RB_GET_XTRABACKUP_GIT_POSITION_ERR   EOFFSET(14)
#define RB_GET_PULLHOST_PARAM_FROM_METADB_ERROR EOFFSET(15)
#define RB_GET_RBHOST_PARAM_FROM_METADB_ERROR   EOFFSET(16)

// Defined in error_code.cc
extern const char *g_error_num_str[];

class GlobalErrorNum {
public:
  GlobalErrorNum() { err_num_ = EOK; }
  GlobalErrorNum(int err_num) { err_num_ = err_num; }
  int get_err_num() { return err_num_; }
  void set_err_num(int errnum) { err_num_ = errnum; }
  const char *get_err_num_str();
  bool Ok() { return err_num_ == EOK; }
  std::string EintToStr(int errnum) {
      return std::to_string(errnum);
  }
  std::string EintToStr() {
      return std::to_string(err_num_);
  }

private:
  int err_num_;
};

}
#endif