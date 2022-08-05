/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "error_code.h"

namespace kunlun
{

const char *g_error_num_str[] = {
  "OK",                                       /*EOFFSET(0)*/
  "Invalid Request Protocal",                 /*EOFFSET(1)*/
  "Invalid Response Protocal",                /*EOFFSET(2)*/
  "Get pull mysql unix socket error",           /*EOFFSET(3)*/
  //"Read rebuild node mysql conf file error"           /*EOFFSET(4)*/
  "Get rebuild node directory parameter error",          /*EOFFSET(4)*/
  "Get rebuild node mysql unix socket error",           /*EOFFSET(5)*/
  "Xtracback data from pull host error",           /*EOFFSET(6)*/
  "Checksum data from pull host error",           /*EOFFSET(7)*/
  "Backup rebuild node data error",           /*EOFFSET(8)*/
  "Cleanup rebuild node data error",           /*EOFFSET(9)*/
  "Xtracback recover data error",           /*EOFFSET(10)*/
  "Restart recover mysql error",           /*EOFFSET(11)*/
  "Connect recover mysql by unix socket error",           /*EOFFSET(12)*/
  "Execute change master sql error",           /*EOFFSET(13)*/
  "Get xtrabackup gtid position error",           /*EOFFSET(14)*/

  // New item should add above
  "Undefined Error Number and Relating Readable Information" /*undefined*/
};

#define ERROR_STR_DEFINED_NUM (sizeof(g_error_num_str) / sizeof(char *))
const char *GlobalErrorNum::get_err_num_str() {
  if(err_num_ == 0)
    return g_error_num_str[0];

  if ((err_num_ + 1 - EERROR_NUM_START) > ERROR_STR_DEFINED_NUM) {
    return g_error_num_str[ERROR_STR_DEFINED_NUM - 1];
  }
  return g_error_num_str[err_num_ - EERROR_NUM_START];
}

}