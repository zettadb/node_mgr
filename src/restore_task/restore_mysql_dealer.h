/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "request_dealer/request_dealer.h"

namespace kunlun {
class MySQLRestoreDealer : public RequestDealer {
  typedef RequestDealer super;

public:
  explicit MySQLRestoreDealer(const char *request_json_str)
      : super(request_json_str) {}

  virtual ~MySQLRestoreDealer() {}
  bool virtual Deal() override;
  bool virtual constructCommand() override;
  bool fetchMetaInfoFromMetadataCluster();
  void virtual AppendExtraToResponse(Json::Value &) override;

private:
  bool make_metacluster_conn_str();

private:
  std::string metacluster_conn_str_;
  std::string hdfs_addr_;
  std::string restore_time_str_;
  std::string port_;

};
} // namespace kunlun
