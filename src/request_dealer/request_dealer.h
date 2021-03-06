/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _NODE_MANAGER_REQUEST_DEALER_H_
#define _NODE_MANAGER_REQUEST_DEALER_H_

#include "zettalib/biodirectpopen.h"
#include "zettalib/errorcup.h"
#include "util_func/meta_info.h"
#include "json/json.h"
#include <string>

class RequestDealer : public kunlun::ErrorCup {
public:
  explicit RequestDealer(const char *request_json_cstr)
      : request_json_str_(request_json_cstr), popen_p_(nullptr),
        deal_success_(false) {}
  ~RequestDealer();

  bool ParseRequest();
  bool Deal();
  std::string FetchResponse();

private:
  bool protocalValid();
  bool pingPong();
  bool executeCommand();
  void constructCommand();
  bool getPathsSpace();
  bool checkPortIdle();
  bool installStorage();
  bool installComputer();
  bool deleteStorage();
  bool deleteComputer();
  bool backupShard();
  bool restoreStorage();
  bool restoreComputer();
  bool controlInstance();
  bool updateInstance();
  bool nodeExporter();
  std::string getStatusStr();
  std::string getInfo();

private:
  // forbid copy
  RequestDealer(const RequestDealer &rht) = delete;
  RequestDealer &operator=(const RequestDealer &rht) = delete;

private:
  std::string request_json_str_;
  Json::Value json_root_;
  std::string execute_command_;
  kunlun::BiodirectPopen *popen_p_;
  bool deal_success_;
  std::string deal_info_;
  kunlun::ClusterRequestTypes request_type_;
};

#endif /*_NODE_MANAGER_REQUEST_DEALER_H_*/
