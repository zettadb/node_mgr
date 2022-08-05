/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#pragma once
#include "request_dealer/request_dealer.h"

namespace kunlun
{

class PostgresUninstallDealer : public RequestDealer {
  typedef RequestDealer super;

public:
  explicit PostgresUninstallDealer(const char *request_json_str)
      : super(request_json_str), install_prefix_(""), data_prefix_(""),
      exporter_port_("") {}

  virtual ~PostgresUninstallDealer() {}
  bool virtual Deal() override;
  bool virtual constructCommand() override;
  bool fetchMetaInfoFromMetadataCluster();
  void virtual AppendExtraToResponse(Json::Value &) override;

private:
  std::string install_prefix_;
  std::string data_prefix_;
  std::string exporter_port_;
};

}