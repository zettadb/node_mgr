/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#pragma once
#include "request_dealer/request_dealer.h"

namespace kunlun
{

class MysqldExporterUninstallDealer : public ErrorCup {
public:
  explicit MysqldExporterUninstallDealer(const std::string& exporter_port)
      : exporter_port_(exporter_port) {}

  virtual ~MysqldExporterUninstallDealer() {}
  bool Deal();

private:
  std::string exporter_port_;
};

class PostgresExporterUninstallDealer : public ErrorCup {
public:
  explicit PostgresExporterUninstallDealer(const std::string& exporter_port)
      : exporter_port_(exporter_port) {}

  virtual ~PostgresExporterUninstallDealer() {}
  bool Deal();

private:
  std::string exporter_port_;
};

class NodeExporterUninstallDealer : public RequestDealer {
    typedef RequestDealer super;

public:
  explicit NodeExporterUninstallDealer(const char *request_json_str)
      : super(request_json_str), exporter_port_("") {}

  virtual ~NodeExporterUninstallDealer() {}
  bool virtual Deal() override;
  bool virtual constructCommand() override;
  bool fetchMetaInfoFromMetadataCluster() { return true; }
  void virtual AppendExtraToResponse(Json::Value &) override {}

private:
  std::string exporter_port_;
};

} // namespace kunlun
