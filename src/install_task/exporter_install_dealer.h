/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#pragma once
#include "request_dealer/request_dealer.h"

namespace kunlun
{

class MysqldExporterInstallDealer : public ErrorCup {
public:
  explicit MysqldExporterInstallDealer(const std::string& exporter_port)
      : exporter_port_(exporter_port) {}

  virtual ~MysqldExporterInstallDealer() {}
  bool Deal();

private:
  std::string exporter_port_;
};

class PostgresExporterInstallDealer : public ErrorCup {
public:
    explicit PostgresExporterInstallDealer(const std::string& exporter_port)
      : exporter_port_(exporter_port) {}

  virtual ~PostgresExporterInstallDealer() {}
  bool Deal();

private:
  std::string exporter_port_;
};

class NodeExporterInstallDealer : public RequestDealer {
  typedef RequestDealer super;

public:
  explicit NodeExporterInstallDealer(const char *request_json_str)
      : super(request_json_str), exporter_port_("") {}

  virtual ~NodeExporterInstallDealer() {}
  bool virtual Deal() override;
  bool virtual constructCommand() override;
  bool fetchMetaInfoFromMetadataCluster() { return true; }
  void virtual AppendExtraToResponse(Json::Value &) override;

private:
  std::string exporter_port_;
};

} // namespace kunlun
