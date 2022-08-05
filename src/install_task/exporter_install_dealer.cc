/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "exporter_install_dealer.h"
#include "zettalib/op_log.h"
#include "instance_info.h"

extern std::string prometheus_path;
extern int64_t prometheus_port_start;

namespace kunlun
{

bool MysqldExporterInstallDealer::Deal() {
    return true;
}

bool PostgresExporterInstallDealer::Deal() {
    return true;
}


bool NodeExporterInstallDealer::constructCommand() {
  Json::Value para_json = json_root_["paras"];
  //std::string command_name = para_json["command_name"].asString();
  exporter_port_ = std::to_string(prometheus_port_start); //para_json["exporter_port"].asString();

  execute_command_ = string_sprintf("cd %s/node_exporter; ./node_exporter --web.listen-address=:%lu &",
        prometheus_path.c_str(), prometheus_port_start);
  return true;
}

bool NodeExporterInstallDealer::Deal() {
    bool ret = executeCommand();
    if(ret) {
        //KLOG_INFO("add watch node_exporter");
        Instance_info::get_instance()->add_node_exporter(exporter_port_);
    }
    return ret;
}

void NodeExporterInstallDealer::AppendExtraToResponse(Json::Value &root) {
    Json::Value para_json = json_root_["paras"];
    root["exporter_port"] = exporter_port_;
    return;
}

}
