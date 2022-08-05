/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "exporter_uninstall_dealer.h"
#include "zettalib/op_log.h"
#include "instance_info.h"

extern std::string prometheus_path;

namespace kunlun
{

bool MysqldExporterUninstallDealer::Deal() {
    return true;
}

bool PostgresExporterUninstallDealer::Deal() {
    return true;
}

bool NodeExporterUninstallDealer::constructCommand() {
  
  return true;
}

bool NodeExporterUninstallDealer::Deal() {
    //KLOG_INFO("remove node_exporter watch");
    Json::Value para_json = json_root_["paras"];
    //std::string command_name = para_json["command_name"].asString();
    exporter_port_ = para_json["exporter_port"].asString();
    
    Instance_info::get_instance()->remove_node_exporter(exporter_port_);
    deal_success_ = true;
    deal_info_ = "success";
    return true;
}

}