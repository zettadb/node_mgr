#include "request_dealer.h"
//#include "log.h"
#include "zettalib/op_log.h"
#include "string.h"
#include "zettalib/biodirectpopen.h"
#include "zettalib/tool_func.h"
#include "instance_info.h"
#include "job.h"
#include <algorithm>
#include <vector>
#include "rebuild_node/rebuild_node.h"
#include "util_func/meta_info.h"

#ifndef NDEBUG
#include "node_debug/node_debug.h"
#endif

std::string node_mgr_util_path;
std::string node_mgr_tmp_data_path;
extern std::string local_ip;

bool RequestDealer::ParseRequest() {
  Json::Reader reader;
  bool ret = reader.parse(request_json_str_, json_root_);
  if (!ret) {
    setErr("%s", reader.getFormattedErrorMessages().c_str());
    return false;
  }
  if (!protocalValid()) {
    return false;
  }
  
  return true;
}

bool RequestDealer::Deal() {

  bool ret = false;
  switch (request_type_) {
  case kunlun::kPingPongType:
    ret = pingPong();
    break;
  case kunlun::kExecuteCommandType:
    ret = executeCommand();
    break;
  case kunlun::kCheckPortIdleType:
    ret = checkPortIdle();
    break;
  case kunlun::kGetPathsSpaceType:
    ret = getPathsSpace();
    break;
  case kunlun::kInstallStorageType:
    ret = installStorage();
    break;
  case kunlun::kInstallComputerType:
    ret = installComputer();
    break;
  case kunlun::kDeleteStorageType:
    ret = deleteStorage();
    break;
  case kunlun::kDeleteComputerType:
    ret = deleteComputer();
    break;
  case kunlun::kControlInstanceType:
    ret = controlInstance();
    break;
  case kunlun::kUpdateInstanceType:
    ret = updateInstance();
    break;
  //case kunlun::kNodeExporterType:
  //  ret = nodeExporter();
  //  break;
  case kunlun::kRebuildNodeType:
    ret = rebuildNode();
    break;

  case kunlun::kKillMysqlType:
    ret = KillMysqlByPort();
    break;

#ifndef NDEBUG
  case kunlun::kNodeDebugType:
    ret = kunlun::nodeDebug(json_root_["paras"]);
    deal_success_ = ret;
    break;
#endif

  default:
    setErr("Unrecongnized job type");
    break;
  }

  return ret;
}

bool RequestDealer::protocalValid() {
  bool ret = false;

  if (!json_root_.isMember("cluster_mgr_request_id")) {
    setErr("'cluster_mgr_request_id' field is missing");
    goto end;
  }
  if (!json_root_.isMember("paras")) {
    setErr("'paras' field missing");
    goto end;
  }
  if (!json_root_.isMember("job_type")) {
    setErr("job_type field missing");
    goto end;
  }

  request_type_ = kunlun::GetReqTypeEnumByStr(json_root_["job_type"].asString().c_str());
  if(request_type_ == kunlun::kRequestTypeMax){
    setErr("job_type no support");
    goto end;
  }

  return true;

end:
  return ret;
}

std::string RequestDealer::getStatusStr() {
  return deal_success_ ? "success" : "failed";
}

std::string RequestDealer::getInfo(){
  if(deal_info_.length())
    return deal_info_;
    
  if (!deal_success_){
    return getErr();
  }
  return deal_info_;
}

std::string RequestDealer::FetchResponse() {

  Json::Value root;
  root["cluster_mgr_request_id"] = json_root_["cluster_mgr_request_id"].asString();
  root["task_spec_info"] = json_root_["task_spec_info"].asString();
  root["status"] = getStatusStr();

  Json::Value info_json;
  Json::Reader reader;
  bool ret = reader.parse(getInfo().c_str(), info_json);
  if (ret) {
    root["info"] = info_json;
  } else {
    root["info"] = kunlun::TrimResponseInfo(getInfo());
  }

  AppendExtraToResponse(root);
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  return writer.write(root);

}

void RequestDealer::AppendExtraToResponse(Json::Value &root){
  return;
}

bool RequestDealer::pingPong() {
  KLOG_INFO( "ping pong");
  deal_success_ = true;
  deal_info_ = "success";
  return deal_success_;
}

bool RequestDealer::executeCommand() {

  bool ret = constructCommand();
  if(!ret){
    KLOG_ERROR("construct Command failed");
    return false;
  }
  KLOG_INFO("Will execute : {}", execute_command_);
  popen_p_ = new kunlun::BiodirectPopen(execute_command_.c_str());
  ret = popen_p_->Launch("r");

  if (!ret || popen_p_->get_chiled_status() != 0) {
    FILE *stderr_fp = popen_p_->getReadStdErrFp();
    if(stderr_fp == nullptr){
      setErr("%s",popen_p_->getErr());
      KLOG_ERROR("Biooppen launch failed: {}",popen_p_->getErr());
      deal_info_ = popen_p_->getErr();
      deal_success_ = false;
      return false;
    }
    char buffer[8192];
    if (fgets(buffer, 8192, stderr_fp) != nullptr) {
      KLOG_ERROR("Biopopen excute failed and stderr: {}", buffer);
      deal_info_ = buffer;
      deal_info_ = kunlun::TrimResponseInfo(deal_info_);
    }
    setErr("child return code: %d, popen_err: %s, command stderr: %s",
           popen_p_->get_chiled_status(), popen_p_->getErr(), buffer);
    KLOG_ERROR("child return code: {}, popen_err: {}, command stderr: {}, command: {}",
           popen_p_->get_chiled_status(), popen_p_->getErr(), buffer, execute_command_);
    deal_success_ = false;
    return false;
  }
  deal_success_ = true;
  deal_info_ = "success";
  return true;
}

bool RequestDealer::constructCommand() {
  Json::Value para_json = json_root_["paras"];
  std::string command_name = para_json["command_name"].asString();
  Json::Value para_json_array = para_json["command_para"];
  std::string para_str = command_name + " ";
  for (Json::Value::ArrayIndex i = 0; i < para_json_array.size(); i++) {
    if (i > 0) {
      para_str.append(" ");
      para_str.append(para_json_array[i].asString());
      continue;
    }
    para_str.append(para_json_array[i].asString());
  }
  execute_command_ = para_str;

  // find the util which has the same name with command_name
  bool found_command_local = true;
  std::vector<std::string> utils;
  std::string util_abs_path =
      kunlun::ConvertToAbsolutePath(node_mgr_util_path.c_str());
  std::string util_abs_tmp_data_path =
      kunlun::ConvertToAbsolutePath(node_mgr_tmp_data_path.c_str());
  if (util_abs_path.empty()) {
    KLOG_ERROR("kunlun::ConvertToAbsolutePath {} faild: {}", node_mgr_util_path,
               strerror(errno));
    setErr("kunlun::ConvertToAbsolutePath %s faild: %s", node_mgr_util_path.c_str(),
               strerror(errno));
    return false;
  }
  if (util_abs_tmp_data_path.empty()) {
    KLOG_ERROR("kunlun::ConvertToAbsolutePath {} faild: {}. set to current dir",
               node_mgr_tmp_data_path, strerror(errno));
    util_abs_tmp_data_path = kunlun::ConvertToAbsolutePath("./");
  }

  int ret = kunlun::GetFileListFromPath(util_abs_path.c_str(), utils);
  if (ret != 0) {
    setErr("%s", strerror(ret));
    return false;
    // TODO: how to handle the errinfo
  }
  if (utils.size() == 0) {
    found_command_local = false;
  }
  auto iter = find(utils.begin(), utils.end(), command_name);
  if (iter == utils.end()) {
    found_command_local = false;
  } else {
    execute_command_ = util_abs_path + "/" + execute_command_;
  }

  execute_command_ = kunlun::StringReplace(
      execute_command_, "TMP_DATA_PATH_PLACE_HOLDER", util_abs_tmp_data_path);

  return true;
}
RequestDealer::~RequestDealer() {
  if (popen_p_) {
    delete popen_p_;
  }
}

bool RequestDealer::getPathsSpace() {
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Instance_info::get_instance()->get_path_space(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::checkPortIdle(){
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Instance_info::get_instance()->check_port_idle(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::installStorage() {
  Json::Value para_json = json_root_["paras"];

  deal_success_ = Job::get_instance()->job_install_storage(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::installComputer() {
  Json::Value para_json = json_root_["paras"];

  deal_success_ = Job::get_instance()->job_install_computer(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::deleteStorage() {
  Json::Value para_json = json_root_["paras"];

  deal_success_ = Job::get_instance()->job_delete_storage(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::deleteComputer() {
  Json::Value para_json = json_root_["paras"];

  deal_success_ = Job::get_instance()->job_delete_computer(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::backupShard() {
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Job::get_instance()->job_backup_shard(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::backupCompute() {
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Job::get_instance()->job_backup_compute(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::restoreStorage() {
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Job::get_instance()->job_restore_storage(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::restoreComputer() {
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Job::get_instance()->job_restore_computer(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::controlInstance(){
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Job::get_instance()->job_control_instance(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::updateInstance() {
  Json::Value para_json = json_root_["paras"];

  std::string instance_type = para_json["instance_type"].asString();
  if(instance_type == "meta_instance")
    deal_success_ = Instance_info::get_instance()->get_meta_instance();
  else if(instance_type == "storage_instance")
    deal_success_ = Instance_info::get_instance()->get_storage_instance();
  else if(instance_type == "computer_instance")
    deal_success_ = Instance_info::get_instance()->get_computer_instance();

  return deal_success_;
}

bool RequestDealer::nodeExporter(){
  Json::Value para_json = json_root_["paras"];
  deal_success_ = Job::get_instance()->job_node_exporter(para_json, deal_info_);
  return deal_success_;
}

bool RequestDealer::rebuildNode() {
  Json::Value para_json = json_root_["paras"];
  if(!para_json.isMember("port")) {
    KLOG_ERROR("input parameter no port");
    deal_success_ = false;
    return false;
  }

  std::string port = para_json["port"].asString();
  int int_port = atoi(port.c_str());

  KLOG_INFO("Will Start Rebuild Node, port {}", port);
  Instance_info::get_instance()->toggle_auto_pullup(false, int_port);

  std::vector<std::string> cmd_params;
  Json::Value command_para = para_json["command_para"];
  for(unsigned int i=0; i<command_para.size(); i++) {
    std::string param = command_para[i].asString();
    cmd_params.emplace_back(param);
  }
    
  CRbNode rbnode(cmd_params[0], atoi(cmd_params[1].c_str()), cmd_params[2], cmd_params[3], 
              atoi(cmd_params[4].c_str()), cmd_params[5], cmd_params[6], cmd_params[7], cmd_params[8]);
  deal_success_ = rbnode.Run();
  Instance_info::get_instance()->toggle_auto_pullup(true, int_port);
  return deal_success_;
}

bool RequestDealer::KillMysqlByPort() {
  Json::Value para_json = json_root_["paras"];
  if(!para_json.isMember("port")) {
    KLOG_ERROR("input parameter no port");
    deal_success_ = false;
    return false;
  }
  std::string port = para_json["port"].asString();
  std::string sql = string_sprintf("select datadir from %s.server_nodes where hostaddr='%s' and machine_type='storage'",
          KUNLUN_METADATA_DB_NAME, local_ip.c_str());
  kunlun::MysqlResult result;
  bool ret = Instance_info::get_instance()->send_stmt(sql.c_str(), &result);
  if(!ret) {
    KLOG_ERROR("get port {} datadir from server_nodes failed: {}", port, 
            Instance_info::get_instance()->getErr());
    deal_success_ = false;
    return false;
  }
  if(result.GetResultLinesNum() != 1) {
    KLOG_ERROR("get port {} datadir record too many", port);
    deal_success_ = false;
    return false;
  }
  std::string datadir = result[0]["datadir"];
  std::string cmd = kunlun::string_sprintf("./util/safe_killmysql %s %s", port.c_str(), datadir.c_str());
  
  KLOG_INFO("Will execute : {}", cmd);
  kunlun::BiodirectPopen bi_open(cmd.c_str());
  ret = bi_open.Launch("r");
  if(!ret || bi_open.get_chiled_status() != 0) {
    KLOG_ERROR("safe_killmysql failed");
    deal_success_ = false;
    return false;
  }
  deal_success_ = true;
  return true;
}
