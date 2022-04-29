#include "request_dealer.h"
#include "log.h"
#include "string.h"
#include "zettalib/biodirectpopen.h"
#include "zettalib/tool_func.h"
#include "instance_info.h"
#include "job.h"
#include <algorithm>
#include <vector>

std::string node_mgr_util_path;
std::string node_mgr_tmp_data_path;

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
  case kunlun::kBackupShardType:
    ret = backupShard();
    break;
  case kunlun::kRestoreStorageType:
    ret = restoreStorage();
    break;
  case kunlun::kRestoreComputerType:
    ret = restoreComputer();
    break;
  case kunlun::kControlInstanceType:
    ret = controlInstance();
    break;
  case kunlun::kUpdateInstanceType:
    ret = updateInstance();
    break;
  case kunlun::kNodeExporterType:
    ret = nodeExporter();
    break;

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
  if (!json_root_.isMember("job_type")) {
    setErr("'job_type' field is missing");
    goto end;
  }
  if (!json_root_.isMember("paras")) {
    setErr("'paras' field missing");
    goto end;
  }
  if (!json_root_.isMember("job_type")) {
    setErr("'job_type' field missing");
    goto end;
  }

  request_type_ = kunlun::GetReqTypeEnumByStr(json_root_["job_type"].asString().c_str());
  if(request_type_ == kunlun::kRequestTypeMax){
    setErr("'job_type' no support");
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
    root["info"] = getInfo();
  }

  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  return writer.write(root);
}

bool RequestDealer::pingPong(){
  syslog(Logger::INFO, "ping pong");
  deal_success_ = true;
  deal_info_ = "success";
  return deal_success_;
}

bool RequestDealer::executeCommand(){

  constructCommand();
  syslog(Logger::INFO, "Will execute : %s", execute_command_.c_str());
  popen_p_ = new kunlun::BiodirectPopen(execute_command_.c_str());
  bool ret = popen_p_->Launch("rw");
  if (!ret) {
    setErr("child return code: %d, %s", popen_p_->get_chiled_status(),
           popen_p_->getErr());
    
    return false;
  }
  FILE *stderr_fp = popen_p_->getReadStdErrFp();
  char buffer[8192];
  if (fgets(buffer, 8192, stderr_fp) != nullptr) {
    setErr("stderr: %s, return code: %d", buffer,
           popen_p_->get_chiled_status());
    syslog(Logger::ERROR, "Biopopen stderr: %s", buffer);
    return false;
  }
  deal_success_ = true;
  deal_info_ = "success";
  return true;
}

void RequestDealer::constructCommand() {
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
    syslog(Logger::ERROR, "kunlun::ConvertToAbsolutePath %s faild: %s",
           node_mgr_util_path.c_str(), strerror(errno));
  }
  if (util_abs_tmp_data_path.empty()) {
    syslog(Logger::ERROR,
           "kunlun::ConvertToAbsolutePath %s faild: %s. set to current dir",
           node_mgr_tmp_data_path.c_str(), strerror(errno));
    util_abs_tmp_data_path = kunlun::ConvertToAbsolutePath("./");
  }

  int ret = kunlun::GetFileListFromPath(util_abs_path.c_str(), utils);
  if (ret != 0) {
    setErr("%s", strerror(ret));
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

  return;
}
RequestDealer::~RequestDealer() {
  if (popen_p_) {
    delete popen_p_;
  }
}

bool RequestDealer::getPathsSpace() {
  Json::Value para_json = json_root_["paras"];
  std::vector<std::string> vec_paths;

  vec_paths.emplace_back(para_json["path0"].asString());
  vec_paths.emplace_back(para_json["path1"].asString());
  vec_paths.emplace_back(para_json["path2"].asString());
  vec_paths.emplace_back(para_json["path3"].asString());

  deal_success_ = Instance_info::get_instance()->get_path_space(vec_paths, deal_info_);
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
