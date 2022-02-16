#include "request_dealer.h"
#include "log.h"
#include "string.h"
#include "zettalib/biodirectpopen.h"
#include "zettalib/tool_func.h"
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
  constructCommand();
  return true;
}

bool RequestDealer::Deal() {

  syslog(Logger::INFO, "Will execute : %s", execute_command_.c_str());
  popen_p_ = new kunlun::BiodirectPopen(execute_command_.c_str());
  bool ret = popen_p_->Launch("r");
  if (!ret) {
    setErr("%s", popen_p_->getErr());
    return false;
  }
  FILE *stderr_fp = popen_p_->getReadStdErrFp();
  char buffer[1024];
  if (fgets(buffer, 1024, stderr_fp) == nullptr) {
    return true;
  }
  return false;
}

bool RequestDealer::protocalValid() {
  bool ret = false;
  if (!json_root_.isMember("command_name")) {
    setErr("'command_name' field missing");
    goto end;
  }
  if (!json_root_.isMember("para") || !json_root_["para"].isArray()) {
    setErr("'para' field is missing or is not a json array object");
    goto end;
  }
  if (!json_root_.isMember("cluster_mgr_request_id")) {
    setErr("'cluster_mgr_request_id' field is missing");
    goto end;
  }

  return true;

end:
  return ret;
}

std::string RequestDealer::FetchResponse() { return "{\"status\":\"ok\"}"; }

void RequestDealer::constructCommand() {
  std::string command_name = json_root_["command_name"].asString();
  Json::Value para_json_array = json_root_["para"];
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
