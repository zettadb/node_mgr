#include "request_dealer.h"
#include "zettalib/tool_func.h"
#include "zettalib/biodirectpopen.h"
#include "string.h"
#include <vector>
#include <algorithm>

bool RequestDealer::ParseRequest() {
  Json::Reader reader;
  bool ret = reader.parse(request_json_str_, json_root_);
  if (!ret) {
    setErr("%s", reader.getFormattedErrorMessages().c_str());
    return false;
  }
  if(!protocalValid()){
    return false;
  }
  constructCommand();
  return true;
}

bool RequestDealer::Deal(){
  //confirm the PATH of the command
  std::string command_name = json_root_["command_name"].asString();

  // find the util which has the same name with command_name
  bool found_command_local = true;
  std::vector<std::string> utils;
  std::string util_abs_path = kunlun::ConvertToAbsolutePath("../util"); 
  int ret = kunlun::GetFileListFromPath(util_abs_path.c_str(),utils);
  if (ret !=0){
    setErr("%s",strerror(ret));
    return false;
  }
  if(utils.size() == 0){
    found_command_local = false;
  }
  auto iter = find(utils.begin(),utils.end(),command_name);
  if(iter == utils.end()){
    found_command_local = false;
  }
  else{
    command_name = util_abs_path + "/" + command_name; 
  }

  popen_p_ = new kunlun::BiodirectPopen(command_name.c_str());
  ret = popen_p_->Launch("r");
  if (!ret){
    setErr("%s",popen_p_->getErr());
    return false;
  }
  FILE *stderr_fp = popen_p_->getReadStdErrFp();
  char buffer[512];
  if(fgets(buffer,1024,stderr_fp) == nullptr){
    return true;
  }
  return false;
}

bool RequestDealer::protocalValid() {
  bool ret = false;
  if (!json_root_.isMember("command_name")){
    setErr("'command_name' field missing");
    goto end;
  }
  if (!json_root_.isMember("para") || !json_root_["para"].isArray()){
    setErr("'para' field is missing or is not a json array object");
    goto end;
  }
  if (!json_root_.isMember("cluster_mgr_request_id")){
    setErr("'cluster_mgr_request_id' field is missing");
    goto end;
  }

  return true;
end:
  return ret;
}

std::string RequestDealer::FetchResponse(){
  return "";
}

void RequestDealer::constructCommand() {
  Json::Value json_root_;
  std::string command_name = json_root_["command_name"].asString();
  std::string bin_path = json_root_["bin_path"].asString();
  Json::Value para_json_array = json_root_["para"];
  std::string para_str;
  for (Json::Value::ArrayIndex i = 0; i < para_json_array.size(); i++) {
    if (i > 0) {
      para_str.append(" ");
      para_str.append(para_json_array[i].asString());
      continue;
    }
    para_str.append(para_json_array[i].asString());
  }
  execute_command_ = para_str;
  return;
}
RequestDealer::~RequestDealer(){
  delete popen_p_;
}
