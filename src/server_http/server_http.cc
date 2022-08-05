#include "server_http.h"
#include "backup_task/backup_dealer.h"
#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "install_task/mysql_install_dealer.h"
#include "install_task/mysql_uninstall_dealer.h"
#include "install_task/postgres_install_dealer.h"
#include "install_task/postgres_uninstall_dealer.h"
#include "install_task/exporter_install_dealer.h"
#include "install_task/exporter_uninstall_dealer.h"
#include "request_dealer/request_dealer.h"
#include "restore_task/restore_mysql_dealer.h"
#include "restore_task/restore_postgres_dealer.h"
#include "strings.h"
#include "sys.h"
#include "zettalib/biodirectpopen.h"
#include "zettalib/microsec_interval.h"
#include "zettalib/op_log.h"
#include "zettalib/tool_func.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <thread>
#include <unistd.h>

int64_t node_mgr_brpc_http_port;
extern std::string node_mgr_tmp_data_path;
extern std::string node_mgr_util_path;
extern std::string local_ip;
static RequestDealer *RequestDealerFactory(brpc::Controller *cntl);
#define READ_BUFF_LEN 4096
#define SEND_BUFFER_SIZE 1024

using namespace kunlun;

struct DoDealArg {
  brpc::Controller *cntl;
  RequestDealer *dealer;
  google::protobuf::Closure *done;
};

static void *DoDeal(void *para){
  DoDealArg *args = (DoDealArg *)para;
  args->dealer->Deal();
  std::string response_l = args->dealer->FetchResponse();
  args->cntl->http_response().set_content_type("text/plain");
  args->cntl->response_attachment().append(response_l);
  args->done->Run();
  delete args->dealer;
  delete args;
  return nullptr;
}

struct ShellServiceArg {
  butil::intrusive_ptr<brpc::ProgressiveAttachment> pa;
  brpc::Controller *cntl;
  std::string cmd;
};

static void *DoShellCmd(void *para) {
  std::unique_ptr<ShellServiceArg> arg(static_cast<ShellServiceArg *>(para));
  std::unique_ptr<kunlun::BiodirectPopen> biopopen(
      new kunlun::BiodirectPopen(arg->cmd.c_str()));
  char buffer[SEND_BUFFER_SIZE] = {'\0'};
  int ret = 0;
  FILE *stdout_fp = nullptr;
  FILE *stderr_fp = nullptr;

  ret = biopopen->Launch("r");
  if (!ret) {
    // TODO: Wraper need to be modified to encode error info
    // WrapTheFailedResponse(para, strerror(errno));
    KLOG_ERROR("Open File to be trasmitted failed: {}", strerror(errno));
    return nullptr;
  }
  stdout_fp = biopopen->getReadStdOutFp();
  stderr_fp = biopopen->getReadStdErrFp();

  // response stdout and stderr
  snprintf(buffer, SEND_BUFFER_SIZE, "===STDOUT===\n");
  while (arg->pa->Write(buffer, SEND_BUFFER_SIZE) < 0) {
    bthread_usleep(1);
  }
  bzero((void *)buffer, SEND_BUFFER_SIZE);

  while (fgets(buffer, SEND_BUFFER_SIZE, stdout_fp)) {
    while (arg->pa->Write(buffer, SEND_BUFFER_SIZE) < 0) {
      bthread_usleep(1);
    }
    bzero((void *)buffer, SEND_BUFFER_SIZE);
  }

  snprintf(buffer, SEND_BUFFER_SIZE, "===STDERR===\n");
  while (arg->pa->Write(buffer, SEND_BUFFER_SIZE) < 0) {
    bthread_usleep(1);
  }
  bzero((void *)buffer, SEND_BUFFER_SIZE);
  while (fgets(buffer, SEND_BUFFER_SIZE, stderr_fp)) {
    while (arg->pa->Write(buffer, SEND_BUFFER_SIZE) < 0) {
      bthread_usleep(1);
    }
    bzero((void *)buffer, SEND_BUFFER_SIZE);
  }
  return nullptr;
}

void HttpServiceImpl::Shell(google::protobuf::RpcController *cntl_base,
                            const HttpRequest *request, HttpResponse *response,
                            google::protobuf::Closure *done) {

  brpc::ClosureGuard done_gurad(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  std::string orig_request = cntl->request_attachment().to_string();
  Json::Reader reader;
  Json::Value root;
  bool ret = reader.parse(orig_request, root);
  if (!ret) {
    // TODO
    return;
  }

  // Sync Deal request here
  KLOG_INFO("Shell service get original request from client: {}",
         cntl->request_attachment().to_string());
  std::unique_ptr<ShellServiceArg> para(new ShellServiceArg);
  para->pa = cntl->CreateProgressiveAttachment();
  para->cmd = root["command"].asString();
  para->cntl = cntl;

  bthread_t th;
  bthread_start_background(&th, nullptr, DoShellCmd, para.release());
}

void HttpServiceImpl::Emit(google::protobuf::RpcController *cntl_base,
                           const HttpRequest *request, HttpResponse *response,
                           google::protobuf::Closure *done) {
  brpc::ClosureGuard done_gurad(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  // Sync Deal request here
  KLOG_INFO("get original request from cluster_mgr: {}",
         cntl->request_attachment().to_string());

  // TODO: make a wrapper to do this json stuff
  std::string response_l;
  RequestDealer *dealer = RequestDealerFactory(cntl);
  if (dealer == nullptr) {
    dealer = new RequestDealer(cntl->request_attachment().to_string().c_str());
  }
  if (!dealer->ParseRequest()) {
    response_l = dealer->FetchResponse();
    KLOG_ERROR("Parse Cluster mgr request failed: {}", dealer->getErr());
    cntl->http_response().set_content_type("text/plain");
    cntl->response_attachment().append(response_l);
    delete dealer;
    return;
  }
  //deal the request async
  done_gurad.release();

  DoDealArg * para = new DoDealArg();
  para->dealer = dealer;
  para->cntl = cntl;
  para->done = done;

  bthread_t th;
  bthread_start_background(&th, nullptr, DoDeal, (void *)para);
}

struct Args {
  butil::intrusive_ptr<brpc::ProgressiveAttachment> pa;
  brpc::Controller *cntl;
  std::string resolved_file_path;
  int64_t bytes_per_second = 5242880;
};

static void WrapTheFailedResponse(void *para, const char *info) {
  Args *args = static_cast<Args *>(para);
  brpc::Controller *cntl = args->cntl;
  // set resonse status to 500 (Internal error)
  // cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
  Json::Value root;
  root["status"] = "failed";
  root["info"] = std::string(info);
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string res = writer.write(root);
  args->pa->Write(res.c_str(), res.size());
}

// return none empty string means successfully
static std::string ReolveFilename(const std::string &orig_filename) {
  std::string node_mgr_data_abs_path =
      kunlun::ConvertToAbsolutePath(node_mgr_tmp_data_path.c_str());
  std::string file_abs_path = node_mgr_data_abs_path + "/" + orig_filename;

  bool ret = kunlun::CheckFileExists(file_abs_path.c_str());
  if (!ret) {
    KLOG_INFO("FileService File Not Found: {}", file_abs_path);
    return "";
  }
  KLOG_INFO( "FileService File Found: {}", file_abs_path);
  return file_abs_path;
}

static void *SendFile(void *para) {
  std::unique_ptr<Args> args(static_cast<Args *>(para));
  std::string resolved = args->resolved_file_path;

  // timeout interval is 100 microsecond
  int interval_para_microsec = 2;
  kunlun::MicroSecInterval timer(interval_para_microsec, 0);
  int64_t bytes_per_sec = args->bytes_per_second;
  int64_t bytes_send_counter = 0;
  int inner_check_counter = 0;

  char buff[SEND_BUFFER_SIZE] = {'\0'};
  size_t ret = 0;

  int fd = open(resolved.c_str(), O_RDONLY);
  if (fd < 0) {
    WrapTheFailedResponse(para, strerror(errno));
    KLOG_ERROR("Open File to be trasmitted failed: {}", strerror(errno));
    goto end;
  }

  for (;;) {
    ret = read(fd, buff, SEND_BUFFER_SIZE);
    if (ret < 0) {
      WrapTheFailedResponse(para, strerror(errno));
      KLOG_ERROR( "Read File failed: {}", strerror(errno));
      break;
    } else if (ret == 0) {
      // syslog(Logger::INFO,"read ret 0");
      break;
    } else {
      for (;;) {
        if (timer.timeout()) {
          // do check
          inner_check_counter++;
          if (inner_check_counter >= (1000000 / interval_para_microsec)) {
            // traffic control under seconde level
            inner_check_counter = 0;
            bytes_send_counter = 0;
          }
          if (bytes_send_counter >= bytes_per_sec) {
            // stop and wait in the rest of the one seconde long
            // bthread_usleep counter as microseconde
            bthread_usleep(
                ((1000000 / interval_para_microsec) - inner_check_counter) *
                interval_para_microsec);
            inner_check_counter = 0;
            bytes_send_counter = 0;
            continue;
          }
        }

        while (args->pa->Write(buff, ret) < 0) {
          bthread_usleep(1);
        }
        bytes_send_counter += ret;
        break;
      }
    }
  }

end:
  close(fd);
  return nullptr;
}

void FileServiceImpl::default_method(google::protobuf::RpcController *cntl_base,
                                     const HttpRequest *request,
                                     HttpResponse *response,
                                     google::protobuf::Closure *done) {

  brpc::ClosureGuard done_guard(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);
  const std::string &filename = cntl->http_request().unresolved_path();
  resolved_file_path_ = ReolveFilename(filename);
  if (resolved_file_path_.empty()) {
    cntl->http_response().set_status_code(
        brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
    char buff[4096] = {'\0'};
    sprintf(buff, "File Not Found: %s", filename.c_str());
    Json::Value root;
    root["status"] = "failed";
    root["info"] = buff;
    Json::FastWriter writer;
    writer.omitEndingLineFeed();
    std::string res = writer.write(root);
    cntl->response_attachment().append(res);
    return;
  }

  std::unique_ptr<Args> para(new Args);
  para->pa = cntl->CreateProgressiveAttachment();
  para->cntl = cntl;
  para->resolved_file_path = resolved_file_path_;

  std::string request_attachment = cntl->request_attachment().to_string();
  Json::Value root;
  Json::Reader reader;
  reader.parse(request_attachment,root);
  if(root.isMember("traffic_limit")){
    para->bytes_per_second = ::atoll(root["traffic_limit"].asCString()); 
  }

  bthread_t th;
  bthread_start_background(&th, nullptr, SendFile, para.release());
}

brpc::Server *NewHttpServer() {
  HttpServiceImpl *http_service = new HttpServiceImpl();
  FileServiceImpl *file_service = new FileServiceImpl();
  brpc::Server *server = new brpc::Server();
  if (server->AddService(http_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    KLOG_ERROR( "Add http service to brpc::Server failed,");
    return nullptr;
  }
  if (server->AddService(file_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    KLOG_ERROR("Add file service to brpc::Server failed,");
    return nullptr;
  }
  
  brpc::ServerOptions *options = new brpc::ServerOptions();
  options->idle_timeout_sec = -1;
  if (server->Start(node_mgr_brpc_http_port, options) != 0) {
    KLOG_ERROR("http server start failed,");
    return nullptr;
  }
  KLOG_INFO( "brpc http server start on {} successfully",
         node_mgr_brpc_http_port);
  return server;
}
static RequestDealer *RequestDealerFactory(brpc::Controller *cntl) {
  Json::Reader reader;
  Json::Value root;
  bool ret = reader.parse(cntl->request_attachment().to_string().c_str(), root);
  if (!ret) {
    // TODO: log error info
    KLOG_ERROR("RequestDealFactory failed: {}",
               reader.getFormattedErrorMessages());
    return nullptr;
  }
  if (!root.isMember("job_type")) {
    KLOG_ERROR("RequestDealFactory failed: Missing `job_type` filed");
    return nullptr;
  }
  std::string type_str = root["job_type"].asString();
  auto request_type = kunlun::GetReqTypeEnumByStr(type_str.c_str());
  switch (request_type) {
  case kunlun::kInstallMySQLType: {
    return new kunlun::MySQLInstallDealer(
        cntl->request_attachment().to_string().c_str());
  }
  case kunlun::kUninstallMySQLType: {
    return new kunlun::MySQLUninstallDealer(
        cntl->request_attachment().to_string().c_str());
  }
  case kunlun::kInstallPostgresType: {
    return new kunlun::PostgresInstallDealer(
        cntl->request_attachment().to_string().c_str());
  }
  case kunlun::kUninstallPostgresType: {
    return new kunlun::PostgresUninstallDealer(
        cntl->request_attachment().to_string().c_str());
  }
  case kunlun::kRestoreMySQLType: {
    return new kunlun::MySQLRestoreDealer(
        cntl->request_attachment().to_string().c_str());
  }
  case kunlun::kRestorePostGresType: {
    return new kunlun::PostGresRestoreDealer(
        cntl->request_attachment().to_string().c_str());
  }
  case kunlun::kBackupShardType:
  case kunlun::kBackupComputeType: {
    return new kunlun::BackUpDealer(
        cntl->request_attachment().to_string().c_str());
  }
  case kunlun::kInstallNodeExporterType:
    return new kunlun::NodeExporterInstallDealer(
      cntl->request_attachment().to_string().c_str()
    );
  case kunlun::kUninstallNodeExporterType:
    return new kunlun::NodeExporterUninstallDealer(
      cntl->request_attachment().to_string().c_str()
    );

  default:
    break;
  }
  KLOG_DEBUG("Unrecongnized job_type {}", type_str);
  return nullptr;
}
