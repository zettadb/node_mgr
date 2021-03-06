#include "server_http.h"
#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "log.h"
#include "request_dealer/request_dealer.h"
#include "strings.h"
#include "zettalib/microsec_interval.h"
#include "zettalib/tool_func.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int64_t node_mgr_brpc_http_port;
extern std::string node_mgr_tmp_data_path;
void HttpServiceImpl::Emit(google::protobuf::RpcController *cntl_base,
                           const HttpRequest *request, HttpResponse *response,
                           google::protobuf::Closure *done) {

  brpc::ClosureGuard done_gurad(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  // Sync Deal request here
  syslog(Logger::INFO, "get original request from cluster_mgr: %s",
         cntl->request_attachment().to_string().c_str());

  // TODO: make a wrapper to do this json stuff
  std::string response_l;
  RequestDealer dealer(cntl->request_attachment().to_string().c_str());
  if (!dealer.ParseRequest()) {
    response_l = dealer.FetchResponse();
    syslog(Logger::ERROR, "Parse Cluster mgr request failed: %s",
           dealer.getErr());
    goto end;
  }

  dealer.Deal();
  response_l = dealer.FetchResponse();
  goto end;

end:
  cntl->http_response().set_content_type("text/plain");
  cntl->response_attachment().append(response_l);
  return;
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
    syslog(Logger::INFO, "FileService File Not Found: %s",
           file_abs_path.c_str());
    return "";
  }
  syslog(Logger::DEBUG1, "FileService File Found: %s", file_abs_path.c_str());
  return file_abs_path;
}

#define SEND_BUFFER_SIZE 1024
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
    syslog(Logger::ERROR, "Open File to be trasmitted failed: %s",
           strerror(errno));
    goto end;
  }

  for (;;) {
    ret = read(fd, buff, SEND_BUFFER_SIZE);
    if (ret < 0) {
      WrapTheFailedResponse(para, strerror(errno));
      syslog(Logger::ERROR, "Read File failed: %s", strerror(errno));
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
    syslog(Logger::ERROR, "Add service to brpc::Server failed,");
    return nullptr;
  }
  if (server->AddService(file_service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
    syslog(Logger::ERROR, "Add service to brpc::Server failed,");
    return nullptr;
  }
  brpc::ServerOptions *options = new brpc::ServerOptions();
  options->idle_timeout_sec = -1;
  if (server->Start(node_mgr_brpc_http_port, options) != 0) {
    syslog(Logger::ERROR, "http server start failed,");
    return nullptr;
  }
  syslog(Logger::INFO, "brpc http server start on %d successfully",
         node_mgr_brpc_http_port);
  return server;
}
