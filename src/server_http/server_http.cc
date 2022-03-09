#include "server_http.h"
#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "log.h"
#include "request_dealer/request_dealer.h"
#include "strings.h"
#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

int64_t node_mgr_brpc_http_port;
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
  
};

static void WrapTheFailedResponse(void *para, const char *info) {
  Args *args = static_cast<Args *>(para);
  brpc::Controller *cntl = args->cntl;
  // set resonse status to 500 (Internal error)
  //cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
  Json::Value root;
  root["status"] = "failed";
  root["info"] = std::string(info);
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  std::string res = writer.write(root);
  args->pa->Write(res.c_str(), res.size());
}

static std::string ReolveFilename(const std::string &orig_filename) {
  return "/home/summerxwu/code/kunlun/service/node_mgr/build/output/log/1";
}

#define SEND_BUFFER_SIZE 512
static void *SendFile(void *para) {
  std::unique_ptr<Args> args(static_cast<Args *>(para));
  const std::string &filename = args->cntl->http_request().unresolved_path();
  std::string resolved = ReolveFilename(filename);

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
    } else if (ret < SEND_BUFFER_SIZE) {
      // eof
      args->pa->Write(buff, ret);
      break;
    } else {
      args->pa->Write(buff, ret);
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
  std::string resolved = ReolveFilename(filename);
 // cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
  std::unique_ptr<Args> para(new Args);
  para->pa = cntl->CreateProgressiveAttachment();
  para->cntl = cntl;
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
