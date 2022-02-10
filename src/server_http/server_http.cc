#include "server_http.h"
#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "log.h"
#include "strings.h"

void HttpServiceImpl::Emit(google::protobuf::RpcController *cntl_base,
                           const HttpRequest *request, HttpResponse *response,
                           google::protobuf::Closure *done)
{

  brpc::ClosureGuard done_gurad(done);
  brpc::Controller *cntl = static_cast<brpc::Controller *>(cntl_base);

  // Sync Deal request here
  syslog(Logger::ERROR, "get request: %s",
         cntl->request_attachment().to_string());
  fprintf(stderr, "get request: %s\n", cntl->request_attachment().to_string().c_str());
  sleep(10);

  // TODO: make a wrapper to do this json stuff

  // here done_guard will be release and _done->Run() will be invoked
  return;
}

brpc::Server *NewHttpServer()
{
  HttpServiceImpl *service = new HttpServiceImpl();
  brpc::Server *server = new brpc::Server();
  if (server->AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0)
  {
    syslog(Logger::ERROR, "Add service to brpc::Server failed,");
    return nullptr;
  }
  brpc::ServerOptions *options = new brpc::ServerOptions();
  options->idle_timeout_sec = -1;
  if (server->Start(8010, options) != 0)
  {
    syslog(Logger::ERROR, "http server start failed,");
    return nullptr;
  }
  return server;
}
