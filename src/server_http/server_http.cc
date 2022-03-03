#include "server_http.h"
#include "bthread/bthread.h"
#include "butil/iobuf.h"
#include "log.h"
#include "strings.h"
#include "request_dealer/request_dealer.h"

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
  if(!dealer.ParseRequest()){
    response_l = dealer.FetchResponse();
    syslog(Logger::ERROR,"Parse Cluster mgr request failed: %s",dealer.getErr());
    goto end;
  }

  dealer.Deal();
  response_l = dealer.FetchResponse();
  goto end;

end:
  cntl->http_response().set_content_type("text/plain");
  cntl->response_attachment().append(response_l);
  return ;
}

brpc::Server *NewHttpServer() {
  HttpServiceImpl *service = new HttpServiceImpl();
  brpc::Server *server = new brpc::Server();
  if (server->AddService(service, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
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
