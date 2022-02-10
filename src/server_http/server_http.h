#ifndef _NODE_MGR_HTTP_SERVER_H_
#define _NODE_MGR_HTTP_SERVER_H_

#include "brpc/server.h"
#include "log.h"
#include "proto/nodemng.pb.h"
#include "rapidjson/allocators.h"
#include "rapidjson/document.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"
#include "rapidjson/writer.h"
#include "zettalib/errorcup.h"

using namespace kunlunrpc;
class HttpServiceImpl : public kunlunrpc::HttpService,
                        public kunlun::ErrorCup {
public:
  HttpServiceImpl(){};
  virtual ~HttpServiceImpl(){};

  void Emit(google::protobuf::RpcController *, const HttpRequest *,
              HttpResponse *, google::protobuf::Closure *);

  bool ParseBodyToJsonDoc(const std::string &, rapidjson::Document *);

private:
};

extern brpc::Server *NewHttpServer();

#endif /*_NODE_MGR_HTTP_SERVER_H_*/
