/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include <brpc/channel.h>
#include <brpc/progressive_reader.h>
#include <butil/logging.h>
#include <error.h>
#include <gflags/gflags.h>
#include <json/json.h>
#include <string>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <zettalib/errorcup.h>
#include <zettalib/tool_func.h>

DEFINE_string(host, "", "host information");
DEFINE_string(port, "", "port information");
DEFINE_string(cmd, "", "command with argument");

class MyProgressiveReader : public brpc::ProgressiveReader,
                            public kunlun::ErrorCup {
public:
  MyProgressiveReader()
      : fd_(-1), finished_(false), tmp_file_path_(""), success_(true){};
  ~MyProgressiveReader(){
      // unlink(tmp_file_path_.c_str());
  };
  bool Init() {
    tmp_file_path_ =
        kunlun::string_sprintf("./.kunluntest_client.tmp.%d", getpid());
    fd_ = open(tmp_file_path_.c_str(), O_CREAT | O_RDWR,
               S_IRUSR | S_IWUSR | S_IXUSR);
    if (fd_ < 0) {
      setErr("Open() Failed: %s", strerror(errno));
      return false;
    }

    return true;
  }
  virtual butil::Status OnReadOnePart(const void *data,
                                      size_t length) override {
    butil::Status status;

    int ret = write(fd_, data, length);
    if (ret < 0) {
      status.set_error(errno, "Write() Failed: %s", strerror(errno));
      goto end;
    }
  end:
    return status;
  }
  virtual void OnEndOfMessage(const butil::Status &status) override {
    finished_ = true;
    if (!status.ok()) {
      setErr("%s", status.error_cstr());
      success_ = false;
    }
    close(fd_);
    FILE *fp = fopen(tmp_file_path_.c_str(), "r");
    if (!fp) {
      fprintf(stderr, "%s", strerror(errno));
    }
    char buffer[1024] = {'\0'};
    while (fgets(buffer, 1024, fp)) {
      write(1, buffer, 1024);
      bzero((void *)buffer, 1024);
    }
    fclose(fp);
  }

  bool Finish() { return finished_; }
  bool Ok() { return success_; }

private:
  int fd_;
  bool finished_;
  std::string tmp_file_path_;
  bool success_;
};

int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, false);

  char usage[2048] = {'\0'};
  if (argc < 2) {
    sprintf(usage, "./test_client -host=\"ip\" -port=\"port\" -cmd=\"command "
                   "string with argument\" ");
    fprintf(stderr, "Usage: %s\n", usage);
    exit(-1);
  }

  brpc::Channel channel;
  brpc::ChannelOptions options;
  std::string url = kunlun::string_sprintf(
      "http://%s:%s/HttpService/Shell", FLAGS_host.c_str(), FLAGS_port.c_str());

  options.timeout_ms = 3600000; // 1 hour
  options.protocol = "http";
  options.max_retry = 5;
  if (channel.Init(url.c_str(), "", &options) != 0) {
    fprintf(stderr, "Fail to initialize channel");
    exit(-1);
  }

  brpc::Controller cntl;
  cntl.http_request().uri() = url;
  cntl.http_request().set_method(brpc::HTTP_METHOD_POST);

  Json::FastWriter writer;
  Json::Value root;
  root["command"] = FLAGS_cmd;
  writer.omitEndingLineFeed();
  cntl.request_attachment().append(writer.write(root));

  cntl.response_will_be_read_progressively();
  channel.CallMethod(NULL, &cntl, NULL, NULL, NULL);
  if (cntl.Failed()) {
    fprintf(stderr, "%s", cntl.ErrorText().c_str());
    exit(-1);
  }

  auto reader = new MyProgressiveReader();
  bool ret = reader->Init();
  if (!ret) {
    fprintf(stderr, "%s", reader->getErr());
    delete reader;
    exit(-1);
  }

  cntl.ReadProgressiveAttachmentBy(reader);

  while (!reader->Finish()) {
    sleep(1);
  }
  if (!reader->Ok()) {
    fprintf(stderr, "%s", reader->getErr());
    delete reader;
    exit(-1);
  }

  delete reader;
  exit(0);
}
