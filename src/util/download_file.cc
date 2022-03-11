/*
  Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include <brpc/channel.h>
#include <brpc/progressive_reader.h>
#include <butil/logging.h>
#include <gflags/gflags.h>
#include <string>
#include <sys/fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <zettalib/errorcup.h>
#include <zettalib/tool_func.h>
#include <json/json.h>

DEFINE_string(url, "", "URL of the request reource");
DEFINE_string(out_prefix, "", "Downloaded File path prefix");
DEFINE_string(out_filename, "", "Downloaded File name");
DEFINE_int64(traffic_limit, 5242880, "Download traffic limit, default is 5242880(5 MB)");
DEFINE_bool(
    output_override, false,
    "Whether override the output file if destination has the same name or not");

class MyProgressiveReader : public brpc::ProgressiveReader,
                            public kunlun::ErrorCup {
public:
  MyProgressiveReader()
      : fd_(-1), finished_(false), file_name_("outfile"), path_prefix_("./"),
        success_(true){};
  ~MyProgressiveReader(){};
  bool Init() {
    if(!FLAGS_out_prefix.empty()){
      path_prefix_ = FLAGS_out_prefix;
    }
    if(!FLAGS_out_filename.empty()){
      file_name_ = FLAGS_out_filename;
    }
    std::string path = kunlun::ConvertToAbsolutePath(path_prefix_.c_str());
    if (path.empty()) {
      setErr("out_prefix %s is not exists", path_prefix_.c_str());
      return false;
    }
    path += "/";
    path += file_name_;

    if (kunlun::CheckFileExists(path.c_str())) {
      // remove firs
      if (!FLAGS_output_override) {
        setErr("%s already exists!", path.c_str());
        return false;
      }
      // unlink file
      unlink(path.c_str());
    }
    fd_ = open(path.c_str(), O_CREAT | O_WRONLY, S_IRUSR | S_IWUSR | S_IXUSR);
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
  }

  bool Finish() { return finished_; }
  bool Ok() { return success_; }

private:
  int fd_;
  bool finished_;
  std::string file_name_;
  std::string path_prefix_;
  bool success_;
};
int main(int argc, char *argv[]) {
  google::ParseCommandLineFlags(&argc, &argv, false);

  char usage[2048] = {'\0'};
  if(argc < 2){
    sprintf(usage,"./download_files -url=\"http://address:port/FileService/FilePath\" -out_prefix=\"prefix\" -out_filename=\"filename\" -output_override=false");
    fprintf(stderr,"Usage: %s\n",usage);
    exit(-1);
  }

  brpc::Channel channel;
  brpc::ChannelOptions options;

  options.timeout_ms = 3600000; // 1 hour
  options.protocol = "http";
  options.max_retry = 5;
  if (channel.Init(FLAGS_url.c_str(), "", &options) != 0) {
    fprintf(stderr, "Fail to initialize channel");
    exit(-1);
  }

  brpc::Controller cntl;
  cntl.http_request().uri() = FLAGS_url;
  cntl.http_request().set_method(brpc::HTTP_METHOD_POST);

  char buff[1024] = {'\0'};
  sprintf(buff,"%ld",FLAGS_traffic_limit);
  Json::Value root;
  root["traffic_limit"] = buff; 
  Json::FastWriter writer;
  writer.omitEndingLineFeed();
  cntl.request_attachment().append(writer.write(buff));

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
    exit(-1);
  }

  cntl.ReadProgressiveAttachmentBy(reader);

  while (!reader->Finish()) {
    sleep(1);
  }
  if (!reader->Ok()) {
    fprintf(stderr, "%s", reader->getErr());
    exit(-1);
  }
  exit(0);
}
