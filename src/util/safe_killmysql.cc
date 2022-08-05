/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include <stdio.h>
#include <vector>
#include <string>
#include <fstream>
#include "util_func/file_traverse.h"
#include "zettalib/biodirectpopen.h"

using namespace kunlun;

std::string trim ( const std::string& from , const std::string& chars = " \t\n\r" ) {
    size_t start = from.find_first_not_of (chars);
    size_t end = from.find_last_not_of (chars);
    if (start == std::string::npos) {
        return "";
    }

    return from.substr (start , end + 1 - start);
}

void TokenizeCmdLine(const char* src, size_t src_len, std::vector<std::string>& vec) {
    char sub[src_len] = {0};
    size_t b_pos = 0, i=0;
    for(i=0; i<src_len; i++) {
        if(src[i] == '\0') {
            if(i > b_pos) {
                memset(sub, 0, src_len);
                memcpy(sub, src+b_pos, i-b_pos);
                vec.push_back(sub);
                b_pos = i+1;
            }
            i = i+1;
        }
    }
    if(i > b_pos) {
        memset(sub, 0, src_len);
        memcpy(sub, src+b_pos, i-b_pos);
        vec.push_back(sub);
    }
}

std::string GetFileContent(const std::string& path) {
    std::string cont;
    std::ifstream fin(path.c_str(), std::ios::in);
    if(!fin.is_open()) {
        fprintf(stderr, "open file: %s failed: %d, %s", path.c_str(), errno, strerror(errno));
        return cont;
    }

    std::string sbuf;
    while(std::getline(fin, sbuf)) {
        trim(sbuf);
        cont = cont+sbuf;
    }

    return cont;
}

class GetKillMysqlPid {
public:
    GetKillMysqlPid(const std::vector<std::string>& kill_tags, const std::string& match_port, 
                    const std::string& data_dir) : kill_tags_(kill_tags), match_port_(match_port),
                    data_dir_(data_dir) {}
    virtual ~GetKillMysqlPid() {} 

    int execute(const std::string& path, FileType f_type) {
        if(f_type == CREG) {
            std::string file = path.substr(path.rfind("/")+1);
            if(file == "comm") {
                std::string tags = GetFileContent(path);
                if(tags.empty())
                    return 1;

                if(std::find(kill_tags_.begin(), kill_tags_.end(), tags) != kill_tags_.end()) {
                    std::string sub_path = path.substr(0, path.rfind("/"));
                    std::string cmd_path = sub_path + "/cmdline";
                    std::string cont = GetFileContent(cmd_path);
                    std::vector<std::string> cont_vec;
                    TokenizeCmdLine(cont.c_str(), cont.length(), cont_vec);
                    std::string match_tag = "--defaults-file="+data_dir_+"/"+match_port_+"/data/"+match_port_+".cnf";
                    int match_flag = 0;
                    for(size_t i=0; i<cont_vec.size(); i++) {
                        if(cont_vec[i] == match_tag) {
                            match_flag = 1;
                            break;
                        }
                    }
                    //if(cont.find(match_tag) != std::string::npos) {
                    if(match_flag) {         
                        std::string pid = sub_path.substr(sub_path.rfind("/")+1);
                        pids_.push_back(pid);
                    }
                }
            }
        }
        return 0;
    }

    int directory_ignore(const std::string& path) {
        return 0;
    }

    int skip_directory(const std::string& path) {
        std::string tag = path.substr(path.rfind("/")+1);
        if(tag == "task")
            return 1;
        return 0;
    }
    int scan_level() {
        return 2;
    }

    const std::vector<std::string> GetPids() const {
        return pids_;
    }
private:  
    std::vector<std::string> pids_;
    std::vector<std::string> kill_tags_;
    std::string match_port_;
    std::string data_dir_;
};

int main(int argc, char* argv[]) {
    if(argc != 3) {
        fprintf(stderr, "Usage: %s port datadir\n", argv[0]);
        return -1;
    }
    std::string port = argv[1];
    std::string datadir = argv[2];

    std::vector<std::string> kill_tags;
    kill_tags.push_back("mysqld");
    kill_tags.push_back("mysqld_safe");
    GetKillMysqlPid *killMysql = new GetKillMysqlPid(kill_tags, port, datadir);
    CFile_traverse<GetKillMysqlPid> file_trav("/proc", killMysql);
    int ret = file_trav.parse();
    if(ret) {
        fprintf(stderr, "traverse /proc file failed: %s", file_trav.getErr());
        return 1;
    }
    std::vector<std::string> pids = killMysql->GetPids();
    if(pids.size() == 0) {
        //fprintf(stderr, "get port: %s mysql pid failed\n", port.c_str());
        return 0;
    }
    
    std::string buff = "kill -9 ";
    for(size_t i=0; i<pids.size(); i++)
        buff = buff + pids[i] + " ";
    
    BiodirectPopen bi_open(buff.c_str());
    if(!bi_open.Launch("rc")) {
        fprintf(stderr, "biodirect popen lanuch cmd: %s failed: %s\n", buff.c_str(), bi_open.getErr());
        return 1;
    }
    if(bi_open.get_chiled_status()) {
        fprintf(stderr, "biodirect popen get child status failed\n");
        return 1;
    }
    return 0;
}
