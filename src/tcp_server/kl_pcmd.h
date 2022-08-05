/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _NODE_MGR_KL_PCMD_H_
#define _NODE_MGR_KL_PCMD_H_
#include "zettalib/errorcup.h"
#include "buffer.hpp"

#include <stdio.h>
#include <string>
#include <errno.h>
#include <sched.h>
#include <vector>

using namespace kunlun;

namespace kunlun_tcp
{

class KCProcStat : public ErrorCup {
public:
   KCProcStat() : pid_(0), ppid_(0) {}
   virtual ~KCProcStat() {}
   bool Parse(const char * buf);
   
   std::string String () {
      Buffer buff;
      buff.Printf ("pid:%lu,bin:%s,state:%s,ppid:%lu" , pid_,
               binName_.c_str () , state_.c_str () , ppid_);
      return buff.AsString ();
   }

   pid_t GetPpid() const {
      return ppid_;
   }
   const std::string& GetbinName() const {
      return binName_;
   }

private:
   pid_t pid_;
   std::string binName_;
   std::string state_;
   pid_t ppid_;
};

class KPopenCmd : public ErrorCup {
public:
   KPopenCmd(uint64_t timeout) : timeout_(timeout), child_pid_(0),
         read_Fp_(NULL), read_Fd_(-1), write_Fp_(NULL), write_Fd_(-1), self_kill_(false) {
   }

   virtual ~KPopenCmd();
   bool OpenCmd ( const char *command , const char *mode , bool waitToExec = true );
   int Close (bool ignoreWait=false);
   void CloseWriteFp ();
   void CloseReadFp();
   void CloseAllFdFromVec(std::vector<int>& fd_vec);

   bool KillChildExecClose(pid_t ppid);
   bool GetProcStat(pid_t pid, KCProcStat& stat);
   void WaitChildToExec();
   std::string GetCmdPath();

   pid_t GetChildPid () const {
      return child_pid_;
   }
   FILE* GetReadFp () {
      return read_Fp_;
   }

   FILE* GetWriteFp () {
      return write_Fp_;
   }

   int GetReadFd () const {
      return read_Fd_;
   }

   int GetWriteFd () const {
      return write_Fd_;
   }

private:
   bool openImp(const char *command , const char *mode , bool waitToExec);

private:
   uint64_t timeout_;
   std::string cmd_;
   pid_t child_pid_;

   FILE * read_Fp_;
   int read_Fd_;

   FILE * write_Fp_;
   int write_Fd_;

   bool self_kill_;
};

} // namespace kunlun_tcp

#endif