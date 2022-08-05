/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "zettalib/tool_func.h"
#include "kl_pcmd.h"
#include "tcp_comm.h"
#include <sys/wait.h>
#include <fcntl.h>

namespace kunlun_tcp 
{

bool KCProcStat::Parse(const char * buf) {
    std::vector<std::string> vec = StringTokenize(buf , " ");
    if (vec.size () < 4) {
        setErr("get buf is not valid ,size < 4,buf:%s" , buf);
        return false;
    }

    pid_ = strtoull(vec[0].c_str(), NULL, 10);
    binName_ = trim(vec[1], "()");
    state_ = vec[2];
    ppid_ = strtoull(vec[3].c_str(), NULL, 10);

    return true;
}

KPopenCmd::~KPopenCmd() {
    Close();
}

void KPopenCmd::CloseAllFdFromVec(std::vector<int>& fd_vec) {
    for(auto fd : fd_vec) {
        ::close(fd);
    }
    fd_vec.clear();
}

bool KPopenCmd::OpenCmd( const char *command , const char *mode , bool waitToExec) {
    return openImp (command , mode , waitToExec);
}

bool KPopenCmd::openImp(const char *command , const char *mode , bool waitToExec) {
    int parentReadFds[2];
    parentReadFds[0] = parentReadFds[1] = -1;
    int parentWriteFds[2];
    parentWriteFds[0] = parentWriteFds[1] = -1;

    int do_read = 0;
    int do_write = 0;
    bool redirectErrToOutput = false;
    int do_cloexec = 1;

    while (*mode != '\0') {
        switch (*mode++) {
            case 'r': {
                do_read = 1;
                break;
            }
            case 'w': {
                do_write = 1;
                break;
            }
            case 'e': {
                do_cloexec = 1;
                break;
            }
            case '2': {
                redirectErrToOutput = true;
                break;
            }
            default: {
                setErr("unknown mode:%s" , mode);
                return false;
            }
        }
    }

    if ((!do_read) && (!do_write)) { 
        setErr("mode must have read or write read[%d]/write[%d] op,mode:%s" , 
                    do_read , do_write , mode);
        return false;
    }

    int parentRd = -1;
    int parentWr = -1;
    int childRd = -1;
    int childWr = -1;
    int childEWr = -1;
    std::vector<int> smallFdVec;
    if (do_read) {
        while (1) {
            if (pipe (parentReadFds) < 0) {
                setErr("pipe2 error:%d,%s" , errno , strerror (errno));
                CloseAllFdFromVec (smallFdVec);
                return false;
            }
            if (parentReadFds[0] <= 2 || parentReadFds[1] <= 2) { 
                smallFdVec.push_back (parentReadFds[0]);
                smallFdVec.push_back (parentReadFds[1]);
                continue;
            }
            parentRd = parentReadFds[0];
            childWr = parentReadFds[1];
            break;
        }
    }
    
    if (do_write) {
        while (1) {
            if (pipe (parentWriteFds) < 0) {
                setErr("pipe2 error:%d,%s" , errno , strerror (errno));
                CloseAllFdFromVec(smallFdVec);
                return false;
            }
            if (parentWriteFds[0] <= 2 || parentWriteFds[1] <= 2) {
                smallFdVec.push_back (parentWriteFds[0]);
                smallFdVec.push_back (parentWriteFds[1]);
                continue;
            }
            parentWr = parentWriteFds[1];
            childRd = parentWriteFds[0];
            break;
        }
    }

    CloseAllFdFromVec (smallFdVec);

    if (do_cloexec) {
        if (do_read) {
            SetFdCloexec(parentRd);
            SetFdCloexec(childWr);
        }
        if (do_write) {
            SetFdCloexec(parentWr);
            SetFdCloexec(childRd);
        }
    }
    
    child_pid_ = fork ();
    if (child_pid_ < 0) {
        if (do_read) {
            ::close (parentReadFds[0]);
            ::close (parentReadFds[1]);
        }
        if (do_write) {
            ::close (parentWriteFds[0]);
            ::close (parentWriteFds[1]);
        }

        setErr("fork error:%d:%s" , errno , strerror (errno));
        return false;
    } else if (0 == child_pid_) {
        if (childWr != -1 && childWr != STDOUT_FILENO) {
            dup2 (childWr , STDOUT_FILENO);
            childWr = STDOUT_FILENO;
        }

        if (redirectErrToOutput) {
            if (parentReadFds[1] != STDERR_FILENO) {
                dup2 (parentReadFds[1] , STDERR_FILENO);
            }
            childEWr = STDERR_FILENO;
        }

        if (childRd != -1 && childRd != STDIN_FILENO) {
            dup2 (childRd , STDIN_FILENO);
            childRd = STDIN_FILENO;
        }

        if (do_read) {
            ::close (parentReadFds[0]);
            if (parentReadFds[1] != childWr && parentReadFds[1] != childEWr) {
                ::close (parentReadFds[1]);
            }
        }
        if (do_write) {
            if (parentWriteFds[0] != childRd) {
                ::close (parentWriteFds[0]);
            }
            ::close (parentWriteFds[1]);
        }

        if (childRd >= 0) {
            fcntl (childRd , F_SETFD , 0);
        }
        if (childWr >= 0) {
            fcntl (childWr , F_SETFD , 0);
        }
        if (childEWr >= 0) {
            fcntl (childEWr , F_SETFD , 0);
        }

        signal (SIGPIPE , SIG_DFL);

        const char *new_argv[4];
        new_argv[0] = "sh";
        new_argv[1] = "-c";
        new_argv[2] = command;
        new_argv[3] = NULL;
            
        (void) execv ("/bin/sh" , (char * const *) new_argv);
        _exit ((1 << 8) | 127);
        setErr("after execv,can not reach here ");
        return false;
    } else {
        if (parentRd != -1) {
            read_Fp_ = fdopen (parentRd , "r");
            if (!read_Fp_) {
                setErr("fdopen parent_end:%d,mode:r is error:%d:%s", 
                            parentRd , errno , strerror (errno));
                ::close (parentReadFds[0]);
                ::close (parentReadFds[1]);
                return false;
            } else {
                ::close (parentReadFds[1]);
            }
            read_Fd_ = parentRd;
        }

        if (parentWr != -1) {
            write_Fp_ = fdopen (parentWr , "w");
            if (!write_Fp_) {
                setErr("fdopen parent_end:%d,mode:w is error:%d:%s" , 
                            parentWr , errno , strerror (errno));
                ::close (parentWriteFds[0]);
                ::close (parentWriteFds[1]);
                return false;
            } else {
                ::close (parentWriteFds[0]);
            }
            write_Fd_ = parentWr;
        }

        cmd_ = command;

        if (waitToExec) {
            WaitChildToExec();
        }

        if(self_kill_) {
            Close();
            return false;
        }
        return true;
    }
}

void KPopenCmd::WaitChildToExec() {
    std::string curBin = ConvertToAbsolutePath(GetCmdPath().c_str());

    if (curBin.size ()) {
        KCProcStat stat;
        int waitCount = 0;
        bool getStat = false;
        usleep (1000 * 4);

        uint64_t beginTime = GetClockMonoMSec();
        while (true == (getStat = GetProcStat (child_pid_ , stat))) {
            if ((uint64_t) getpid () == stat.GetPpid()) {
                if (curBin == stat.GetbinName()) { 
                    uint64_t curTime = GetClockMonoMSec();
                    if (curTime - beginTime <= 1000) {
                        usleep (1000 * 4);
                        continue;
                    }
                    ++waitCount;

                    if (waitCount > 10) {
                        int ret = kill (child_pid_ , SIGKILL);
                        if(!ret)
                            self_kill_ = true;
                        setErr("child stat:%s is waiting too long,kill ret:%d \n" , 
                                stat.String().c_str (), ret);
                        break;
                    }
            
                    sleep (1);
                } else {
                    break;
                }
            } else {
                setErr("curBin:%s,getpid:%lu != stat:%s\n", curBin.c_str () , (uint64_t )getpid () ,
                                stat.String().c_str ());
                break;
            }
        }
    } 
}

std::string KPopenCmd::GetCmdPath() {
    char current_absolute_path[512] = { 0 };
    int cnt = readlink ("/proc/self/exe" , current_absolute_path , sizeof(current_absolute_path) - 1);
    if (cnt < 0 || cnt >= (int) sizeof(current_absolute_path) - 1) {
        setErr("readlink error:%d,%s" , errno , strerror(errno));
        return "";
    }
    else {
        return current_absolute_path;
    }
}
   
int KPopenCmd::Close(bool ignoreWait) {
    if ((!read_Fp_) && (!write_Fp_)) {
        return 0;
    }

    if (read_Fp_) {
        fclose (read_Fp_);
        read_Fp_ = NULL;
        read_Fd_ = -1;
    }

    if (write_Fp_) {
        fclose (write_Fp_);
        write_Fp_ = NULL;
        write_Fd_ = -1;
    }

    int status = 1 << 8;

    if (child_pid_ <= 0) {
        return 0;
    }

    if (timeout_ > 0) { 
        if(!KillChildExecClose(getpid ())) {
            fprintf (stderr , "kill sub pid:%ld error:%s\n", child_pid_ , getErr());
        } else {
            fprintf (stderr , "kill sub pid:%ld success\n", child_pid_);
        }
    }

    if (!ignoreWait) {
        while (waitpid (child_pid_ , &status , 0) < 0) {
            if (errno != EINTR) {
                break;
            }
        }
    } else {
        status = 0;
    }

    child_pid_ = 0;

    return WEXITSTATUS(status);
}
   
void KPopenCmd::CloseWriteFp() {
    if (write_Fp_) {
        fclose (write_Fp_);
        write_Fp_ = NULL;
        write_Fd_ = -1;
    }
}

void KPopenCmd::CloseReadFp() {
    if (read_Fp_) {
        fclose (read_Fp_);
        read_Fp_ = NULL;
        read_Fd_ = -1;
    }
}

bool KPopenCmd::GetProcStat(pid_t pid, KCProcStat& stat ) {
    Buffer cmd;
    cmd.Printf("/proc/%lu/stat" , (uint64_t)pid);

    Buffer buff;
    char errbuf[1024] = {0};
    if (!ReadBufferFromFile(cmd.c_str () , buff , 1024, errbuf)) {
        return false;
    }

    if (!stat.Parse(buff.c_str())) {
        return false;
    }

    return true;
}

bool KPopenCmd::KillChildExecClose(pid_t ppid) {
    KCProcStat stat;
    if (!GetProcStat (child_pid_ , stat)) {
        setErr("%s", stat.getErr());
        return false;
    }
    if (stat.GetPpid() != (uint64_t) ppid) {
        setErr("get child proc stat is:%s,not match fater pid:%ld", stat.String().c_str (), 
                        (long ) ppid);
        return false;
    }
    int r = kill (child_pid_ , 9);
    if (r < 0) {
        setErr("kill child pid:%ld error:%d,%s" , (long ) child_pid_ , errno , strerror(errno));
        return false;
    }
    else {
        return true;
    }
}

}