/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _NODE_MGR_TCP_SERVER_H_
#define _NODE_MGR_TCP_SERVER_H_
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <event2/event_compat.h>
#include <vector>
#include "zettalib/op_log.h"
//#include "zettalib/biodirectpopen.h"
#include "zettalib/zthread.h"
#include "tcp_comm.h"
#include "kl_pcmd.h"

using namespace kunlun;

namespace kunlun_tcp {

class KWaitChildPid : public ZThread {
public:
    KWaitChildPid() {
        memset(errinfo, 0, 1024);
    }
    virtual ~KWaitChildPid() {}

	int run();
	void AddChildPid(pid_t pid);
	bool CheckPidStat(pid_t pid);
	pid_t GetFirstChildPid();
	
private:
	std::mutex mux_;
	std::vector<pid_t> childpids_;
    char errinfo[1024];
};

class KConnServer : public KConnBase
{
public:
	KConnServer(int sockfd) : KConnBase(sockfd) {}
	virtual ~KConnServer() {}

	const std::string& GetRemoteIp() const{
		return remote_ip_;
	}
	void SetRemoteIp(const std::string& remote_ip){
		remote_ip_ = remote_ip;
	}

	int GetRemotePort() const{
		return remote_port_;
	}
	void SetRemotePort(int remote_port) {
		remote_port_ = remote_port;
	}

	const std::string& GetLocalIp() const {
		return local_ip_;
	}
	void SetLocalIp(const std::string& local_ip){
		local_ip_ = local_ip;
	}

	int GetLocalPort() const{
		return local_port_;
	}
	void SetLocalPort(int local_port){
		local_port_ = local_port;
	}

	const std::string& GetUser() const{
		return user_;
	}
	void SetUser(const std::string& user){
		user_ = user;
	}
	
	const std::string& GetPwd() const {
		return pwd_;
	}
	void SetPwd(const std::string& pwd) {
		pwd_ = pwd;
	}
	
public:
	bool CheckPidExit( pid_t pid , int& retcode);
	
private:
	std::string remote_ip_;
	int remote_port_;
	std::string local_ip_;
	int local_port_;
	std::string user_;
	std::string pwd_;
};

class AsyncIoServer : public AsyncIoBase
{
public:
	AsyncIoServer(int jobtype) : jobtype_(jobtype), servconn_ (NULL), 
                fstat_(NULL), bIsEnd_(false) {}
	virtual ~AsyncIoServer() {}

	void SetIsEnd(bool bIsEnd) {
		bIsEnd_ = bIsEnd;
	}
	bool GetIsEnd() const {
		return bIsEnd_;
	}
	
	int run();
    void StartWork ( KConnServer * servconn, KFileStat* fstat);

	int KReadFileMsg();
	int KReadCommandOutput();

private:
	int jobtype_;
	KConnServer* servconn_;
	KFileStat* fstat_;
	bool bIsEnd_;
};

class KServerHandle: public KCommHandle, public ZThread {
public:
    KServerHandle() : servstat_(KL_INITIALIZE),servconn_(NULL),
                    kp_file_(NULL), server_rd_(NULL), rpoll_time_(5000) {}

    virtual ~KServerHandle() {
    	if(kp_file_) {
			delete kp_file_;
			kp_file_ = NULL;
		}
		if(server_rd_) {
			delete server_rd_;
			server_rd_ = NULL;
		}
		if(servconn_) {
        	delete servconn_;
			servconn_ = NULL;
		}
    }
	
    int run();
    void StartWork ( KConnServer* servconn );
	void SetRPollTime(uint32_t rpoll_time) {
        rpoll_time_ = rpoll_time;
    }

	KConnServer* GetServConn(){
		return servconn_;
	}
	Task_State GetState(){
		return servstat_;
	}
	void SetState(Task_State servstat){
		servstat_ = servstat;
	}
	
	KPopenCmd* GetKpFile(){
		return kp_file_;
	}

	AsyncIoServer* GetServerIoRd() {
		return server_rd_;
	}

protected:
	void HandleClientMsg();
    int KlCommandHandle(KlCmdReq* cmdreq);
    int KlPopenExecCommand(const char *cmd, const char* mode);
    int KlSystemExecCommand(const char *cmd);
    int KlFileHandle(KlFileReq* filereq);
    int KlTcpMsgWriteCommand(Buffer& sbuf, uint32_t rChecksum);
    void ClearTmpEnv(int rc);

	bool CheckChildState();
    void SetAconnState(Aconn_State state);
	
private:
	Task_State servstat_;
	KConnServer* servconn_;
	KPopenCmd* kp_file_;
	AsyncIoServer *server_rd_;
    uint32_t rpoll_time_;
};

class CTcpBase {
public:
    CTcpBase(const std::string& conf_file, const std::string& local_ip, int32_t tcp_port) : tcp_sock_(-1), 
                        conf_file_(conf_file), local_ip_(local_ip), listen_port_(tcp_port) {}
    virtual ~CTcpBase() {}

    int Init();
    void Run();
    void ParseTcpServerConf();
    const std::string GetLocalIp() const {
        return local_ip_;
    }
    int32_t GetListenPort() const {
        return listen_port_;
    }
public:
    static void AcceptCB(evutil_socket_t fd, short which, void* v);
private:
    int tcp_sock_;
    std::string conf_file_;
    
    //tcp_server.conf
    std::string log_file_path_;
    int max_log_file_size_;
    std::string local_ip_;
    int32_t listen_port_;
    uint32_t rpoll_time_;
};

}
#endif