/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include <event2/event.h>
#include <event2/bufferevent.h>
#include <event2/buffer.h>
#include <event2/listener.h>
#include <event2/util.h>
#include <event2/event_compat.h>
#include "tcp_server/tcp_comm.h"

using namespace kunlun_tcp;

class KConnClient : public KConnBase {
public:
    KConnClient(int sockfd) : KConnBase(sockfd) {}
    virtual ~KConnClient() {}

    int GetRdTimeout() const {
        return rd_timeout_;
    }
    void SetRdTimeout(int rd_timeout) {
        rd_timeout_ = rd_timeout;
    }

    void ClientNotifyExit(int extCode=-1, bool bSend=true);

private:
    std::string server_ip_;
    int server_port_;
    int rd_timeout_;
};

class AsyncIoClient : public AsyncIoBase {
public:
    AsyncIoClient() : clientconn_(NULL), fstat_(NULL) {}
    virtual ~AsyncIoClient() {}

    int run();
    void StartWork(KConnClient* clientconn, KFileStat* fstat);

    KFileStat* GetFstat() {
        return fstat_;
    }
protected:
    int ReadStdInMsg();
private:
    KConnClient *clientconn_;
    KFileStat *fstat_;
};

class KClientHandle : public KCommHandle {
public:
    KClientHandle() : cli_state_(KL_INITIALIZE), clientconn_(NULL),
            cli_rd_(NULL) {}
    virtual ~KClientHandle() {
        if(cli_rd_)
            delete cli_rd_;
        if(clientconn_)
            delete clientconn_;
    }
    Task_State GetState() const {
        return cli_state_;
    }
    void SetState(Task_State cli_state) {
        cli_state_ = cli_state;
    }

    KConnClient *GetClientConn() {
        return clientconn_;
    }
    AsyncIoClient *GetCliRd() {
        return cli_rd_;
    }
    void SetCliRd(AsyncIoClient* cli_rd) {
        cli_rd_ = cli_rd;
    }

    void SetCmdBeginTime(uint64_t cmd_btime) {
        cmd_btime_ = cmd_btime;
    }
    uint64_t GetCmdBeginTime() const {
        return cmd_btime_;
    }
public:
    static void SockCB(evutil_socket_t fd, short which, void* v);

public:
    int ConnectServer(const std::string& conn_ip, int conn_port, int rd_timeout);
    void SockTimeoutHandle(evutil_socket_t fd, void* v);
    void ReadSockMsgHandle(evutil_socket_t fd, void* v);
    bool CheckCommandTimeout();
    void CheckInCommandChecksum(KlCommReq* commreq);
    void GetCommandResult(Buffer& sbuf);
    void GetCommandResult(KlCommResp* commresp);
    int SendCommandReq(const std::string& cmd, const std::string& mode, uint32_t cmd_type);

private:
    Task_State cli_state_;
    KConnClient *clientconn_;
    AsyncIoClient *cli_rd_;
    uint64_t cmd_btime_;
};
