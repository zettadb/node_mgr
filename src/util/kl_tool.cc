/*
  Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

  This source code is licensed under Apache 2.0 License,
  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include <iostream>
#include "kl_tool.h"
#include "tcp_server/tcp_error.h"
#include "boost/program_options.hpp"

namespace po = boost::program_options;

void KConnClient::ClientNotifyExit(int extCode, bool bSend) {
    if(bSend) {
		if(ReplyCommAnswer(KL_TCP_ERR_INTERNAL_ERROR, KL_QUIT_OP)) {
			fprintf(stderr, "Send stdin quit msg to server failed: %s\n", getErr());
		}
	}
	exit(extCode);
}

void AsyncIoClient::StartWork(KConnClient* clientconn, KFileStat* fstat) {
    clientconn_ = clientconn;
    fstat_ = fstat;
    start();
}

int AsyncIoClient::run() {
    return ReadStdInMsg();
}

int AsyncIoClient::ReadStdInMsg() {
    int numread = 0;
	int fd = STDIN_FILENO;
	if(UnsetNonBlock(fd)) {
		fprintf(stderr,"Set stdin fd: %d block failed\n", fd);
		clientconn_->ClientNotifyExit();
	}
	
	int64_t g_timeout = clientconn_->GetTimeout();

	uint64_t begintime, endtime;
	begintime = GetClockMonoMSec();
	if(clientconn_->ApplyBuffMem(rdCmdBuf_, BUFSIZE+packetHeadLen)) {
		fprintf(stderr, "Read cmd buff failed: %s\n", clientconn_->getErr());
		clientconn_->ClientNotifyExit();
	}
	
	if(clientconn_->PackCommHeader(KL_PACK_OP, rdCmdBuf_)) {
		fprintf(stderr, "%s\n", clientconn_->getErr());
		clientconn_->ClientNotifyExit();
	}
	
	while(1) {	
		rdCmdBuf_.SetWriteIndex(packetHeadLen);
     
		numread = BlockReadCommand(fd);
		if(numread < 0) {
			fprintf(stderr, "Read stdin msg failed: %s\n", getErr());
			clientconn_->ClientNotifyExit();
		} else if (0 == numread) {
            if(clientconn_->ReplyCommAnswer(0, KL_END_OP)) {
				fprintf(stderr, "Send stdin end msg to server failed: %s\n", clientconn_->getErr());
				clientconn_->ClientNotifyExit(-1, false);
			}
			break;
        }
		
		uint32_t hash=0;
		if(clientconn_->CalcSendChecksum(rdCmdBuf_, packetHeadLen, hash)) {
			fprintf(stderr, "Calculate msg checksum failed: %s\n", clientconn_->getErr());
			clientconn_->ClientNotifyExit();
		}
		
		fstat_->KlMsgChecksum(!(clientconn_->GetKlType() & KL_NCHECKSUM), hash);
		
		if(clientconn_->KlSendMsg(rdCmdBuf_, hash)) {
			fprintf(stderr, "Send stdin input msg to server failed: %s\n", clientconn_->getErr());
			clientconn_->ClientNotifyExit(-1, false);
		}
		
		endtime = GetClockMonoMSec();
		g_timeout -= KlCostMsec(begintime, endtime);
		begintime = endtime;
		if(g_timeout <= 0) {
			fprintf(stderr, "kl_tool: send data flow timeout!\n");
			if(clientconn_->ReplyCommAnswer(0, KL_TIMEOUT_OP)) {
				fprintf(stderr, "Send cmd timeout msg to server failed: %s\n", clientconn_->getErr());
			}
			clientconn_->ClientNotifyExit();
		}
	}
	return 0;
}

int KClientHandle::ConnectServer(const std::string& conn_ip, int conn_port, int rd_timeout) {
    struct sockaddr * addr = NULL;
	struct sockaddr_in servaddr;
	socklen_t socklen = 0;

	if(conn_ip.empty()) {
		setErr("Connect server ip is empty, please check");
		return -1;
	}

	if(conn_port <= 0 ) {
		setErr("Connect server port is wrong, please check");
		return -1;
	}

	int sockfd = -1; 
	sockfd = socket(AF_INET, SOCK_STREAM, 0);
	if(sockfd < 0) {
		setErr("Can't Create Socket Handle[%s:%d][%d:%s]",conn_ip.c_str(),
                conn_port,errno,strerror(errno));
		return -1;
	}

	if(evutil_make_socket_nonblocking(sockfd) < 0) {
		setErr("kl_tool set sockfd: %d nonblock failed", sockfd);
		close(sockfd);
		return -1;
	}

	int on = 1;
	if(setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on)) < 0) {
		setErr("kl_tool set sockfd: %d keepalive failed", sockfd);
		close(sockfd);
		return -1;
	}

	bzero (&servaddr , sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr (conn_ip.c_str());
    servaddr.sin_port = htons (conn_port);

    addr = (struct sockaddr *) &servaddr;
    socklen = sizeof(struct sockaddr_in);

	errno = 0;
	if(connect(sockfd , addr , socklen) < 0) {
		if(errno == EINPROGRESS) {
			struct pollfd fds[1];
		    bzero (fds , sizeof(fds));
		    fds[0].fd = sockfd;
		    fds[0].events = POLLOUT;

again:
			int ret = poll (fds , 1 , rd_timeout);
			if(ret < 0) {
				if(errno == EINTR) 
					goto again;
				
				close(sockfd);
	            setErr("Connect poll error[%d,%s]" , errno , strerror (errno));
	            return -1;
			} else if(ret == 0) {
				close(sockfd);
				setErr("Connect Socket Handle timeout");
				return -1;
			}
		} else {
			setErr("Connect Socket Handle[%s:%d][%d:%s]",
                            conn_ip.c_str(),conn_port,errno,strerror(errno));
			close(sockfd);
			return -1;
		}
	}

	clientconn_ = new KConnClient(sockfd);
	clientconn_->SetRdTimeout(rd_timeout);
	return 0;
}

void KClientHandle::SockCB(evutil_socket_t fd, short which, void* v) {
    KClientHandle* cli_hd = static_cast<KClientHandle*>(v);
    if(which & EV_TIMEOUT) 
        cli_hd->SockTimeoutHandle(fd, v);
    else  
        cli_hd->ReadSockMsgHandle(fd, v);
}

bool KClientHandle::CheckCommandTimeout() {
    uint64_t endtime = GetClockMonoMSec();
	if(KlCostMsec(cmd_btime_, endtime) > clientconn_->GetTimeout()) {
		if(clientconn_->ReplyCommAnswer(0, KL_TIMEOUT_OP)) {
			fprintf(stderr, "Send cmd timeout msg to server failed: %s\n", clientconn_->getErr());
		}
		return true;
	}
	return false;
}

void KClientHandle::SockTimeoutHandle(evutil_socket_t fd, void* v) {
    if(cli_state_ == KL_SEND_COMMAND_SERVER) {
		if(CheckCommandTimeout()) {
			fprintf(stderr, "Execute command timeout!\n");
			clientconn_->ClientNotifyExit();
		}
	}
	return;
}

void KClientHandle::ReadSockMsgHandle(evutil_socket_t fd, void* v) {
    int len = 0;

	uint32_t rChecksum=0;
	KlCommHead klhead;
	KlCommReq commreq;
	KlCommResp commresp;
	
again:
	recvBuf_.Clear();
	len = clientconn_->KlReadMsg(recvBuf_, &rChecksum, clientconn_->GetRdTimeout());
	if(len <= 0 ) {
		if(len == KL_TCP_ERR_SOCKET_READ_TIMEOUT) {
			usleep(1000 * 10);
			goto again;
		}
		fprintf(stderr, "Read sockfd: %d failed: %s\n", clientconn_->GetSockfd(), clientconn_->getErr());
		clientconn_->ClientNotifyExit(-1,false);
	}

	if(KlDeserializeHeadPacket(recvBuf_, &klhead)) {
		fprintf(stderr, "Deserialze recv head packet failed, so quit!\n");
		clientconn_->ClientNotifyExit();
	}

	if(!clientconn_->IsSeqEqual(klhead.innseq_)) {
		fprintf(stderr, "Innseq is not match expect seq: %d, but receive seq: %d\n", 
                    clientconn_->GetInnseq(), klhead.innseq_);
		clientconn_->ClientNotifyExit();
	}

	if(!clientconn_->ChecksumCompareResult(recvBuf_, 0, rChecksum)) {
		fprintf(stderr, "Recv comm packet checksum failed: %s\n", clientconn_->getErr());
		clientconn_->ClientNotifyExit();
	}
	
	switch(klhead.cmdtype_) {
		case KL_CHECKSUM_OP: {
			if(KlDeserializeCommReqPacket(recvBuf_, &commreq)) {
				fprintf(stderr, "Deserialze recv command checksum packet failed, so quit!\n");
				clientconn_->ClientNotifyExit();
			}
			CheckInCommandChecksum(&commreq);
		}
		break;
		case KL_PACK_OP: {
			GetCommandResult(recvBuf_);
		}
		break;
		case KL_END_OP: {
			if(KlDeserializeCommRespPacket(recvBuf_, &commresp)) {
				fprintf(stderr, "Deserialze recv command reponse packet failed, so quit!\n");
				clientconn_->ClientNotifyExit();
			}
			GetCommandResult(&commresp);
		}
		break;
		default:
		break;
	}
	
	return;
}

void KClientHandle::GetCommandResult(Buffer& sbuf) {
    if(CheckCommandTimeout()) {
		fprintf(stderr, "Execute command timeout!\n");
		clientconn_->ClientNotifyExit();
	}

	uint32_t hash=0;
	if(clientconn_->CalcSendChecksum(sbuf, 0, hash)) {
		fprintf(stderr,"Calculate send msg checksum failed: %s\n", clientconn_->getErr());
		clientconn_->ClientNotifyExit();
	}
	fstat_->KlMsgChecksum(!(clientconn_->GetKlType() & KL_NCHECKSUM), hash);

	if(KlWriten(STDOUT_FILENO, sbuf.c_str(), sbuf.length()) != sbuf.length()) {
		fprintf(stderr, "Write msg to stdout failed!\n");
		clientconn_->ClientNotifyExit();
	}
	return;
}

void KClientHandle::GetCommandResult(KlCommResp* commresp) {
    if(CheckCommandTimeout()) {
		fprintf(stderr, "Execute command timeout!\n");
		clientconn_->ClientNotifyExit();
	}
	
	if(commresp->errcode_)
		fprintf(stderr, "%s\n", kl_tcp_err(commresp->errcode_));
	
	exit(commresp->errcode_);
}

int KClientHandle::SendCommandReq(const std::string& cmd, const std::string& mode, uint32_t cmd_type) {
    int ret = 0;
	Buffer oArchive;
	oArchive.AdvanceWriteIndex(2 * kHeaderLen);

	KlCmdReq cmdreq;
	cmdreq.ratelimit_ = 0;
	cmdreq.open_type_ = cmd_type;
	cmdreq.cmd_req_.Printf("%s", cmd.c_str());
	cmdreq.cmd_mode_.Printf("%s", mode.c_str());

	KlCommHead klhead;
	InitKlCommHead(&klhead, KL_CMD_OP, clientconn_->GetInnseq());

	ret = ret ? : KlSerializeCommHead(oArchive, &klhead);
	ret = ret ? : KlSerializeCmdReqBody(oArchive, &cmdreq);
	if(ret)
		return ret;

	uint32_t hash=0;
	ret = clientconn_->CalcSendChecksum(oArchive, packetHeadLen, hash);
	if(ret)
		return ret;
	
	return clientconn_->KlSendMsg(oArchive, hash);
}

void KClientHandle::CheckInCommandChecksum(KlCommReq* commreq) {
    unsigned long recv_checksum;
    char *endptr = NULL;
    recv_checksum = strtoul(commreq->comm_req_.c_str(), &endptr, 10);
    if(!(endptr == NULL || *endptr == '\0')) 
        recv_checksum = 0;
    
    if(fstat_->GetChecksum() != recv_checksum) {
        fprintf(stderr, "Checksum failed!\n");
        clientconn_->ClientNotifyExit();
    }
}

int main(int argc, char** argv) {
    uint64_t g_timeout = 0;
    std::string host, command;
    int kl_type = 0, rd_timeout;
    int kl_file, kl_daemon, kl_nchecksum, kl_tkill;

    po::options_description cmdline_options("kl_tool options");
    cmdline_options.add_options()
        ("help", "help message")
        ("host", po::value<std::string>(&host), "connect node_mgr ip:port")
        ("timeout", po::value<uint64_t>(&g_timeout)->default_value(86400000), "kl_tool command timeout")
        ("file", po::value<int>(&kl_file)->default_value(0), "kl_tool transmit file")
        ("daemon", po::value<int>(&kl_daemon)->default_value(0), "kl_tool execute command daemon")
        ("no_checksum", po::value<int>(&kl_nchecksum)->default_value(0), "kl_tool send msg without checksum")
        ("kill_child", po::value<int>(&kl_tkill)->default_value(0), "kl_tool execute command timeout to kill command")
        ("rd_timeout", po::value<int>(&rd_timeout)->default_value(5000), "kl_tool read socket timeout")
        ("command", po::value<std::string>(&command), "kl_tool execute command")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, cmdline_options), vm);
    po::notify(vm);

    if(vm.count("help")) {
        std::cout << "Usage: kl_tool command line [options]\n";
        std::cout << cmdline_options;
        return 0;
    }

    if(host.empty() || command.empty()) {
        fprintf(stderr, "input parameter miss host or command, please check!\n");
        return -1;
    }

    if(kl_file)
        kl_type |= KL_FILE_TRANSMIT;
    if(kl_daemon)
        kl_type |= KL_DAEMON;
    if(kl_nchecksum)
        kl_type |= KL_NCHECKSUM;
    if(kl_tkill)
        kl_type |= KL_TIMEOUTKILLCHILD;

    if(kl_type & KL_FILE_TRANSMIT) {
        fprintf(stdout, "file transmit no support!\n");
        return -1;
    } 

    std::string server_ip = host.substr(0, host.rfind(":"));
    std::string server_port = host.substr(host.rfind(":")+1);
    KClientHandle *cli_hd = new KClientHandle();
    if(cli_hd->ConnectServer(server_ip, atoi(server_port.c_str()), rd_timeout)) {
        fprintf(stderr, "Connect server failed: %s\n", cli_hd->getErr());
        return -1;
    }

    cli_hd->GetClientConn()->SetTimeout(g_timeout);
    if(cli_hd->GetClientConn()->ApplyBuffMem(cli_hd->GetBuffer(), BUFSIZE + packetHeadLen)) {
        fprintf(stderr, "apply recv buff failed: %s\n", cli_hd->getErr());
        return -1;
    }

    if(cli_hd->SendCommandReq(command, "rw", 0)) {
        fprintf(stderr, "send command to server failed: %s\n", cli_hd->GetClientConn()->getErr());
        return -1;
    }

    uint64_t begin_time = GetClockMonoMSec();
    cli_hd->SetCmdBeginTime(begin_time);

    struct event_base* ev_base = event_base_new();
	if(!ev_base) {
		fprintf(stderr, "Event_base_new create failed\n");
		return -1;
	}

	struct timeval one_sec = {1,0};

	struct event* sock_event = event_new(ev_base, cli_hd->GetClientConn()->GetSockfd(),  
	    						EV_TIMEOUT | EV_READ | EV_PERSIST, &KClientHandle::SockCB, cli_hd);
	if(!sock_event) {
		fprintf(stderr, "Create socket watch event failed\n");
		return -1;
	}
	event_add(sock_event, &one_sec);

    AsyncIoClient *cli_rd = new AsyncIoClient();
    KFileStat* fstat = new KFileStat();
	fstat->SetChecksum(0);
	fstat->SetFd(STDIN_FILENO);
    cli_rd->StartWork(cli_hd->GetClientConn(), fstat);
    cli_hd->SetCliRd(cli_rd);
    cli_hd->SetFstat(fstat);
    cli_hd->SetState(KL_SEND_COMMAND_SERVER);

	event_base_dispatch(ev_base);
	event_base_free(ev_base);

    return 0;
}