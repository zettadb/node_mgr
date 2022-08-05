/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#include "tcp_server.h"
#include "tcp_error.h"
#include "tcp_protocol.h"
#include "util_func/meta_info.h"
#include <boost/property_tree/ptree.hpp>
#include <boost/property_tree/ini_parser.hpp>
#include <boost/program_options.hpp>
#include <iostream>

#define MAX_LISTEN_SOCKS 16
#define TCP_LISTEN_BACKLOG 128
kunlun_tcp::CTcpBase* g_tcp_base = nullptr;
kunlun_tcp::KWaitChildPid* g_child_pid = nullptr;

namespace kunlun_tcp 
{

pid_t KWaitChildPid::GetFirstChildPid() {
	std::lock_guard<std::mutex> lk(mux_);
	pid_t pid = 0;

	if(childpids_.size() > 0) {
		std::vector<pid_t>::iterator it = childpids_.begin();
		pid = *it;
		childpids_.erase(it);
	}
	return pid; 
}

void KWaitChildPid::AddChildPid(pid_t pid) {
	std::lock_guard<std::mutex> lk(mux_);
	childpids_.push_back(pid);
}

bool KWaitChildPid::CheckPidStat(pid_t pid) {
	int rc = 0;
    return CheckPidStatus(pid, rc, errinfo);
}

int KWaitChildPid::run() {
    while(m_state) {
        pid_t pid = GetFirstChildPid();
        if(pid > 0) {
            if(!CheckPidStat(pid)) {
                AddChildPid(pid);
                sleep(1);
            }
        } else 
            sleep(1);
    }
    return 0;
}

bool KServerHandle::CheckChildState() {
	if(server_rd_) {
		if(server_rd_->GetIsEnd())
			return true;
	}

	return false;
}

void KServerHandle::StartWork(KConnServer* servconn ) {
    servconn_ = servconn;
    start();
}

int KServerHandle::run() {
    pthread_detach(pthread_self());
    HandleClientMsg();
    if(kp_file_) {
        kp_file_->Close(true);
    }
    servconn_->CloseSockfd();
    delete this;
    return 0;
}

void KServerHandle::HandleClientMsg() {
    int len=0, r=0;
	uint32_t rChecksum=0;
	
	bool ExitLoop = false;
    while ((!r) && (!ExitLoop)) {	
		recvBuf_.Clear();
		if(CheckChildState()) {	
			ExitLoop = true;
			r = KL_TCP_ERR_PTHREAD_CHILD_QUIT;
			continue;
		}
		len = servconn_->KlReadMsg(recvBuf_, &rChecksum, rpoll_time_);
		if(len <= 0) {	
			if(len == KL_TCP_ERR_SOCKET_READ_TIMEOUT) {
				usleep(1000* 10);
				continue;
			}
			KLOG_ERROR("client handler conn read {}:{} failed: {}", servconn_->GetRemoteIp(), 
                            servconn_->GetRemotePort(), servconn_->getErr());
			r = KL_TCP_ERR_CONN_CLOSED;
		} else { 
			KlCommHead klhead;
			if(KlDeserializeHeadPacket(recvBuf_, &klhead)) {
				KLOG_ERROR("parse recv head packet failed, so close conn!");
				r = KL_TCP_ERR_CONN_CLOSED;
			}

			if((klhead.cmdtype_ == KL_QUIT_OP) && !r) {
				KLOG_ERROR("client {}:{} connection quit", servconn_->GetRemoteIp(),
                                servconn_->GetRemotePort());
				r = KL_TCP_ERR_CONN_CLOSED;
			}

			if(!r) {
				if(servstat_ != KL_INITIALIZE) {
					if(!servconn_->IsSeqEqual(klhead.innseq_)) {
						KLOG_ERROR("innseq is not match expect seq: {}, but receive seq: {}", 
                                servconn_->GetInnseq(), klhead.innseq_);
						r = KL_TCP_ERR_CONN_CLOSED;
					}
				} else
					servconn_->SetInnseq(klhead.innseq_);
			}

			if(!r) {
				switch(servstat_) {
					case KL_INITIALIZE: {
                        SetState(KL_AUTHORIZE);
					}
					//break;
					case KL_AUTHORIZE: {
						if(!servconn_->ChecksumCompareResult(recvBuf_, 0, rChecksum)) {
							KLOG_ERROR("recv control packet checksum failed, {}", servconn_->getErr());
							r = KL_TCP_ERR_CONN_CLOSED;
						} else {
							if(klhead.cmdtype_ == KL_CMD_OP) {
								KlCmdReq cmdreq;
								if(KlDeserializeCmdReqPacket(recvBuf_, &cmdreq)) {
									KLOG_ERROR("deserialze recv cmd packet failed, so close conn!");
									r = KL_TCP_ERR_CONN_CLOSED;
								}
								r = KlCommandHandle(&cmdreq);
								if(r) {
									KLOG_ERROR("conn {}:{} exec command failed", 
                                            servconn_->GetRemoteIp(), servconn_->GetRemotePort());
									servconn_->ReplyCommAnswer(r, KL_END_OP);
								}

								if((r == 0) &&(cmdreq.open_type_ == 1)) {
									KLOG_INFO("command open type: {} execute over, so quit", 
                                                            cmdreq.open_type_);
									servconn_->ReplyCommAnswer(r, KL_END_OP);
								}
							}

							if(klhead.cmdtype_ == KL_FILE_OP) {
								KlFileReq filereq;
								if(KlDeserializeFileReqPacket(recvBuf_, &filereq)) {
									KLOG_ERROR("deserialze recv file packet failed, so close conn!");
									r = KL_TCP_ERR_CONN_CLOSED;
								}
								r = KlFileHandle(&filereq);
								if(r) {
									KLOG_ERROR("conn {}:{} do_file failed: {}", servconn_->GetRemoteIp(),
                                            servconn_->GetRemotePort(), servconn_->getErr());
								}

								if(servconn_->ReplyCommAnswer(r, KL_RESP_OP, getErr())) {
									KLOG_ERROR("send recv target file ans failed: {}",servconn_->getErr());
									r = KL_TCP_ERR_CONN_CORRUPT;
								}
							}
						}
						
					}
					break;
					case KL_CMD_INPUT:  {
						if(klhead.cmdtype_ == KL_END_OP) {
							if(!servconn_->ChecksumCompareResult(recvBuf_, 0, rChecksum)) {
								KLOG_ERROR("recv command packet checksum failed, %s", servconn_->getErr());
								r = KL_TCP_ERR_CONN_CLOSED;
							} else {
								if(servconn_->SendChecksumPacket(KL_CHECKSUM_OP, fstat_->GetChecksum())) {
									KLOG_ERROR("send command reply checksum failed: {}", servconn_->getErr());
									r = KL_TCP_ERR_CONN_CORRUPT;
								}

								kp_file_->CloseWriteFp();
								ExitLoop = true;  
							}
						} else if(klhead.cmdtype_ == KL_PACK_OP) {
							if(!servconn_->ChecksumCompareResult(recvBuf_, 0, rChecksum)) {
								KLOG_ERROR("recv command packet checksum failed, {}", servconn_->getErr());
								r = KL_TCP_ERR_CONN_CLOSED;
							} else {
								r = KlTcpMsgWriteCommand(recvBuf_, rChecksum);
								if(r) {
									if(servconn_->ReplyCommAnswer(r, KL_END_OP)) {
										KLOG_ERROR("send write msg to cmd result failed: {}", servconn_->getErr());
										r = KL_TCP_ERR_CONN_CLOSED;
									}
								}
							}
						} else if(klhead.cmdtype_ == KL_TIMEOUT_OP) {
							if(!servconn_->ChecksumCompareResult(recvBuf_, 0, rChecksum)) {
								KLOG_ERROR("recv command packet checksum failed, {}", servconn_->getErr());
								r = KL_TCP_ERR_CONN_CLOSED;
							} else {
								if(servconn_->GetKlType() & KL_TIMEOUTKILLCHILD) {	
                                    int rc = kill(kp_file_->GetChildPid(), 9);
									if(rc < 0) {
                                        KLOG_ERROR("In timeout kill pid: {} failed: {}", kp_file_->GetChildPid(), strerror(errno));
									} else
										KLOG_INFO("In timeout kill pid: %d success", kp_file_->GetChildPid());
								}

								KLOG_INFO("cmd execute timeout, so quit");
								r = KL_TCP_ERR_CONN_CLOSED;
							}
						}	
					}
					break;
					default:
					break;
				}
			}
		}
	}

	if(r) 
        SetAconnState(KL_CERROR);
	
	ClearTmpEnv(r);	
	return;
}

void KServerHandle::ClearTmpEnv(int rc) {
	if (server_rd_) {
        if (kp_file_) {
            kp_file_->CloseWriteFp();
        }
		
		if(!(rc == KL_TCP_ERR_PTHREAD_CHILD_QUIT)) {
			server_rd_->WaitQuit();
		}
    }
}

void KServerHandle::SetAconnState(Aconn_State state) {
    if(server_rd_ && servstat_ == KL_CMD_INPUT) {
		server_rd_->SetAconnState(state);
		if (kp_file_) {
            kp_file_->CloseWriteFp();
        }
	}
}

int KServerHandle::KlCommandHandle(KlCmdReq* cmdreq) {
    Buffer pcmd;
	servconn_->SetRateLimit(cmdreq->ratelimit_);
	
	pcmd.Printf("%s", cmdreq->cmd_req_.c_str());
	std::string cmd_mode = cmdreq->cmd_mode_.AsString();
	if(cmd_mode.empty())
		cmd_mode = "rwc";
	
	KLOG_INFO("Handle command: {} mode: {}, open_type: {}", pcmd.AsString(), cmd_mode,
													cmdreq->open_type_);

	int ret = 0;
	if(cmdreq->open_type_ == 0)
		ret = KlPopenExecCommand(pcmd.c_str(), cmd_mode.c_str());
	else
		ret = KlSystemExecCommand(pcmd.c_str());
	return ret;
}

int KServerHandle::KlPopenExecCommand(const char *cmd, const char* mode) {
	kp_file_ = new KPopenCmd(0);
	if(!kp_file_->OpenCmd(cmd, mode, true)) {
		KLOG_ERROR("bi_popen cmd: {} failed {}", cmd, kp_file_->getErr());
        return KL_TCP_ERR_INTERNAL_ERROR;
	}

    servstat_ = KL_CMD_INPUT;

    fstat_ = new KFileStat();
    fstat_->SetChecksum(0);
    fstat_->SetFd(kp_file_->GetReadFd());
    fstat_->SetFilePid(kp_file_->GetChildPid());
    server_rd_ = new AsyncIoServer(0);
    server_rd_->StartWork(servconn_, fstat_);
    return 0;
}

int KServerHandle::KlSystemExecCommand(const char *cmd) {
    return system(cmd);
}

int KServerHandle::KlFileHandle(KlFileReq* filereq) {
    return 0;
}

int KServerHandle::KlTcpMsgWriteCommand(Buffer& sbuf, uint32_t rChecksum) {
    return 0;
}

void AsyncIoServer::StartWork(KConnServer * servconn, KFileStat* fstat) {
    servconn_ = servconn;
    fstat_ = fstat;
    start();
}

int AsyncIoServer::run() {
    pthread_detach(pthread_self());
    int ret = 0;
    switch(jobtype_) {
        case 0:
            ret = KReadCommandOutput();
            break;
        case 1:
            ret = KReadFileMsg();
            break;
        default:
            break;
    }
    return ret;
}

int AsyncIoServer::KReadFileMsg() {
    return 0;
}

int AsyncIoServer::KReadCommandOutput() {
    int ret = 0, get_pid_status = 0;

    if (SetNonBlock(fstat_->GetFd())) {
        KLOG_ERROR("set fd: {} noblock failed" , fstat_->GetFd());
        ret = KL_TCP_ERR_INTERNAL_ERROR;
    }

    if (!ret && servconn_->ApplyBuffMem(rdCmdBuf_ , BUFSIZE + packetHeadLen)) {
        KLOG_ERROR("read cmd buffer failed: {}" , servconn_->getErr ());
        ret = KL_TCP_ERR_INTERNAL_ERROR;
    }

    if (!ret && servconn_->PackCommHeader(KL_PACK_OP , rdCmdBuf_)) {
        KLOG_ERROR("Async_ioread pack msg head failed: {}" , servconn_->getErr ());
        ret = KL_TCP_ERR_INTERNAL_ERROR;
    }

	KLOG_INFO("read fstat fd: {}", fstat_->GetFd());
    while (1) {
        if ((GetAconnState() != KL_CNOMRAL) || ret)
            break;

        rdCmdBuf_.SetWriteIndex (packetHeadLen);
        int len = PollReadCommand(fstat_->GetFd() , KL_TIMEOUT);
        if (len == -1) { 
            KLOG_ERROR("read cmd stdout msg failed: {}", getErr());
            ret = KL_TCP_ERR_FILE_READ_FAILED;
            break;
        } else if (len == -2) {	
            ret = KL_TCP_ERR_FILE_READ_END;
            break; 
        } else if(len == 0) {
		    if (!(servconn_->GetKlType() & KL_DAEMON)) { 
            		if (servconn_->CheckPidExit(fstat_->GetFilePid(), ret)) {
                		get_pid_status = 1;
                		break;
           		 }
        	}
	    }

        if (len > 0 && GetAconnState() == KL_CNOMRAL) {
            uint32_t hash = 0;
            if (servconn_->CalcSendChecksum(rdCmdBuf_, packetHeadLen, hash)) {
                KLOG_ERROR("calulate send msg checksum failed: {}" , servconn_->getErr ());
                ret = KL_TCP_ERR_INTERNAL_ERROR;
                break;
            }

            fstat_->KlMsgChecksum(!(servconn_->GetKlType() & KL_NCHECKSUM) , hash);

            if (servconn_->KlSendMsg(rdCmdBuf_ , hash)) {
                KLOG_ERROR("Async_ioread send msg to network failed: {}" , servconn_->getErr());
				SetAconnState(KL_CERROR);
				ret = KL_TCP_ERR_CONN_CORRUPT;
				break;
            }
        }
    }

	if(GetAconnState() == KL_CNOMRAL) {  
	    if (servconn_->SendChecksumPacket(KL_CHECKSUM_OP , fstat_->GetChecksum ())) {
	        KLOG_ERROR("send command read checksum failed: {}" , servconn_->getErr ());
			SetAconnState(KL_CERROR);
	        ret = KL_TCP_ERR_CONN_CORRUPT;
	    }
	}

    if (!get_pid_status) { 
    	int tmpRc = 0;
        while (!servconn_->CheckPidExit(fstat_->GetFilePid(), tmpRc)) { 
            if (!ret && GetAconnState() == KL_CNOMRAL) {
                usleep(1000 * 500);
            } else {
				KLOG_ERROR("socket conn status: {}, insert pid: {} to recycle thread", GetAconnState(), 
                                fstat_->GetFilePid());
                g_child_pid->AddChildPid(fstat_->GetFilePid());
				break;
            }

			if(!ret)
				ret = tmpRc;
        }
    }

    if (ret != KL_TCP_ERR_CONN_CORRUPT && GetAconnState() == KL_CNOMRAL) {
		if(ret == KL_TCP_ERR_FILE_READ_END) 
			ret = 0;
		
        if (servconn_->ReplyCommAnswer(ret , KL_END_OP)) {
            KLOG_ERROR("send command result failed: {}, so quit" , servconn_->getErr ());
        }
    }

    bIsEnd_ = true;
    NotifyQuit();
    return 0;
}

bool KConnServer::CheckPidExit( pid_t pid , int& retcode) {
    char errinfo[4096] = {0};
    bool ret = CheckPidStatus(pid, retcode, errinfo);
    if(ret) {
        KLOG_ERROR("check pid status {},{}", retcode, errinfo);
    }
    return ret;
}

int CTcpBase::Init() {
    ParseTcpServerConf();

    std::string log_path = log_file_path_.substr(0, log_file_path_.rfind("/"));
    std::string log_prefix = log_file_path_.substr(log_file_path_.rfind("/")+1);
    Op_Log::getInstance()->init(log_path, log_prefix, max_log_file_size_);
    
    int socklen;
	tcp_sock_ = socket(AF_INET, SOCK_STREAM, 0);
	if(tcp_sock_ == -1) {
		KLOG_ERROR("create socket failed, {}", errno);
		return 1;
	}

	if(evutil_make_listen_socket_reuseable(tcp_sock_) < 0) {
		KLOG_ERROR("set socket reusable failed");
		close(tcp_sock_);
		return 1;
	}
		
	struct sockaddr_in my_addr;
	bzero(&my_addr, sizeof(my_addr));

	my_addr.sin_family = AF_INET;
	my_addr.sin_port = htons(listen_port_);
	inet_pton(AF_INET, local_ip_.c_str(), &my_addr.sin_addr);
	socklen = sizeof(struct sockaddr_in);

	if(bind(tcp_sock_, (struct sockaddr*)&my_addr, sizeof(my_addr)) !=0) {
		KLOG_ERROR("kl_server socket bind failed {}", errno);
        close(tcp_sock_);
		return 1;
	}

	if(listen(tcp_sock_, TCP_LISTEN_BACKLOG) < 0 ) {
		KLOG_ERROR("kl_server socket listen failed {}", errno);
        close(tcp_sock_);
		return 1;
	}
	return 0;
}

void CTcpBase::ParseTcpServerConf() {
    try {
        boost::property_tree::ptree pt;
        boost::property_tree::ini_parser::read_ini(conf_file_, pt);
        max_log_file_size_ = pt.get<int>("Base_Conf.max_log_file_size");
        log_file_path_ = pt.get<std::string>("Base_Conf.log_file_path");
        rpoll_time_ = pt.get<uint32_t>("Base_Conf.rpoll_time");
    } catch(...) {
        fprintf(stderr, "get tcp_server.cnf failed, so use default parameters\n");
        log_file_path_ = "../log/kl_server";
        max_log_file_size_ = 500;
        rpoll_time_ = 5000;
    }
}

void CTcpBase::Run() {
    struct event_base* ev_base = event_base_new();
	if(!ev_base) {
		KLOG_ERROR("event_base_new create failed {}", errno);
		return;
	}

	//g_sshconf.set_ev_base(ev_base);
    struct event* event_listen = event_new(ev_base, tcp_sock_, EV_READ | EV_PERSIST, 
                    &CTcpBase::AcceptCB, this);
	if(!event_listen) {
		KLOG_ERROR("event_new create failed {}", errno);
		return;
	}

	event_add(event_listen, NULL);
	event_base_dispatch(ev_base);
	event_base_free(ev_base);
	return;
}

void CTcpBase::AcceptCB(evutil_socket_t fd, short which, void* v) {
    int on = 1;
	evutil_socket_t sockfd;    
	struct sockaddr_in client;    
	socklen_t len = sizeof(client);    
	sockfd = ::accept(fd, (struct sockaddr*)&client, &len);

	if(setsockopt(sockfd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on)) < 0) {
		KLOG_ERROR("tcp server set sockfd: {} keepalive failed {}",sockfd, errno);
		close(sockfd);
		return;
	}

	if(evutil_make_socket_nonblocking(sockfd) < 0) {
		KLOG_ERROR("tcp server set sockfd: {} nonblock failed {}", sockfd, errno);
		close(sockfd);
		return;
	}
		
	char *remote_ip = GetSocketAddress(sockfd, 1, 1);
	if(!remote_ip) {
		KLOG_ERROR("get sockfd: {} socket address failed", sockfd, errno);
		close(sockfd);
		return;
	}
	
	int remote_port = GetSockPort(sockfd, 0);
	std::string re_ip;
	re_ip.assign(remote_ip);
		
	KConnServer *serv_conn = new KConnServer(sockfd);
	serv_conn->SetRemoteIp(re_ip);
	serv_conn->SetRemotePort(remote_port);
	serv_conn->SetLocalIp(g_tcp_base->GetLocalIp());
	serv_conn->SetLocalPort(g_tcp_base->GetListenPort());
		
	KLOG_INFO("Connection from {} port {} on {} port {}", 
				re_ip, remote_port, g_tcp_base->GetLocalIp(), g_tcp_base->GetListenPort());

	KServerHandle* ksyn = new KServerHandle();
	if(serv_conn->ApplyBuffMem(ksyn->GetBuffer(), BUFSIZE + packetHeadLen)) {
		KLOG_ERROR("apply recv buffer failed: {}", ksyn->getErr());
		delete ksyn;
	} else
		ksyn->StartWork(serv_conn);  //start thread to handle client command

	free(remote_ip);
}

}

void KlSignalHandle() {
	signal (SIGINT , SIG_IGN);
    signal (SIGHUP , SIG_IGN);
    signal (SIGQUIT , SIG_IGN);
    signal (SIGPIPE , SIG_IGN);
    signal (SIGTTOU , SIG_IGN);
    signal (SIGTTIN , SIG_IGN);
    //  signal (SIGCHLD , SIG_IGN);
    signal (SIGTERM , SIG_IGN);

    struct sigaction sig;
    sig.sa_handler = SIG_IGN;
    sig.sa_flags = 0;
    sigemptyset (&sig.sa_mask);
    sigaction (SIGPIPE , &sig , NULL);
}

namespace po = boost::program_options;

int main(int argc, char** argv) {
    std::string localip, base_path;
    int listen_port;
    std::string conf_file;

    po::options_description cmdline_options("kl_server options");
    cmdline_options.add_options()
        ("help", "help message")
        ("base_path", po::value<std::string>(&base_path), "tcp server run base path")
        ("localip", po::value<std::string>(&localip), "tcp server bind local ip")
        ("listen_port", po::value<int>(&listen_port)->default_value(0), "tcp server listen port")
        ("conf_file", po::value<std::string>(&conf_file)->default_value("../conf/tcp_server.cnf"), 
                    "tcp server configuration file")
    ;

    po::variables_map vm;
    po::store(po::parse_command_line(argc, argv, cmdline_options), vm);
    po::notify(vm);

    if(vm.count("help")) {
        std::cout << "Usage: kl_server command line [options]\n";
        std::cout << cmdline_options;
        return 0;
    }

    if(localip.empty() || conf_file.empty() || listen_port == 0) {
        fprintf(stderr, "input parameter miss localip, conf_file or listen_port, please check!\n");
        return -1;
    }

    g_tcp_base = new kunlun_tcp::CTcpBase(conf_file, localip, listen_port);
    if(g_tcp_base->Init()) {
        KLOG_ERROR("tcp server init failed");
        return -1;
    }
    KLOG_INFO("tcp_server init success, start run ...");

    g_child_pid = new kunlun_tcp::KWaitChildPid();
    if(g_child_pid->start()) {
        KLOG_ERROR("start kl recycle child pid thread failed");
    }

    g_tcp_base->Run();
    return 0;
}