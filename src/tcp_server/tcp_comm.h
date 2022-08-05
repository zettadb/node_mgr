/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _NODE_MGR_TCP_COMM_H_
#define _NODE_MGR_TCP_COMM_H_
#include <unistd.h>
#include <string>
#include <vector>
#include <shadow.h>
#include <pwd.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <signal.h>  
#include <sys/socket.h>
#include <sys/poll.h>  
#include <netinet/in.h> 
#include <netinet/tcp.h> 
#include <arpa/inet.h>  
#include <netdb.h>
#include <chrono>
#include <mutex>
#include <condition_variable>
#include "zettalib/errorcup.h"
#include "zettalib/zthread.h"
#include "buffer.hpp"
#include "tcp_protocol.h"

using namespace kunlun;

namespace kunlun_tcp {
#define BUFSIZE 		131072    //128k
#define CLIBUFSIZE 		8192
#define KL_TIMEOUT      5000

const static size_t kHeaderLen = sizeof(int32_t);
const static size_t packetHeadLen = 2 * kHeaderLen + sizeof(KlCommHead);
const static size_t FIXED_TIMEOUT = 86400000;
const static size_t waitThreadTimeout = KL_TIMEOUT + 3000 ; //make sure poll timeout > read timeout

#define KL_FILE_TRANSMIT 		0x01   //file transfer
#define KL_FILE_COVER    		0x02   //file transfer can overwrite
#define KL_DAEMON        		0x04	//command daemon
#define KL_NCHECKSUM     		0x08	//close checksum
#define KL_TIMEOUTKILLCHILD		0x10	//kill command when timeout

#define	FILEMODEMASK	(S_ISUID|S_ISGID|S_IRWXU|S_IRWXG|S_IRWXO)
#define CHECKSUM_TYPE	1		//mumhash

char* GetSocketAddress(int sock, int remote, int flags);
int KLGetNameInfo(const struct sockaddr *sa, size_t salen, char *host, size_t hostlen,
							char *serv, size_t servlen, int flags);

int GetSockPort(int sock, int local);
ssize_t KlWriten(int fd , const void *vptr , size_t n , int ts_us = 0);

inline uint64_t GetClockMonoMSec() {
    struct timespec tm;
    clock_gettime (CLOCK_MONOTONIC , &tm);
    return tm.tv_sec * 1000 + tm.tv_nsec / 1000000;
}

unsigned int KlMurmurHashCode ( const char * key , unsigned short len );
int UnsetNonBlock(int fd);
int SetNonBlock(int fd);
bool SetFdCloexec(int fd); 
int64_t KlCostMsec(uint64_t begintime, uint64_t endtime);

//unsigned int get_random();
uint32_t CalcSendContentChecksum(Buffer& buff, size_t posIndex);
size_t ReadBufferFromFD(int fd, Buffer& fdbuff, size_t buflen);
bool ReadBufferFromFile(const char* fileName, Buffer& fdbuff, size_t buflen, char* errbuf);
//int write_ssh_pid_file(pid_t pid, const std::string& filename);

bool directory_is_exist(const std::string& path);
long long strtonum(const char* numstr, long long minval, long long maxval,
		const char **errstrp);

typedef enum {		//async get socket status 
	KL_CNOMRAL,
	KL_CTIMEOUT,
	KL_CERROR,
	KL_CLOSE
} Aconn_State;

class AsyncIoBase : public ZThread, public ErrorCup{
public:
	AsyncIoBase():quit_(false),aconnstate_(KL_CNOMRAL)
	{}

	virtual ~AsyncIoBase() {}

	void NotifyQuit() {
        std::unique_lock<std::mutex> lk(qmutex_);
        quit_ = true;
        qcond_.notify_all();
	}

	void WaitQuit(int timeout) {
		std::unique_lock<std::mutex> lk (qmutex_);
        qcond_.wait_for(lk, std::chrono::milliseconds(timeout));
	}

	void WaitQuit() {
		std::unique_lock<std::mutex> lk (qmutex_);
		qcond_.wait(lk, [this] {
			return (quit_ == true);
		});
	}

	void SetAconnState(Aconn_State aconnstate) {	
        std::lock_guard<std::mutex> lk(mutex_);
		aconnstate_ = aconnstate;
	}
	Aconn_State GetAconnState() {
		std::lock_guard<std::mutex> lk(mutex_);
		return aconnstate_;
	}
	
public:
	int PollReadCommand(int fd, uint64_t timeout);
	int BlockReadCommand(int fd);
	
	Buffer rdCmdBuf_;
private:
	bool quit_;
	std::mutex qmutex_;
	std::mutex mutex_;
    std::condition_variable qcond_;
	volatile Aconn_State aconnstate_;
};

typedef enum {
	KL_INITIALIZE,
		
	//server status
	KL_AUTHORIZE,
	KL_CMD_INPUT,
	KL_FILE_RECV_STAT,
	KL_FILE_RECV_MSG,
	KL_FILE_RECV_CHECKSUM_RESULT,
	KL_FILE_SEND_STAT,
	KL_FILE_SEND_MSG_BEGIN,
	KL_FILE_SEND_MSG_RUN,
	KL_CONN_END,

	//client status
	KL_SEND_HANDLESHAKE_SERVER,
	KL_SEND_COMMAND_SERVER,
	KL_SEND_INPUT_MSG_SERVER,
	KL_SEND_FILE_TRANSLATE_TYPE,
	KL_SEND_FILE_STATUS_SERVER,
	KL_SEND_FILE_MSG_SERVER,
	KL_SEND_FILE_WAIT_RESULT,
	KL_CHECK_LOCAL_RECV_FILE,
	KL_RECV_FILE_STATUS,
	KL_RECV_FILE_MSG,
	KL_RECV_FILE_CHECKSUM,

	//TODO ...
	KL_ALL_STATUS
} Task_State;

class KConnBase : public ErrorCup {
public:
	KConnBase(int sockfd) : sockfd_(sockfd),kltype_(0),
					timeout_(0), ratelimit_(0),innseq_(0) {}
	virtual ~KConnBase() {}

	void SetSockfd(int sockfd) {
		sockfd_ = sockfd;
	}
	int GetSockfd() const {
		return sockfd_;
	}

	void SetKlType(int kltype) {
		kltype_ = kltype;
	}
	int GetKlType() const {
		return kltype_;
	}

	void SetInnseq(uint32_t innseq) {
		innseq_ = innseq;
	}
	uint32_t GetInnseq() const {
		return innseq_;
	}

	uint32_t GetRateLimit() const {
		return ratelimit_;
	}
	void SetRateLimit(uint32_t ratelimit) {
		ratelimit_ = ratelimit;
	}

	int64_t GetTimeout() const {
		return timeout_;
	}
	void SetTimeout(int64_t timeout) {
		timeout_ = timeout;
	}

public:
	int KlSendMsg(Buffer& buf, uint32_t hash);
	int KlReadMsg(Buffer& recvBuf, uint32_t* rChecksum, int microsecond);
	void CloseSockfd();

	int PackCommHeader(uint32_t cmdtype, Buffer& sbuf);
	int SendChecksumPacket(uint32_t cmdtype, unsigned long checksum);
	int SendCommPacket(uint32_t cmdtype, Buffer& sbuf);
	int SendCommPacket(uint32_t cmdtype, const char* buff, int bufLen);
	int ReplyCommAnswer(int retcode, uint32_t cmdtype, const std::string& cmdstr="");
	int CalcSendChecksum(Buffer& sbuf, size_t posIndex, uint32_t& hash);

	bool ChecksumCompareResult(Buffer& sbuf, size_t posIndex, uint32_t rChecksum);
	void ConnSeqAddOne();
	bool IsSeqEqual(uint32_t rSeq);

	int ApplyBuffMem(Buffer& cBuff, size_t cbufLen);
	
protected:
	int KlPollRead(char* buffer, int buflen, int microsecond);
	int KlPollWrite(const char* buffer, int buflen);
	
private:
	int sockfd_;
	int kltype_;
	int64_t timeout_;
	uint32_t ratelimit_;
	uint32_t innseq_;
};

class KFileStat : public ErrorCup {
public:
	KFileStat(): fd_(-1), checksum_(0) {}

	virtual ~KFileStat() {
		CloseFd();
	}
	
	int GetAppendType() const {
		return appendtype_;
	}
	void SetAppendType(int appendtype) {
		appendtype_ = appendtype;
	}

	const std::string& GetAssignDir() const {
		return assigndir_;
	}
	void SetAssignDir(const std::string& assigndir) {
		assigndir_ = assigndir;
	}

	const std::string& GetTmpFile() const {
		return tmpfile_;
	}
	void SetTmpFile(const std::string& tmpfile) {
		tmpfile_ = tmpfile;
	}

	const std::string& GetDstFile() const {
		return dstfile_;
	}
	void SetDstFile(const std::string& dstfile) {
		dstfile_ = dstfile;
	}

	int GetFd() {
		return fd_;
	}
	void SetFd(int fd) {
		fd_ = fd;
	}

	void CloseFd() {
		if(fd_ >= 0) {
			close(fd_);
			fd_ = -1;
		}
	}

	size_t GetFileLen() const{
		return filelen_;
	}
	void SetFileLen(size_t filelen) {
		filelen_ = filelen;
	}

	size_t GetWritePos() const {
		return writepos_;
	}
	void SetWritePos(size_t writepos) 
	{
		writepos_ = writepos;
	}

	unsigned long GetChecksum() const  {
		return checksum_;
	}
	void SetChecksum(unsigned long checksum) {
		checksum_ = checksum;
	}

	pid_t GetFilePid() const {
		return file_pid_;
	}
	void SetFilePid(pid_t file_pid) {
		file_pid_ = file_pid;
	}

public:
	void KlMsgChecksum(int checksum_type, const char * buffer, int len);
	void KlMsgChecksum(int checksum_type, unsigned int hash);
	int KlSafeWriteFile(int type);
	
private:
	int fd_;
	std::string assigndir_;
	std::string tmpfile_;
	std::string dstfile_;
	size_t filelen_;
	size_t writepos_;
	int appendtype_;
	unsigned long checksum_;
	pid_t  file_pid_;
};

class KCommHandle : public ErrorCup {
public:
	KCommHandle() : fstat_(NULL) {}

	virtual ~KCommHandle() {
		if(fstat_) {
			delete fstat_;
			fstat_ = NULL;
		}
	}

	KFileStat* GetFstat(){
		return fstat_;
	}
	void SetFstat(KFileStat* fstat){
		fstat_ = fstat;
	}

	Buffer& GetBuffer() {
		return recvBuf_;
	}

public:
	int KlDeserializeHeadPacket(Buffer& buff, KlCommHead *oh);
	int KlDeserializeCmdReqPacket(Buffer& buff, KlCmdReq* CmdReq);
    int KlDeserializeCommReqPacket(Buffer& buff, KlCommReq* CommReq);
    int KlDeserializeCommRespPacket(Buffer& buff, KlCommResp* CommResp);
	int KlDeserializeFileReqPacket(Buffer& buff, KlFileReq* FileReq);

	KFileStat *fstat_;
	Buffer recvBuf_;
};

}
#endif