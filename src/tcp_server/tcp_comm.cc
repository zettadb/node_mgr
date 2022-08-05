/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "tcp_comm.h"
#include <fcntl.h>
#include <sys/wait.h>
#include "tcp_error.h"
#include "tcp_protocol.h"

namespace kunlun_tcp {

size_t strlcpy(char *dst, const char *src, size_t siz) {
	char *d = dst;
	const char *s = src;
	size_t n = siz;
	if(n != 0) {
		while(--n != 0) {
			if((*d++ = *s++) == '\0')
				break;
		}
	}
	if(n == 0) {
		if(siz != 0)
			*d = '\0';
		while(*s++)
			;
	}
	return (s - src - 1);
}

char *xstrdup(const char *str){
	if(str == NULL)
		return NULL;
	size_t len;
	char *cp;
	len = strlen(str)+1;
	cp = (char*)malloc(len);
	if(NULL == cp) {
		return NULL;
	}
	strlcpy(cp, str, len);
	return cp;
}

unsigned int KlMurmurHashCode( const char * key , unsigned short len )  {
	// 'm' and 'r' are mixing constants generated offline.
	// They're not really 'magic', they just happen to work well.

	uint32_t seed = 2773;
	const uint32_t m = 0x5bd1e995;
	const int r = 24;

	// Initialize the hash to a 'random' value
	uint32_t h = seed ^ len;

	// Mix 4 bytes at a time into the hash

	const unsigned char * data = (const unsigned char *) key;

	while (len >= 4) {
		uint32_t k = *(uint32_t *) data;

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;

		data += 4;
		len -= 4;
	}

	// Handle the last few bytes of the input array

	switch (len) {
		case 3:
			h ^= data[2] << 16;
		case 2:
			h ^= data[1] << 8;
		case 1:
			h ^= data[0];
			h *= m;
	};

	// Do a few final mixes of the hash to ensure the last few
	// bytes are well-incorporated.

	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}

ssize_t KlWriten(int fd , const void *vptr , size_t n , int ts_us ) {
    size_t nleft;
    ssize_t nread;
    char *ptr;

    ptr = (char*) vptr;
    nleft = n;
    while (nleft > 0) {
        if ((nread = write (fd , ptr , nleft)) <= 0) {
            if (errno == EINTR || errno == EAGAIN) {
                nread = 0; 
                if (ts_us) {
                    usleep (ts_us);
                }
            }
            else {
                return (-1);
            }
        }

        nleft -= nread;
        ptr += nread;
    }
    return (n - nleft);
}

int UnsetNonBlock(int fd) {
    int val = fcntl(fd, F_GETFL);
	if (val < 0) {
		fprintf(stderr, "fcntl(%d, F_GETFL): %s\n", fd, strerror(errno));
		return (-1);
	}
	if (!(val & O_NONBLOCK)) {
		return (0);
	}
	
	val &= ~O_NONBLOCK;
	if (fcntl(fd, F_SETFL, val) == -1) {
		fprintf(stderr, "fcntl(%d, F_SETFL, ~O_NONBLOCK): %s\n",
		    fd, strerror(errno));
		return (-1);
	}
	return (0);
}

bool SetFdCloexec(int fd) {
	int val;
    if ((val = fcntl (fd , F_GETFD , 0)) == -1) {
        fprintf(stderr, "fcntl F_GETFD error:[%d:%s]" , errno , strerror (errno));
        return false;
    }
    if (fcntl (fd , F_SETFD , val | FD_CLOEXEC) == -1) {
        fprintf(stderr, "fcntl F_SETFD error:[%d:%s]" , errno , strerror (errno));
        return false;
    }

    return true;
}

int SetNonBlock(int fd) {
    int val = fcntl(fd, F_GETFL);
	if (val < 0) {
		fprintf(stderr, "fcntl(%d, F_GETFL): %s\n", fd, strerror(errno));
		return (-1);
	}
	if (val & O_NONBLOCK) {
		return (0);
	}
	
	val |= O_NONBLOCK;
	if (fcntl(fd, F_SETFL, val) == -1) {
		fprintf(stderr, "fcntl(%d, F_SETFL, O_NONBLOCK): %s\n", fd,
		    strerror(errno));
		return (-1);
	}
	return (0);
}

uint32_t CalcSendContentChecksum(Buffer& buff, size_t posIndex) {
	char* ptr = (char*)buff.GetRawBuffer() + posIndex;
	size_t checklen = buff.length() - posIndex;
	return KlMurmurHashCode(ptr, checklen);
}

size_t ReadBufferFromFD(int fd, Buffer& fdbuff, size_t buflen) {
	size_t nleft = buflen;
    size_t nread;
    char *ptr = (char*)fdbuff.GetRawBuffer();

    while (nleft > 0) {
        if ((nread = read (fd , ptr , nleft)) < 0) {
            if (errno == EINTR || errno == EAGAIN) {
                nread = 0; /* and call read() again */
            }
            else {
                return (-1);
            }
        }
        else if (nread == 0) {
            break; /* EOF */
        }

        nleft -= nread;
        ptr += nread;
    }
    return (buflen - nleft); /* return >= 0 */
}

bool ReadBufferFromFile(const char* fileName, Buffer& fdbuff, size_t buflen, char* errbuf) {
	int rfd = open (fileName , O_RDONLY);
    if (rfd < 0) {
		snprintf(errbuf, 1024, "open file[%s] fail[%d,%s]" , fileName , errno , strerror (errno));
        return false;
    }

    int ret = ReadBufferFromFD(rfd , fdbuff , buflen - 1);
    if (ret < 0) {
		snprintf(errbuf, 1024, "readn file[%s] fail[%d,%s]" , fileName , errno , strerror (errno));
		fdbuff.Clear();
        close (rfd);
        return false;
    }

    close (rfd);
    return true;
}

char* GetSocketAddress(int sock, int remote, int flags) {
	struct sockaddr_in addr;
	socklen_t addrlen;
	char ntop[1025];
	int r;

	addrlen = sizeof(addr);
	memset(&addr, 0, sizeof(addr));

	if(remote) {
		if(getpeername(sock, (struct sockaddr *)&addr, &addrlen) != 0)
			return NULL;
	} else {
		if(getsockname(sock, (struct sockaddr *)&addr, &addrlen) != 0)
			return NULL;
	}

	switch(addr.sin_family) {
		case AF_INET:
		case AF_INET6:
			if((r = KLGetNameInfo((struct sockaddr *)&addr, addrlen, ntop, sizeof(ntop), NULL, 0, flags)) != 0) {
				return NULL;
			}
			return xstrdup(ntop);
			break;
		default:
			return NULL;
	}
}

int KLGetNameInfo(const struct sockaddr *sa, size_t salen, char *host, size_t hostlen,
							char *serv, size_t servlen, int flags) {
	struct sockaddr_in *sin = (struct sockaddr_in *)sa;
	struct hostent *hp;
	char tmpserv[16];

	if(sa->sa_family != AF_INET && sa->sa_family != AF_INET6)
		return -1;

	if(serv != NULL) {
		snprintf(tmpserv, sizeof(tmpserv), "%d", ntohs(sin->sin_port));
		if(strlcpy(serv, tmpserv, servlen) >= servlen)
			return -1;
	}

	if(host != NULL) {
		if(flags & 1) {
			if(strlcpy(host, inet_ntoa(sin->sin_addr), hostlen) >= hostlen)
				return -1;
			else
				return 0;
		} else {
			hp = gethostbyaddr((char *)&sin->sin_addr, sizeof(struct in_addr), AF_INET);
			if(hp == NULL)
				return -1;

			if(strlcpy(host, hp->h_name, hostlen) >= hostlen)
				return -1;
			else
				return 0;
		}
	}
	return 0;
}

int64_t KlCostMsec(uint64_t begintime, uint64_t endtime) {
    int64_t cost = endtime - begintime;
    if ( cost <= 0 ) {
        cost = 0;
    }

    return cost;
}

int GetSockPort(int sock, int local){
	struct sockaddr_in from;
	socklen_t fromlen;
	char strport[32];
	int r;

	fromlen = sizeof(from);
	memset(&from, 0, sizeof(from));

	if(local) {
		if(getsockname(sock, (struct sockaddr *)&from, &fromlen) < 0 )
			return 0;
	} else {
		if(getpeername(sock, (struct sockaddr *)&from, &fromlen) < 0 )
			return -1;
	}

	if(from.sin_family == AF_INET6)
		fromlen = sizeof(struct sockaddr_in6);

	if(from.sin_family != AF_INET6 && from.sin_family != AF_INET)
		return 0;

	if((r = KLGetNameInfo((struct sockaddr *)&from, fromlen, NULL, 0, strport, sizeof(strport), 4))!=0) {
		return 0;
	}

	return atoi(strport);
}

int KConnBase::KlPollWrite(const char* buffer, int buflen) {
    size_t nleft;
    ssize_t nwritten;
    const char *ptr;

    int ret;

    ptr =  buffer;
    nleft = buflen;

    struct pollfd fds[1];
    bzero (fds , sizeof(fds));
    fds[0].fd = sockfd_;
    fds[0].events = POLLOUT;

    while (nleft > 0 ) {
        ret = poll (fds , 1 , -1);
        if (ret < 0) {
        	if(errno == EINTR) continue;
            setErr("conn write poll error[%d,%s]" , errno , strerror (errno));
            return -1;
        } else if (0 == ret) {
            setErr("poll write timeout"); 
            return -1;
        }

        if ((nwritten = ::send (sockfd_ , (void*) ptr , nleft , MSG_DONTWAIT)) < 0) {
            if ( errno == EINTR || errno == EAGAIN) {
                nwritten = 0;
            } else {
                setErr("send error[%d:%s]" , errno , strerror (errno));
                return (-1);
            }
        }

        nleft -= nwritten;
        ptr += nwritten;
    }

    return (buflen - nleft);
}

int KConnBase::KlPollRead(char* buffer, int buflen, int microsecond) {
    size_t nleft;
    ssize_t nreaden;
    const char *ptr;

    int ret;

    ptr = (char*) buffer;
    nleft = buflen;

	uint64_t begintime, endtime;
	begintime = GetClockMonoMSec();
	int leftmicrosecond = microsecond;

    struct pollfd fds[1];
    bzero (fds , sizeof(fds));
    fds[0].fd = sockfd_;
    fds[0].events = POLLIN;

    while (nleft > 0 && leftmicrosecond > 0) {
        ret = poll (fds , 1 , leftmicrosecond);
        if (ret < 0) {
        	if(errno == EINTR) continue;
            setErr("conn read poll error[%d,%s]" , errno , strerror (errno));
            return -1;
        } else if (0 == ret) {
        	if(nleft == buflen) {		//timeout, can't read data
	            setErr("poll read timeout");
	            return -2;
			} else {
				setErr("read partition packet len: %ld", buflen-nleft);
	            return -1;	
			}
        }

        if ((nreaden = ::recv (sockfd_ , (void*) ptr , nleft , MSG_DONTWAIT)) < 0) {  //unblock read
            if ( errno == EINTR || errno == EAGAIN) {
                nreaden = 0;
            } else {
                setErr("conn read recv error [%d:%s]" , errno , strerror (errno));
                return -1; 
            }
        } else if (0 == nreaden) { //read end
			setErr("socket close by peer");
			return (-1);		
        }

        int on = 1;
        if (0 != setsockopt (sockfd_ , IPPROTO_TCP , TCP_QUICKACK , &on , sizeof(on))) {
            ;
        }

        nleft -= nreaden;
        ptr += nreaden;

        endtime = GetClockMonoMSec();

        int cost = KlCostMsec(begintime , endtime);
        leftmicrosecond = microsecond - cost;
    }

    if (nleft > 0) {
        setErr("read partition because timeout should read[%d],just read[%d]" , 
                        buflen , buflen - (int) nleft);
        return -1;
    }

    return buflen;
}

int KConnBase::KlSendMsg(Buffer& buf, uint32_t hash) {
    size_t oLen = buf.length();
	if(oLen <= 2 * kHeaderLen) {
		setErr("send msg packet is too small");
		return 1;
	}
	
	buf.SetWriteIndex(0);
	int sendLen = htonl(oLen - kHeaderLen);
	if(buf.Write(&sendLen, kHeaderLen) != kHeaderLen) {
		setErr("send msg packet to buffer failed");
		return 1;
	}

	uint32_t checksum = htonl(hash);
	if(buf.Write(&checksum, kHeaderLen) != kHeaderLen) {
		setErr("send msg packet checksum to buffer failed");
		return 1;
	}
	
	buf.SetWriteIndex(oLen);
	int wlen = KlPollWrite(buf.c_str(), buf.length());
	
	return (wlen == buf.length() ? 0 : 1);
}

int KConnBase::KlReadMsg(Buffer& rBuf, uint32_t *rChecksum, int microsecond) {
    int r = 0;
	r = KlPollRead((char*)rBuf.GetRawBuffer(), kHeaderLen, microsecond);
	if(r < 0) {
		if(r == -2) 
			return KL_TCP_ERR_SOCKET_READ_TIMEOUT;
		return KL_TCP_ERR_INTERNAL_ERROR;
	}

	int buf_len = ntohl (*(int*) rBuf.c_str());
	if(buf_len == 0)
		return 0;

	if(buf_len <= kHeaderLen) {
		setErr("recv packet length too small: %d", buf_len);
		return KL_TCP_ERR_INTERNAL_ERROR;	
	}
	
	if(buf_len > BUFSIZE + packetHeadLen) {
		setErr("conv data len failed: %d", buf_len);
		return KL_TCP_ERR_INTERNAL_ERROR;	
	}
	
	r = KlPollRead((char*)rBuf.GetRawBuffer(), buf_len, microsecond);
	if(r != buf_len) {
		return KL_TCP_ERR_INTERNAL_ERROR;	
	}
	rBuf.SetWriteIndex(buf_len);
	if(KlDeserializeUint32(&rBuf, rChecksum)) {
		setErr("deserialize recv checksum failed");
		return KL_TCP_ERR_INTERNAL_ERROR;
	}

	return r;
}

void KConnBase::CloseSockfd() {
    if(sockfd_ >= 0) {
        close(sockfd_);
        sockfd_ = -1;
    }
}

int KConnBase::SendChecksumPacket(uint32_t cmdtype, unsigned long checksum) {
    int ret = 0;
	Buffer oArchive;
	oArchive.AdvanceWriteIndex(2 * kHeaderLen);
	
	KlCommReq commreq;
	commreq.comm_req_.Printf("%lu", checksum);
	
	KlCommHead klhead;
	InitKlCommHead(&klhead, cmdtype, innseq_);
	
	ret = ret ? : KlSerializeCommHead(oArchive, &klhead);
	ret = ret ? : KlSerializeCommReqBody(oArchive, &commreq);
	if(ret)
		return ret;

	uint32_t hash=0;
	ret = CalcSendChecksum(oArchive, packetHeadLen, hash);
	if(ret)
		return ret;
	
	return KlSendMsg(oArchive, hash);
}

int KConnBase::ApplyBuffMem(Buffer& cBuff, size_t cbufLen) {
    if(!cBuff.Reserve(cbufLen)) {
        setErr("apply buffer memory failed");
        return -1;
    }
    return 0;
}

int KConnBase::PackCommHeader(uint32_t cmdtype, Buffer& sbuf) {
	int ret = 0;
	sbuf.AdvanceWriteIndex(2 * kHeaderLen);
	klCommHead klhead;
	InitKlCommHead(&klhead, cmdtype, innseq_);
	ret = KlSerializeCommHead(sbuf, &klhead);
	if(ret) {
		setErr("pack comm message header failed");
	}
	return ret;
}

int KConnBase::ReplyCommAnswer(int retcode, uint32_t cmdtype, const std::string& cmdstr) {
    int ret = 0;
	Buffer oArchive;
	oArchive.AdvanceWriteIndex(2 * kHeaderLen);
	
	KlCommResp commresp;
	commresp.errcode_ = retcode;
	commresp.errinfo_.Printf("%s", cmdstr.c_str());
	
	KlCommHead klhead;
	InitKlCommHead(&klhead, cmdtype, innseq_);
	ret = ret ? : KlSerializeCommHead(oArchive, &klhead);
	ret = ret ? : KlSerializeRespCommBody(oArchive, &commresp);
	if(ret)
		return ret;

	uint32_t hash=0;
	ret = CalcSendChecksum(oArchive, packetHeadLen, hash);
	if(ret) 
		return ret;
	
	return KlSendMsg(oArchive, hash);
}

int KConnBase::CalcSendChecksum(Buffer& sbuf, size_t posIndex, uint32_t& hash) {
    if(sbuf.length() < posIndex) {
		setErr("calculate message is empty");
		return 1;
	}
	
	if(!(kltype_ & KL_NCHECKSUM)) {
		if(posIndex)
			hash = CalcSendContentChecksum(sbuf, posIndex);
		else
			hash = KlMurmurHashCode(sbuf.c_str(), sbuf.length());
	}else
		hash = sbuf.length() - posIndex;

	return 0;
}

bool KConnBase::ChecksumCompareResult(Buffer& sbuf, size_t posIndex, uint32_t rChecksum) {
	uint32_t hash=0;
	if(CalcSendChecksum(sbuf, posIndex, hash)) {
		return false;
	}

	if(hash != rChecksum) {
		setErr("recv msg checksum compare failed");
		return false;
	}

	return true;
}

void KConnBase::ConnSeqAddOne() {
	innseq_ += 1;
}

bool KConnBase::IsSeqEqual(uint32_t rSeq) {
	if(innseq_ != rSeq)
		return false;

	return true;
}

int KConnBase::SendCommPacket(uint32_t cmdtype, Buffer& sbuf) {
	return SendCommPacket(cmdtype, sbuf.c_str(), sbuf.length());
}

int KConnBase::SendCommPacket(uint32_t cmdtype, const char* buff, int bufLen) {
	int rc = 0;
	Buffer oArchive;
	oArchive.AdvanceWriteIndex(2 * kHeaderLen);
	
	KlCommReq commreq;
	if(commreq.comm_req_.Write(buff, bufLen) != bufLen) {
		setErr("send msg write buffer failed");
		return 1;
	}

	KlCommHead klhead;
	InitKlCommHead(&klhead, cmdtype, innseq_);
	rc = rc ? : KlSerializeCommHead(oArchive, &klhead);
	rc = rc ? : KlSerializeCommReqBody(oArchive, &commreq);
	if(rc)
		return rc;
	
	uint32_t hash=0;
	rc = CalcSendChecksum(oArchive, packetHeadLen, hash);
	if(rc)
		return rc;
	
	return KlSendMsg(oArchive, hash);
}

void KFileStat::KlMsgChecksum(int checksum_type, const char *buffer, int len) {
	if(checksum_type) {
		if(checksum_ == 0) 
			checksum_ = KlMurmurHashCode(buffer, len);
		else
			checksum_ ^= KlMurmurHashCode(buffer, len);
	} else
		checksum_ += len;
}

void KFileStat::KlMsgChecksum(int checksum_type, unsigned int hash) {
	if(checksum_type) {
		if(checksum_ == 0) 
			checksum_ = hash;
		else
			checksum_ ^= hash;
	} else
		checksum_ += hash;
}

std::mutex file_mutex_;
int KFileStat::KlSafeWriteFile(int type) {
	std::lock_guard<std::mutex> guard(file_mutex_);
	int ret = access(dstfile_.c_str(), F_OK);       
	if((ret != -1) && !type) {                                        
		setErr("target file: %s is exist", dstfile_.c_str());      
		return KL_TCP_ERR_FILE_EXIST;
	}
	if(rename(tmpfile_.c_str(), dstfile_.c_str())) {
		setErr("rename file:%s failed: %s", tmpfile_.c_str(),strerror(errno));
		return KL_TCP_ERR_INTERNAL_ERROR;
	}
	return 0;
}

int KCommHandle::KlDeserializeHeadPacket(Buffer& buff, KlCommHead *oh) {
    return KlDeserializeCommHead(buff, oh);
}

int KCommHandle::KlDeserializeCmdReqPacket(Buffer& buff, KlCmdReq* CmdReq) {
    return KlDeserializeCmdReqBody(buff, CmdReq);
}

int KCommHandle::KlDeserializeCommReqPacket(Buffer& buff, KlCommReq* CommReq) {
    return KlDeserializeCommReqBody(buff, CommReq);
}

int KCommHandle::KlDeserializeCommRespPacket(Buffer& buff, KlCommResp* CommResp){
    return KlDeserializeRespCommBody(buff, CommResp);
}

int KCommHandle::KlDeserializeFileReqPacket(Buffer& buff, KlFileReq* FileReq) {
    return KlDeserializeFileReqBody(buff, FileReq);
}

int AsyncIoBase::PollReadCommand(int fd, uint64_t timeout) {
    struct pollfd fds[1];
	bzero (fds , sizeof(fds));
	fds[0].fd = fd;
	fds[0].events = POLLIN;

	int ret = poll (fds , 1 , timeout);
    if (ret < 0) {
    	if(errno == EINTR) {
			setErr("poll interrupt");
			return 0;
		}
        setErr("poll error[%d,%s]" , errno , strerror (errno));
        return -1;
    } else if (0 == ret) {
		setErr("poll timeout");
	    return 0;
    }

	//int err = 0;
    int rdNum = read(fd, (void*)rdCmdBuf_.GetRawWriteBuffer(), BUFSIZE);
    if (rdNum < 0) {
        if (errno == EINTR || errno == EAGAIN) {
			setErr("read interrupt");
            return 0;
        }
        else {
            setErr("Buffer Read error[%d,%s]" , errno , strerror (errno));
            return -1;
        }
    }
    else if (0 == rdNum) {
        setErr("read end");
        return -2;
    }

	rdCmdBuf_.AdvanceWriteIndex(rdNum);
	return rdNum;
}

int AsyncIoBase::BlockReadCommand(int fd) {
    int rdNum = 0;
	
again:
	rdNum = read(fd, (void*)rdCmdBuf_.GetRawWriteBuffer(), BUFSIZE);
    if (rdNum < 0) {
        if (errno == EINTR || errno == EAGAIN) {
			usleep (1000 * 4);
            goto again;
        }
        else {
            setErr("Buffer Read error[%d,%s]" , errno , strerror (errno));
            return -1;
        }
    }
    else if (0 == rdNum) {
        setErr("read end");
        return 0;
    }
	
	rdCmdBuf_.AdvanceWriteIndex(rdNum);
	return rdNum;
}
 
}