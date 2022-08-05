/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef _NODE_MGR_TCP_PROTOCOL_H_
#define _NODE_MGR_TCP_PROTOCOL_H_
#include <sys/types.h>
#include <netinet/in.h>
#include "buffer.hpp"

namespace kunlun_tcp 
{
			
static const int KL_CMD_OP=0;			
static const int KL_FILE_OP=1;					
static const int KL_PACK_OP=2;			
static const int KL_CHECKSUM_OP=3;		
static const int KL_RESP_OP=4; 		
static const int KL_TIMEOUT_OP=5;		
static const int KL_END_OP=6;			
static const int KL_QUIT_OP=7;			

#define ENCRYPT_TYPE 0		//support encrypt

/*  kl_tcp msg package
*
*       +--------------+------------------+------------------+------------+
*       | length       | mumhash checksum |  KlCommHead      |  payload   |
*       +--------------+------------------+------------------+------------+
*
*/

#pragma pack(1)

const int KlCommHeadSupportCompress = 1;
const int KlCommHeadIsCompressed = 1 << 1;

typedef struct klCommHead{
	uint32_t len_;       
    uint32_t cmdtype_; 	
	uint16_t encrypt_; 	
	uint32_t innseq_;  
	
 	char varbuf[0];

    klCommHead () {
        bzero (this , sizeof(*this));
    }
} KlCommHead;

#pragma pack()

void InitKlCommHead(KlCommHead* oh, uint32_t cmdtype, uint32_t innseq);
int KlSerializeCommHead(Buffer& oArchive, KlCommHead* oh);
int KlDeserializeCommHead(Buffer& iArchive, KlCommHead* oh);

typedef struct {
	int32_t errcode_;		//0 -- success !0 --- failed
	Buffer errinfo_;
} KlCommResp;

int KlSerializeRespCommBody(Buffer& oArchive, KlCommResp* CommResp);
int KlDeserializeRespCommBody(Buffer& iArchive, KlCommResp* CommResp);

//CmdReq
typedef struct {
	uint32_t ratelimit_;
	Buffer cmd_req_;
	uint32_t open_type_;
	Buffer cmd_mode_;
} KlCmdReq;

int KlSerializeCmdReqBody(Buffer& oArchive, KlCmdReq* CommReq);
int KlDeserializeCmdReqBody(Buffer& iArchive, KlCmdReq* CommReq);

//PackReq
typedef struct {
	Buffer comm_req_;
} KlCommReq;

int KlSerializeCommReqBody(Buffer& oArchive, KlCommReq* CommReq);
int KlDeserializeCommReqBody(Buffer& iArchive, KlCommReq* CommReq);

typedef struct {
	uint16_t req_type_;
	Buffer file_name_;
} KlFileReq;
int KlSerializeFileReqBody(Buffer& oArchive, KlFileReq* FileReq);
int KlDeserializeFileReqBody(Buffer& iArchive, KlFileReq* FileReq);

int KlSerializeUint32(Buffer* oa, const uint32_t *d);
int KlSerializeInt32(Buffer* oa, const int32_t *d);
int KlSerializeUint16(Buffer* oa, uint16_t *d);
int KlDeserializeUint32(Buffer* ia, uint32_t *d);

}

#endif
