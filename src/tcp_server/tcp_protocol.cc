/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "tcp_protocol.h"

namespace kunlun_tcp
{
    
void InitKlCommHead(KlCommHead* oh, uint32_t cmdtype, uint32_t innseq) {
	oh->len_ = sizeof(KlCommHead);
	oh->cmdtype_ = cmdtype;
	oh->encrypt_ = ENCRYPT_TYPE;
	oh->innseq_ = innseq;
}

int KlSerializeUint32(Buffer* oa, const uint32_t *d) {
	int r = 0;
	uint32_t i = htonl(*d);
	r = oa->Write(&i, sizeof(uint32_t));
	if(r != sizeof(uint32_t))
		return 1;
		
	return 0;
}

int KlSerializeInt32(Buffer* oa, const int32_t *d) {
	int r = 0;
	int32_t i = htonl(*d);
	r = oa->Write(&i, sizeof(int32_t));
	if(r != sizeof(int32_t))
		return 1;
		
	return 0;
}


int KlSerializeUint16(Buffer* oa, uint16_t *d) {
	int r = 0;
	uint16_t i = htons(*d);
	r = oa->Write(&i, sizeof(uint16_t));
	if(r != sizeof(uint16_t))
		return 1;
		
	return 0;
}

int KlSerializeBuffer(Buffer* oa, const Buffer *b) {
	uint32_t r = 0;
	uint32_t len = b->length();
	r = KlSerializeUint32(oa, &len);
	if (r)
		return 1;

	if(len == 0)
		return 0;

	r = oa->Write(b->c_str() , len);
	if( r != len)
		return 1;
	return 0;
}


int KlDeserializeUint32(Buffer* ia, uint32_t *d) {
	int r = 0;
	r = ia->Read(d, sizeof(uint32_t));
	if(r != sizeof(uint32_t))
		return 1;

	*d = ntohl(*d);
	return 0;
}

int KlDeserializeInt32(Buffer* ia, int32_t *d) {
	int r = 0;
	r = ia->Read(d, sizeof(int32_t));
	if(r != sizeof(int32_t))
		return 1;

	*d = ntohl(*d);
	return 0;
}


int KlDeserializeUint16(Buffer* ia, uint16_t *d) {
	int r = 0;
	r = ia->Read(d, sizeof(uint16_t));
	if(r != sizeof(uint16_t))
		return 1;

	*d = ntohs(*d);
	return 0;
}

int KlDeserializeInt16(Buffer* ia, int16_t *d) {
	int r = 0;
	r = ia->Read(d, sizeof(int16_t));
	if(r != sizeof(int16_t))
		return 1;

	*d = ntohs(*d);
	return 0;
}

int KlDeserializeBuffer(Buffer* ia, Buffer *b) {
	uint32_t r = 0;
	uint32_t len = 0;
	r = KlDeserializeUint32(ia, &len);
	if(r)
		return 1;
	
	if(len == 0)
		return 0;
	
	r = ia->Read(b, len);
	if(r != len)
		return 1;
	return 0;
}


int KlSerializeCommHead(Buffer& oArchive, KlCommHead* oh) {
	int rc = 0;
	rc = rc ? : KlSerializeUint32(&oArchive, &oh->len_);
	rc = rc ? : KlSerializeUint32(&oArchive, &oh->cmdtype_);
	rc = rc ? : KlSerializeUint16(&oArchive, &oh->encrypt_);
	rc = rc ? : KlSerializeUint32(&oArchive, &oh->innseq_);

	return rc;
}

int KlDeserializeCommHead(Buffer& iArchive, KlCommHead* oh) {
	int rc = 0;
	rc = rc ? : KlDeserializeUint32(&iArchive, &oh->len_);
	rc = rc ? : KlDeserializeUint32(&iArchive, &oh->cmdtype_);
	rc = rc ? : KlDeserializeUint16(&iArchive, &oh->encrypt_);
	rc = rc ? : KlDeserializeUint32(&iArchive, &oh->innseq_);
	return rc;
}

int KlSerializeCmdReqBody(Buffer& oArchive, KlCmdReq* CmdReq) {
	int rc = 0;
	rc = rc ? : KlSerializeUint32(&oArchive, &CmdReq->ratelimit_);
	rc = rc ? : KlSerializeBuffer(&oArchive, &CmdReq->cmd_req_);
	rc = rc ? : KlSerializeUint32(&oArchive, &CmdReq->open_type_);
	rc = rc ? : KlSerializeBuffer(&oArchive, &CmdReq->cmd_mode_);
	return rc;
}

int KlDeserializeCmdReqBody(Buffer& iArchive, KlCmdReq* CmdReq) {
	int rc = 0;
	rc = rc ? : KlDeserializeUint32(&iArchive, &CmdReq->ratelimit_);
	rc =  rc ? : KlDeserializeBuffer(&iArchive, &CmdReq->cmd_req_);
	rc = rc ? : KlDeserializeUint32(&iArchive, &CmdReq->open_type_);
	rc =  rc ? : KlDeserializeBuffer(&iArchive, &CmdReq->cmd_mode_);
	return rc;
}


int KlSerializeCommReqBody(Buffer& oArchive, KlCommReq* CommReq) {
	int rc = 0;
	rc = rc ? : KlSerializeBuffer(&oArchive, &CommReq->comm_req_);
	return rc;
}

int KlDeserializeCommReqBody(Buffer& iArchive, KlCommReq* CommReq) {
	int rc = 0;
	rc =  rc ? : KlDeserializeBuffer(&iArchive, &CommReq->comm_req_);
	return rc;
}

int KlSerializeFileReqBody(Buffer& oArchive, KlFileReq* FileReq) {
	int rc = 0;
	rc = rc ? : KlSerializeUint16(&oArchive, &FileReq->req_type_);
	rc = rc ? : KlSerializeBuffer(&oArchive, &FileReq->file_name_);
	return rc;
}

int KlDeserializeFileReqBody(Buffer& iArchive, KlFileReq* FileReq) {
	int rc = 0;
	rc = rc ? : KlDeserializeUint16(&iArchive, &FileReq->req_type_);
	rc = rc ? : KlDeserializeBuffer(&iArchive, &FileReq->file_name_);
	return rc;
}


int KlSerializeRespCommBody(Buffer& oArchive, KlCommResp* CommResp) {
	int rc = 0;
	rc = rc ? : KlSerializeInt32(&oArchive, &CommResp->errcode_);
	rc = rc ? : KlSerializeBuffer(&oArchive, &CommResp->errinfo_);
	return rc;
}

int KlDeserializeRespCommBody(Buffer& iArchive, KlCommResp* CommResp) {
	int rc = 0;
	rc = rc ? : KlDeserializeInt32(&iArchive, &CommResp->errcode_);
	rc = rc ? : KlDeserializeBuffer(&iArchive, &CommResp->errinfo_);
	return rc;
}

}


