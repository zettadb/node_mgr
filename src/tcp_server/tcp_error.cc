/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "tcp_error.h"
#include <errno.h>
#include <string.h>

namespace kunlun_tcp
{
    
const char * kl_tcp_err(int n) {
	switch (n) {
	case KL_TCP_ERR_SUCCESS:
		 return "Ok";
	case KL_TCP_ERR_FILE_EXIST:
	 	 return "File already exists";
	case KL_TCP_ERR_FILE_CREATE_FAILED:
	 	return "Create file failed";
	case KL_TCP_ERR_INTERNAL_ERROR:
		 return "Unexpected internal error";
	 case KL_TCP_ERR_ALLOC_FAIL:
		 return "Memory allocation failed";
	 case KL_TCP_ERR_MESSAGE_INCOMPLETE:
		 return "Incomplete message";
	 case KL_TCP_ERR_INVALID_FORMAT:
		 return "Invalid format";
	 case KL_TCP_ERR_BIGNUM_IS_NEGATIVE:
		 return "Bignum is negative";
	 case KL_TCP_ERR_STRING_TOO_LARGE:
		 return "String is too large";
	 case KL_TCP_ERR_BIGNUM_TOO_LARGE:
		 return "Bignum is too large";
	 case KL_TCP_ERR_ECPOINT_TOO_LARGE:
		 return "Elliptic curve point is too large";
	 case KL_TCP_ERR_NO_BUFFER_SPACE:
		 return "Insufficient buffer space";
	 case KL_TCP_ERR_INVALID_ARGUMENT:
		 return "Invalid argument";
	 case KL_TCP_ERR_KEY_BITS_MISMATCH:
		 return "Key bits do not match";
	 case KL_TCP_ERR_EC_CURVE_INVALID:
		 return "Invalid elliptic curve";
	 case KL_TCP_ERR_KEY_TYPE_MISMATCH:
		 return "Key type does not match";
	 case KL_TCP_ERR_KEY_TYPE_UNKNOWN:
		 return "Unknown or unsupported key type";
	 case KL_TCP_ERR_EC_CURVE_MISMATCH:
		 return "Elliptic curve does not match";
	 case KL_TCP_ERR_EXPECTED_CERT:
		 return "Plain key provided where certificate required";
	 case KL_TCP_ERR_KEY_LACKS_CERTBLOB:
		 return "Key lacks certificate data";
	 case KL_TCP_ERR_KEY_CERT_UNKNOWN_TYPE:
		 return "Unknown/unsupported certificate type";
	 case KL_TCP_ERR_KEY_CERT_INVALID_SIGN_KEY:
		 return "Invalid certificate signing key";
	 case KL_TCP_ERR_KEY_INVALID_EC_VALUE:
		 return "Invalid elliptic curve value";
	 case KL_TCP_ERR_SIGNATURE_INVALID:
		 return "Incorrect signature";
	 case KL_TCP_ERR_LIBCRYPTO_ERROR:
		 return "Error in libcrypto";  /* XXX fetch and return */
	 case KL_TCP_ERR_UNEXPECTED_TRAILING_DATA:
		 return "Unexpected bytes remain after decoding";
	 case KL_TCP_ERR_SYSTEM_ERROR:
		 return strerror(errno);
	 case KL_TCP_ERR_KEY_CERT_INVALID:
		 return "Invalid certificate";
	 case KL_TCP_ERR_AGENT_COMMUNICATION:
		 return "Communication with agent failed";
	 case KL_TCP_ERR_AGENT_FAILURE:
		 return "Agent refused operation";
	 case KL_TCP_ERR_DH_GEX_OUT_OF_RANGE:
		 return "DH GEX group out of range";
	 case KL_TCP_ERR_DISCONNECTED:
		 return "Disconnected";
	 case KL_TCP_ERR_MAC_INVALID:
		 return "Message authentication code incorrect";
	 case KL_TCP_ERR_NO_CIPHER_ALG_MATCH:
		 return "No matching cipher found";
	 case KL_TCP_ERR_NO_MAC_ALG_MATCH:
		 return "No matching MAC found";
	 case KL_TCP_ERR_NO_COMPRESS_ALG_MATCH:
		 return "No matching compression method found";
	 case KL_TCP_ERR_NO_KEX_ALG_MATCH:
		 return "No matching key exchange method found";
	 case KL_TCP_ERR_NO_HOSTKEY_ALG_MATCH:
		 return "No matching host key type found";
	 case KL_TCP_ERR_PROTOCOL_MISMATCH:
		 return "Protocol version mismatch";
	 case KL_TCP_ERR_NO_PROTOCOL_VERSION:
		 return "Could not read protocol version";
	 case KL_TCP_ERR_NO_HOSTKEY_LOADED:
		 return "Could not load host key";
	 case KL_TCP_ERR_NEED_REKEY:
		 return "Rekeying not supported by peer";
	 case KL_TCP_ERR_PASSPHRASE_TOO_SHORT:
		 return "Passphrase is too short (minimum five characters)";
	 case KL_TCP_ERR_FILE_CHANGED:
		 return "File changed while reading";
	 case KL_TCP_ERR_KEY_UNKNOWN_CIPHER:
		 return "Key encrypted using unsupported cipher";
	 case KL_TCP_ERR_KEY_WRONG_PASSPHRASE:
		 return "Incorrect passphrase supplied to decrypt private key";
	 case KL_TCP_ERR_KEY_BAD_PERMISSIONS:
		 return "Bad permissions";
	 case KL_TCP_ERR_KEY_CERT_MISMATCH:
		 return "Certificate does not match key";
	 case KL_TCP_ERR_KEY_NOT_FOUND:
		 return "Password vertify failed";
	 case KL_TCP_ERR_AGENT_NOT_PRESENT:
		 return "Agent not present";
	 case KL_TCP_ERR_AGENT_NO_IDENTITIES:
		 return "Agent contains no identities";
	 case KL_TCP_ERR_BUFFER_READ_ONLY:
		 return "Internal error: buffer is read-only";
	 case KL_TCP_ERR_KRL_BAD_MAGIC:
		 return "KRL file has invalid magic number";
	 case KL_TCP_ERR_KEY_REVOKED:
		 return "Key is revoked";
	 case KL_TCP_ERR_CONN_CLOSED:
		 return "Connection closed";
	 case KL_TCP_ERR_CONN_TIMEOUT:
		 return "Connection timed out";
	 case KL_TCP_ERR_CONN_CORRUPT:
		 return "Connection corrupted";
	 case KL_TCP_ERR_PROTOCOL_ERROR:
		 return "Protocol error";
	 case KL_TCP_ERR_FILE_OPEN_FAILED:
	 	return "Open file error";
	 case KL_TCP_ERR_GET_FILENAME_FAILED:
	 	return "Get file name error";
	 case KL_TCP_ERR_GET_FILESTAT_FAILED:
	 	return "Get file stat error";
	 case KL_TCP_ERR_DIRECTORY_FAILED:
	 	return "Directory is not support";
	 case KL_TCP_ERR_FILE_READ_FAILED:
	 	return "Read file error";
	 case KL_TCP_ERR_FILE_WRITE_FAILED:
	 	return "Write file error";
	 case KL_TCP_ERR_FILE_POSE_FAILED:
	 	return "Set file pos error";
	 case KL_TCP_ERR_WRITE_XML_FAILED:
	 	return "Write xml error";
	 case KL_TCP_ERR_CHECKSUM_FAILED:
	 	return "Checksum failed";
	 case KL_TCP_ERR_PTHREAD_CHILD_QUIT:
	 	return "Child pthread quit";
	case KL_TCP_ERR_FILE_READ_END:
	 	return "Read file end";
	case KL_TCP_ERR_FILE_DIRECTOR_FAILED:
		return "Directory of file does not exist";
	case KL_TCP_ERR_USER_NOT_FOUND:
		return "Input user does not exist";
	case KL_TCP_ERR_NOT_ROOT_GET_KEY_FAILED:
		return "Check passwd failed in noroot status";
	default:
		 return "Unknown error";
	 }
 }

 }
