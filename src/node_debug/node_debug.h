/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/
#ifndef _NODE_MGR_NODE_DEBUG_H_
#define _NODE_MGR_NODE_DEBUG_H_

#ifndef NDEBUG
#include "json/json.h"

namespace kunlun 
{
bool nodeDebug(Json::Value& root);
}

int _nm_keyword(const char* keyword, int state);

#define NM_DEBUG_EXECUTE_IF(keyword, a1)    \
   do {                                     \
      if(_nm_keyword((keyword), 1)) {       \
         a1                                 \
      }                                     \
   } while(0)

#else

#define NM_DEBUG_EXECUTE_IF(keywork, a1) \
do {                                   \
} while(0)

#endif
#endif