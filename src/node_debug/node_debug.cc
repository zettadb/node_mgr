/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef NDEBUG
#include "node_debug.h"
#include <list>
#include "zettalib/tool_func.h"
#include <algorithm>
//#include "log.h"
#include "zettalib/op_log.h"

std::list<std::string> g_debug_keys;

namespace kunlun 
{

bool nodeDebug(Json::Value& root) {
    if(root.isMember("keyword") && root.isMember("op_type")) {
        std::string keywords = root["keyword"].asString();
        std::vector<std::string> key_vec = StringTokenize(keywords, ",");
        std::string op_type = root["op_type"].asString();
        KLOG_INFO("node_debug keywords: {}, op_type: {}", keywords, op_type);
        if(op_type == "add") {
            for(auto it : key_vec) {
                if(std::find(g_debug_keys.begin(), g_debug_keys.end(), it) == g_debug_keys.end())
                    g_debug_keys.emplace_back(it);
            }
        } else if(op_type == "del") {
            for(auto it : key_vec) {
                for(std::list<std::string>::iterator itb = g_debug_keys.begin(), 
                        ite = g_debug_keys.end(); itb != ite; ) {
                    if(*itb == it)
                        itb = g_debug_keys.erase(itb);
                    else
                        ++itb;
                }
            }
        }
    }
    return true;
}

}

int _nm_keyword(const char* keyword, int state) {
    for(auto it : g_debug_keys) {
        if(strncmp(it.c_str(), keyword, it.length()) == 0)
            return 1;
    }
    return 0;
}

#endif