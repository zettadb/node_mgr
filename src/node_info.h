/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef NODE_INFO_H
#define NODE_INFO_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"
#include "job.h"

#include <pthread.h>
#include <vector>
#include <string>
#include <algorithm>

class Node
{
public:
	std::string ip;
	int port;
	std::string user;
	std::string pwd;
	std::string path;
	std::string cluster;
	std::string shard;
	Node(std::string &ip_, int port_, std::string &user_, std::string &pwd_):
		ip(ip_),port(port_),user(user_),pwd(pwd_){ }
};

class Node_info
{
public:
	std::vector<Node*> vec_meta_node;
	std::vector<Node*> vec_storage_node;
	std::vector<Node*> vec_computer_node;
	
private:
	static Node_info *m_inst;
	std::vector<std::string> vec_local_ip;
	Node_info();
	
public:
	~Node_info();
	static Node_info *get_instance()
	{
		if (!m_inst) m_inst = new Node_info();
		return m_inst;
	}
	void start_instance();
	void get_local_ip();
	bool check_local_ip(std::string &ip);

	void get_local_node();
	void get_local_node(cJSON *root);
	int get_meta_node();
	int get_storage_node();
	int get_computer_node();
};

#endif // !NODE_INFO_H
