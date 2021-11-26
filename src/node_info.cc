/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "global.h"
#include "log.h"
#include "cjson.h"
#include "job.h"
#include "node_info.h"
#include "mysql_conn.h"
#include "pgsql_conn.h"
#include "http_client.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <signal.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/ioctl.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>
#include <net/if.h>
#include <arpa/inet.h>
#include <errno.h>
#include <signal.h>
#include <unistd.h>
#include <iostream>

Node_info* Node_info::m_inst = NULL;

int64_t pullup_wait_const = 9;

int64_t stmt_retries = 3;
int64_t stmt_retry_interval_ms = 500;

std::string cluster_mgr_http_ip;
int64_t cluster_mgr_http_port = 0;

extern int64_t thread_work_interval;
extern std::string mysql_install_path;
extern std::string pgsql_install_path;
extern std::string cluster_install_path;

Node::Node(Node_type type_, std::string &ip_, int port_, std::string &user_, std::string &pwd_):
	type(type_),ip(ip_),port(port_),user(user_),pwd(pwd_),
	mysql_conn(NULL),pgsql_conn(NULL),pullup_wait(0)
{

}
		
Node::~Node()
{
	if(mysql_conn != NULL)
	{
		delete mysql_conn;
	}
	if(pgsql_conn != NULL)
	{
		delete pgsql_conn;
	}
}

Node_info::Node_info()
{

}

Node_info::~Node_info()
{
	for (auto &node:vec_meta_node)
		delete node;
	vec_meta_node.clear();
	for (auto &node:vec_storage_node)
		delete node;
	vec_storage_node.clear();
	for (auto &node:vec_computer_node)
		delete node;
	vec_computer_node.clear();
}

void Node_info::start_instance()
{
	get_local_ip();
}

void Node_info::get_local_ip()
{
	int fd, num;
	struct ifreq ifq[16];
	struct ifconf ifc;

	fd = socket(AF_INET, SOCK_DGRAM, 0);
	if(fd < 0)
	{
		syslog(Logger::ERROR, "socket failed");
		return ;
	}
	
	ifc.ifc_len = sizeof(ifq);
	ifc.ifc_buf = (caddr_t)ifq;
	if(ioctl(fd, SIOCGIFCONF, (char *)&ifc))
	{
		syslog(Logger::ERROR, "ioctl failed\n");
		close(fd);
		return ;
	}
	num = ifc.ifc_len / sizeof(struct ifreq);
	if(ioctl(fd, SIOCGIFADDR, (char *)&ifq[num-1]))
	{
		syslog(Logger::ERROR, "ioctl failed\n");
		close(fd);
		return ;
	}
	close(fd);

	for(int i=0; i<num; i++)
	{
		char *tmp_ip = inet_ntoa(((struct sockaddr_in*)(&ifq[i].ifr_addr))-> sin_addr);
		//syslog(Logger::INFO, "tmp_ip=%s", tmp_ip);
		//if(strcmp(tmp_ip, "127.0.0.1") != 0)
		{
			vec_local_ip.push_back(tmp_ip);
		}
	}

	//for(auto &ip: vec_local_ip)
	//	syslog(Logger::INFO, "vec_local_ip=%s", ip.c_str());
}

bool Node_info::check_local_ip(std::string &ip)
{
	for(auto &local_ip: vec_local_ip)
		if(ip == local_ip)
			return true;
	
	return false;
}

void Node_info::get_local_node()
{
	get_meta_node();
	get_storage_node();
	get_computer_node();
}

void Node_info::get_local_node(cJSON *root)
{
	cJSON *item;
	
	item = cJSON_GetObjectItem(root, "node_type");
	if(item == NULL)
	{
		get_meta_node();
		get_storage_node();
		get_computer_node();
		return;
	}

	if(strcmp(item->valuestring, "meta_node"))
		get_meta_node();
	else if(strcmp(item->valuestring, "storage_node"))
		get_storage_node();
	else if(strcmp(item->valuestring, "computer_node"))
		get_computer_node();
}

int Node_info::get_meta_node()
{
	std::unique_lock<std::mutex> lock(mutex_node_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_node");
	cJSON_AddStringToObject(root, "node_type", "meta_node");
	int ip_count = 0;
	for(auto &local_ip: vec_local_ip)
	{
		std::string node_ip = "node_ip" + std::to_string(ip_count++);
		cJSON_AddStringToObject(root, node_ip.c_str(), local_ip.c_str());
	}
	
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + cluster_mgr_http_ip + ":" + std::to_string(cluster_mgr_http_port);
	
	std::string result_str;
	int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
	free(cjson);
	
	if(ret == 0)
	{
		//syslog(Logger::INFO, "result_str=%s", result_str.c_str());
		cJSON *ret_root;
		cJSON *ret_item;

		ret_root = cJSON_Parse(result_str.c_str());
		if(ret_root == NULL)
			return -5;

		for (auto &node:vec_meta_node)
			delete node;
		vec_meta_node.clear();

		int node_count = 0;
		while(true)
		{
			std::string node_str = "meta_node" + std::to_string(node_count++);
			item = cJSON_GetObjectItem(ret_root, node_str.c_str());
			if(item == NULL)
				break;
			
			std::string ip;
			int port;
			std::string user;
			std::string pwd;

			sub_item = cJSON_GetObjectItem(item, "ip");
			if(sub_item == NULL)
				break;
			ip = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "port");
			if(sub_item == NULL)
				break;
			port = sub_item->valueint;

			sub_item = cJSON_GetObjectItem(item, "user");
			if(sub_item == NULL)
				break;
			user = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "pwd");
			if(sub_item == NULL)
				break;
			pwd = sub_item->valuestring;

			Node *node = new Node(Node::META, ip, port, user, pwd);
			vec_meta_node.push_back(node);

			MYSQL_CONN *conn = new MYSQL_CONN();
			node->mysql_conn = conn;
		}

	}

	//get the path of meta node
	for (auto &node:vec_meta_node)
	{
		int retry = stmt_retries;
		auto conn = node->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select @@datadir"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				node->path = row[0];
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();
	}

	return ret;
}

int Node_info::get_storage_node()
{
	std::unique_lock<std::mutex> lock(mutex_node_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_node");
	cJSON_AddStringToObject(root, "node_type", "storage_node");
	int ip_count = 0;
	for(auto &local_ip: vec_local_ip)
	{
		std::string node_ip = "node_ip" + std::to_string(ip_count++);
		cJSON_AddStringToObject(root, node_ip.c_str(), local_ip.c_str());
	}
	
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + cluster_mgr_http_ip + ":" + std::to_string(cluster_mgr_http_port);
	
	std::string result_str;
	int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
	free(cjson);
	
	if(ret == 0)
	{
		//syslog(Logger::INFO, "result_str=%s", result_str.c_str());
		cJSON *ret_root;
		cJSON *ret_item;

		ret_root = cJSON_Parse(result_str.c_str());
		if(ret_root == NULL)
			return -5;

		for (auto &node:vec_storage_node)
			delete node;
		vec_storage_node.clear();

		int node_count = 0;
		while(true)
		{
			std::string node_str = "storage_node" + std::to_string(node_count++);
			item = cJSON_GetObjectItem(ret_root, node_str.c_str());
			if(item == NULL)
				break;
		
			std::string ip;
			int port;
			std::string user;
			std::string pwd;

			sub_item = cJSON_GetObjectItem(item, "ip");
			if(sub_item == NULL)
				break;
			ip = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "port");
			if(sub_item == NULL)
				break;
			port = sub_item->valueint;

			sub_item = cJSON_GetObjectItem(item, "user");
			if(sub_item == NULL)
				break;
			user = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "pwd");
			if(sub_item == NULL)
				break;
			pwd = sub_item->valuestring;

			Node *node = new Node(Node::STORAGE, ip, port, user, pwd);
			vec_storage_node.push_back(node);

			sub_item = cJSON_GetObjectItem(item, "cluster");
			if(sub_item == NULL)
				break;
			node->cluster = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "shard");
			if(sub_item == NULL)
				break;
			node->shard = sub_item->valuestring;

			MYSQL_CONN *conn = new MYSQL_CONN();
			node->mysql_conn = conn;
		}
	}

	//get the path of storage node
	for (auto &node:vec_storage_node)
	{
		int retry = stmt_retries;
		auto conn = node->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select @@datadir"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				node->path = row[0];
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();
	}

	return ret;
}

int Node_info::get_computer_node()
{
	std::unique_lock<std::mutex> lock(mutex_node_);

	cJSON *root;
	cJSON *item;
	cJSON *sub_item;
	char *cjson;
	
	root = cJSON_CreateObject();
	cJSON_AddStringToObject(root, "job_type", "get_node");
	cJSON_AddStringToObject(root, "node_type", "computer_node");
	int ip_count = 0;
	for(auto &local_ip: vec_local_ip)
	{
		std::string node_ip = "node_ip" + std::to_string(ip_count++);
		cJSON_AddStringToObject(root, node_ip.c_str(), local_ip.c_str());
	}
	
	cjson = cJSON_Print(root);
	cJSON_Delete(root);
	
	std::string post_url = "http://" + cluster_mgr_http_ip + ":" + std::to_string(cluster_mgr_http_port);
	
	std::string result_str;
	int ret = Http_client::get_instance()->Http_client_post_para(post_url.c_str(), cjson, result_str);
	free(cjson);
	
	if(ret == 0)
	{
		//syslog(Logger::INFO, "result_str=%s", result_str.c_str());
		cJSON *ret_root;
		cJSON *ret_item;

		ret_root = cJSON_Parse(result_str.c_str());
		if(ret_root == NULL)
			return -5;

		for (auto &node:vec_computer_node)
			delete node;
		vec_computer_node.clear();

		int node_count = 0;
		while(true)
		{
			std::string node_str = "computer_node" + std::to_string(node_count++);
			item = cJSON_GetObjectItem(ret_root, node_str.c_str());
			if(item == NULL)
				break;
		
			std::string ip;
			int port;
			std::string user;
			std::string pwd;

			sub_item = cJSON_GetObjectItem(item, "ip");
			if(sub_item == NULL)
				break;
			ip = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "port");
			if(sub_item == NULL)
				break;
			port = sub_item->valueint;

			sub_item = cJSON_GetObjectItem(item, "user");
			if(sub_item == NULL)
				break;
			user = sub_item->valuestring;

			sub_item = cJSON_GetObjectItem(item, "pwd");
			if(sub_item == NULL)
				break;
			pwd = sub_item->valuestring;

			Node *node = new Node(Node::COMPUTER, ip, port, user, pwd);
			vec_computer_node.push_back(node);

			sub_item = cJSON_GetObjectItem(item, "cluster");
			if(sub_item == NULL)
				break;
			node->cluster = sub_item->valuestring;

			PGSQL_CONN *conn = new PGSQL_CONN();
			node->pgsql_conn = conn;
		}
	}

	//get the path of computer node
	for (auto &node:vec_computer_node)
	{
		int retry = stmt_retries;
		auto conn = node->pgsql_conn;
		
		while(retry--)
		{
			if(conn->connect("postgres", node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to pgsql error ip=%s,port=%d,user=%s,psw=%s", 
								node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str());
				continue;
			}
		
			if(conn->send_stmt(PG_COPYRES_TUPLES, "SELECT setting FROM pg_settings WHERE name='data_directory'"))
				continue;

			if(PQntuples(conn->result) == 1)
			{
				//syslog(Logger::INFO, "presult = %s", PQgetvalue(conn->result,0,0));
				node->path = PQgetvalue(conn->result,0,0);
			}
			
			break;
		}
		conn->free_pgsql_result();
	}

	return ret;
}

void Node_info::set_auto_pullup(int seconds, int port)
{
	std::unique_lock<std::mutex> lock(mutex_node_);

	if(port<=0)		//done on all of the port
	{
		for (auto &node:vec_meta_node)
			node->pullup_wait = seconds;

		for (auto &node:vec_storage_node)
			node->pullup_wait = seconds;

		for (auto &node:vec_computer_node)
			node->pullup_wait = seconds;
	}
	else	//find the node compare by port
	{
		for (auto &node:vec_meta_node)
		{
			if(node->port == port)
			{
				node->pullup_wait = seconds;
				return;
			}
		}

		for (auto &node:vec_storage_node)
		{
			if(node->port == port)
			{
				node->pullup_wait = seconds;
				return;
			}
		}

		for (auto &node:vec_computer_node)
		{
			if(node->port == port)
			{
				node->pullup_wait = seconds;
				return;
			}
		}
	}
	
}

void Node_info::keepalive_nodes()
{
	std::unique_lock<std::mutex> lock(mutex_node_);

	/////////////////////////////////////////////////////////////
	// keep alive of meta
	for (auto &node:vec_meta_node)
	{
		if(node->pullup_wait<0)
			continue;
		else if(node->pullup_wait>0)
		{
			node->pullup_wait -= thread_work_interval;
			if(node->pullup_wait<0)
				node->pullup_wait = 0;
			continue;
		}
	
		int retry = stmt_retries;
		auto conn = node->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select version()"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				if (strcasestr(row[0], "kunlun-storage"))
					break;
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();

		if(retry<0)
		{
			syslog(Logger::ERROR, "meta_node ip=%s, port=%d, retry=%d",node->ip.c_str(),node->port,retry);
			node->pullup_wait = pullup_wait_const;

			std::string cmd = "cd " + mysql_install_path + ";./startmysql.sh " + std::to_string(node->port);
			syslog(Logger::INFO, "start mysql cmd : %s",cmd.c_str());
			//system(cmd.c_str());
			
			FILE* pfd;
			pfd = popen(cmd.c_str(), "r");
			if(!pfd)
			{
				syslog(Logger::ERROR, "pullup node error %s" ,cmd.c_str());
				return;
			}
			pclose(pfd);
		}
	}

	/////////////////////////////////////////////////////////////
	// keep alive of storage
	for (auto &node:vec_storage_node)
	{
		if(node->pullup_wait<0)
			continue;
		else if(node->pullup_wait>0)
		{
			node->pullup_wait -= thread_work_interval;
			if(node->pullup_wait<0)
				node->pullup_wait = 0;
			continue;
		}
	
		int retry = stmt_retries;
		auto conn = node->mysql_conn;
		
		while(retry--)
		{
			if(conn->connect(NULL, node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to mysql error ip=%s,port=%d,user=%s,psw=%s", 
								node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str());
				continue;
			}
			
			if(conn->send_stmt(SQLCOM_SELECT, "select version()"))
				continue;
		
			MYSQL_ROW row;
			if ((row = mysql_fetch_row(conn->result)))
			{
				//syslog(Logger::INFO, "row[]=%s",row[0]);
				if (strcasestr(row[0], "kunlun-storage"))
					break;
			}
			else
			{
				continue;
			}
		
			break;
		}
		conn->free_mysql_result();

		if(retry<0)
		{
			syslog(Logger::ERROR, "storage_node ip=%s, port=%d, retry=%d",node->ip.c_str(),node->port,retry);
			node->pullup_wait = pullup_wait_const;

			std::string cmd = "cd " + mysql_install_path + ";./startmysql.sh " + std::to_string(node->port);
			syslog(Logger::INFO, "start mysql cmd : %s",cmd.c_str());
			//system(cmd.c_str());
			
			FILE* pfd;
			pfd = popen(cmd.c_str(), "r");
			if(!pfd)
			{
				syslog(Logger::ERROR, "pullup node error %s" ,cmd.c_str());
				return;
			}
			pclose(pfd);
		}
	}

	/////////////////////////////////////////////////////////////
	// keep alive of computer
	for (auto &node:vec_computer_node)
	{
		if(node->pullup_wait<0)
			continue;
		else if(node->pullup_wait>0)
		{
			node->pullup_wait -= thread_work_interval;
			if(node->pullup_wait<0)
				node->pullup_wait = 0;
			continue;
		}

		int retry = stmt_retries;
		auto conn = node->pgsql_conn;
		
		while(retry--)
		{
			if(conn->connect("postgres", node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str()))
			{
				syslog(Logger::ERROR, "connect to pgsql error ip=%s,port=%d,user=%s,psw=%s", 
								node->ip.c_str(), node->port, node->user.c_str(), node->pwd.c_str());
				continue;
			}
		
			if(conn->send_stmt(PG_COPYRES_TUPLES, "select version()"))
				continue;

			if(PQntuples(conn->result) == 1)
			{
				//syslog(Logger::INFO, "presult = %s", PQgetvalue(conn->result,0,0));
				if (strcasestr(PQgetvalue(conn->result,0,0), "PostgreSQL"))
					break;
			}
			
			break;
		}
		conn->free_pgsql_result();

		if(retry<0)
		{
			syslog(Logger::ERROR, "computer_node ip=%s, port=%d, retry=%d",node->ip.c_str(),node->port,retry);
			node->pullup_wait = pullup_wait_const;

			std::string cmd = "cd " + pgsql_install_path + ";python2 start_pg.py port=" + std::to_string(node->port);
			syslog(Logger::INFO, "start pgsql cmd : %s",cmd.c_str());
			//system(cmd.c_str());
			
			FILE* pfd;
			pfd = popen(cmd.c_str(), "r");
			if(!pfd)
			{
				syslog(Logger::ERROR, "pullup node error %s" ,cmd.c_str());
				return;
			}
			pclose(pfd);
		}
	}

}

