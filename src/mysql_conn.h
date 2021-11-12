/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef MYSQL_CONN_H
#define MYSQL_CONN_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"
#include "log.h"
#include "mysql/mysql.h"
#include "mysql/errmsg.h"
#include "mysql/mysqld_error.h"
#include "mysql/server/private/sql_cmd.h"

class MYSQL_CONN
{
public:
	MYSQL_RES *result;
private:
    bool connected;
	enum_sql_command sqlcmd;
	MYSQL conn;
	bool mysql_get_next_result();
	int handle_mysql_error(const char *stmt_ptr = NULL, size_t stmt_len = 0);
	bool handle_mysql_result();
	void free_mysql_result();
public:
	MYSQL_CONN()
	{
		connected = false;
		result = NULL;
	}
	~MYSQL_CONN() { close_conn(); }

	bool send_stmt(enum_sql_command sqlcom_, const char *stmt);
	int connect(const char *database, const char *ip, int port, const char *user, const char *pwd);
	void close_conn();
};

#endif // !MYSQL_CONN_H
