/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef PGSQL_CONN_H
#define PGSQL_CONN_H
#include "sys_config.h"
#include <errno.h>
#include "global.h"
#include "log.h"
#include "pgsql/libpq-fe.h"

class PGSQL_CONN
{
private:
	bool connected;
	PGconn	   *conn;
	PGresult   *result;
	void free_pgsql_result();
public:
	PGSQL_CONN()
	{
		connected = false;
		result = NULL;
	}
	~PGSQL_CONN() { close_conn(); }

	int send_stmt(int pgres, const char *stmt);
	int connect(const char *database, const char *ip, int port, const char *user, const char *pwd);
	void close_conn();
};

#endif // !PGSQL_CONN_H
