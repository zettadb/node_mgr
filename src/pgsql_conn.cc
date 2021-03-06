/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "pgsql_conn.h"
#include <unistd.h>

int PGSQL_CONN::connect(const char *database, const char *ip, int port, const char *user, const char *pwd)
{
	if(connected && db == database)
		return 0;
	else
		close_conn();

	char conninfo[256];
	sprintf(conninfo, "dbname=%s host=%s port=%d user=%s password=%s",
						database, ip, port, user, pwd);

	conn = PQconnectdb(conninfo);

	if (PQstatus(conn) != CONNECTION_OK)
	{
		PQfinish(conn);
		syslog(Logger::ERROR, "Connected to pgsql fail: %s", PQerrorMessage(conn));
		return 1;
	}

	db = database;
	connected = true;
		
	return 0;
}

void PGSQL_CONN::close_conn()
{
	if(connected)
	{
		free_pgsql_result();
		PQfinish(conn);
		connected = false;
	}
}

void PGSQL_CONN::free_pgsql_result()
{
    if (result != NULL)
    {
		PQclear(result);
		result = NULL;
    }
}

int PGSQL_CONN::send_stmt(int pgres, const char *stmt)
{
	if (!connected)
	{
		syslog(Logger::ERROR, "pgsql need to connect first");
		return 1;
	}

	int ret = 0;
	result = PQexec(conn, stmt);

	if(pgres == PG_COPYRES_TUPLES)
	{
		if (PQresultStatus(result) != PGRES_TUPLES_OK)
		{
			syslog(Logger::ERROR, "PQresultStatus error: %s", PQerrorMessage(conn));
			close_conn();
			ret = 1;
		}
	}
	else
	{
		if (PQresultStatus(result) != PGRES_COMMAND_OK)
		{
			syslog(Logger::ERROR, "PQresultStatus error: %s", PQerrorMessage(conn));
			close_conn();
			ret = 1;
		}
	}

	return ret;
}

