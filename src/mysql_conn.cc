/*
   Copyright (c) 2019-2021 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#include "mysql_conn.h"
#include <unistd.h>

// config variables
int64_t mysql_connect_timeout = 3;
int64_t mysql_read_timeout = 3;
int64_t mysql_write_timeout = 3;
int64_t mysql_max_packet_size = 1024*1024*1024;
// not configurable for now
bool mysql_transmit_compress = false;

#define IS_MYSQL_CLIENT_ERROR(err) (((err) >= CR_MIN_ERROR && (err) <= CR_MAX_ERROR) || ((err) >= CER_MIN_ERROR && (err) <= CER_MAX_ERROR))

int MYSQL_CONN::connect(const char *database, const char *ip, int port, const char *user, const char *pwd)
{
	if (connected) 
		close_conn();

    mysql_init(&conn);
    //mysql_options(mysql, MYSQL_OPT_NONBLOCK, 0); always do sync send
    mysql_options(&conn, MYSQL_OPT_CONNECT_TIMEOUT, &mysql_connect_timeout);
    mysql_options(&conn, MYSQL_OPT_READ_TIMEOUT, &mysql_read_timeout);
    mysql_options(&conn, MYSQL_OPT_WRITE_TIMEOUT, &mysql_write_timeout);
    mysql_options(&conn, MYSQL_OPT_MAX_ALLOWED_PACKET, &mysql_max_packet_size);

    if (mysql_transmit_compress)
        mysql_options(&conn, MYSQL_OPT_COMPRESS, NULL);

    // Never reconnect, because that messes up txnal status.
    my_bool reconnect = 0;
    mysql_options(&conn, MYSQL_OPT_RECONNECT, &reconnect);

    /* Returns 0 when done, else flag for what to wait for if need to block. */
    MYSQL *ret = mysql_real_connect(&conn, ip, user, pwd, database, port, NULL, 
    		CLIENT_MULTI_STATEMENTS |(mysql_transmit_compress ? MYSQL_OPT_COMPRESS : 0));
    if (!ret)
    {
        handle_mysql_error();
        return -1;
    }

    connected = true; // check_mysql_instance_status() needs this set to true here.

    return 0;
}

/*
 * Receive mysql result from mysql server.
 * For SELECT stmt, make MYSQL_RES result ready to this->result; For others,
 * update affected rows.
 *
 * @retval true on error, false on success.
 * */
bool MYSQL_CONN::handle_mysql_result()
{
    int status = 1;

    if (sqlcmd == SQLCOM_SELECT)
    {
        /*
         * Iff the cmd isn't a SELECT stmt, mysql_use_result() returns NULL and
         * mysql_errno() is 0.
         * */
        MYSQL_RES *mysql_res = mysql_store_result(&conn);
        if (mysql_res)
        {
            if (result)
			{
                syslog(Logger::ERROR, "MySQL result not consumed/freed before sending a SELECT statement.");
				return true;
			}
            else
            {
                result = mysql_res;
                goto end;
            }
        }
        else if (mysql_errno(&conn))
        {
            handle_mysql_error();
			return true;
        }
        else
            Assert(mysql_field_count(&conn) == 0);

        /*
         * The 1st result isn't SELECT result, fetch more for it.
         * */
        if (!mysql_get_next_result() && !result)
        {
        	syslog(Logger::ERROR, "A SELECT statement returned no results.");
			return true;
        }
    }
    else
    {
        do {
	        uint64_t n = mysql_affected_rows(&conn);
	        if (n == (uint64_t)-1)
            {
	           handle_mysql_error();
               return true;
            }
	        // TODO: handle RETURNING result later, and below Assert will need be removed.
	        Assert(mysql_field_count(&conn) == 0);

			/*
             * mysql_next_result() return value:
			 * 		more results? -1 = no, >0 = error, 0 = yes (keep looping)
             * Note that mariadb's client library doesn't return -1 to indicate
             * no more results, we have to call mysql_more_results() to see if
             * there are more results.
             * */
            if (mysql_more_results(&conn))
            {
			    if ((status = mysql_next_result(&conn)) > 0)
                {
	                handle_mysql_error();
                    return true;
                }
            }
            else
                break;
		} while (status == 0);
    }
end:
    return false;
}

/*
 * Send SQL statement [stmt, stmt_len) to mysql node in sync
 * @retval true on error, false on success.
 * */
bool MYSQL_CONN::send_stmt(enum_sql_command sqlcom_, const char *stmt)
{
	if (!connected)
	{
		syslog(Logger::ERROR, "mysql need to connect first");
		return true;
	}

    // previous result must have been freed.
    Assert(result == NULL);
    sqlcmd = sqlcom_;
    int ret = mysql_real_query(&conn, stmt, strlen(stmt));
    if (ret != 0)
    {
        handle_mysql_error(stmt, strlen(stmt));
        return true;
    }
    if (handle_mysql_result())
        return true;
    return false;
}

void MYSQL_CONN::free_mysql_result()
{
    if (result)
    {
        mysql_free_result(result);
        result = NULL;
    }

    // consume remaining results if any to keep mysql conn state clean.
    while (mysql_get_next_result())
        ;

    sqlcmd = SQLCOM_END;
}

/*
 * @retval: whether there are more results of any stmt type.
 * */
bool MYSQL_CONN::mysql_get_next_result()
{
    int status = 0;

    if (result) {
        mysql_free_result(result);
        result = NULL;
    }

    while (true)
    {
        if (mysql_more_results(&conn))
        {
            if ((status = mysql_next_result(&conn)) > 0)
            {
                handle_mysql_error();
                return false;
            }

			if (status == -1)
				return false;
			Assert(status == 0);
        }
        else
            return false;

        MYSQL_RES *mysql_res = mysql_store_result(&conn);
        if (mysql_res)
        {
            result = mysql_res;
            break;
        }
        else if (mysql_errno(&conn))
        {
            handle_mysql_error();
			return false;
        }
        else
            Assert(mysql_field_count(&conn) == 0);

    }

    return true;
}

void MYSQL_CONN::close_conn()
{
	if (!connected) return;

    Assert(!result);
    mysql_close(&conn);
    connected = false;
}

int MYSQL_CONN::handle_mysql_error(const char *stmt_ptr, size_t stmt_len)
{
    int ret = mysql_errno(&conn);

	char errmsg_buf[512];
    errmsg_buf[0] = '\0';
    strncat(errmsg_buf, mysql_error(&conn), sizeof(errmsg_buf) - 1);

	if (result) free_mysql_result();

	const char *extra_msg = "";
    /*
     * Only break the connection for client errors. Errors returned by server
	 * are not caused by the connection.
     * */
	const bool is_mysql_client_error = IS_MYSQL_CLIENT_ERROR(ret);
    if (is_mysql_client_error)
    {
		close_conn();
		extra_msg = ", and disconnected from the node";
    }
	syslog(Logger::ERROR, "Got error executing '%s' : {%u: %s}%s.",
		   stmt_ptr ? stmt_ptr : "<none>", ret, errmsg_buf, extra_msg);

    return ret;
}


