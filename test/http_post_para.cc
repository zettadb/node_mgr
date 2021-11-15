#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <errno.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <vector>
#include <string>

// g++ -o http_post_para http_post_para.cc
// ./http_post_para http://127.0.0.1:9998 {\"key\":\"value\"}

#define BUFSIZE 2048		//2K
#define HTTP_DEFAULT_PORT 	80

#define HTTP_POST_PARA_STR "POST /%s HTTP/1.0\r\n\
HOST: %s:%d\r\n\
Accept: */*\r\n\
Content-Type:application/x-www-form-urlencoded\r\n\
Content-Length: %ld\r\n\r\n\
%s"

#define HTTP_POST_FILE_STR1 "POST /%s HTTP/1.0\r\n\
HOST: %s:%d\r\n\
Accept: */*\r\n\
Content-Type: multipart/form-data; boundary=----WebKitFormBoundaryZjrentBBjYWJ7gXp\r\n\
Content-Length: %ld\r\n\r\n"

#define HTTP_POST_FILE_STR2 "------WebKitFormBoundaryZjrentBBjYWJ7gXp\r\n\
Content-Disposition: form-data; name=\"parameters\"\r\n\r\n\
%s\r\n\
------WebKitFormBoundaryZjrentBBjYWJ7gXp\r\n\
Content-Disposition: form-data; name=\"filename\"; filename=\"%s\"\r\n\
Content-Type: application/octet-stream\r\n\r\n"

#define HTTP_POST_FILE_STR3 "\r\n------WebKitFormBoundaryZjrentBBjYWJ7gXp--\r\n"


bool Http_client_parse_url(const char *url, char *ip, int *port, char *path)
{
    char *cStart, *cEnd;
    int len = 0;
    if(!url || !ip || !port || !path)
        return false;
 
    cStart = (char *)url;
 
    if(strncmp(cStart, "http://", strlen("http://")) == 0)
        cStart += strlen("http://");
	else
        return false;

	//get ip and path
    cEnd = strchr(cStart, '/');
    if(cEnd != NULL)
	{
        len = cEnd - cStart;
        memcpy(ip, cStart, len);
        ip[len] = '\0';
        if(*(cEnd + 1) != '\0')
			strcpy(path, cEnd+1);
		else
			*path = '\0';
    }
	else
    {
    	strcpy(ip, cStart);
    }
	
    //get port and reset ip
    cStart = strchr(ip, ':');
    if(cStart != NULL)
	{
        *cStart++ = '\0';
        *port = atoi(cStart);
    }
	else
	{
        *port = HTTP_DEFAULT_PORT;
    }
 
    return true;
}

bool Http_client_content_length(const char* buf, int *length)
{
	char *cStart, *cEnd;
	bool ret = false;
	std::string str;

	cStart = strstr((char*)buf, "Content-Length:");
	if(cStart == NULL)
		return false;

	cStart = cStart + strlen("Content-Length:");
	cEnd = strstr(cStart, "\r\n");
	if(cEnd == NULL)
		return false;

	str = std::string(cStart, cEnd - cStart);
	*length = atoi(str.c_str());
	if(*length <= 0)
		return false;

	return true;
}

int Http_client_socket(const char *ip, int port)
{
	struct hostent *hoste;
	struct sockaddr_in server_addr; 
	int socket_fd;

	if((hoste = gethostbyname(ip))==NULL)
		return -1;

	server_addr.sin_family = AF_INET;
	server_addr.sin_port = htons(port);
	server_addr.sin_addr = *((struct in_addr *)hoste->h_addr);

	if((socket_fd = socket(AF_INET,SOCK_STREAM,0)) == -1)
		return -1;

	if(connect(socket_fd, (struct sockaddr *)&server_addr,sizeof(struct sockaddr)) == -1)
		return -1;

	return socket_fd;
}


/*
 * @param
 * url : http post
 * post_str : post parameter
 * result_str : return parameter
 *
 * @retval 
 *	0 : success
 *  -1 : url error
 *  -2 : post_str is to long
 *  -3 : http connect fail
 *  -4 : http return error
 */
int Http_client_post_para(const char *url, const char *post_str, std::string &result_str)
{
	char ip[100] = {'\0'};
	char path[256] = {'\0'};
	int port;
	int socket_fd;
	char http_buf[BUFSIZE];
	int ret = 0;

	if(!Http_client_parse_url(url, ip, &port, path))
	{
		printf("url fail: %s\n", url);
		return -1;
	}

	int n = snprintf(http_buf, BUFSIZE, HTTP_POST_PARA_STR, path, ip, port, strlen(post_str), post_str);
	if(n >= sizeof(http_buf)-1)
	{
		printf("post_str %s is to long\n", post_str);
		return -2;
	}
	
	socket_fd = Http_client_socket(ip,port);
	if(socket_fd < 0)
	{
		printf("http connect fail\n");
		return -3;
	}
	
	send(socket_fd, http_buf, n, 0);

	n = recv(socket_fd, http_buf, sizeof(http_buf)-1, 0);
	if(n>0)
	{
		http_buf[n] = '\0';
		if(strstr((char*)http_buf, "200 OK") == NULL)
		{
			printf("http return code error\n");
			ret = -4;
		}
		else
		{
			int content_length = 0;
			if(!Http_client_content_length(http_buf, &content_length))
			{
				printf("get content length fail\n");
				ret = -4;
			}
			else
			{
				char *result_start = strstr((char*)http_buf, "\r\n\r\n");
				if(result_start == NULL)
					ret = -4;
				else
					result_str = std::string(result_start+4, content_length);
			}
		}
	}

	close(socket_fd);

	return ret;
}

int main(int argc, char **argv)
{
	if (argc != 3)
	{
		printf("\nUsage: ./http_post_para http://127.0.0.1:9998 paramemter\n");
		return 1;
	}
	
	std::string result_str;
	Http_client_post_para(argv[1], argv[2], result_str);
	
	printf("result_str=%s\n", result_str.c_str());
	
	return 0;
}