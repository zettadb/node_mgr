/*
   Copyright (c) 2019-2022 ZettaDB inc. All rights reserved.

   This source code is licensed under Apache 2.0 License,
   combined with Common Clause Condition 1.0, as detailed in the NOTICE file.
*/

#ifndef __FILE_TRAVERSE_H__
#define __FILE_TRAVERSE_H__

#include <dirent.h>
#include <string>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <iostream>
#include <algorithm>
#include <sys/types.h>
#include <sys/stat.h>

#define CFILE_ERR_BUF 4096

typedef enum {
	CBLK,
	CCHR,
	CDIR,
	CFIFO,
	CLNK,
	CREG,
	CSOCK,
	CUNKNOWN
} FileType;

template<typename T>
class CFile_traverse {
public:
	CFile_traverse(const std::string& parse_path, T* filter) :
			m_parse_path(parse_path), m_cur_fpos(""), m_filter(filter), m_cur_level(0) {
	}

	virtual ~CFile_traverse() {
		if(m_filter) {
			delete m_filter;
		}
	}

	int parse();
	const char* getErr() const {
		return m_err;
	}

private:
	int parse_inn(const std::string& path);
	FileType dev_to_filetype(const std::string& d_name);

private:
	std::string m_parse_path;
	std::string m_cur_fpos;
	T* m_filter;
	int m_cur_level;
	char m_err[CFILE_ERR_BUF];
};

/*
 * 0 -- success
 * 1 -- failed
 */
template<typename T>
int CFile_traverse<T>::parse() {
	if(m_parse_path.empty()) {
		snprintf(m_err, CFILE_ERR_BUF, "parse path is empty");
		return 1;
	}
	return parse_inn(m_parse_path);
}

template<typename T>
int CFile_traverse<T>::parse_inn(const std::string& path) {
	if(m_cur_level >= m_filter->scan_level())
		return 0;
	m_cur_level++;

	char f_path[PATH_MAX] = {0};
	m_cur_fpos = m_cur_fpos + path;

	DIR *dirp;
	struct dirent *dp;
	snprintf(f_path, PATH_MAX, "%s", m_cur_fpos.c_str());
	dirp = opendir(f_path);
	if(dirp == NULL) {
		if(m_filter->directory_ignore(m_cur_fpos)) { //open directory failed, check whether set ignore tag。
			snprintf(m_err, CFILE_ERR_BUF, "open parse path: %s failed: [%d, %s]", m_cur_fpos.c_str(), errno, strerror(errno));
			return 1;
		}

		m_cur_fpos = m_cur_fpos.substr(0, m_cur_fpos.rfind("/"));
		return 0;
	}

	for(;;) {
		errno = 0;
		dp = readdir(dirp);
		if(dp == NULL) {
			if(errno) {
				snprintf(m_err, CFILE_ERR_BUF, "read dir parse path: %s failed: [%d, %s]", m_cur_fpos.c_str(), errno, strerror(errno));
				if(m_filter->directory_ignore(m_cur_fpos)) { //read content failed, check whether set ignore tag。
					if(closedir(dirp))
						snprintf(m_err, CFILE_ERR_BUF, "close dir path: %s failed: [%d, %s]", m_cur_fpos.c_str(), errno, strerror(errno));
					return 1;
				}
			}

			break;
		}

		//skip . and ..
		if(strcmp(dp->d_name, ".") == 0 || strcmp(dp->d_name, "..") == 0)
			continue;

		FileType f_type = dev_to_filetype(m_cur_fpos+"/"+std::string(dp->d_name));

		if(m_filter->execute(m_cur_fpos+"/"+std::string(dp->d_name), f_type)) {
			snprintf(m_err, CFILE_ERR_BUF, "execute filter func failed");
			if(closedir(dirp))
				snprintf(m_err, CFILE_ERR_BUF, "close dir path: %s failed: [%d, %s]", m_cur_fpos.c_str(), errno, strerror(errno));
			return 1;
		}

		if((f_type == CDIR) && !m_filter->skip_directory(m_cur_fpos+"/"+std::string(dp->d_name))) {   //if sub_directory, call parse_inn
			if(parse_inn("/"+std::string(dp->d_name))) {
				if(closedir(dirp))
					snprintf(m_err, 1024, "close dir path: %s failed: [%d, %s]", m_cur_fpos.c_str(), errno, strerror(errno));
				return 1;
			}
		}
	}

	if(closedir(dirp)) {
		snprintf(m_err, CFILE_ERR_BUF, "close dir path: %s failed: [%d, %s]", m_cur_fpos.c_str(), errno, strerror(errno));
		return 1;
	} 

	if(m_cur_level > 0)
		m_cur_level--;

	m_cur_fpos = m_cur_fpos.substr(0, m_cur_fpos.rfind("/"));
	return 0;
}

template<typename T>
FileType CFile_traverse<T>::dev_to_filetype(const std::string& d_name) {
	FileType f_type = CUNKNOWN;
	struct stat sb;
	if(stat(d_name.c_str(), &sb) == -1) {
		return f_type;
	}

	switch (sb.st_mode & S_IFMT) {
		case S_IFBLK:  f_type = CBLK;  break;
	    case S_IFCHR:  f_type = CCHR;  break;
	    case S_IFDIR:  f_type = CDIR;  break;
	    case S_IFIFO:  f_type = CFIFO;               break;
	    case S_IFLNK:  f_type = CLNK;                 break;
	    case S_IFREG:  f_type = CREG;            break;
	    case S_IFSOCK: f_type = CSOCK;                  break;
	    default:       f_type = CUNKNOWN;                break;
	}

	return f_type;
}

#endif