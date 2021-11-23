#  Copyright (c) 2019 ZettaDB inc. All rights reserved.
#  This source code is licensed under Apache 2.0 License,
#  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

CMAKE_MINIMUM_REQUIRED(VERSION 3.10)
PROJECT(node_mgr VERSION 1.0)
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
	set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Werror -O2")
	set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Werror -O2")
endif()

if (NOT EXISTS ../include/mysql)
	execute_process(COMMAND mkdir -p ../include
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
	execute_process(COMMAND tar -C ../include -xzf ../resources/mysql_client.tgz
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endif()

if (NOT EXISTS ../include/pgsql)
	execute_process(COMMAND mkdir -p ../include
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
	execute_process(COMMAND cp ../resources/pgsql/ ../include/ -rf
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endif()

if (NOT EXISTS ../include/hdfs)
	execute_process(COMMAND mkdir -p ../include
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
	execute_process(COMMAND cp ../resources/hdfs/ ../include/ -rf
		WORKING_DIRECTORY ${CMAKE_SOURCE_DIR})
endif()

link_directories(${CMAKE_SOURCE_DIR}/../lib)
add_executable(node_mgr
config.cc log.cc main.cc os.cc sys.cc thread_manager.cc http_server.cc http_client.cc 
job.cc node_info.cc mysql_conn.cc pgsql_conn.cc cjson.cc hdfs_client.cc)
configure_file(sys_config.h.in sys_config.h)
target_include_directories(node_mgr PUBLIC
		"${PROJECT_BINARY_DIR}"
		"${PROJECT_SOURCE_DIR}/../include")
target_link_libraries(node_mgr mariadb pthread pq hdfs jvm)

install(TARGETS node_mgr DESTINATION bin)
install(DIRECTORY ../lib/ DESTINATION lib)
install(DIRECTORY ../resources/ DESTINATION resources
	PATTERN "mysql_client.tgz" EXCLUDE)
install(FILES ../README.md ../LICENSE ../NOTICE DESTINATION .)
install(FILES ./node_mgr_safe DESTINATION bin)

MACRO(REPORT_CXX_FLAGS)
  MESSAGE(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
  FOREACH(BUILD_TYPE "" _DEBUG _RELWITHDEBINFO _RELEASE _MINSIZEREL)
    SET(flag "CMAKE_CXX_FLAGS${BUILD_TYPE}")
    MESSAGE(STATUS "${flag}: ${${flag}}")
  ENDFOREACH()
ENDMACRO()

REPORT_CXX_FLAGS()

