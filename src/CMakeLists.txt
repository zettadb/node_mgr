#  Copyright (c) 2019 ZettaDB inc. All rights reserved.
#  This source code is licensed under Apache 2.0 License,
#  combined with Common Clause Condition 1.0, as detailed in the NOTICE file.

CMAKE_MINIMUM_REQUIRED(VERSION 3.10)
PROJECT(node_mgr VERSION 1.0)
set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED True)

set(CMAKE_BUILD_TYPE "Debug")
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
	set(CMAKE_C_FLAGS_DEBUG "${CMAKE_C_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
	set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Werror -DENABLE_DEBUG")
elseif(CMAKE_BUILD_TYPE STREQUAL "Release")
	set(CMAKE_C_FLAGS_RELEASE "${CMAKE_C_FLAGS_RELEASE} -Werror -O2")
	set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -Werror -O2")
endif()

set(ZETTALIB_INCLUDE_PATH "/home/snowao/code/kunlun/zettalib/build/output/include")
set(ZETTALIB_LIB_PATH "/home/snowao/code/kunlun/zettalib/build/output/lib")

if (NOT DEFINED ZETTALIB_INCLUDE_PATH)
	message(FATAL_ERROR "Variables ZETTALIB_INCLUDE_PATH must be specified")
elseif (NOT DEFINED ZETTALIB_LIB_PATH)
	message(FATAL_ERROR "Variables ZETTALIB_LIB_PATH must be specified")
endif()

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=1)
link_directories(${PROJECT_SOURCE_DIR}/vendor/lib)
link_directories(${ZETTALIB_LIB_PATH})
add_subdirectory(server_http)
add_executable(node_mgr
config.cc log.cc main.cc os.cc sys.cc thread_manager.cc mysql_conn.cc pgsql_conn.cc instance_info.cc 
http_server.cc http_client.cc job.cc cjson.cc)
configure_file(sys_config.h.in sys_config.h)
target_include_directories(node_mgr PUBLIC
		"${PROJECT_BINARY_DIR}"
		"${PROJECT_SOURCE_DIR}/vendor/include"
		"${ZETTALIB_INCLUDE_PATH}")
target_link_libraries(node_mgr server_http libzettalib.a libbrpc.a libprotobuf.a libgflags.a libleveldb.a mariadb pq pthread libssl.a libcrypto.a dl z)

install(TARGETS node_mgr DESTINATION bin)
install(FILES ../README.md ../LICENSE ../NOTICE DESTINATION .)

MACRO(REPORT_CXX_FLAGS)
  MESSAGE(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
  FOREACH(BUILD_TYPE "" _DEBUG _RELWITHDEBINFO _RELEASE _MINSIZEREL)
    SET(flag "CMAKE_CXX_FLAGS${BUILD_TYPE}")
    MESSAGE(STATUS "${flag}: ${${flag}}")
  ENDFOREACH()
ENDMACRO()

REPORT_CXX_FLAGS()

