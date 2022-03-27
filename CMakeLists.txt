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
set(VENDOR_OUTPUT_PATH "/home/snowao/code/kunlun/zettalib/src/vendor/vendor_output")

if (NOT DEFINED ZETTALIB_INCLUDE_PATH)
	message(FATAL_ERROR "Variables ZETTALIB_INCLUDE_PATH must be specified")
elseif (NOT DEFINED ZETTALIB_LIB_PATH)
	message(FATAL_ERROR "Variables ZETTALIB_LIB_PATH must be specified")
endif()

add_definitions(-D_GLIBCXX_USE_CXX11_ABI=1)
link_directories(${VENDOR_OUTPUT_PATH}/lib)
link_directories(${ZETTALIB_LIB_PATH})

add_subdirectory(src/server_http)
add_subdirectory(src/request_dealer)
add_subdirectory(src/util)

add_executable(node_mgr
  src/config.cc 
  src/log.cc 
  src/main.cc 
  src/os.cc 
  src/sys.cc 
  src/thread_manager.cc 
  src/mysql_conn.cc 
  src/pgsql_conn.cc 
  src/instance_info.cc 
  src/http_server.cc 
  src/http_client.cc 
  src/job.cc 
  src/cjson.cc)
configure_file(src/sys_config.h.in sys_config.h)
target_include_directories(node_mgr PUBLIC
	"${PROJECT_BINARY_DIR}"
  "${VENDOR_OUTPUT_PATH}/include"
  "${ZETTALIB_INCLUDE_PATH}")
target_link_libraries(node_mgr 
  server_http 
  request_dealer
  libzettalib.a 
  libbrpc.a 
  libprotobuf.a 
  libgflags.a 
  libleveldb.a 
  libjsoncpp.a
  libmariadb.a 
  libpq.a 
  pthread 
  libssl.a 
  libcrypto.a 
  dl
  z
)
set(CMAKE_INSTALL_PREFIX "${PROJECT_BINARY_DIR}/output")

install(TARGETS node_mgr DESTINATION bin)
install(TARGETS delay_demo DESTINATION bin/util)
install(TARGETS download_file DESTINATION bin/util)
install(FILES 
  resource/util/mydumper 
  resource/util/myloader
  resource/util/tablecatchup
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  DESTINATION bin/util)
install(FILES 
  resource/script/start_node_mgr.sh 
  resource/script/stop_node_mgr.sh 
  resource/script/restart_node_mgr.sh 
  PERMISSIONS OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE
  DESTINATION bin)
install(FILES resource/node_mgr.cnf DESTINATION conf)
install(DIRECTORY DESTINATION log)
install(DIRECTORY DESTINATION data)

MACRO(REPORT_CXX_FLAGS)
  MESSAGE(STATUS "CMAKE_C_FLAGS: ${CMAKE_C_FLAGS}")
  FOREACH(BUILD_TYPE "" _DEBUG _RELWITHDEBINFO _RELEASE _MINSIZEREL)
    SET(flag "CMAKE_CXX_FLAGS${BUILD_TYPE}")
    MESSAGE(STATUS "${flag}: ${${flag}}")
  ENDFOREACH()
ENDMACRO()

REPORT_CXX_FLAGS()

