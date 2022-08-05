add_executable(delay_demo delay_demo.cc )
add_executable(download_file download_file.cc )
add_executable(safe_killmysql safe_killmysql.cc)
add_executable(rebuild_node_tool rebuild_node_tool.cc ../util_func/error_code.cc ../util_func/meta_info.cc)
add_executable(test_client test_client.cc )
add_executable(kunlun_flashback kunlun_flashback.cc)
add_executable(kl_tool kl_tool.cc ../tcp_server/tcp_comm.cc 
          ../tcp_server/tcp_protocol.cc ../tcp_server/tcp_error.cc
          ../tcp_server/buffer.cc ../tcp_server/tcp_error.cc)

include_directories(
  "${PROJECT_SOURCE_DIR}/src"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${VENDOR_OUTPUT_PATH}/include"
  "${ZETTALIB_INCLUDE_PATH}"
)
link_directories(
  "${VENDOR_OUTPUT_PATH}/lib"
  "${ZETTALIB_LIB_PATH}"
  )

set(LocalLibrariesList 
  "libzettalib.a"
  "libbrpc.a" 
  "libprotobuf.a"
  "libgflags.a" 
  "libleveldb.a"
  "libmariadb.a" 
  "libjsoncpp.a"
  "libevent.a"
  "libboost_program_options.a"
  "libssl.a" 
  "libcrypto.a"
  "pthread"
  "dl"
  "z"
  )
target_link_libraries(download_file ${LocalLibrariesList})
target_link_libraries(kl_tool ${LocalLibrariesList})
target_link_libraries(safe_killmysql ${LocalLibrariesList})
target_link_libraries(rebuild_node_tool ${LocalLibrariesList})
target_link_libraries(test_client ${LocalLibrariesList})
target_link_libraries(kunlun_flashback ${LocalLibrariesList})
