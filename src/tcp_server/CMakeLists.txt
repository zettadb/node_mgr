add_executable(kl_server 
                tcp_server.cc 
                tcp_comm.cc 
                tcp_protocol.cc 
                tcp_error.cc
                buffer.cc
                kl_pcmd.cc
                tcp_error.cc
                ../util_func/meta_info.cc)

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
  "libmariadb.a"
  "libevent.a"
  "libboost_program_options.a"
  "libssl.a" 
  "libcrypto.a"
  "pthread"
  "dl"
  "z"
  )
target_link_libraries(kl_server ${LocalLibrariesList})
