add_executable(delay_demo delay_demo.cc )
add_executable(download_file download_file.cc )
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
  "libjsoncpp.a"
  "libssl.a" 
  "libcrypto.a"
  "pthread"
  "dl"
  "z"
  )
target_link_libraries(download_file ${LocalLibrariesList})
