add_executable(delay_demo delay_demo.cc )
add_executable(download_file download_file.cc )
include_directories(
  "${PROJECT_SOURCE_DIR}/src"
  "${CMAKE_CURRENT_SOURCE_DIR}"
  "${PROJECT_SOURCE_DIR}/src/vendor/include"
  "${ZETTALIB_INCLUDE_PATH}"
)
link_directories(
  "${PROJECT_SOURCE_DIR}/src/vendor/lib"
  "${ZETTALIB_LIB_PATH}"
  )

set(LocalLibrariesList 
  "libzettalib.a"
  "libbrpc.a" 
  "libprotobuf.a"
  "libgflags.a" 
  "libleveldb.a" 
  "libjsoncpp.a"
  "pthread"
  "libssl.a" 
  "libcrypto.a"
  "dl"
  "z"
  )
target_link_libraries(download_file ${LocalLibrariesList})
