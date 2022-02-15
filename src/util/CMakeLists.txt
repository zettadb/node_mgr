add_executable(delay_demo delay_demo.cc )
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
