add_library(request_dealer OBJECT request_dealer.cc )
target_include_directories(request_dealer INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(request_dealer PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(request_dealer PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(request_dealer PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(request_dealer PUBLIC "${ZETTALIB_INCLUDE_PATH}")
