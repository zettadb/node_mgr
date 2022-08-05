add_library(node_debug OBJECT 
    node_debug.cc )
target_include_directories(node_debug INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(node_debug PUBLIC "${PROJECT_SOURCE_DIR}/src")
target_include_directories(node_debug PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(node_debug PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(node_debug PUBLIC "${ZETTALIB_INCLUDE_PATH}")