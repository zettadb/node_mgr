add_library(restore_task OBJECT 
            restore_mysql_dealer.cc 
            restore_postgres_dealer.cc)
target_include_directories(restore_task INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(restore_task PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_include_directories(restore_task PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(restore_task PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(restore_task PUBLIC "${ZETTALIB_INCLUDE_PATH}")
