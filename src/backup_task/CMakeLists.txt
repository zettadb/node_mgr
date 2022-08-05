add_library(backup_task OBJECT 
            backup_dealer.cc)
target_include_directories(backup_task INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(backup_task PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_include_directories(backup_task PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(backup_task PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(backup_task PUBLIC "${ZETTALIB_INCLUDE_PATH}")
