add_library(install_task OBJECT 
            mysql_install_dealer.cc 
            mysql_uninstall_dealer.cc 
            postgres_install_dealer.cc
            postgres_uninstall_dealer.cc
            exporter_install_dealer.cc
            exporter_uninstall_dealer.cc)
target_include_directories(install_task INTERFACE "${CMAKE_CURRENT_SOURCE_DIR}")
target_include_directories(install_task PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/../")
target_include_directories(install_task PUBLIC "${VENDOR_OUTPUT_PATH}/include")
target_include_directories(install_task PUBLIC "${PROJECT_BINARY_DIR}")
target_include_directories(install_task PUBLIC "${ZETTALIB_INCLUDE_PATH}")
