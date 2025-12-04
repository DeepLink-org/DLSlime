function(install_system_library LIB_NAME INSTALL_DIR)
    find_library(${LIB_NAME}_LIB NAMES ${LIB_NAME})
    if(NOT ${LIB_NAME}_LIB)
        message(FATAL_ERROR "${LIB_NAME} library not found. Please install ${LIB_NAME}-dev or ${LIB_NAME}-devel.")
    endif()

    get_filename_component(${LIB_NAME}_REAL_PATH ${${LIB_NAME}_LIB} REALPATH)
    get_filename_component(${LIB_NAME}_DIR ${${LIB_NAME}_LIB} DIRECTORY)
    file(GLOB ${LIB_NAME}_LINKS "${${LIB_NAME}_DIR}/lib${LIB_NAME}.so*")

    install(
        FILES ${${LIB_NAME}_REAL_PATH} ${${LIB_NAME}_LINKS}
        DESTINATION ${INSTALL_DIR}
        PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ WORLD_READ
    )

    message(STATUS "Found ${LIB_NAME} library: ${${LIB_NAME}_REAL_PATH}")
    message(STATUS "Will install ${LIB_NAME} to: ${INSTALL_DIR}")
endfunction()

install_system_library(ibverbs ${DLSLIME_INSTALL_PATH})
install_system_library(numa ${DLSLIME_INSTALL_PATH})

add_library(
    _slime_rdma
    SHARED
    ibv_helper.cpp
    memory_pool.cpp
    rdma_assignment.cpp
    rdma_context.cpp
    rdma_endpoint_v0.cpp
    rdma_buffer.cpp
)

target_link_libraries(_slime_rdma PUBLIC _slime_engine numa ibverbs)

set_target_properties(
    _slime_rdma
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_rdma
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
