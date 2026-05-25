set(RDMA_SOURCES
    ibv_helper.cpp
    memory_pool.cpp
    rdma_assignment.cpp
    rdma_channel.cpp
    rdma_context.cpp
    rdma_endpoint.cpp
    rdma_future.cpp
    rdma_worker.cpp
)
add_library(_slime_rdma SHARED ${RDMA_SOURCES})

target_link_libraries(_slime_rdma PUBLIC _slime_device _slime_engine _slime_obs numa ibverbs)

set_target_properties(
    _slime_rdma
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

target_include_directories(_slime_rdma PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/../../..>
    $<INSTALL_INTERFACE:include/dlslime>
)

install(
    TARGETS
    _slime_rdma
    EXPORT dlslimeTargets
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
