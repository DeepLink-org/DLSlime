add_library(
    _slime_rdma
    SHARED
    memory_pool.cpp
    rdma_assignment.cpp
    rdma_context.cpp
    rdma_scheduler.cpp
    rdma_endpoint.cpp
)

target_link_libraries(_slime_rdma PUBLIC _slime_engine _slime_utils)

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
