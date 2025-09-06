add_library(
    _slime_engine
    SHARED
    assignment.cpp
)

set_target_properties(
    _slime_engine
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_engine
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)

if (BUILD_NVLINK)
    add_subdirectory(nvlink)
endif()

if (BUILD_NVSHMEM)
    add_subdirectory(nvshmem)
endif()

if (BUILD_RDMA)
    add_subdirectory(rdma)
endif()
