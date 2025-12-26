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

target_include_directories(_slime_engine PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    $<INSTALL_INTERFACE:include/dlslime>
)

install(
    TARGETS
    _slime_engine
    EXPORT dlslimeTargets
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)

if (BUILD_NVLINK)
    add_subdirectory(nvlink)
endif()

if (BUILD_ASCEND_DIRECT)
    add_subdirectory(ascend_direct)
endif()

if (BUILD_NVSHMEM)
    add_subdirectory(nvshmem)
endif()

if (BUILD_RDMA)
    add_subdirectory(rdma)
endif()
