add_library(
    _slime_nvlink
    SHARED
    memory_pool.cpp
    nvlink_transport.cpp
)

target_link_libraries(_slime_nvlink PUBLIC CUDA::cudart)

set_target_properties(
    _slime_nvlink
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_nvlink
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
