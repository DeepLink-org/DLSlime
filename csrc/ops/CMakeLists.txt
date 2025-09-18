add_library(
    _slime_ops
    SHARED
    intra/all_gather_ll/all_gather_ll.cu
    intra/all_gather_ll/all_gather_ll_buffer.cpp
)

target_link_libraries(_slime_ops CUDA::cudart)

set_target_properties(
    _slime_ops
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_ops
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
