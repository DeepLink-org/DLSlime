set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC")
set(CUDA_SEPARABLE_COMPILATION ON)

# adapted from torch
list(APPEND CMAKE_CUDA_FLAGS
    "-DENABLE_FAST_DEBUG -O3 \
    --ptxas-options=--verbose,--register-usage-level=10,--warn-on-local-memory-usage \
    -DONNX_NAMESPACE=onnx_c2 \
    -gencode arch=compute_90,code=sm_90 \
    --expt-relaxed-constexpr \
    -Xcudafe --diag_suppress=cc_clobber_ignored \
    -Xcudafe --diag_suppress=field_without_dll_interface \
    -Xcudafe --diag_suppress=base_class_has_different_dll_interface \
    -Xcudafe --diag_suppress=dll_interface_conflict_none_assumed \
    -Xcudafe --diag_suppress=dll_interface_conflict_dllexport_assumed \
    -Xcudafe --diag_suppress=bad_friend_decl \
    --expt-extended-lambda"
)

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
