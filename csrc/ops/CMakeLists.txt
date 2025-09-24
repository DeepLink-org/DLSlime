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

set(OPS_SRC)
set(OPS_INCLUDE_DIRS ${TORCH_INCLUDE_DIRS})
set(OPS_LIBRARIES ${TORCH_LIBRARIES})

if (BUILD_INTRA_OPS)
    set(
        INTRA_OPS_SRC
        intra_ll/all_gather_intra_ll/all_gather_intra_ll.cu
        intra_ll/all_gather_intra_ll/all_gather_intra_ll_buffer.cpp
    )
    list(APPEND OPS_SRC ${INTRA_OPS_SRC})
    set(
        INTRA_OPS_LIBRARIES
        CUDA::cudart
    )
    list(APPEND OPS_LIBRARIES ${INTRA_OPS_LIBRARIES})
endif()

if (BUILD_INTER_OPS)
    # find NVShmem
    find_package(NVSHMEM REQUIRED HINTS ${NVSHMEM_ROOT_DIR}/lib/cmake/nvshmem)

    add_library(nvshmem ALIAS nvshmem::nvshmem)
    add_library(nvshmem_host ALIAS nvshmem::nvshmem_host)
    add_library(nvshmem_device ALIAS nvshmem::nvshmem_device)

    set(
        INTER_OPS_SRC
        inter_ll/all_gather_inter_ll/all_gather_inter_ll.cu
        inter_ll/all_gather_inter_ll/all_gather_inter_ll_buffer.cpp
    )
    list(APPEND OPS_SRC ${INTER_OPS_SRC})
    set(
        INTER_OPS_INCLUDE_DIRS
        ${NVSHMEM_INCLUDE_DIR}
        /usr/local/cuda/include
    )
    list(APPEND OPS_INCLUDE_DIRS ${INTER_OPS_INCLUDE_DIRS})
    set(
        INTER_OPS_LIBRARIES
        nvshmem CUDA::cudart cudadevrt mlx5
    )
    list(APPEND OPS_LIBRARIES ${INTER_OPS_LIBRARIES})
endif ()

add_library(
    _slime_ops
    SHARED
    ${OPS_SRC}
)

if (BUILD_INTER_OPS)
    set_target_properties(_slime_ops PROPERTIES
            POSITION_INDEPENDENT_CODE ON
            CXX_STANDARD_REQUIRED ON
            CUDA_STANDARD_REQUIRED ON
            CXX_STANDARD 17
            CUDA_STANDARD 17
            CUDA_SEPARABLE_COMPILATION ON
    )
endif()

target_compile_definitions(_slime_ops PRIVATE "-DTORCH_API_INCLUDE_EXTENSION_H")
target_compile_definitions(_slime_ops PRIVATE "-DTORCH_EXTENSION_NAME=_slime_ops")

target_include_directories(_slime_ops PUBLIC ${OPS_INCLUDE_DIRS})

target_link_options(_slime_ops PRIVATE "-Wl,--no-as-needed")
target_link_libraries(_slime_ops ${OPS_LIBRARIES})

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
