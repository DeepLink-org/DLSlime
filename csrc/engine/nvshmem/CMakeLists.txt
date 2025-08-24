# NOTES: this CMake is only for debugging; for setup, please use Torch extension
# cmake_minimum_required(VERSION 3.10)
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

set(USE_SYSTEM_NVTX on)
set(CUDA_ARCH_LIST "9.0" CACHE STRING "List of CUDA architectures to compile")
set(TORCH_CUDA_ARCH_LIST "${CUDA_ARCH_LIST}")

find_package(CUDAToolkit REQUIRED)
find_package(NVSHMEM REQUIRED HINTS ${NVSHMEM_ROOT_DIR}/lib/cmake/nvshmem)

add_library(nvshmem ALIAS nvshmem::nvshmem)
add_library(nvshmem_host ALIAS nvshmem::nvshmem_host)
add_library(nvshmem_device ALIAS nvshmem::nvshmem_device)

link_directories(${CUDA_TOOLKIT_ROOT_DIR}/lib ${NVSHMEM_LIB_DIR})

add_subdirectory(kernels)

add_library(
    _slime_nvshmem
    SHARED
    nvshmem_context.cpp
)

target_include_directories(
    _slime_nvshmem
    PUBLIC
    /usr/local/cuda/include ${NVSHMEM_INCLUDE_DIR}
)

target_link_libraries(
    _slime_nvshmem PUBLIC
    _slime_engine
    _slime_utils
    ${EP_CUDA_LIBRARIES}
)

set_target_properties(
    _slime_nvshmem
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_nvshmem
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
