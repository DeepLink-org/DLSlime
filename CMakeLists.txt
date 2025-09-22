cmake_minimum_required(VERSION 3.16)

project(_slime C CXX)

include(${CMAKE_CURRENT_LIST_DIR}/cmake/utils.cmake)

# Slime options for transfer engine
slime_option(BUILD_RDMA                "Build RDMA"                        ON)
slime_option(BUILD_NVLINK              "Build NVLINK"                     OFF)
slime_option(BUILD_NVSHMEM             "Build NVSHMEM"                    OFF)
slime_option(BUILD_ASCEND_DIRECT       "Build Ascend direct transport"    OFF)

# Slime options for ops
slime_option(BUILD_OPS                 "Build collective ops"             OFF)

# Slime options for custom python wrapper
slime_option(BUILD_PYTHON              "Build python wrapper"             OFF)

# Slime options for torch plugin
slime_option(USE_GLOO_BACKEND          "Build gloo backend"               OFF)
slime_option(BUILD_TORCH_PLUGIN        "Build torch plugin"               OFF)

# Slime options for test
slime_option(BUILD_BENCH               "Build transfer engine benchmark"  OFF)
slime_option(BUILD_TEST                "Build test"                       OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(DLSLIME_INSTALL_PATH "lib" CACHE STRING "Library installation directory")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (BUILD_NVSHMEM OR BUILD_NVLINK OR BUILD_OPS)
    enable_language(CUDA)
    set(CMAKE_CUDA_STANDARD 17)
    set(CMAKE_CUDA_ARCHITECTURES "90")
    find_package(CUDAToolkit REQUIRED)
    include_directories(${CUDAToolkit_INCLUDE_DIRS})
endif()

if (BUILD_PYTHON)
    set(DLSLIME_INSTALL_PATH "${SKBUILD_PROJECT_NAME}")
else()
    set(DLSLIME_INSTALL_PATH "lib")
endif()

if (BUILD_TORCH_PLUGIN OR BUILD_OPS)
    include(${CMAKE_CURRENT_LIST_DIR}/cmake/torch.cmake)
endif()

include_directories(csrc)
add_subdirectory(csrc)

if (BUILD_BENCH)
    add_subdirectory(bench/cpp)
endif (BUILD_BENCH)

if (BUILD_TEST)
    add_subdirectory(tests/cpp)
endif (BUILD_TEST)
