cmake_minimum_required(VERSION 3.16)

project(_slime C CXX)

include(${CMAKE_CURRENT_LIST_DIR}/cmake/utils.cmake)

# TODO: Add multi transport mode option
# slime_option(BUILD_RDMA         "Build RDMA"                        ON)
slime_option(BUILD_NVLINK       "Build NVLINK"                     OFF)
slime_option(BUILD_BENCH        "Build transfer engine benchmark"  OFF)
slime_option(BUILD_EXAMPLE      "Build example"                    OFF)
slime_option(BUILD_PYTHON       "Build python wrapper"             OFF)
slime_option(BUILD_TEST         "Build test"                       OFF)
slime_option(BUILD_TORCH_PLUGIN "Build torch plugin"               OFF)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(DLSLIME_INSTALL_PATH "lib" CACHE STRING "Library installation directory")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if (BUILD_NVLINK)
enable_language(CUDA)
find_package(CUDAToolkit REQUIRED)

# Supported NVIDIA architectures.
set(CUDA_SUPPORTED_ARCHS "7.0;7.2;7.5;8.0;8.6;8.7;8.9;9.0;10.0;10.1;12.0")
endif()

if (BUILD_TORCH_PLUGIN)
run_python(TORCH_ENABLE_ABI
    "import torch; print(torch._C._GLIBCXX_USE_CXX11_ABI)"
    "Failed to find torch ABI info"
)
add_compile_definitions("-D_GLIBCXX_USE_CXX11_ABI=${TORCH_ENABLE_ABI}")
endif()

include_directories(csrc)
add_subdirectory(csrc)

if (BUILD_EXAMPLE)
add_subdirectory(example/cpp)
endif (BUILD_EXAMPLE)

if (BUILD_BENCH)
add_subdirectory(bench/cpp)
endif (BUILD_BENCH)

if (BUILD_TEST)
add_subdirectory(tests/cpp)
endif (BUILD_TEST)
