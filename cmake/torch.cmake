# Python Wrapper

# find pybind
set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 REQUIRED)

# find python3 and add include directories
find_package(Python3 3.6 REQUIRED COMPONENTS Interpreter Development)
include_directories(${Python3_INCLUDE_DIRS})

set(PYTHON_SUPPORTED_VERSIONS "3.8" "3.9" "3.10" "3.11" "3.12")

# Torch Compile FLAGS
run_python("Torch_DIR" "import torch; print(torch.__file__.rsplit(\"/\", 1)[0])" "Cannot find torch DIR")

run_python(TORCH_ENABLE_ABI
    "import torch; print(int(torch._C._GLIBCXX_USE_CXX11_ABI))"
    "Failed to find torch ABI info"
)


# 1. Use run_python to get the current Torch version
# Note: Use .split('+')[0] to remove possible cuda suffix (e.g. 2.9.0+cu118), ensuring CMake can compare correctly
run_python(
    "TORCH_CURRENT_VERSION"
    "import torch; print(torch.__version__.split('+')[0])"
    "Cannot get torch version"
)

if(TORCH_CURRENT_VERSION VERSION_GREATER_EQUAL "2.9")
    message(STATUS "Torch version ${TORCH_CURRENT_VERSION} >= 2.9, using predefined TORCH_ENABLE_ABI.")
    set(Torch_PYBIND11_BUILD_ABI ${TORCH_ENABLE_ABI})
else()
    message(STATUS "Torch version ${TORCH_CURRENT_VERSION} < 2.9, querying internal ABI string.")
    run_python(
        "Torch_PYBIND11_BUILD_ABI"
        "import torch; print(torch._C._PYBIND11_BUILD_ABI)"
        "Cannot get TORCH_PYBIND11_BUILD_ABI"
    )
endif()

message(STATUS "Final Torch_PYBIND11_BUILD_ABI: ${Torch_PYBIND11_BUILD_ABI}")

run_python("TORCH_WITH_CUDA" "import torch; print(torch.cuda.is_available())" "Cannot find torch DIR")

# Include Directories
set(
    TORCH_INCLUDE_DIRS
    ${Torch_DIR}/include
    ${Torch_DIR}/include/torch/csrc/api/include/
)

# Libraries
set(TORCH_LIBRARIES
    ${Torch_DIR}/lib/libtorch.so
    ${Torch_DIR}/lib/libtorch_python.so
    ${Torch_DIR}/lib/libc10.so
)

if (${TORCH_WITH_CUDA})
    message(STATUS TORCH_WITH_CUDA: ${TORCH_WITH_CUDA})
    set(TORCH_LIBRARIES ${TORCH_LIBRARIES} ${Torch_DIR}/lib/libc10_cuda.so ${Torch_DIR}/lib/libtorch_cuda.so)
endif()

message(STATUS "find torch:" ${Torch_DIR})
message(STATUS "find TORCH_LIBRARIES.")
message(STATUS "find TORCH_INCLUDE_DIRS.")
message(STATUS "TORCH ABI:" ${TORCH_ENABLE_ABI})
message(STATUS "TORCH PYBIND ABI:" ${Torch_PYBIND11_BUILD_ABI})


# Common Compilation Flags
add_compile_options("-DTORCH_API_INCLUDE_EXTENSION_H")
add_compile_options("-D_GLIBCXX_USE_CXX11_ABI=${TORCH_ENABLE_ABI}")
if(TORCH_CURRENT_VERSION VERSION_LESS "2.9")
    add_compile_options(-DPYBIND11_BUILD_ABI=\"${Torch_PYBIND11_BUILD_ABI}\")
endif()
