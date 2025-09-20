# Python Wrapper
slime_option(USE_MACA "USE in MACA Platform" OFF)

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

run_python(
    "Torch_PYBIND11_BUILD_ABI"
    "import torch; print(torch._C._PYBIND11_BUILD_ABI)"
    "Cannot get TORCH_PYBIND11_BUILD_ABI"
)

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

message(STATUS "find TORCH_LIBRARIES.")
message(STATUS "find TORCH_INCLUDE_DIRS.")

# Common Compilation Flags
add_compile_definitions("_GLIBCXX_USE_CXX11_ABI=${TORCH_ENABLE_ABI}")
add_compile_definitions(PYBIND11_BUILD_ABI=\"${Torch_PYBIND11_BUILD_ABI}\")

if (USE_MACA)
    add_compile_definitions("USE_MACA")
    message(STATUS "Build DLSlime under meta platform.")
endif()
