# Python Wrapper
slime_option(USE_MACA "USE in MACA Platform" OFF)

set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 REQUIRED)

set(PYTHON_SUPPORTED_VERSIONS "3.8" "3.9" "3.10" "3.11" "3.12")

pybind11_add_module(
    _slime_torch
    backend.cpp
)

run_python("Torch_DIR" "import torch; print(torch.__file__.rsplit(\"/\", 1)[0])" "Cannot find torch DIR")

target_include_directories(_slime_torch PRIVATE ${Torch_DIR}/include)
target_include_directories(_slime_torch PRIVATE ${Torch_DIR}/include/torch/csrc/api/include/)

target_compile_definitions(_slime_torch PRIVATE
    "-DTORCH_API_INCLUDE_EXTENSION_H"
)
target_compile_definitions(_slime_torch PRIVATE
    "-DTORCH_EXTENSION_NAME=_slime_torch"
)

run_python(
    "Torch_PYBIND11_BUILD_ABI"
    "import torch; print(torch._C._PYBIND11_BUILD_ABI)"
    "Cannot get TORCH_PYBIND11_BUILD_ABI"
)

target_compile_definitions(
    _slime_torch PRIVATE
    -DPYBIND11_BUILD_ABI=\"${Torch_PYBIND11_BUILD_ABI}\"
)

if (USE_MACA)
    target_compile_definitions(_slime_torch PRIVATE "-DUSE_MACA")
endif()

target_link_options(_slime_torch PRIVATE "-Wl,--no-as-needed")


target_link_libraries(
    _slime_torch
    PRIVATE
    ${Torch_DIR}/lib/libtorch.so
    ${Torch_DIR}/lib/libtorch_cuda.so
    ${Torch_DIR}/lib/libtorch_python.so
    ${Torch_DIR}/lib/libc10.so
    ${Torch_DIR}/lib/libc10_cuda.so
    _slime_gloo
    _slime_utils
)

set_target_properties(
    _slime_torch
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN};${TORCH_INSTALL_PREFIX}/lib"
)

install(
    TARGETS
    _slime_torch
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)

