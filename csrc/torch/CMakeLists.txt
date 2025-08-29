# Python Wrapper
slime_option(USE_MACA "USE in MACA Platform" OFF)

set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 REQUIRED)

set(PYTHON_SUPPORTED_VERSIONS "3.8" "3.9" "3.10" "3.11" "3.12")

if (USE_GLOO_BACKEND)
    set(BACKEND_SRC gloo_backend.cpp)
    set(ENGINE_LIBRARIES _slime_gloo _slime_utils)
else ()
    set(BACKEND_SRC slime_backend.cpp)
    set(ENGINE_LIBRARIES _slime_utils _slime_engine _slime_rdma)
endif ()

pybind11_add_module(
    _slime_torch
    ${BACKEND_SRC}
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

set(TORCH_LIBRARIES
    ${Torch_DIR}/lib/libtorch.so
    ${Torch_DIR}/lib/libtorch_python.so
    ${Torch_DIR}/lib/libc10.so
)
run_python("TORCH_WITH_CUDA" "import torch; print(torch.cuda.is_available())" "Cannot find torch DIR")
if (${TORCH_WITH_CUDA})
    message(STATUS TORCH_WITH_CUDA: ${TORCH_WITH_CUDA})
    set(TORCH_LIBRARIES ${TORCH_LIBRARIES} ${Torch_DIR}/lib/libc10_cuda.so ${Torch_DIR}/lib/libtorch_cuda.so)
endif()

target_link_options(_slime_torch PRIVATE "-Wl,--no-as-needed")
target_link_libraries(
    _slime_torch
    PRIVATE
    ${TORCH_LIBRARIES}
    ${ENGINE_LIBRARIES}
)

set_target_properties(
    _slime_torch
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN};${Torch_DIR}/lib"
)

install(
    TARGETS
    _slime_torch
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
