# Python Wrapper
set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 REQUIRED)

set(PYTHON_SUPPORTED_VERSIONS "3.8" "3.9" "3.10" "3.11" "3.12")

pybind11_add_module(
    _slime_torch
    backend.cpp
)

find_package(Torch REQUIRED)
find_library(TORCH_PYTHON_LIBRARY torch_python PATHS "${TORCH_INSTALL_PREFIX}/lib")

target_include_directories(_slime_torch PRIVATE ${TORCH_INCLUDE_DIRS})

target_compile_definitions(_slime_torch PRIVATE
    "-DTORCH_API_INCLUDE_EXTENSION_H"
)
target_compile_definitions(_slime_torch PRIVATE
    "-DTORCH_EXTENSION_NAME=_slime_torch"
)
target_compile_definitions(_slime_torch PRIVATE
    "-DPYBIND11_BUILD_ABI=\"_cxxabi1011\""
)

target_link_libraries(
    _slime_torch
    PRIVATE
    ${TORCH_PYTHON_LIBRARY}
    ${TORCH_LIBRARIES}
    _slime_gloo
    _slime_gloo_common
    _slime_gloo_rendezvous
    _slime_gloo_transport
    _slime_gloo_transport_ibverbs
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
