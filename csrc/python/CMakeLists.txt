# Python Wrapper
set(PYBIND11_FINDPYTHON ON)
run_python("pybind11_DIR" "import pybind11; print(pybind11.__file__.rsplit(\"/\", 1)[0])" "Cannot find pybind11 DIR")
find_package(pybind11 REQUIRED PATHS ${pybind11_DIR})

set(PYTHON_SUPPORTED_VERSIONS "3.8" "3.9" "3.10" "3.11" "3.12")

pybind11_add_module(
    _slime_c
    bind.cpp
)

set(_slime_c_link_libraries
    _slime_rdma
    _slime_gloo
    _slime_gloo_common
    _slime_gloo_rendezvous
    _slime_gloo_transport
    _slime_gloo_transport_ibverbs
)

if (BUILD_NVLINK)
    target_compile_definitions(_slime_c PRIVATE -DBUILD_NVLINK)
    list(APPEND _slime_c_link_libraries _slime_nvlink CUDA::cudart)
endif()

target_link_libraries(_slime_c PRIVATE ${_slime_c_link_libraries})

set_target_properties(
    _slime_c
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_c
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
