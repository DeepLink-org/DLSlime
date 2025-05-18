# Python Wrapper
set(PYBIND11_FINDPYTHON ON)
find_package(pybind11 REQUIRED)

set(PYTHON_SUPPORTED_VERSIONS "3.8" "3.9" "3.10" "3.11" "3.12")

pybind11_add_module(
    _slime_c
    bind.cpp
)

if (BUILD_NVLINK)
target_compile_definitions(_slime_c PRIVATE -DBUILD_NVLINK)
target_link_libraries(_slime_c PRIVATE _slime_rdma _slime_nvlink CUDA::cudart)
else ()
target_link_libraries(_slime_c PRIVATE _slime_rdma _slime_meta)
endif()

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
