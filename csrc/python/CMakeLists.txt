# Python Wrapper
set(PYBIND11_FINDPYTHON ON)
run_python("pybind11_DIR" "import pybind11; print(pybind11.__file__.rsplit(\"/\", 1)[0])" "Cannot find pybind11 DIR")
find_package(pybind11 REQUIRED PATHS ${pybind11_DIR})

set(PYTHON_SUPPORTED_VERSIONS "3.8" "3.9" "3.10" "3.11" "3.12")

pybind11_add_module(
    _slime_c
    bind.cpp
)

set(_slime_c_link_libraries "")
set(BIND_INSTALL_RPATH "\${ORIGIN}")

if (BUILD_NVLINK)
    target_compile_definitions(_slime_c PRIVATE -DBUILD_NVLINK)
    list(APPEND _slime_c_link_libraries _slime_nvlink CUDA::cudart)
endif()

if (BUILD_RDMA)
    target_compile_definitions(_slime_c PRIVATE -DBUILD_RDMA)
    list(APPEND _slime_c_link_libraries _slime_rdma)
endif()

if (BUILD_NVSHMEM)
    target_compile_definitions(_slime_c PRIVATE -DBUILD_NVSHMEM)
    list(APPEND _slime_c_link_libraries _slime_nvshmem)
endif()

if (BUILD_INTRA_OPS OR BUILD_INTER_OPS)
    if (BUILD_INTRA_OPS)
        target_compile_definitions(_slime_c PRIVATE -DBUILD_INTRA_OPS)
    endif()
    if (BUILD_INTER_OPS)
        target_compile_definitions(_slime_c PRIVATE -DBUILD_INTER_OPS)
    endif()
    target_compile_definitions(_slime_c PRIVATE "-DTORCH_API_INCLUDE_EXTENSION_H")
    target_compile_definitions(_slime_c PRIVATE "-DTORCH_EXTENSION_NAME=_slime_c")
    list(APPEND _slime_c_link_libraries ${_slime_c_link_libraries} _slime_ops ${TORCH_LIBRARIES})
    set(BIND_INSTALL_RPATH "\${ORIGIN}:${Torch_DIR}/lib")
endif()

target_link_libraries(_slime_c PRIVATE ${_slime_c_link_libraries})

set_target_properties(
    _slime_c
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH ${BIND_INSTALL_RPATH}
)

install(
    TARGETS
    _slime_c
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
