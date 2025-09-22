# Python Wrapper
slime_option(USE_MACA "USE in MACA Platform" OFF)

if (USE_GLOO_BACKEND)
    set(BACKEND_SRC gloo_backend.cpp)
    set(ENGINE_LIBRARIES _slime_gloo)
else ()
    set(BACKEND_SRC slime_backend.cpp)
    set(ENGINE_LIBRARIES _slime_engine _slime_rdma)
endif ()

pybind11_add_module(
    _slime_torch
    ${BACKEND_SRC}
)

target_include_directories(_slime_torch PRIVATE ${TORCH_INCLUDE_DIRS})

target_compile_definitions(_slime_torch PRIVATE "-DTORCH_API_INCLUDE_EXTENSION_H")
target_compile_definitions(_slime_torch PRIVATE "-DTORCH_EXTENSION_NAME=_slime_torch")

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
