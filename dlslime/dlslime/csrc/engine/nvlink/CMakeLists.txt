set(CMAKE_VERBOSE_MAKEFILE ON)

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -O3 -fPIC")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -fPIC")
set(CUDA_SEPARABLE_COMPILATION ON)

add_library(
    _slime_nvlink
    SHARED
    memory_pool.cpp
    nvlink_endpoint.cpp
)

target_link_libraries(_slime_nvlink PUBLIC CUDA::cudart)

set_target_properties(
    _slime_nvlink
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_nvlink
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
