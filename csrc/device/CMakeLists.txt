set(DEVICE_SRCS "")
set(DEVICE_LIBS "")

if (USE_CUDA)
    message(STATUS "Building DLSlime with CUDA Backend")
    enable_language(CUDA)
    set(CMAKE_CUDA_STANDARD 17)
    set(CMAKE_CUDA_ARCHITECTURES "90")
    find_package(CUDAToolkit REQUIRED)
    include_directories(${CUDAToolkit_INCLUDE_DIRS})
    list(APPEND DEVICE_SRCS cuda/device_cuda.cpp)
    list(APPEND DEVICE_LIBS CUDA::cudart CUDA::cuda_driver cuda)
    add_compile_definitions("SLIME_USE_CUDA")

if (USE_MACA)
    message(STATUS "Building DLSlime under meta platform.")
    add_compile_definitions("USE_MACA")
    add_compile_definitions("SLIME_USE_MACA")
endif()

elseif (USE_ASCEND)
    message(STATUS "Building DLSlime with Ascend Backend")
    list(APPEND DEVICE_SRCS ascend/device_ascend.cpp)

elseif (USE_CAMB)
    message(STATUS "Building DLSlime with Ascend Backend")
    list(APPEND DEVICE_SRCS ascend/device_camb.cpp)

else()
    message(STATUS "Building DLSlime with CPU Backend")
    list(APPEND DEVICE_SRCS cpu/device_cpu.cpp)
endif()

add_library(
    _slime_device
    SHARED
    ${DEVICE_SRCS}
)

target_link_libraries(_slime_device PRIVATE ${DEVICE_LIBS})

set_target_properties(
    _slime_device
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

install(
    TARGETS
    _slime_device
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
