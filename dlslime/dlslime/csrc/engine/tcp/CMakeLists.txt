# asio is header-only.  Try find_package, fall back to manual detection.
find_package(asio QUIET)
if(NOT asio_FOUND)
    if(EXISTS /usr/include/asio.hpp)
        add_library(asio::asio INTERFACE IMPORTED)
        target_include_directories(asio::asio INTERFACE /usr/include)
    elseif(EXISTS /usr/include/boost/asio.hpp)
        add_library(asio::asio INTERFACE IMPORTED)
        target_include_directories(asio::asio INTERFACE /usr/include/boost)
        target_compile_definitions(asio::asio INTERFACE ASIO_STANDALONE)
    else()
        message(FATAL_ERROR "asio not found. Install libasio-dev or boost.")
    endif()
endif()

add_library(_slime_tcp SHARED
    tcp_memory_pool.cpp
    tcp_connection_pool.cpp
    tcp_context.cpp
    tcp_session.cpp
    tcp_endpoint.cpp
)

target_compile_definitions(_slime_tcp PRIVATE ASIO_STANDALONE)

# Workaround: asio awaitable.hpp uses std::exchange without including <utility>
target_compile_options(_slime_tcp PRIVATE -include utility)

if (USE_CUDA)
    find_package(CUDAToolkit REQUIRED)
    target_compile_definitions(_slime_tcp PRIVATE USE_CUDA)
    target_include_directories(_slime_tcp PRIVATE ${CUDAToolkit_INCLUDE_DIRS})
    target_link_libraries(_slime_tcp PUBLIC CUDA::cudart)
endif()

target_link_libraries(_slime_tcp PUBLIC
    asio::asio
    _slime_device
    _slime_engine
)

set_target_properties(_slime_tcp PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "${ORIGIN}"
)

install(TARGETS _slime_tcp
    EXPORT dlslimeTargets
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)
