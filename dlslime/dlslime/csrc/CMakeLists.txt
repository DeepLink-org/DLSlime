add_subdirectory(observability)
add_subdirectory(device)
add_subdirectory(engine)

add_library(
    _slime_topology
    SHARED
    topology.cpp
)

set_target_properties(
    _slime_topology
    PROPERTIES
    BUILD_WITH_INSTALL_RPATH TRUE
    INSTALL_RPATH "\${ORIGIN}"
)

target_include_directories(_slime_topology PUBLIC
    $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/..>
    $<INSTALL_INTERFACE:include/dlslime>
)

add_library(dlslime INTERFACE)
target_link_libraries(dlslime INTERFACE _slime_engine _slime_device _slime_topology _slime_obs)

if(BUILD_RDMA)
    target_link_libraries(dlslime INTERFACE _slime_rdma)
endif()

if(BUILD_TCP)
    target_link_libraries(dlslime INTERFACE _slime_tcp)
endif()

# rpc/ has no independent C++ consumers (the session API is all
# pybind11). It is compiled straight into _slime_c.so by the python
# subdirectory below. Keep the source files here as a marker so future
# C++-only consumers can opt in by adding rpc_session.cpp to their
# target_sources, but DO NOT build a separate shared lib -- it would
# need libpython at link time and we cannot guarantee the right
# version is on LD_LIBRARY_PATH at install time.

install(
    TARGETS dlslime
    EXPORT dlslimeTargets
)

install(
    TARGETS
    _slime_topology
    EXPORT dlslimeTargets
    LIBRARY DESTINATION ${DLSLIME_INSTALL_PATH}
)

install(
    FILES topology.h
    DESTINATION include/dlslime/csrc
)

if(BUILD_PYTHON)
    add_subdirectory(python)
endif()

if(BUILD_TORCH_PLUGIN)
    add_subdirectory(torch)
endif()
