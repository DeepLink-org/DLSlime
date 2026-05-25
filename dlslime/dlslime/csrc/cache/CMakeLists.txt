# DLSlimeCache sources.
#
# V0 is in-process pure C++/STL with no RDMA dependency. Sources are
# folded into the python extension `_slime_c` rather than built as a
# standalone shared lib — the bindings need pybind11 / libpython at
# link time and we cannot guarantee the right libpython is on
# LD_LIBRARY_PATH at install time. See ../python/CMakeLists.txt for
# the actual compilation site.
#
# This file exists so that the layout matches the rest of csrc/ and so
# future C++-only consumers (e.g., a standalone gRPC cache server in
# V1) have an opt-in target to add their sources to.
#
# To use the cache from C++ in another target:
#
#     target_sources(<your_target> PRIVATE
#         ${CMAKE_CURRENT_SOURCE_DIR}/cache/cache_server.cpp)
#     target_include_directories(<your_target> PRIVATE
#         ${CMAKE_CURRENT_SOURCE_DIR}/..)
