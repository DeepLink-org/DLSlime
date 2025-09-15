add_executable(
    sendrecv
    sendrecv.cpp
)

target_include_directories(sendrecv PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    sendrecv
    PUBLIC
    _slime_engine _slime_rdma gflags zmq pthread
)

if (BUILD_ASCEND_DIRECT)
    add_subdirectory(ascend_direct)
endif()
