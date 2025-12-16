add_executable(
    send_test
    send_test.cpp
)

target_include_directories(send_test PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    send_test
    PUBLIC
    _slime_engine _slime_rdma gflags zmq pthread
)


add_executable(
    recv_test
    recv_test.cpp
)

target_include_directories(recv_test PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    recv_test
    PUBLIC
    _slime_engine _slime_rdma gflags zmq pthread
)



if (BUILD_ASCEND_DIRECT)
    add_subdirectory(ascend_direct)
endif()
