add_executable(
    EP_send_test
    EP_send_test.cpp
)

target_include_directories(EP_send_test PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    EP_send_test
    PUBLIC
    _slime_engine _slime_rdma gflags zmq
)


add_executable(
    EP_recv_test
    EP_recv_test.cpp
)

target_include_directories(EP_recv_test PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    EP_recv_test
    PUBLIC
    _slime_engine _slime_rdma gflags zmq
)
