# add_executable(
#     send_test
#     send_test.cpp
# )

# target_include_directories(send_test PUBLIC ${ZeroMQ_INCLUDE_DIRS})

# target_link_libraries(
#     send_test
#     PUBLIC
#     _slime_engine _slime_rdma gflags zmq
# )


# add_executable(
#     recv_test
#     recv_test.cpp
# )

# target_include_directories(recv_test PUBLIC ${ZeroMQ_INCLUDE_DIRS})

# target_link_libraries(
#     recv_test
#     PUBLIC
#     _slime_engine _slime_rdma gflags zmq
# )


add_executable(
    sendrecv
    sendrecv.cpp
)

target_include_directories(sendrecv PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    sendrecv
    PUBLIC
    ${SLIME_LIB} pthread
    _slime_engine _slime_rdma gflags zmq
)