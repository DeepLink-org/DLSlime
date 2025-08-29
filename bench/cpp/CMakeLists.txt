add_executable(
    transfer_bench
    transfer_bench.cpp
)

target_include_directories(transfer_bench PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    transfer_bench
    PUBLIC
    _slime_engine _slime_rdma gflags zmq
)

add_executable(
    scheduler_bench
    scheduler_bench.cpp
)

target_include_directories(scheduler_bench PUBLIC ${ZeroMQ_INCLUDE_DIRS})

target_link_libraries(
    scheduler_bench
    PUBLIC
    _slime_engine _slime_rdma gflags zmq numa
)
