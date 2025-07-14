#include "engine/rdma/rdma_context.h"

#include <gflags/gflags.h>
#include <zmq.hpp>
#include <chrono>
#include <thread>
#include <cstdlib>



using json = nlohmann::json;
using namespace slime;


DEFINE_string(DEVICE_NAME, "rxe_0", "device name");
DEFINE_uint32(IB_PORT, 1, "device name");
DEFINE_string(LINK_TYPE, "RoCE", "IB or RoCE");

DEFINE_string(PEER_ADDR, "192.168.247.128", "peer IP address");
DEFINE_int32(PORT, 5555, "ZMQ port");

DEFINE_uint64(BLOCK_SIZE, 4096, "block size");
DEFINE_uint64(BATCH_SIZE, 64, "batch size");

DEFINE_uint64(DURATION, 10, "duration (s)");




int main(int argc, char** argv)
{
    // 初始化RDMA上下文(设备信息)
    RDMAContext RDMA_ctx;
    RDMA_ctx.init(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE);

    // 注册内存
    const size_t buf_size = FLAGS_BATCH_SIZE * FLAGS_BLOCK_SIZE;
    void *buf = malloc(buf_size);
    memset(buf, 0xAA, buf_size);

    RDMA_ctx.register_memory_region("KEY", (uintptr_t) buf, buf_size);

    // 控制面连接(走TCP)
    zmq::context_t zmq_ctx(1);
    zmq::socket_t  sock(zmq_ctx, ZMQ_REQ);
    sock.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT));


    // 使用zmq交换RDMA信息
    zmq::message_t LOCAL_INFO(RDMA_ctx.endpoint_info().dump());
    sock.send(LOCAL_INFO, zmq::send_flags::none);

    zmq::message_t PEER_INFO;
    if (auto recv_result = sock.recv(PEER_INFO); !recv_result) {
    std::cerr << "Failed to receive message: " << std::endl;
    return -1;}
    RDMA_ctx.connect(json::parse(PEER_INFO.to_string()));


    // 启动进程
    RDMA_ctx.launch_future();

    auto s_time = std::chrono::steady_clock::now();
    uint64_t total = 0;

    while(std::chrono::steady_clock::now() - s_time < std::chrono::seconds(FLAGS_DURATION))
    {
        AssignmentBatch send_batch;
        for (int n = 0; n < FLAGS_BATCH_SIZE; n++)
            send_batch.push_back(Assignment("KEY", n * FLAGS_BLOCK_SIZE, n * FLAGS_BLOCK_SIZE, FLAGS_BLOCK_SIZE));

        auto RDMA_atx = RDMA_ctx.submit(OpCode::SEND, send_batch);
        RDMA_atx->wait();
        total += FLAGS_BATCH_SIZE * FLAGS_BLOCK_SIZE;
    }


    auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - s_time).count();
    std::cout << "吞吐量: "
              << total / duration / (1 << 20) << " MB/s\n";

    RDMA_ctx.stop_future();
    free(buf);

    return 0;
}
