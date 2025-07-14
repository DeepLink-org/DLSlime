#include "engine/rdma/rdma_context.h"

#include <gflags/gflags.h>
#include <zmq.hpp>
#include <chrono>
#include <thread>
#include <cstdlib>


using json = nlohmann::json;
using namespace slime;

DEFINE_string(DEVICE_NAME, "rxe_0", "RDMA device name");
DEFINE_string(LINK_TYPE, "RoCE", "IB or RoCE");
DEFINE_int32(IB_PORT, 1, "RDMA port number");
DEFINE_int32(PORT, 5555, "ZMQ control port");
DEFINE_int32(BATCH_SIZE, 64, "Batch size for RDMA operations");
DEFINE_int32(BLOCK_SIZE, 4096, "Block size in bytes");


int main(int argc, char** argv)
{
     

    RDMAContext RDMA_ctx;
    RDMA_ctx.init(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE, 16);

    // 注册内存
    const size_t buf_size = FLAGS_BATCH_SIZE * FLAGS_BLOCK_SIZE;
    void *buf = malloc(buf_size);
    memset(buf, 0, buf_size);

    RDMA_ctx.register_memory_region("KEY", (uintptr_t) buf, buf_size);
    std::cout<<"连接成功："<<std::endl;
    // 控制面连接(走TCP)
    zmq::context_t zmq_ctx(1);
    zmq::socket_t  sock(zmq_ctx, ZMQ_REP);
    //sock.connect("tcp://192.168.247.128" ":" + std::to_string(FLAGS_PORT));
    sock.bind("tcp://*:" + std::to_string(FLAGS_PORT));
    std::cout<<"连接成功："<<std::endl;
    // 交换QP信息
    zmq::message_t PEER_INFO;
    if (auto recv_result = sock.recv(PEER_INFO); !recv_result) {
    std::cerr << "Failed to receive message: " << std::endl;
    return -1;}
    std::cout<<"连接成功："<<std::endl;
    zmq::message_t LOCAL_INFO(RDMA_ctx.endpoint_info().dump());
    sock.send(LOCAL_INFO, zmq::send_flags::none);
    RDMA_ctx.connect(json::parse(PEER_INFO.to_string()));
    std::cout<<"连接成功："<<std::endl;
    // 启动进程
    RDMA_ctx.launch_future();
    std::cout<<"开始等待接收："<<std::endl;
    while (1)
    {
        AssignmentBatch recv_batch;
        for (int n = 0; n < FLAGS_BATCH_SIZE; n++)
            recv_batch.push_back(Assignment("KEY", n * FLAGS_BLOCK_SIZE, n * FLAGS_BLOCK_SIZE, FLAGS_BLOCK_SIZE));

        auto RDMA_atx = RDMA_ctx.submit(OpCode::RECV, recv_batch);
        RDMA_atx->wait();

        for (int n = 0; n < FLAGS_BLOCK_SIZE; n++) {
            if (((uint8_t*)buf)[n] != 0xAA) {
                std::cout << "Data verification failed" << "\n";
                break;
            }
        }
        std::cout<<"数据验证成功："<<std::endl;
        memset(buf, 0, buf_size);
    }
    std::cout<<"结束接收："<<std::endl;
    RDMA_ctx.stop_future();
    free(buf);

    return 0;
}
