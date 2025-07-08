#include "engine/rdma/rdma_endpoint.cpp"

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

DEFINE_string(PEER_ADDR, "192.168.254.128", "peer IP address");
DEFINE_int32(PORT_DATA, 5557, "ZMQ control port");
DEFINE_int32(PORT_MRCN, 5558, "ZMQ control port");

DEFINE_uint64(BLOCK_SIZE, 4096, "block size");
DEFINE_uint64(BATCH_SIZE, 256, "batch size");

DEFINE_uint64(DURATION, 10, "duration (s)");



int main(int argc, char** argv)
{   

    auto send_ep = std::make_shared<RDMAEndpoint>();
    //std::shared_ptr<RDMAEndpoint> send_ep = std::make_shared<RDMAEndpoint>();  

    std::cout<<"Init... "<<std::endl;
    send_ep->init(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE);

    const size_t buf_size = FLAGS_BATCH_SIZE * FLAGS_BLOCK_SIZE;
    void *buf = malloc(buf_size);
    memset(buf, 0xAA, buf_size);


    std::cout<<"register memory region"<<std::endl;
    send_ep->data_ctx->register_memory_region("KEY", (uintptr_t) buf, buf_size);


    zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_mr(2);

    zmq::socket_t  sock_data(zmq_ctx_data, ZMQ_REQ);
    zmq::socket_t  sock_mr(zmq_ctx_mr, ZMQ_REQ);

    sock_data.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_DATA));
    sock_mr.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_MRCN));


    zmq::message_t EP_DATA(send_ep->data_ctx->endpoint_info().dump());
    zmq::message_t EP_MR(send_ep->mem_region_ctx->endpoint_info().dump());

    sock_data.send(EP_DATA, zmq::send_flags::none);

    zmq::message_t PEER_EP_DATA;


    if (auto recv_result = sock_data.recv(PEER_EP_DATA); !recv_result) 
    {
        std::cerr << "Failed to receive message: " << std::endl;
        return -1;
    }


    sock_mr.send(EP_MR, zmq::send_flags::none);
    zmq::message_t PEER_EP_MR;
    if (auto recv_result = sock_mr.recv(PEER_EP_MR); !recv_result) 
    {
        std::cerr << "Failed to receive message: " << std::endl;
        return -1;
    }


    send_ep->connect(json::parse(PEER_EP_DATA.to_string()), json::parse(PEER_EP_MR.to_string()));

    auto s_time = std::chrono::steady_clock::now();
    uint64_t total = 0;
    std::cout<<"等待发送"<<std::endl;
    int cnt = 0;
    //while(std::chrono::steady_clock::now() - s_time < std::chrono::seconds(FLAGS_DURATION))
    while(cnt <= 10)
    {
        AssignmentBatch send_batch;
        for (int n = 0; n < FLAGS_BATCH_SIZE; n++)
            send_batch.push_back(Assignment("KEY", n * FLAGS_BLOCK_SIZE, n * FLAGS_BLOCK_SIZE, FLAGS_BLOCK_SIZE));
            
        send_ep->send(send_batch);
        total += FLAGS_BATCH_SIZE * FLAGS_BLOCK_SIZE;
        cnt += 1;
    }


    auto duration = std::chrono::duration<double>(std::chrono::steady_clock::now() - s_time).count();
    std::cout << "吞吐量: " 
              << total / duration / (1 << 20) << " MB/s\n";


    free(buf);

    return 0;
}