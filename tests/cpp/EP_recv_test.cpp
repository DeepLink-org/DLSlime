#include "engine/rdma/rdma_endpoint.cpp"

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
DEFINE_int32(PORT_DATA, 5557, "ZMQ control port");
DEFINE_int32(PORT_MRCN, 5558, "ZMQ control port");
DEFINE_int32(BATCH_SIZE, 256, "Batch size for RDMA operations");
DEFINE_int32(BLOCK_SIZE, 4096, "Block size in bytes");



int main(int argc, char** argv)
{
    
    auto recv_ep = std::make_shared<RDMAEndpoint>();


    std::cout<<"Init... "<<std::endl;
    recv_ep->init(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE);
    std::cout<<"Init buffer... "<<std::endl;
    const size_t buf_size = FLAGS_BATCH_SIZE * FLAGS_BLOCK_SIZE;
    void *buf = malloc(buf_size);
    memset(buf, 0, buf_size);

    std::cout<<"register memory region"<<std::endl;
    recv_ep->data_ctx->register_memory_region("KEY", (uintptr_t) buf, buf_size);

    zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_mr(2);


    zmq::socket_t  sock_data(zmq_ctx_data, ZMQ_REP);
    zmq::socket_t  sock_mr(zmq_ctx_mr, ZMQ_REP);

    sock_data.bind("tcp://*:" + std::to_string(FLAGS_PORT_DATA));
    sock_mr.bind("tcp://*:" + std::to_string(FLAGS_PORT_MRCN));



    zmq::message_t EP_DATA;
    if (auto recv_result = sock_data.recv(EP_DATA); !recv_result) 
    {
        std::cerr << "Failed to receive message: " << std::endl;
        return -1;
    }
    zmq::message_t LOCAL_DATA_INFO(recv_ep->data_ctx->endpoint_info().dump());
    sock_data.send(LOCAL_DATA_INFO, zmq::send_flags::none);

    zmq::message_t EP_MR;
    if (auto recv_result = sock_mr.recv(EP_MR); !recv_result) 
    {
        std::cerr << "Failed to receive message: " << std::endl;
        return -1;
    }

    

    zmq::message_t LOCAL_MR_INFO(recv_ep->mem_region_ctx->endpoint_info().dump());
    sock_mr.send(LOCAL_MR_INFO, zmq::send_flags::none);
    recv_ep->connect(json::parse(EP_DATA.to_string()), json::parse(EP_MR.to_string()));

    int cnt = 0;

    std::cout<<"接待接收"<<std::endl;
    while (cnt <= 10)
    {
        AssignmentBatch recv_batch;
        for (int n = 0; n < FLAGS_BATCH_SIZE; n++)
            recv_batch.push_back(Assignment("KEY", n * FLAGS_BLOCK_SIZE, n * FLAGS_BLOCK_SIZE, FLAGS_BLOCK_SIZE));


        recv_ep->recv(recv_batch);
        int data_cnt = 0;
        for (int n = 0; n < FLAGS_BLOCK_SIZE; n++) {
            if (((uint8_t*)buf)[n] != 0xAA) {
                //std::cout << "Data verification failed" << "\n";
                //break;
                data_cnt += 1;
            }
            else {
               //  std::cout<<"数据验证成功："<<std::endl;
            }
        }

        if (data_cnt == 0)
            std::cout<<"数据验证成功："<<std::endl;
        memset(buf, 0, buf_size); 
        cnt += 1;

    }


    std::cout<<"结束接收："<<std::endl;
    //recv_ep->disconnect();
    recv_ep->data_ctx->stop_future();
    recv_ep->mem_region_ctx->stop_future();
    free(buf);




    return 0;
}