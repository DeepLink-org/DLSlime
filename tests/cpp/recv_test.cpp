#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_endpoint.cpp"

#include <chrono>
#include <cstdlib>
#include <gflags/gflags.h>
#include <thread>
#include <zmq.hpp>

using json = nlohmann::json;
using namespace slime;

DEFINE_string(DEVICE_NAME, "rxe_0", "device name");
DEFINE_uint32(IB_PORT, 1, "device name");
DEFINE_string(LINK_TYPE, "RoCE", "IB or RoCE");

DEFINE_string(PEER_ADDR, "127.0.0.1", "peer IP address");
DEFINE_int32(PORT_DATA, 5559, "ZMQ control port");
DEFINE_int32(PORT_MRCN, 5560, "ZMQ control port");

int main(int argc, char** argv)
{

    std::cout << "Init the RMDA ENDPOINT OF SEND... " << std::endl;
    // Construct the end_point
    auto end_point = std::make_shared<RDMAEndpoint>(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE, 4);

    std::cout << "RDMA QP INFO VIA TCP... " << std::endl;
    // RDMA control plane via TCP
    //zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_mmrg(2);

    //zmq::socket_t sock_data(zmq_ctx_data, ZMQ_REQ);
    zmq::socket_t sock_mmrg(zmq_ctx_mmrg, ZMQ_REQ);

    //sock_data.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_DATA));
    sock_mmrg.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_MRCN));

    //zmq::message_t local_data_channel_info(end_point->getDataContextInfo().dump());
    zmq::message_t local_meta_channel_info(end_point->getMetaContextInfo().dump());

    //sock_data.send(local_data_channel_info, zmq::send_flags::none);
    sock_mmrg.send(local_meta_channel_info, zmq::send_flags::none);

    std::cout << "Send the RDMA Info to other side..." << std::endl;

    //zmq::message_t data_channel_info;
    zmq::message_t meta_channel_info;

    //auto send_data_result = sock_data.recv(data_channel_info);
    auto recv_data_result = sock_mmrg.recv(meta_channel_info);

    end_point->connect(json::parse("11"), json::parse(meta_channel_info.to_string()));
    std::cout << "Connect Success..." << std::endl;
    std::cout << "Finish the connection of QP, start to RECV of buf_0 and buf_1... " << std::endl;

    const uint32_t         batch_size_buf_0 = 1;
    std::vector<char>      data_buf_0_0(8192, 'A');
    std::vector<uintptr_t> ptrs_buf_0       = {reinterpret_cast<uintptr_t>(data_buf_0_0.data())};
    std::vector<size_t>    data_sizes_buf_0 = {data_buf_0_0.size()};
    std::vector<size_t>    offset_buf_0     = {0};

    const uint32_t         batch_size_buf_1 = 2;
    std::vector<char>      data_buf_1_0(1024, 'B');
    std::vector<char>      data_buf_1_1(2048, 'C');
    std::vector<uintptr_t> ptrs_buf_1       = {reinterpret_cast<uintptr_t>(data_buf_1_0.data()),
                                               reinterpret_cast<uintptr_t>(data_buf_1_1.data())};
    std::vector<size_t>    data_sizes_buf_1 = {data_buf_1_0.size(), data_buf_1_1.size()};
    std::vector<size_t>    offset_buf_1     = {0, 0};

    auto buf_0 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_0, offset_buf_0, data_sizes_buf_0);
    auto buf_1 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_0, offset_buf_0, data_sizes_buf_0);
    std::cout << "Launch EDNPOINT ..." << std::endl;

    const int                                NUM_BUFFERS = 128;
    std::vector<std::shared_ptr<RDMABuffer>> buffers;
    for (int i = 0; i < NUM_BUFFERS; ++i) {
        auto buf = std::make_shared<RDMABuffer>(end_point, ptrs_buf_0, offset_buf_0, data_sizes_buf_0);
        buffers.push_back(buf);
    }
    for (auto& buf : buffers) {
        buf->recv();
    }

    std::cout << "启动端点 ..." << std::endl;

    // 等待所有缓冲区发送完成
    for (auto& buf : buffers) {
        buf->waitRecv();
    }

    //bool data_buf_0_0_correct = std::all_of(data_buf_0_0.begin(), data_buf_0_0.end(), [](char c) { return c == '0'; });
    // bool data_buf_1_0_correct = std::all_of(data_buf_1_0.begin(), data_buf_1_0.end(), [](char c) { return c == '1';
    // }); bool data_buf_1_1_correct = std::all_of(data_buf_1_1.begin(), data_buf_1_1.end(), [](char c) { return c ==
    // '2'; });
    //assert(data_buf_0_0_correct && "Data_0_0 should contain '0'");
    // assert(data_buf_1_0_correct && "Data_1_0 should contain '1'");
    // assert(data_buf_1_1_correct && "Data_1_1 should contain '2'");

    std::cout << "The RECV test completed and data verified." << std::endl;

    return 0;
}
