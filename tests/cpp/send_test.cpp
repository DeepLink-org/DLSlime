#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_endpoint.h"
#include <chrono>
#include <cstdlib>
#include <gflags/gflags.h>
#include <memory>
#include <thread>
#include <zmq.hpp>

using json = nlohmann::json;
using namespace slime;

DEFINE_string(DEVICE_NAME, "rxe_0", "RDMA device name");
DEFINE_string(LINK_TYPE, "RoCE", "IB or RoCE");
DEFINE_int32(IB_PORT, 1, "RDMA port number");
DEFINE_int32(PORT_DATA, 5557, "ZMQ DATA port");
DEFINE_int32(PORT_META, 5558, "ZMQ META port");

int main(int argc, char** argv)
{

    std::cout << "Init the RMDA ENDPOINT OF SEND... " << std::endl;
    // Construct the end_point
    auto end_point = std::make_shared<RDMAEndpoint>(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE, 1);

    std::cout << "RDMA QP INFO VIA TCP... " << std::endl;
    // RDMA control plane via TCP
    zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_meta(2);

    zmq::socket_t sock_data(zmq_ctx_data, ZMQ_REP);
    zmq::socket_t sock_meta(zmq_ctx_meta, ZMQ_REP);

    sock_data.bind("tcp://*:" + std::to_string(FLAGS_PORT_DATA));
    sock_meta.bind("tcp://*:" + std::to_string(FLAGS_PORT_META));

    zmq::message_t data_channel_info;
    zmq::message_t meta_channel_info;
    auto           data_channel_info_res = sock_data.recv(data_channel_info);
    auto           meta_channel_info_res = sock_meta.recv(meta_channel_info);

    std::cout << "Send the RDMA Info to other side..." << std::endl;
    zmq::message_t local_data_channel_info(end_point->dataCtxInfo().dump());
    zmq::message_t local_meta_channel_info(end_point->metaCtxInfo().dump());

    sock_data.send(local_data_channel_info, zmq::send_flags::none);
    sock_meta.send(local_meta_channel_info, zmq::send_flags::none);

    end_point->connect(json::parse(data_channel_info.to_string()), json::parse(meta_channel_info.to_string()));

    std::cout << "Connect Success..." << std::endl;
    std::cout << "Finish the connection of QP, start to SEND of buf_0 and buf_1..." << std::endl;

    const uint32_t    batch_size_buf_0 = 1;
    std::vector<char> data_buf_0(8192, '0');

    uintptr_t ptrs_buf_0       = reinterpret_cast<uintptr_t>(data_buf_0.data());
    size_t    data_sizes_buf_0 = data_buf_0.size();
    size_t    offset_buf_0     = 0;

    const uint32_t    batch_size_buf_1 = 1;
    std::vector<char> data_buf_1(8192, '1');

    uintptr_t ptrs_buf_1       = reinterpret_cast<uintptr_t>(data_buf_1.data());
    size_t    data_sizes_buf_1 = data_buf_1.size();
    size_t    offset_buf_1     = 0;

    const uint32_t    batch_size_buf_2 = 1;
    std::vector<char> data_buf_2(8192, '2');

    uintptr_t ptrs_buf_2       = reinterpret_cast<uintptr_t>(data_buf_2.data());
    size_t    data_sizes_buf_2 = data_buf_2.size();
    size_t    offset_buf_2     = 0;

    const uint32_t    batch_size_buf_3 = 3;
    std::vector<char> data_buf_3(8192, '3');

    uintptr_t ptrs_buf_3       = reinterpret_cast<uintptr_t>(data_buf_3.data());
    size_t    data_sizes_buf_3 = data_buf_3.size();
    size_t    offset_buf_3     = 0;

    const uint32_t    batch_size_buf_4 = 4;
    std::vector<char> data_buf_4(8192, '4');

    uintptr_t ptrs_buf_4       = reinterpret_cast<uintptr_t>(data_buf_4.data());
    size_t    data_sizes_buf_4 = data_buf_4.size();
    size_t    offset_buf_4     = 0;

    const uint32_t    batch_size_buf_5 = 5;
    std::vector<char> data_buf_5(8192, '5');

    uintptr_t ptrs_buf_5       = reinterpret_cast<uintptr_t>(data_buf_5.data());
    size_t    data_sizes_buf_5 = data_buf_5.size();
    size_t    offset_buf_5     = 0;

    const uint32_t    batch_size_buf_6 = 5;
    std::vector<char> data_buf_6(8192, '6');

    uintptr_t ptrs_buf_6       = reinterpret_cast<uintptr_t>(data_buf_6.data());
    size_t    data_sizes_buf_6 = data_buf_6.size();
    size_t    offset_buf_6     = 0;

    const uint32_t    batch_size_buf_7 = 5;
    std::vector<char> data_buf_7(8192, '7');

    uintptr_t ptrs_buf_7       = reinterpret_cast<uintptr_t>(data_buf_7.data());
    size_t    data_sizes_buf_7 = data_buf_7.size();
    size_t    offset_buf_7     = 0;

    const uint32_t    batch_size_buf_8 = 5;
    std::vector<char> data_buf_8(8192, '8');

    uintptr_t ptrs_buf_8       = reinterpret_cast<uintptr_t>(data_buf_8.data());
    size_t    data_sizes_buf_8 = data_buf_8.size();
    size_t    offset_buf_8     = 0;

    // const uint32_t    batch_size_buf_1 = 2;
    // std::vector<char> data_buf_1_0(1024, '1');
    // std::vector<char> data_buf_1_1(2048, '2');

    // std::vector<uintptr_t> ptrs_buf_1       = {reinterpret_cast<uintptr_t>(data_buf_1_0.data()),
    //                                            reinterpret_cast<uintptr_t>(data_buf_1_1.data())};
    // std::vector<size_t>    data_sizes_buf_1 = {data_buf_1_0.size(), data_buf_1_1.size()};
    // std::vector<size_t>    offset_buf_1     = {0, 0};

    auto buf_0 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_0, offset_buf_0, data_sizes_buf_0);
    auto buf_1 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_1, offset_buf_1, data_sizes_buf_1);
    auto buf_2 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_2, offset_buf_2, data_sizes_buf_2);
    auto buf_3 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_3, offset_buf_3, data_sizes_buf_3);
    auto buf_4 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_4, offset_buf_4, data_sizes_buf_4);
    auto buf_5 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_5, offset_buf_5, data_sizes_buf_5);
    auto buf_6 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_6, offset_buf_6, data_sizes_buf_6);
    auto buf_7 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_7, offset_buf_7, data_sizes_buf_7);
    auto buf_8 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_8, offset_buf_8, data_sizes_buf_8);
    // auto buf_1 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_1, offset_buf_1, data_sizes_buf_1);

    std::cout << "Launch EDNPOINT ..." << std::endl;

    // buf_1->send();
    buf_0->send();
    buf_1->send();
    buf_2->send();
    buf_3->send();
    buf_4->send();
    buf_5->send();
    buf_6->send();
    buf_7->send();
    buf_8->send();
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Wait SEND Complete..." << std::endl;
    buf_0->waitSend();
    buf_1->waitSend();
    buf_2->waitSend();
    buf_3->waitSend();
    buf_4->waitSend();
    buf_5->waitSend();
    buf_6->waitSend();
    buf_7->waitSend();
    buf_8->waitSend();

    std::cout << "The SEND test completed." << std::endl;

    return 0;
}
