#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_endpoint_v0.cpp"

#include <chrono>
#include <cstdlib>
#include <gflags/gflags.h>
#include <thread>
#include <zmq.hpp>

using json = nlohmann::json;
using namespace slime;

DEFINE_string(DEVICE_NAME, "rxe_0", "RDMA device name");
DEFINE_string(LINK_TYPE, "RoCE", "IB or RoCE");
DEFINE_int32(IB_PORT, 1, "RDMA port number");
DEFINE_int32(PORT_DATA, 5557, "ZMQ DATA port");
DEFINE_int32(PORT_META, 5558, "ZMQ META port");
DEFINE_string(PEER_ADDR, "127.0.0.1", "peer IP address");

int main(int argc, char** argv)
{

    std::cout << "Init the RMDA ENDPOINT OF RECV... " << std::endl;
    auto end_point = std::make_shared<RDMAEndpointV0>(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE, 1);

    std::cout << "RDMA QP INFO VIA TCP... " << std::endl;
    zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_meta(2);

    zmq::socket_t sock_data(zmq_ctx_data, ZMQ_REQ);
    zmq::socket_t sock_meta(zmq_ctx_meta, ZMQ_REQ);

    sock_data.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_DATA));
    sock_meta.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_META));

    zmq::message_t local_data_channel_info(end_point->dataCtxInfo().dump());
    zmq::message_t local_meta_channel_info(end_point->metaCtxInfo().dump());

    sock_data.send(local_data_channel_info, zmq::send_flags::none);
    sock_meta.send(local_meta_channel_info, zmq::send_flags::none);

    std::cout << "Send the RDMA Info to other side..." << std::endl;

    zmq::message_t data_channel_info;
    zmq::message_t meta_channel_info;

    auto send_data_result = sock_data.recv(data_channel_info);
    auto recv_data_result = sock_meta.recv(meta_channel_info);

    end_point->connect(json::parse(data_channel_info.to_string()), json::parse(meta_channel_info.to_string()));
    std::cout << "Connect Success..." << std::endl;
    std::cout << "Finish the connection of QP, start to RECV of buf_0 and buf_1... " << std::endl;

    const uint32_t    batch_size_buf_0 = 1;
    std::vector<char> data_buf_0(8192, 'A');
    uintptr_t         ptrs_buf_0       = reinterpret_cast<uintptr_t>(data_buf_0.data());
    size_t            data_sizes_buf_0 = data_buf_0.size();
    size_t            offset_buf_0     = 0;

    const uint32_t    batch_size_buf_1 = 1;
    std::vector<char> data_buf_1(8192, 'B');
    uintptr_t         ptrs_buf_1       = reinterpret_cast<uintptr_t>(data_buf_1.data());
    size_t            data_sizes_buf_1 = data_buf_1.size();
    size_t            offset_buf_1     = 0;

    const uint32_t    batch_size_buf_2 = 1;
    std::vector<char> data_buf_2(8192, 'C');
    uintptr_t         ptrs_buf_2       = reinterpret_cast<uintptr_t>(data_buf_2.data());
    size_t            data_sizes_buf_2 = data_buf_2.size();
    size_t            offset_buf_2     = 0;

    const uint32_t    batch_size_buf_3 = 1;
    std::vector<char> data_buf_3(8192, 'D');
    uintptr_t         ptrs_buf_3       = reinterpret_cast<uintptr_t>(data_buf_3.data());
    size_t            data_sizes_buf_3 = data_buf_3.size();
    size_t            offset_buf_3     = 0;

    const uint32_t    batch_size_buf_4 = 1;
    std::vector<char> data_buf_4(8192, 'E');
    uintptr_t         ptrs_buf_4       = reinterpret_cast<uintptr_t>(data_buf_4.data());
    size_t            data_sizes_buf_4 = data_buf_4.size();
    size_t            offset_buf_4     = 0;

    const uint32_t    batch_size_buf_5 = 1;
    std::vector<char> data_buf_5(8192, 'F');
    uintptr_t         ptrs_buf_5       = reinterpret_cast<uintptr_t>(data_buf_5.data());
    size_t            data_sizes_buf_5 = data_buf_5.size();
    size_t            offset_buf_5     = 0;

    std::vector<char> data_buf_6(8192, 'F');
    uintptr_t         ptrs_buf_6       = reinterpret_cast<uintptr_t>(data_buf_6.data());
    size_t            data_sizes_buf_6 = data_buf_6.size();
    size_t            offset_buf_6     = 0;

    std::vector<char> data_buf_7(8192, 'F');
    uintptr_t         ptrs_buf_7       = reinterpret_cast<uintptr_t>(data_buf_7.data());
    size_t            data_sizes_buf_7 = data_buf_7.size();
    size_t            offset_buf_7     = 0;

    std::vector<char> data_buf_8(8192, 'F');
    uintptr_t         ptrs_buf_8       = reinterpret_cast<uintptr_t>(data_buf_8.data());
    size_t            data_sizes_buf_8 = data_buf_8.size();
    size_t            offset_buf_8     = 0;

    auto buf_0 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_0, offset_buf_0, data_sizes_buf_0);
    auto buf_1 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_1, offset_buf_1, data_sizes_buf_1);
    auto buf_2 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_2, offset_buf_2, data_sizes_buf_2);
    auto buf_3 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_3, offset_buf_3, data_sizes_buf_3);
    auto buf_4 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_4, offset_buf_4, data_sizes_buf_4);
    auto buf_5 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_5, offset_buf_5, data_sizes_buf_5);
    auto buf_6 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_6, offset_buf_6, data_sizes_buf_6);
    auto buf_7 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_7, offset_buf_7, data_sizes_buf_7);
    auto buf_8 = std::make_shared<RDMABuffer>(end_point, ptrs_buf_8, offset_buf_8, data_sizes_buf_8);

    std::cout << "Launch EDNPOINT ..." << std::endl;

    buf_0->recv();
    buf_1->recv();
    buf_2->recv();
    buf_3->recv();
    buf_4->recv();
    buf_5->recv();
    buf_6->recv();
    buf_7->recv();
    buf_8->recv();
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Wait RECV Complete..." << std::endl;
    buf_0->waitRecv();
    buf_1->waitRecv();
    buf_2->waitRecv();
    buf_3->waitRecv();
    buf_4->waitRecv();
    buf_5->waitRecv();
    buf_6->waitRecv();
    buf_7->waitRecv();
    buf_8->waitRecv();
    //   buf_1->waitRecv();

    std::cout << data_buf_0[0] << std::endl;
    std::cout << data_buf_1[0] << std::endl;
    std::cout << data_buf_2[0] << std::endl;
    std::cout << data_buf_3[0] << std::endl;
    std::cout << data_buf_4[0] << std::endl;
    std::cout << data_buf_5[0] << std::endl;
    std::cout << data_buf_6[0] << std::endl;
    std::cout << data_buf_7[0] << std::endl;
    std::cout << data_buf_8[0] << std::endl;
    bool data_buf_0_correct = std::all_of(data_buf_0.begin(), data_buf_0.end(), [](char c) { return c == '0'; });
    assert(data_buf_0_correct && "Data_0 should contain '0'");

    bool data_buf_1_correct = std::all_of(data_buf_1.begin(), data_buf_1.end(), [](char c) { return c == '1'; });
    assert(data_buf_1_correct && "Data_1 should contain '1'");

    bool data_buf_2_correct = std::all_of(data_buf_2.begin(), data_buf_2.end(), [](char c) { return c == '2'; });
    assert(data_buf_2_correct && "Data_2 should contain '2'");

    bool data_buf_3_correct = std::all_of(data_buf_3.begin(), data_buf_3.end(), [](char c) { return c == '3'; });
    assert(data_buf_3_correct && "Data_3 should contain '3'");

    bool data_buf_4_correct = std::all_of(data_buf_4.begin(), data_buf_4.end(), [](char c) { return c == '4'; });
    assert(data_buf_4_correct && "Data_4 should contain '4'");

    bool data_buf_5_correct = std::all_of(data_buf_5.begin(), data_buf_5.end(), [](char c) { return c == '5'; });
    assert(data_buf_5_correct && "Data_5 should contain '5'");

    bool data_buf_6_correct = std::all_of(data_buf_6.begin(), data_buf_6.end(), [](char c) { return c == '6'; });
    assert(data_buf_6_correct && "Data_6 should contain '6'");

    bool data_buf_7_correct = std::all_of(data_buf_7.begin(), data_buf_7.end(), [](char c) { return c == '7'; });
    assert(data_buf_7_correct && "Data_7 should contain '7'");

    bool data_buf_8_correct = std::all_of(data_buf_8.begin(), data_buf_8.end(), [](char c) { return c == '8'; });
    assert(data_buf_8_correct && "Data_8 should contain '8'");

    std::cout << "The RECV test completed and data verified." << std::endl;

    return 0;
}
