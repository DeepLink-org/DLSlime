#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_endpoint.h"
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
DEFINE_int32(PORT_DATA, 5557, "ZMQ control port");
DEFINE_int32(PORT_MRCN, 5558, "ZMQ control port");

int main(int argc, char** argv)
{

    std::cout << "Init the RMDA ENDPOINT OF RECV... " << std::endl;
    // Construct the end_point
    auto end_point = std::make_shared<RDMAEndpoint>(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE, 4);

    std::cout << "RDMA QP INFO VIA TCP... " << std::endl;
    // RDMA control plane via TCP
    zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_mmrg(2);

    zmq::socket_t sock_data(zmq_ctx_data, ZMQ_REP);
    zmq::socket_t sock_mmrg(zmq_ctx_mmrg, ZMQ_REP);

    sock_data.bind("tcp://*:" + std::to_string(FLAGS_PORT_DATA));
    sock_mmrg.bind("tcp://*:" + std::to_string(FLAGS_PORT_MRCN));

    zmq::message_t data_channel_info;
    zmq::message_t mmrg_channel_info;
    auto           data_channel_info_res = sock_data.recv(data_channel_info);
    auto           mmrg_channel_info_res = sock_mmrg.recv(mmrg_channel_info);

    std::cout << "Send the RDMA Info to other side..." << std::endl;
    zmq::message_t local_data_channel_info(end_point->getDataContextInfo().dump());
    zmq::message_t local_meta_channel_info(end_point->getMetaContextInfo().dump());

    sock_data.send(local_data_channel_info, zmq::send_flags::none);
    sock_mmrg.send(local_meta_channel_info, zmq::send_flags::none);

    end_point->connect(json::parse(data_channel_info.to_string()), json::parse(mmrg_channel_info.to_string()));
    std::cout << "Connect Success..." << std::endl;
    std::cout << "Finish the connection of QP, start to SEND of buf_0 and buf_1..." << std::endl;

    const uint32_t    batch_size_buf_0 = 1;
    std::vector<char> data_buf_0(8192, '0');

    std::vector<uintptr_t> ptrs_buf_0       = {reinterpret_cast<uintptr_t>(data_buf_0.data())};
    std::vector<size_t>    data_sizes_buf_0 = {data_buf_0.size()};
    std::vector<size_t>    offset_buf_0 = {0};

    const uint32_t    batch_size_buf_1 = 2;
    std::vector<char> data_buf_1_0(1024, '1');
    std::vector<char> data_buf_1_1(2048, '2');

    std::vector<uintptr_t> ptrs_buf_1       = {reinterpret_cast<uintptr_t>(data_buf_1_0.data()),
                                               reinterpret_cast<uintptr_t>(data_buf_1_1.data())};
    std::vector<size_t>    data_sizes_buf_1 = {data_buf_1_0.size(), data_buf_1_1.size()};
    std::vector<size_t>    offset_buf_1 = {0,0};

    RDMABuffer buf_0(end_point, ptrs_buf_0, data_sizes_buf_0, offset_buf_0);
    RDMABuffer buf_1(end_point, ptrs_buf_1, data_sizes_buf_1, offset_buf_1);
    std::cout << "Launch EDNPOINT ..." << std::endl;

    buf_1.send();
    buf_0.send();
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Wait SEND Complete..." << std::endl;
    buf_0.waitSend();
    buf_1.waitSend();

    std::cout << "The SEND test completed." << std::endl;

    return 0;
}
