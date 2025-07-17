#include "engine/rdma/rdma_endpoint.cpp"
#include "engine/rdma/rdma_buffer.h"

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



int main(int argc, char** argv)
{

    std::cout<<"Init the RMDA ENDPOINT OF SEND... "<<std::endl;
    // Construct the sender
    auto sender = std::make_shared<RDMAEndpoint>(
        FLAGS_DEVICE_NAME,
        FLAGS_IB_PORT,
        FLAGS_LINK_TYPE,
        16
    );

    std::cout<<"RDMA QP INFO VIA TCP... "<<std::endl;
    // RDMA control plane via TCP
    zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_mmrg(2);

    zmq::socket_t  sock_data(zmq_ctx_data, ZMQ_REQ);
    zmq::socket_t  sock_mmrg(zmq_ctx_mmrg, ZMQ_REQ);

    sock_data.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_DATA));
    sock_mmrg.connect("tcp://" + FLAGS_PEER_ADDR + ":" + std::to_string(FLAGS_PORT_MRCN));

    zmq::message_t local_data_channel_info(sender->GetDataContextInfo().dump());
    zmq::message_t local_meta_channel_info(sender->GetMetaContextInfo().dump());

    sock_data.send(local_data_channel_info, zmq::send_flags::none);
    sock_mmrg.send(local_meta_channel_info, zmq::send_flags::none);

    // Connect the RECV side
    std::cout << "Connect to the Rx side..." << std::endl;

    zmq::message_t data_channel_info;
    zmq::message_t meta_channel_info;

    auto send_data_result = sock_data.recv(data_channel_info);
    auto recv_data_result = sock_mmrg.recv(meta_channel_info);

    std::cout << "Connect to the Rx side..." << std::endl;
    sender->ContextConnect(json::parse(data_channel_info.to_string()), json::parse(meta_channel_info.to_string()));
    std::cout << "Connect Success..." << std::endl;

    std::cout << "Finish the connection of QP, start to SEND... " << std::endl;

    const uint32_t batch_size = 1;
    std::vector<char> data_0(1024, 'A');
    // std::vector<char> data_2(1024, '2');
    // std::vector<char> data_3(1024, '3');

    std::vector<uintptr_t> ptrs = {
        reinterpret_cast<uintptr_t>(data_0.data())
        // reinterpret_cast<uintptr_t>(data_2.data()),
        // reinterpret_cast<uintptr_t>(data_3.data())
    };
    std::vector<size_t> data_sizes = {data_0.size()
        //data_1.size(), data_2.size(), data_3.size()
    };


    std::vector<char> data_2(1024, 'A');
    // std::vector<char> data_2(1024, '2');
    // std::vector<char> data_3(1024, '3');

    std::vector<uintptr_t> ptrs_1 = {
        reinterpret_cast<uintptr_t>(data_2.data())
        // reinterpret_cast<uintptr_t>(data_2.data()),
        // reinterpret_cast<uintptr_t>(data_3.data())
    };
    std::vector<size_t> data_sizes_1 = {data_2.size()
        //data_1.size(), data_2.size(), data_3.size()
    };

    RDMABuffer buf_0(sender, ptrs, data_sizes, batch_size);
    RDMABuffer buf_1(sender, ptrs_1, data_sizes_1, batch_size);


    std::cout<<"Launch Recv..." << std::endl;
    buf_1.Send();
    buf_0.Send();
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Wait Send Complete..." << std::endl;
    buf_1.WaitSend();
    buf_0.WaitSend();
    // sender.LaunchSend(1);
    // try
    // {
    //     sender.Send(ptrs, data_sizes, batch_size);
    //     std::cout << "Send called successfully with batch size: " << batch_size << std::endl;
    // }
    // catch (const std::exception& e)
    // {
    //     std::cerr << "Send failed: " << e.what() << std::endl;
    //     assert(false);
    // }
    // std::cout << "Main thread working Test..." << std::endl;
    // std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // std::cout << "Main thread working Test..." << std::endl;
    // std::this_thread::sleep_for(std::chrono::milliseconds(200));
    // std::cout << "Main thread working Test..." << std::endl;
    // std::this_thread::sleep_for(std::chrono::milliseconds(200));
    //std::cout << "Wait Send Complete..." << std::endl;
    // sender.Stop();
    //sender.WaitSend();

    std::cout << "SEND endpoint test completed." << std::endl;

    return 0;

}
