#include "engine/rdma/rdma_endpoint.h"
#include "engine/rdma/rdma_buffer.h"
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


int main(int argc, char** argv)
{

    std::cout<<"Init the RMDA ENDPOINT OF RECV... "<<std::endl;
    // Construct the Receiver
    auto receiver = std::make_shared<RDMAEndpoint>(
        FLAGS_DEVICE_NAME,
        FLAGS_IB_PORT,
        FLAGS_LINK_TYPE,
        16
    );

    std::cout<<"RDMA QP INFO VIA TCP... "<<std::endl;
    // RDMA control plane via TCP
    zmq::context_t zmq_ctx_data(2);
    zmq::context_t zmq_ctx_mmrg(2);

    zmq::socket_t  sock_data(zmq_ctx_data, ZMQ_REP);
    zmq::socket_t  sock_mmrg(zmq_ctx_mmrg, ZMQ_REP);

    sock_data.bind("tcp://*:" + std::to_string(FLAGS_PORT_DATA));
    sock_mmrg.bind("tcp://*:" + std::to_string(FLAGS_PORT_MRCN));

    zmq::message_t data_channel_info;
    auto data_channel_info_res = sock_data.recv(data_channel_info);
    zmq::message_t mmrg_channel_info;
    auto mmrg_channel_info_res = sock_mmrg.recv(mmrg_channel_info);

    // Connect the SEND side
    std::cout << "Connect to the Tx side..." << std::endl;
    receiver->ContextConnect(json::parse(data_channel_info.to_string()), json::parse(mmrg_channel_info.to_string()));
    std::cout << "Connect Success..." << std::endl;


    std::cout << "Send the RDMA Info to Tx..." << std::endl;
    zmq::message_t local_data_channel_info(receiver->GetDataContextInfo().dump());
    zmq::message_t local_meta_channel_info(receiver->GetMetaContextInfo().dump());

    sock_data.send(local_data_channel_info, zmq::send_flags::none);
    sock_mmrg.send(local_meta_channel_info, zmq::send_flags::none);


    std::cout << "Finish the connection of QP, start to RECV... " << std::endl;



    const uint32_t batch_size = 1;
    std::vector<char> data_0(1024, '0');
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


    std::vector<char> data_2(1024, '0');
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

    RDMABuffer buf_0(receiver, ptrs, data_sizes, batch_size);
    RDMABuffer buf_1(receiver, ptrs_1, data_sizes_1, batch_size);


    std::cout<<"Launch Recv..." << std::endl;
    buf_0.Recv();
    buf_1.Recv();
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Wait Recv Complete..." << std::endl;
    buf_1.WaitRecv();
    buf_0.WaitRecv();



    //receiver.LaunchRecv(1);

    // try
    // {
    //     receiver.Recv(ptrs, data_sizes, batch_size);
    //     std::cout << "Recv called successfully with batch size: " << batch_size << std::endl;
    // }
    // catch (const std::exception& e)
    // {
    //     std::cerr << "Recv failed: " << e.what() << std::endl;
    //     assert(false);
    // }
    // std::cout << "Wait Recv Complete..." << std::endl;
    // std::cout << "Main thread working Test..." << std::endl;
    // std::cout << "Main thread working Test..." << std::endl;
    // std::cout << "Main thread working Test..." << std::endl;
    // std::cout << "Main thread working Test..." << std::endl;
    // std::cout << "Main thread working Test..." << std::endl;
    // std::cout << "Wait Recv Complete..." << std::endl;

    //receiver.WaitRecv();



    bool data_0_correct = std::all_of(data_0.begin(), data_0.end(), [](char c) { return c == 'A'; });
    // bool data_2_correct = std::all_of(data_2.begin(), data_2.end(), [](char c) { return c == 'C'; });
    // bool data_3_correct = std::all_of(data_3.begin(), data_3.end(), [](char c) { return c == 'D'; });
    assert(data_0_correct && "Data_0 should contain 'A'");
    // assert(data_2_correct && "Data_2 should contain 'C'");
    // assert(data_3_correct && "Data_3 should contain 'D'");

    std::cout << "RECV endpoint test completed. Data verified." << std::endl;

    return 0;

}
