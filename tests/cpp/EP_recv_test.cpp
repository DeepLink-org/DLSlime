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

    std::cout<<"Init the RMDA ENDPOINT OF RECV... "<<std::endl;
    // Construct the Receiver
    RDMAEndpoint receiver(FLAGS_DEVICE_NAME, FLAGS_IB_PORT, FLAGS_LINK_TYPE, 16);
    
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
    receiver.ContextConnect(json::parse(data_channel_info.to_string()), json::parse(mmrg_channel_info.to_string()));
    std::cout << "Connect Success..." << std::endl;


    std::cout << "Send the RDMA Info to Tx..." << std::endl;
    zmq::message_t local_data_channel_info(receiver.GetRecvDataContextInfo().dump());
    zmq::message_t local_meta_channel_info(receiver.GetRecvMetaContextInfo().dump());

    sock_data.send(local_data_channel_info, zmq::send_flags::none);
    sock_mmrg.send(local_meta_channel_info, zmq::send_flags::none);

    std::cout << "Finish the connection of QP, start to RECV... " << std::endl;


    const uint32_t batch_size = 4;
    std::vector<char> data_0(1024, '0');
    std::vector<char> data_1(1024, '1');
    std::vector<char> data_2(1024, '2');
    std::vector<char> data_3(1024, '3');

    void* ptrs[batch_size] = {data_0.data(), data_1.data(), data_2.data(), data_3.data()};
    size_t data_sizes[batch_size] = {data_0.size(), data_1.size(), data_2.size(), data_3.size()};


    std::cout<<"Launch Recv..." << std::endl;
    receiver.Launch();

    try 
    {
        receiver.Recv(ptrs, data_sizes, batch_size);
        std::cout << "Recv called successfully with batch size: " << batch_size << std::endl;
    } 
    catch (const std::exception& e) 
    {
        std::cerr << "Recv failed: " << e.what() << std::endl;
        assert(false);
    }
    std::cout << "Wait Recv Complete..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl; 
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Main thread working Test..." << std::endl;
    std::cout << "Wait Recv Complete..." << std::endl;
    receiver.Stop();
    receiver.WaitRecv();



    bool data_0_correct = std::all_of(data_0.begin(), data_0.end(), [](char c) { return c == 'A'; });
    bool data_1_correct = std::all_of(data_1.begin(), data_1.end(), [](char c) { return c == 'B'; });
    bool data_2_correct = std::all_of(data_2.begin(), data_2.end(), [](char c) { return c == 'C'; });
    bool data_3_correct = std::all_of(data_3.begin(), data_3.end(), [](char c) { return c == 'D'; });
    assert(data_0_correct && "Data_0 should contain 'A'");
    assert(data_1_correct && "Data_1 should contain 'B'");
    assert(data_2_correct && "Data_2 should contain 'C'");
    assert(data_3_correct && "Data_3 should contain 'D'");

    std::cout << "RECV endpoint test completed. Data verified." << std::endl;
   
    return 0;
   
}


