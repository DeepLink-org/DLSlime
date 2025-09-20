#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_config.h"
#include "engine/rdma/rdma_context.h"
#include "utils/json.hpp"
#include "logging.h"

#include <cassert>
#include <chrono>
#include <condition_variable>
#include <future>
#include <gflags/gflags.h>
#include <mutex>
#include <stdexcept>
#include <string>
#include <sys/time.h>
#include <unistd.h>
#include <unordered_map>
#include <zmq.h>
#include <zmq.hpp>

#include <cstdlib>

using json = nlohmann::json;
using namespace slime;

DEFINE_string(mode, "target", "initiator or target");

DEFINE_string(device_name, "mlx5_bond_0", "device name");
DEFINE_uint32(ib_port, 1, "device name");
DEFINE_string(link_type, "RoCE", "IB or RoCE");

DEFINE_string(initiator_endpoint, "", "initiator endpoint");
DEFINE_string(target_endpoint, "", "target endpoint");

DEFINE_uint64(buffer_size, (10ull << 30) + 1, "total size of data buffer");
DEFINE_uint64(block_size, 2048000, "block size");
DEFINE_uint64(batch_size, 160, "batch size");

DEFINE_uint64(duration, 10, "duration (s)");

DEFINE_uint64(concurrent_num, 20, "max concurrent rdma assignment");

json mr_info;

void* memory_allocate_initiator()
{
    SLIME_ASSERT(FLAGS_buffer_size > FLAGS_batch_size * FLAGS_block_size, "buffer_size < batch_size * block_size");
    void* data = (void*)malloc(FLAGS_buffer_size);
    memset(data, 0, FLAGS_buffer_size);
    return data;
}

void* memory_allocate_target()
{
    SLIME_ASSERT(FLAGS_buffer_size > FLAGS_batch_size * FLAGS_block_size, "buffer_size < batch_size * block_size");
    void* data      = (void*)malloc(FLAGS_buffer_size);
    char* byte_data = (char*)data;
    for (int64_t i = 0; i < FLAGS_buffer_size; ++i) {
        byte_data[i] = i % 128;
    }
    return data;
}

bool checkInitiatorCopied(void* data)
{
    char* byte_data = (char*)data;
    for (int64_t i = 0; i < (FLAGS_batch_size * FLAGS_block_size); ++i) {
        if (byte_data[i] != i % 128) {
            SLIME_ASSERT(false, "Transferred data at i = " << i << " not same.");
            return false;
        }
    }
    return true;
}

int connect(RDMAContext& rdma_context, zmq::socket_t& send, zmq::socket_t& recv)
{
    json local_info = rdma_context.endpoint_info();

    zmq::message_t local_msg(local_info.dump());
    send.send(local_msg, zmq::send_flags::none);

    zmq::message_t remote_msg;
    recv.recv(remote_msg, zmq::recv_flags::none);
    std::string remote_msg_str(static_cast<const char*>(remote_msg.data()), remote_msg.size());

    json remote_info = json::parse(remote_msg_str);
    rdma_context.connect(remote_info);
    return 0;
}

int target(RDMAContext& rdma_context)
{
    zmq::context_t context(2);
    zmq::socket_t  send(context, ZMQ_PUSH);
    zmq::socket_t  recv(context, ZMQ_PULL);

    send.connect("tcp://" + FLAGS_initiator_endpoint);
    recv.bind("tcp://" + FLAGS_target_endpoint);

    rdma_context.init(FLAGS_device_name, FLAGS_ib_port, FLAGS_link_type);

    void* data = memory_allocate_target();
    rdma_context.register_memory_region("buffer", (uintptr_t)data, FLAGS_buffer_size);

    SLIME_ASSERT_EQ(connect(rdma_context, send, recv), 0, "Connect Error");

    zmq::message_t term_msg;
    recv.recv(term_msg, zmq::recv_flags::none);
    std::string signal = std::string(static_cast<char*>(term_msg.data()), term_msg.size());
    SLIME_ASSERT(!strcmp(signal.c_str(), "TERMINATE"), "signal error");

    return 0;
}

int initiator(RDMAContext& rdma_context)
{
    zmq::context_t context(2);
    zmq::socket_t  send(context, ZMQ_PUSH);
    zmq::socket_t  recv(context, ZMQ_PULL);

    send.connect("tcp://" + FLAGS_target_endpoint);
    recv.bind("tcp://" + FLAGS_initiator_endpoint);

    rdma_context.init(FLAGS_device_name, FLAGS_ib_port, FLAGS_link_type);

    void* data = memory_allocate_initiator();
    rdma_context.register_memory_region("buffer", (uintptr_t)data, FLAGS_buffer_size);

    SLIME_ASSERT_EQ(connect(rdma_context, send, recv), 0, "Connect Error");

    rdma_context.launch_future();

    uint64_t total_bytes = 0;
    uint64_t total_trips = 0;
    size_t   step        = 0;
    auto     start_time  = std::chrono::steady_clock::now();
    auto     deadline    = start_time + std::chrono::seconds(FLAGS_duration);

    while (std::chrono::steady_clock::now() < deadline) {

        std::vector<uintptr_t> target_offsets, source_offsets;

        for (int i = 0; i < FLAGS_batch_size; ++i) {
            source_offsets.emplace_back(i * FLAGS_block_size);
            target_offsets.emplace_back(i * FLAGS_block_size);
        }

        int done = false;

        std::vector<RDMAAssignmentSharedPtr> rdma_assignment_batch;
        for (int concurrent_id = 0; concurrent_id < FLAGS_concurrent_num; ++concurrent_id) {
            AssignmentBatch batch;
            for (int i = 0; i < FLAGS_batch_size; ++i) {
                batch.push_back(Assignment("buffer", i * FLAGS_block_size, i * FLAGS_block_size, FLAGS_block_size));
            }
            RDMAAssignmentSharedPtr rdma_assignment = rdma_context.submit(OpCode::READ, batch);
            rdma_assignment_batch.emplace_back(rdma_assignment);
            total_bytes += FLAGS_batch_size * FLAGS_block_size;
            total_trips += 1;
        }
        for (RDMAAssignmentSharedPtr& rdma_assignment : rdma_assignment_batch) {
            rdma_assignment->wait();
        }
    }

    auto   end_time   = std::chrono::steady_clock::now();
    double duration   = std::chrono::duration<double>(end_time - start_time).count();
    double throughput = total_bytes / duration / (1 << 20);  // MB/s

    std::cout << "Batch size        : " << FLAGS_batch_size << std::endl;
    std::cout << "Block size        : " << FLAGS_block_size << std::endl;

    std::cout << "Total trips       : " << total_trips << std::endl;
    std::cout << "Total transferred : " << total_bytes / (1 << 20) << " MiB" << std::endl;
    std::cout << "Duration          : " << duration << " seconds" << std::endl;
    std::cout << "Average Latency   : " << duration / total_trips * 1000 << " ms/trip" << std::endl;
    std::cout << "Throughput        : " << throughput << " MiB/s" << std::endl;

    zmq::message_t term_msg("TERMINATE");
    send.send(term_msg, zmq::send_flags::none);

    rdma_context.stop_future();

    SLIME_ASSERT(checkInitiatorCopied(data), "Transferred data not equal!");

    return 0;
}

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    std::cout << "benchmark begin" << std::endl;
    RDMAContext context;
    if (FLAGS_mode == "initiator") {
        return initiator(context);
    }
    else if (FLAGS_mode == "target") {
        return target(context);
    }
    SLIME_ABORT("Unsupported mode: must be 'initiator' or 'target'");
}
