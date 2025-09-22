#include <cassert>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <future>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_map>

#include <gflags/gflags.h>
#include <numa.h>
#include <sys/time.h>
#include <unistd.h>
#include <zmq.h>
#include <zmq.hpp>

#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_config.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_scheduler.h"
#include "engine/rdma/utils.h"

#include "json.hpp"
#include "logging.h"

using json = nlohmann::json;
using namespace slime;

DEFINE_string(mode, "", "initiator or target");

DEFINE_string(device_name, "", "device name");
DEFINE_uint32(ib_port, 1, "device name");
DEFINE_string(link_type, "RoCE", "IB or RoCE");
DEFINE_uint32(num_thread, 2, "total qp num per rdma context");

DEFINE_string(target_endpoint, "10.130.8.139:8000", "target endpoint");
DEFINE_string(initiator_endpoint, "10.130.8.138:8001", "initiator endpoint");

DEFINE_uint64(buffer_size, (2048000 * 160) + 1, "total size of data buffer");
DEFINE_uint64(block_size, 204800, "block size");
DEFINE_uint64(batch_size, 160, "batch size");

DEFINE_uint64(concurrent_num, 20, "max concurrent rdma scheduler assignment");

DEFINE_uint64(duration, 10, "duration (s)");

DEFINE_bool(numa_affinity, true, "numa memory affinity");

const static int NR_SOCKETS = numa_available() == 0 ? numa_num_configured_nodes() : 1;

json mr_info;

zmq::context_t* tcp_context_ = nullptr;
zmq::socket_t*  send_        = nullptr;
zmq::socket_t*  recv_        = nullptr;

// 字符串分割函数
std::vector<std::string> split_device_name(char delimiter)
{
    std::vector<std::string> tokens;
    std::string              token;
    std::istringstream       tokenStream(FLAGS_device_name);
    while (std::getline(tokenStream, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

void resetTcpSockets()
{
    if (send_ != nullptr) {
        send_->close();
        delete send_;
        send_ = nullptr;
    }
    if (recv_ != nullptr) {
        recv_->close();
        delete recv_;
        recv_ = nullptr;
    }
    if (tcp_context_ != nullptr) {
        tcp_context_->close();
        delete tcp_context_;
        tcp_context_ = nullptr;
    }
}

int waitRemoteTeriminate()
{
    zmq::message_t term_msg;
    recv_->recv(term_msg, zmq::recv_flags::none);
    std::string signal = std::string(static_cast<char*>(term_msg.data()), term_msg.size());
    if (signal == "TERMINATE") {
        resetTcpSockets();
        return 0;
    }
    return -1;
}

int teriminate()
{
    zmq::message_t term_msg("TERMINATE");
    send_->send(term_msg, zmq::send_flags::none);
    resetTcpSockets();
    return 0;
}

void init_tcp(std::string remote_endpoint, std::string local_endpoint)
{
    tcp_context_ = new zmq::context_t(2);
    send_        = new zmq::socket_t(*tcp_context_, ZMQ_PUSH);
    recv_        = new zmq::socket_t(*tcp_context_, ZMQ_PULL);
    send_->connect("tcp://" + remote_endpoint);
    recv_->bind("tcp://" + local_endpoint);
}

void init_connection(RDMAScheduler* scheduler, std::string remote_endpoint, std::string local_endpoint)
{
    json           local_info = scheduler->scheduler_info();
    zmq::message_t local_msg(local_info.dump());
    send_->send(local_msg, zmq::send_flags::none);
    zmq::message_t remote_msg;
    recv_->recv(remote_msg, zmq::recv_flags::none);
    std::string remote_msg_str(static_cast<const char*>(remote_msg.data()), remote_msg.size());
    scheduler->connect(json::parse(remote_msg_str));
}

void* memory_allocate_initiator(int socket_id)
{
    SLIME_ASSERT(FLAGS_buffer_size > FLAGS_batch_size * FLAGS_block_size, "buffer_size < batch_size * block_size");
    // void* data = (void*)malloc(FLAGS_buffer_size);
    void* data = numa_alloc_onnode(FLAGS_buffer_size, socket_id);
    memset(data, 0, FLAGS_buffer_size);
    return data;
}

void* memory_allocate_target(int socket_id)
{
    SLIME_ASSERT(FLAGS_buffer_size > FLAGS_batch_size * FLAGS_block_size, "buffer_size < batch_size * block_size");
    void* data      = numa_alloc_onnode(FLAGS_buffer_size, socket_id);
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
            SLIME_ASSERT(false,
                         "Transferred data at i = " << i << " not same. " << int{byte_data[i]} << " vs " << i % 8);
            return false;
        }
    }
    return true;
}

int target()
{

    init_tcp(FLAGS_initiator_endpoint, FLAGS_target_endpoint);

    std::vector<std::string> nic_devices;
    if (FLAGS_device_name.empty())
        nic_devices = available_nic();
    else
        nic_devices = split_device_name(',');

    size_t ndevices = nic_devices.size();
    size_t nsockets = FLAGS_numa_affinity ? NR_SOCKETS : 1;
    nsockets        = ndevices > nsockets ? nsockets : ndevices;

    size_t nsplit = ndevices / nsockets;

    std::vector<void*> data(nsockets, nullptr);
    for (int socket_id = 0; socket_id < nsockets; ++socket_id) {
        data[socket_id] = memory_allocate_target(socket_id);
    }
    size_t         nschedulers = nsockets * FLAGS_num_thread;
    RDMAScheduler* rdma_schs[nschedulers];

    int sch_device_begin_id = 0;

    for (int socket_id = 0; socket_id < nsockets; ++socket_id) {
        std::vector<std::string> sch_devices =
            std::vector<std::string>(nic_devices.begin() + sch_device_begin_id,
                                     nic_devices.begin() + std::min(sch_device_begin_id + nsplit, ndevices));
        sch_device_begin_id += nsplit;

        for (int qpi = 0; qpi < FLAGS_num_thread; qpi++) {
            RDMAScheduler* rdma_sch = new RDMAScheduler(sch_devices);
            rdma_sch->register_memory_region(
                "buffer_" + std::to_string(socket_id), (uintptr_t)data[socket_id], FLAGS_buffer_size);
            std::cout << "Target registered MR: "
                      << "buffer_" << socket_id << std::endl;

            rdma_schs[socket_id * FLAGS_num_thread + qpi] = rdma_sch;
        }
    }
    for (int sch_id = 0; sch_id < nschedulers; ++sch_id) {
        init_connection(rdma_schs[sch_id], FLAGS_initiator_endpoint, FLAGS_target_endpoint);
        std::cout << "Target connected remote" << std::endl;
    }

    waitRemoteTeriminate();

    for (int sch_id = 0; sch_id < nschedulers; ++sch_id)
        delete rdma_schs[sch_id];

    return 0;
}

int initiator()
{
    init_tcp(FLAGS_target_endpoint, FLAGS_initiator_endpoint);

    std::vector<std::string> nic_devices;
    if (FLAGS_device_name.empty())
        nic_devices = available_nic();
    else
        nic_devices = split_device_name(',');

    size_t ndevices = nic_devices.size();
    size_t nsockets = FLAGS_numa_affinity ? NR_SOCKETS : 1;
    nsockets        = ndevices > nsockets ? nsockets : ndevices;

    std::vector<void*> data(nsockets, nullptr);
    for (int socket_id = 0; socket_id < nsockets; ++socket_id) {
        data[socket_id] = memory_allocate_initiator(socket_id);
    }
    size_t nsplit      = ndevices / nsockets;
    size_t nschedulers = nsockets * FLAGS_num_thread;

    RDMAScheduler* rdma_schs[nschedulers];

    int sch_device_begin_id = 0;

    for (int socket_id = 0; socket_id < nsockets; ++socket_id) {
        std::vector<std::string> sch_devices =
            std::vector<std::string>(nic_devices.begin() + sch_device_begin_id,
                                     nic_devices.begin() + std::min(sch_device_begin_id + nsplit, ndevices));
        sch_device_begin_id += nsplit;

        for (int qpi = 0; qpi < FLAGS_num_thread; ++qpi) {
            RDMAScheduler* rdma_sch = new RDMAScheduler(sch_devices);
            rdma_sch->register_memory_region(
                "buffer_" + std::to_string(socket_id), (uintptr_t)data[socket_id], FLAGS_buffer_size);
            std::cout << "Initiator registered MR: "
                      << "buffer_" << socket_id << std::endl;
            rdma_schs[socket_id * FLAGS_num_thread + qpi] = rdma_sch;
        }
    }

    for (int rdma_sch_id = 0; rdma_sch_id < nschedulers; ++rdma_sch_id) {
        init_connection(rdma_schs[rdma_sch_id], FLAGS_target_endpoint, FLAGS_initiator_endpoint);
        std::cout << "Initiator connected remote" << std::endl;
    }

    uint64_t total_bytes = 0;
    uint64_t total_trips = 0;
    size_t   step        = 0;
    auto     start_time  = std::chrono::steady_clock::now();
    auto     deadline    = start_time + std::chrono::seconds(FLAGS_duration);

    while (std::chrono::steady_clock::now() < deadline) {
        RDMASchedulerAssignmentSharedPtrBatch rdma_scheduler_assignment_batch;
        for (int concurrent_id = 0; concurrent_id < FLAGS_concurrent_num; ++concurrent_id) {
            for (int socket_id = 0; socket_id < nsockets; ++socket_id) {
                for (int qpi = 0; qpi < FLAGS_num_thread; ++qpi) {
                    AssignmentBatch batch{};
                    for (int batch_id = 0; batch_id < FLAGS_batch_size; ++batch_id) {
                        Assignment assign = Assignment("buffer_" + std::to_string(socket_id),
                                                       batch_id * FLAGS_block_size,
                                                       batch_id * FLAGS_block_size,
                                                       FLAGS_block_size);
                        batch.emplace_back(assign);
                    }
                    RDMASchedulerAssignmentSharedPtr sch_assignment =
                        rdma_schs[socket_id * FLAGS_num_thread + qpi]->submitAssignment(OpCode::READ, batch);
                    rdma_scheduler_assignment_batch.emplace_back(sch_assignment);
                    total_bytes += FLAGS_batch_size * FLAGS_block_size;
                    total_trips += 1;
                }
            }
        }
        for (RDMASchedulerAssignmentSharedPtr sch_assignment : rdma_scheduler_assignment_batch) {
            sch_assignment->wait();
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

    teriminate();

    for (int sch_id = 0; sch_id < nschedulers; ++sch_id) {
        SLIME_ASSERT(checkInitiatorCopied(data[sch_id / FLAGS_num_thread]), "Transferred data not equal!");
    }

    for (int sch_id = 0; sch_id < nschedulers; ++sch_id)
        delete rdma_schs[sch_id];

    return 0;
}

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    if (FLAGS_mode == "initiator") {
        return initiator();
    }
    else if (FLAGS_mode == "target") {
        return target();
    }
    SLIME_ABORT("Unsupported mode: must be 'initiator' or 'target'");
}
