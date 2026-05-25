#include <gflags/gflags.h>
#include <sys/time.h>

#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <zmq.hpp>

#include "acl/acl.h"
#include "dlslime/csrc/engine/ascend_direct/ascend_direct_endpoint.h"
#include "dlslime/csrc/logging.h"
#include "nanocommon/json.hpp"

DEFINE_string(localhost, "100.97.164.197", "local IP");
DEFINE_int32(local_port, 16777, "Port number");
DEFINE_string(remote_host, "100.97.164.197", "remote IP");
DEFINE_int32(remote_port, 16789, "Port number");

DEFINE_string(mode,
              "initiator",
              "Running mode: initiator or target. Initiator node reads "
              "data blocks from target node");
DEFINE_int32(device_id, 0, "Using local NPU device ID of this machine");

DEFINE_int32(batch_size, 32, "Batch size");
DEFINE_uint64(block_size,
              32768,
              "Block size for smallest transfer request, "
              "next request doubles the size");
DEFINE_uint64(block_iteration, 10, "number of iterations of the block");

DEFINE_string(report_unit, "GB", "Report unit: GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb");
DEFINE_uint32(report_precision, 2, "Report precision");

using namespace dlslime;
using json = nlohmann::json;

const static std::unordered_map<std::string, uint64_t> RATE_UNIT_MP = {{"GB", 1000ull * 1000ull * 1000ull},
                                                                       {"GiB", 1ull << 30},
                                                                       {"Gb", 1000ull * 1000ull * 1000ull / 8},
                                                                       {"MB", 1000ull * 1000ull},
                                                                       {"MiB", 1ull << 20},
                                                                       {"Mb", 1000ull * 1000ull / 8},
                                                                       {"KB", 1000ull},
                                                                       {"KiB", 1ull << 10},
                                                                       {"Kb", 1000ull / 8}};

static inline std::string calculateRate(uint64_t data_bytes, uint64_t duration)
{
    if (!RATE_UNIT_MP.count(FLAGS_report_unit)) {
        SLIME_LOG_WARN("Invalid flag: report_unit only support ",
                       "GB|GiB|Gb|MB|MiB|Mb|KB|KiB|Kb, not support ",
                       FLAGS_report_unit,
                       " . Now use GB(default) as report_unit");
        FLAGS_report_unit = "GB";
    }
    std::ostringstream oss;
    oss << std::fixed << std::setprecision(FLAGS_report_precision)
        << 1.0 * data_bytes * 1000000 / duration / RATE_UNIT_MP.at(FLAGS_report_unit) << " " << FLAGS_report_unit
        << "/s";
    return oss.str();
}

int allocateDevMem(void*& devAddr, size_t size)
{
    // malloc device mem
    aclError ret = aclrtMalloc(&devAddr, size, ACL_MEM_MALLOC_HUGE_ONLY);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to allocate device memory, ret:", ret);
        return ret;
    }

    // malloc host mem
    void* host_addr = nullptr;
    ret             = aclrtMallocHost(&host_addr, size);
    if (ret != ACL_ERROR_NONE || host_addr == nullptr) {
        SLIME_LOG_ERROR("Failed to allocate host memory, ret:", ret);
        return ret;
    }

    // Initialize with pattern
    for (size_t i = 0; i < size; i += sizeof(uint32_t)) {
        *(uint32_t*)((char*)host_addr + i) = 0x111222;
    }

    // copy data from host mem to device mem
    ret = aclrtMemcpy(devAddr, size, host_addr, size, ACL_MEMCPY_HOST_TO_DEVICE);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to copy data from host to device, ret: ", ret);
        aclrtFreeHost(host_addr);
        aclrtFree(devAddr);
        return ret;
    }

    // release host memory
    ret = aclrtFreeHost(host_addr);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to aclrtFreeHost, ret: ", ret);
        return ret;
    }

    return 0;
}

/**
 * @brief Allocate and register memory regions with the endpoint
 * @return Vector of device addresses
 */
std::vector<uintptr_t> allocRegistMem(const std::shared_ptr<AscendDirectEndpoint>& endpoint)
{
    std::vector<uintptr_t> dev_addrs;
    for (int i = 0; i < FLAGS_block_iteration; ++i) {
        uint64_t block_size         = (FLAGS_block_size << i);
        uint64_t batched_block_size = FLAGS_batch_size * block_size;
        void*    dev_addr           = nullptr;
        int      ret                = allocateDevMem(dev_addr, batched_block_size);
        if (ret != 0) {
            SLIME_LOG_ERROR("Failed to allocateDevMem for size ", batched_block_size, ", ret: ", ret);
            return {};
        }

        uintptr_t dev_addr_uintptr = reinterpret_cast<uintptr_t>(dev_addr);

        // NEW INTERFACE: Use mr_key and normalized signature
        uint64_t mr_key = 1000 + i;  // Unique key for each block
        endpoint->register_memory_region(mr_key, dev_addr_uintptr, 0, batched_block_size);

        dev_addrs.push_back(dev_addr_uintptr);
    }
    return dev_addrs;
}

int initiator()
{
    std::cout << "=== Ascend Direct Endpoint Perf Test (Initiator) ===" << std::endl;
    std::cout << "Running on " << FLAGS_localhost << ":" << FLAGS_local_port << std::endl;

    // Create endpoint with new interface
    // NOTE: AdxlEngine uses local_port+1, ZMQ uses local_port for coordination
    auto endpoint = std::make_shared<AscendDirectEndpoint>();
    endpoint->init(FLAGS_localhost, FLAGS_local_port + 1);

    // Allocate and register local memory
    std::vector<uintptr_t> dev_addrs = allocRegistMem(endpoint);
    if (dev_addrs.empty()) {
        SLIME_LOG_ERROR("Failed to allocate memory regions");
        return -1;
    }

    // Exchange endpoint info via ZMQ
    std::cout << "Exchanging endpoint info with " << FLAGS_remote_host << ":" << FLAGS_remote_port << std::endl;
    zmq::context_t ctx;
    zmq::socket_t  socket(ctx, ZMQ_REQ);
    socket.connect("tcp://" + FLAGS_remote_host + ":" + std::to_string(FLAGS_remote_port));

    // Send local endpoint info
    json local_info            = endpoint->endpoint_info();
    local_info["dev_addrs"]    = dev_addrs;  // Include device addresses for remote registration
    std::string local_info_str = local_info.dump();
    socket.send(zmq::buffer(local_info_str), zmq::send_flags::none);

    // Receive remote endpoint info
    zmq::message_t msg;
    auto           recv_res = socket.recv(msg);
    if (!recv_res) {
        SLIME_LOG_ERROR("Failed to receive remote endpoint info");
        return -1;
    }
    std::string remote_info_str(static_cast<char*>(msg.data()), msg.size());
    json        remote_info = json::parse(remote_info_str);

    // Extract remote device addresses
    std::vector<uintptr_t> remote_dev_addrs = remote_info["dev_addrs"].get<std::vector<uintptr_t>>();

    // NEW INTERFACE: Connect to remote endpoint
    endpoint->connect(remote_info);
    std::cout << "Connected to remote endpoint" << std::endl;

    // Register remote memory regions (metadata only)
    for (int i = 0; i < FLAGS_block_iteration; ++i) {
        uint64_t remote_mr_key = 2000 + i;  // Different namespace for remote
        json     remote_mr_info;
        remote_mr_info["addr"]   = remote_dev_addrs[i];
        remote_mr_info["offset"] = 0;
        remote_mr_info["length"] = FLAGS_batch_size * (FLAGS_block_size << i);

        endpoint->register_remote_memory_region(remote_mr_key, "remote_block_" + std::to_string(i), remote_mr_info);
    }

    // Performance test: Read from remote
    std::cout << "\n=== Starting Performance Test ===" << std::endl;
    for (int i = 0; i < FLAGS_block_iteration; ++i) {
        uint64_t block_size = (FLAGS_block_size << i);

        // NEW INTERFACE: Use assign_tuple_t format
        std::vector<assign_tuple_t> assignments;
        for (int j = 0; j < FLAGS_batch_size; ++j) {
            uint64_t local_mr_key  = 1000 + i;
            uint64_t remote_mr_key = 2000 + i;
            uint64_t target_offset = block_size * j;  // Remote offset
            uint64_t source_offset = block_size * j;  // Local offset
            uint64_t length        = block_size;

            assignments.emplace_back(local_mr_key, remote_mr_key, target_offset, source_offset, length);
        }

        // Time the read operation
        struct timeval start_tv, stop_tv;
        gettimeofday(&start_tv, nullptr);

        // NEW INTERFACE: read() returns a future
        auto future = endpoint->read(assignments, nullptr);
        if (future) {
            future->wait();  // Wait for completion
        }

        gettimeofday(&stop_tv, nullptr);
        uint64_t duration = (stop_tv.tv_sec - start_tv.tv_sec) * 1000000.0 + (stop_tv.tv_usec - start_tv.tv_usec);

        std::cout << "Block iteration " << i << " completed: "
                  << "duration " << duration << "us, "
                  << "block_size " << block_size / 1024 << "KB, "
                  << "total_size " << FLAGS_batch_size * block_size / 1024 << "KB, "
                  << "throughput " << calculateRate(FLAGS_batch_size * block_size, duration) << std::endl;
    }

    // Cleanup
    std::cout << "\n=== Test Complete, Releasing Resources ===" << std::endl;
    socket.send(zmq::message_t(0), zmq::send_flags::none);  // Signal completion
    socket.close();
    ctx.close();

    for (uintptr_t addr : dev_addrs) {
        aclrtFree(reinterpret_cast<void*>(addr));
    }

    return 0;
}

int target()
{
    std::cout << "=== Ascend Direct Endpoint Perf Test (Target) ===" << std::endl;
    std::cout << "Running on " << FLAGS_localhost << ":" << FLAGS_local_port << std::endl;

    // Create endpoint with new interface
    // NOTE: AdxlEngine uses local_port+1, ZMQ uses local_port for coordination
    auto endpoint = std::make_shared<AscendDirectEndpoint>();
    endpoint->init(FLAGS_localhost, FLAGS_local_port + 1);

    // Allocate and register local memory
    std::vector<uintptr_t> dev_addrs = allocRegistMem(endpoint);
    if (dev_addrs.empty()) {
        SLIME_LOG_ERROR("Failed to allocate memory regions");
        return -1;
    }

    // Setup ZMQ server
    std::cout << "Waiting for initiator connection..." << std::endl;
    zmq::context_t ctx;
    zmq::socket_t  socket(ctx, ZMQ_REP);

    // Set linger to 0 to avoid hanging on close and allow immediate rebind
    int linger = 0;
    socket.setsockopt(ZMQ_LINGER, &linger, sizeof(linger));

    // Enable immediate port reuse (fixes "Address already in use")
    int reuse = 1;
    socket.setsockopt(ZMQ_TCP_KEEPALIVE, &reuse, sizeof(reuse));

    std::string bind_addr = "tcp://*:" + std::to_string(FLAGS_local_port);
    std::cout << "Binding to " << bind_addr << "..." << std::endl;

    try {
        socket.bind(bind_addr);
        std::cout << "✓ Successfully bound to port " << FLAGS_local_port << std::endl;
    }
    catch (const zmq::error_t& e) {
        SLIME_LOG_ERROR("Failed to bind to port ", FLAGS_local_port, ": ", e.what());
        SLIME_LOG_ERROR("Troubleshooting:");
        SLIME_LOG_ERROR("  1. Wait 30 seconds and try again (TIME_WAIT state)");
        SLIME_LOG_ERROR("  2. Use different port: --local_port=<other_port>");
        SLIME_LOG_ERROR("  3. Check: netstat -tan | grep ", FLAGS_local_port);
        return -1;
    }

    // Receive initiator's endpoint info
    zmq::message_t recv_msg;
    auto           recv_res1 = socket.recv(recv_msg);
    if (!recv_res1) {
        SLIME_LOG_ERROR("Failed to receive initiator endpoint info");
        return -1;
    }
    std::string remote_info_str(static_cast<char*>(recv_msg.data()), recv_msg.size());
    json        remote_info = json::parse(remote_info_str);

    // Extract remote device addresses
    std::vector<uintptr_t> remote_dev_addrs = remote_info["dev_addrs"].get<std::vector<uintptr_t>>();

    // NEW INTERFACE: Connect to remote endpoint
    endpoint->connect(remote_info);
    std::cout << "Connected to initiator endpoint" << std::endl;

    // Register remote memory regions
    for (int i = 0; i < FLAGS_block_iteration; ++i) {
        uint64_t remote_mr_key = 2000 + i;
        json     remote_mr_info;
        remote_mr_info["addr"]   = remote_dev_addrs[i];
        remote_mr_info["offset"] = 0;
        remote_mr_info["length"] = FLAGS_batch_size * (FLAGS_block_size << i);

        endpoint->register_remote_memory_region(remote_mr_key, "remote_block_" + std::to_string(i), remote_mr_info);
    }

    // Send local endpoint info back
    json local_info            = endpoint->endpoint_info();
    local_info["dev_addrs"]    = dev_addrs;
    std::string local_info_str = local_info.dump();
    socket.send(zmq::buffer(local_info_str), zmq::send_flags::none);

    std::cout << "Endpoint info exchanged, waiting for test completion..." << std::endl;

    // Wait for completion signal
    zmq::message_t stop_msg;
    auto           recv_res2 = socket.recv(stop_msg);
    if (!recv_res2) {
        SLIME_LOG_WARN("Failed to receive completion signal (initiator may have disconnected)");
    }

    // Cleanup
    std::cout << "\n=== Target Complete, Releasing Resources ===" << std::endl;
    socket.close();
    ctx.close();

    for (uintptr_t addr : dev_addrs) {
        aclrtFree(reinterpret_cast<void*>(addr));
    }

    return 0;
}

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);

    std::cout << "=== Initializing ACL Runtime ===" << std::endl;
    const char* aclConfigPath = nullptr;
    aclError    ret           = aclInit(aclConfigPath);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to initialize ACL, ret: ", ret);
        return ret;
    }

    ret = aclrtSetDevice(FLAGS_device_id);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to set device ", FLAGS_device_id, ", ret: ", ret);
        return ret;
    }

    std::cout << "ACL initialized on device " << FLAGS_device_id << std::endl;
    std::cout << "Mode: " << FLAGS_mode << std::endl;

    int result = 0;
    if (FLAGS_mode == "initiator") {
        result = initiator();
    }
    else if (FLAGS_mode == "target") {
        result = target();
    }
    else {
        SLIME_LOG_ERROR("Unsupported mode: must be 'initiator' or 'target'");
        return -1;
    }

    // Cleanup ACL
    aclrtResetDevice(FLAGS_device_id);
    aclFinalize();

    return result;
}
