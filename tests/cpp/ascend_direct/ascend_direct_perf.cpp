#include <gflags/gflags.h>
#include <iostream>
#include <memory>
#include <sys/time.h>
#include <zmq.hpp>

#include "acl/acl.h"
#include "engine/ascend_direct_transport/ascend_direct_transport.h"
#include "logging.h"

DEFINE_string(localhost, "100.97.164.197", "local IP");
DEFINE_int32(local_port, 16777, "Port number");
DEFINE_string(remote_host, "100.97.164.197", "remote IP");
DEFINE_int32(remote_port, 16789, "Port number");

DEFINE_string(mode,
              "initiator",
              "Running mode: initiator or target. Initiator node read/write "
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

using namespace slime;

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
        SLIME_LOG_ERROR("Failed to allocate device memory, ret:", ret);
        return ret;
    }

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

    // release resource
    ret = aclrtFreeHost(host_addr);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to aclrtFreeHost, ret: ", ret);
        return ret;
    }

    return 0;
}

std::vector<uintptr_t> allocRegistMem(const std::unique_ptr<AscendDirectContext>& as_ctx)
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
        ret                        = as_ctx->register_memory_region(
            "npu:" + std::to_string(FLAGS_device_id), dev_addr_uintptr, batched_block_size);
        if (ret != 0) {
            SLIME_LOG_ERROR("Failed to registerLocalMemory, ret: ", ret);
            return {};
        }

        dev_addrs.push_back(dev_addr_uintptr);
    }
    return dev_addrs;
}

int initiator()
{
    std::cout << "running initiator on " << FLAGS_localhost << ":" << FLAGS_local_port << std::endl;
    std::unique_ptr<AscendDirectContext> as_ctx = std::make_unique<AscendDirectContext>();
    as_ctx->init(FLAGS_localhost, FLAGS_local_port + 1);
    std::vector<uintptr_t> dev_addrs = allocRegistMem(as_ctx);

    std::cout << "receiving addr info through zmq from " << FLAGS_remote_host << ":" << FLAGS_remote_port << std::endl;
    zmq::context_t ctx;
    zmq::socket_t  socket(ctx, ZMQ_REQ);
    socket.connect("tcp://" + FLAGS_remote_host + ":" + std::to_string(FLAGS_remote_port));
    zmq::message_t send_start_msg(0);
    socket.send(send_start_msg, zmq::send_flags::none);
    zmq::message_t msg;
    auto           recv_status = socket.recv(msg);
    size_t         msg_size    = msg.size();
    if (msg_size % sizeof(uintptr_t) != 0) {
        SLIME_LOG_ERROR("Invalid zmq message size");
        return -1;
    }
    size_t                 vec_size = msg_size / sizeof(uintptr_t);
    uintptr_t*             data_ptr = static_cast<uintptr_t*>(msg.data());
    std::vector<uintptr_t> remote_dev_addrs(data_ptr, data_ptr + vec_size);

    std::cout << "waiting for initiator do ascend data transfer perf" << std::endl;
    for (int i = 0; i < FLAGS_block_iteration; ++i) {
        uint64_t        block_size = (FLAGS_block_size << i);
        AssignmentBatch batched_assignments;
        for (int j = 0; j < FLAGS_batch_size; ++j) {
            std::string mr_key        = "npu:" + std::to_string(FLAGS_device_id);
            uint64_t    source_offset = static_cast<uint64_t>(dev_addrs[i]) + block_size * j;
            uint64_t    target_offset = static_cast<uint64_t>(remote_dev_addrs[i]) + block_size * j;
            batched_assignments.push_back(Assignment(mr_key, target_offset, source_offset, block_size));
        }
        struct timeval start_tv, stop_tv;
        gettimeofday(&start_tv, nullptr);
        as_ctx->read_batch(batched_assignments, FLAGS_remote_host, FLAGS_remote_port + 1);
        gettimeofday(&stop_tv, nullptr);
        uint64_t duration = (stop_tv.tv_sec - start_tv.tv_sec) * 1000000.0 + (stop_tv.tv_usec - start_tv.tv_usec);
        std::cout << "Block iteration " << i << " test completed: duration " << duration << "us, block size "
                  << block_size / 1024 << "KB, total size " << FLAGS_batch_size * block_size / 1024 << "KB, throughput "
                  << calculateRate(FLAGS_batch_size * block_size, duration) << std::endl;
    }

    std::cout << "ending ascend transfer perf, releasing resources" << std::endl;
    // send a message to indicate ending of perf test
    socket.send(zmq::message_t(0), zmq::send_flags::none);
    socket.close();
    ctx.close();
    as_ctx.reset();
    for (uintptr_t addr : dev_addrs) {
        aclrtFree(reinterpret_cast<void*>(addr));
    }
    return 0;
}

int target()
{
    std::cout << "running target on " << FLAGS_localhost << ":" << FLAGS_local_port << std::endl;
    std::unique_ptr<AscendDirectContext> as_ctx = std::make_unique<AscendDirectContext>();
    as_ctx->init(FLAGS_localhost, FLAGS_local_port + 1);
    std::vector<uintptr_t> dev_addrs = allocRegistMem(as_ctx);

    std::cout << "sending addr info through zmq from " << FLAGS_remote_host << ":" << FLAGS_remote_port << std::endl;
    zmq::context_t ctx;
    zmq::socket_t  socket(ctx, ZMQ_REP);  // Use REQ/REP, PUSH/PULL, etc.
    std::cout << "init socket" << std::endl;
    socket.bind("tcp://*:" + std::to_string(FLAGS_local_port));
    std::cout << "here after bind" << std::endl;
    zmq::message_t recv_start_msg;
    auto           recv_status = socket.recv(recv_start_msg);

    size_t         data_size = dev_addrs.size() * sizeof(uintptr_t);
    zmq::message_t msg(data_size);
    memcpy(msg.data(), dev_addrs.data(), data_size);
    std::cout << "here before send" << std::endl;
    socket.send(msg, zmq::send_flags::none);
    std::cout << "here after send" << std::endl;

    std::cout << "waiting for initiator do ascend data transfer perf" << std::endl;
    zmq::message_t recv_stop_msg;
    recv_status = socket.recv(recv_stop_msg);

    std::cout << "ending ascend transfer perf, releasing resources" << std::endl;
    socket.close();
    ctx.close();
    as_ctx.reset();
    for (uintptr_t addr : dev_addrs) {
        aclrtFree(reinterpret_cast<void*>(addr));
    }
    return 0;
}

int main(int argc, char** argv)
{
    gflags::ParseCommandLineFlags(&argc, &argv, false);
    std::cout << "Initializing ACL" << std::endl;
    slime::AscendDirectContext ascend_direct;

    const char* aclConfigPath = NULL;
    aclError    ret           = aclInit(aclConfigPath);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to initialize ACL");
        return ret;
    }

    ret = aclrtSetDevice(FLAGS_device_id);
    if (ret != ACL_ERROR_NONE) {
        SLIME_LOG_ERROR("Failed to set device ACL");
        return ret;
    }

    std::cout << "Running initiator or target" << std::endl;
    if (FLAGS_mode == "initiator") {
        return initiator();
    }
    else if (FLAGS_mode == "target") {
        return target();
    }

    SLIME_LOG_ERROR("Unsupported mode: must be 'initiator' or 'target'");
    exit(EXIT_FAILURE);
    return -1;
}
