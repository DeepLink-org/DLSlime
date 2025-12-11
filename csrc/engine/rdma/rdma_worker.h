#pragma once

#include <mutex>
#include <vector>
#include <thread>
#include <atomic>
#include <memory>
#include <string>
#include <cstdint>

#include "engine/assignment.h"
#include "engine/rdma/rdma_common.h" // 包含 OpCode 等定义
// 注意：不要在这里 include rdma_endpoint_v0.h，使用前置声明避免循环依赖

namespace slime {

class RDMAEndpointV0; // 前置声明
class RDMABuffer;     // 前置声明

class RDMAWorker {
public:
    RDMAWorker(std::string dev_name, int id);
    ~RDMAWorker();

    void start();
    void stop();
    void workerLoop();
    void addEndpoint(std::shared_ptr<RDMAEndpointV0> endpoint);

private:
    struct EndpointTask {
        std::shared_ptr<RDMAEndpointV0> endpoint;
    };

    std::vector<EndpointTask> tasks_;
    std::thread               worker_thread_;
    std::atomic<bool>         running_{false};

    int         worker_id_;
    std::string device_name_;

    std::mutex add_endpoint_mutex_;
};

} // namespace slime