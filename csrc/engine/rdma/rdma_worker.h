#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace slime {

class RDMAEndpointV0; // Forward declaration

class RDMAWorker {
public:
    RDMAWorker(std::string dev_name, int id);
    ~RDMAWorker();

    void start();
    void stop();
    
    // Registers an endpoint to be polled by this worker.
    // Thread-safe.
    void addEndpoint(std::shared_ptr<RDMAEndpointV0> endpoint);

private:
    // Main loop function executed by the worker thread.
    void workerLoop();

    struct EndpointTask {
        std::shared_ptr<RDMAEndpointV0> endpoint;
    };

    // Protects access to tasks_ during dynamic additions.
    std::mutex add_endpoint_mutex_;
    std::vector<EndpointTask> tasks_;

    std::thread       worker_thread_;
    std::atomic<bool> running_{false};

    int         worker_id_;
    std::string device_name_;
};

} // namespace slime
