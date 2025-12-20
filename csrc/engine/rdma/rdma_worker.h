#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace slime {

class RDMAEndpointV0; // Send/Recv Endpoint
class RDMAIOEndpoint; // One-Sided (Read/Write) Endpoint

class RDMAWorker {
public:
    RDMAWorker(std::string dev_name, int id);
    ~RDMAWorker();

    void start();
    void stop();
    
    // Registers a V0 (Send/Recv) endpoint.
    // Thread-safe.
    void addEndpoint(std::shared_ptr<RDMAEndpointV0> endpoint);

    // Registers a IO (Read/Write) endpoint.
    // Thread-safe.
    void addIOEndpoint(std::shared_ptr<RDMAIOEndpoint> endpoint);

private:
    // Main loop function executed by the worker thread.
    void workerLoop();

    // Protects access to task lists during dynamic additions.
    std::mutex add_endpoint_mutex_;
    
    // Stored separately to allow compiler inlining (Static Polymorphism)
    // avoiding virtual function overhead in the hot path.
    std::vector<std::shared_ptr<RDMAEndpointV0>> v0_tasks_;
    std::vector<std::shared_ptr<RDMAIOEndpoint>> io_tasks_;

    std::thread       worker_thread_;
    std::atomic<bool> running_{false};

    int         worker_id_;
    std::string device_name_;
};

} // namespace slime
