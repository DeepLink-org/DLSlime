#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dlslime {

class RDMAEndpoint;  // Forward declaration

class RDMAWorker {
public:
    RDMAWorker(std::string dev_name, int id);
    RDMAWorker(int socket_id, int id);
    ~RDMAWorker();

    void start();
    void stop();

    // Registers a Unified Endpoint (supporting both Msg and IO operations).
    // Thread-safe, non-blocking for the worker loop.
    // Returns the assigned Endpoint ID.
    int64_t addEndpoint(std::shared_ptr<RDMAEndpoint> endpoint);
    void    removeEndpoint(std::shared_ptr<RDMAEndpoint> endpoint);

    int32_t getSocketId() const
    {
        return socket_id_;
    }

private:
    // Main loop function executed by the worker thread.
    void workerLoop();

    // Helper to merge staging endpoints into the main list
    void _merge_new_endpoints();

    // --- Worker Thread Private Data (No Lock Needed) ---
    // Only accessed by worker_thread_
    std::vector<std::shared_ptr<RDMAEndpoint>> endpoints_;

    // --- Thread-Safe Staging Area ---
    // Accessed by control thread (addEndpoint) and worker thread (merge)
    std::mutex                                 staging_mutex_;
    std::vector<std::shared_ptr<RDMAEndpoint>> staging_endpoints_;
    std::vector<std::shared_ptr<RDMAEndpoint>> pending_removals_;

    // Atomic counter for generating unique endpoint IDs
    std::atomic<int64_t> next_endpoint_id_{0};

    // Atomic flag to notify worker of new items
    // alignas(64) prevents false sharing cache line bouncing
    alignas(64) std::atomic<bool> has_new_endpoints_{false};

    // --- Thread Control ---
    std::thread       worker_thread_;
    std::atomic<bool> running_{false};

    int     worker_id_;
    int32_t socket_id_;
};

}  // namespace dlslime
