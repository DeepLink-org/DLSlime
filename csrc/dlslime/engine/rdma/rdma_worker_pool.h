#pragma once

#include <unordered_map>

#include "dlslime/logging.h"
#include "rdma_worker.h"

namespace dlslime {
class GlobalWorkerManager {
public:
    static GlobalWorkerManager& instance()
    {
        static GlobalWorkerManager instance;
        return instance;
    }

    std::shared_ptr<RDMAWorker> get_default_worker(int numa_node = 0)
    {
        std::lock_guard<std::mutex> lock(mutex_);

        if (default_workers_.find(numa_node) == default_workers_.end()) {
            SLIME_LOG_INFO("Initialize a new rdma worker");
            auto worker = std::make_shared<RDMAWorker>(numa_node, 0);
            worker->start();
            default_workers_[numa_node] = worker;
        }
        return default_workers_[numa_node];
    }

private:
    GlobalWorkerManager() = default;
    std::mutex                                           mutex_;
    std::unordered_map<int, std::shared_ptr<RDMAWorker>> default_workers_;
};
}  // namespace dlslime
