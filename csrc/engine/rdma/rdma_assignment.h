#pragma once

#include "engine/assignment.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "utils/json.hpp"

namespace slime {

using json = nlohmann::json;

class RDMAAssignment;
class RDMASchedulerAssignment;

using callback_fn_t                = std::function<void(int)>;
using RDMAAssignmentSharedPtr      = std::shared_ptr<RDMAAssignment>;
using RDMAAssignmentSharedPtrBatch = std::vector<RDMAAssignmentSharedPtr>;

// TODO (Jimy): add timeout check
const std::chrono::milliseconds kNoTimeout = std::chrono::milliseconds::zero();

typedef struct RDMAMetrics {
    std::chrono::steady_clock::time_point arrive{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point done{std::chrono::milliseconds::zero()};

    std::chrono::duration<double> latency()
    {
        return (done - arrive);
    }

} rdma_metrics_t;

typedef struct callback_info {
    callback_info() = default;
    callback_info(OpCode opcode, size_t batch_size, callback_fn_t callback): opcode_(opcode), batch_size_(batch_size)
    {
        if (callback)
            callback_ = std::move(callback);

        metrics.arrive = std::chrono::steady_clock::now();
    }

    callback_fn_t callback_{[this](int code) {
        std::unique_lock<std::mutex> lock(mutex_);
        finished_.fetch_add(1, std::memory_order_relaxed);
        metrics.done = std::chrono::steady_clock::now();
        done_cv_.notify_one();
    }};

    OpCode opcode_;

    size_t batch_size_;

    std::atomic<int>        finished_{0};
    std::condition_variable done_cv_;
    std::mutex              mutex_;

    rdma_metrics_t metrics;

    void wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        done_cv_.wait(lock, [this]() { return finished_ > 0; });
        return;
    }

    bool query()
    {
        return finished_.load() > 0;
    }
} callback_info_t;

class RDMAAssignment {
    friend class RDMAContext;
    friend std::ostream& operator<<(std::ostream& os, const RDMAAssignment& assignment);

public:
    RDMAAssignment(OpCode opcode, AssignmentBatch& batch, callback_fn_t callback = nullptr);

    ~RDMAAssignment()
    {
        delete[] batch_;
        delete callback_info_;
    }

    inline size_t batch_size()
    {
        return batch_size_;
    };

    void wait();
    bool query();

    std::chrono::duration<double> latency()
    {
        return callback_info_->metrics.latency();
    }

    json dump() const;

private:
    OpCode opcode_;

    Assignment* batch_{nullptr};
    size_t      batch_size_;

    callback_info_t* callback_info_;
};

class RDMASchedulerAssignment {
    friend class RDMAScheduler;
    friend std::ostream& operator<<(std::ostream& os, const RDMASchedulerAssignment& assignment);

public:
    RDMASchedulerAssignment(RDMAAssignmentSharedPtrBatch& rdma_assignment_batch):
        rdma_assignment_batch_(std::move(rdma_assignment_batch))
    {
    }
    ~RDMASchedulerAssignment();

    void query();
    void wait();

    std::chrono::duration<double> latency()
    {
        std::vector<std::chrono::duration<double>> durations;
        for (int i = 0; i < rdma_assignment_batch_.size(); ++i) {
            durations.push_back(rdma_assignment_batch_[i]->latency());
        }
        if (durations.empty()) {
            return std::chrono::duration<double>::zero();
        }
        return *std::max_element(durations.begin(), durations.end());
    }

    json dump() const;

private:
    RDMAAssignmentSharedPtrBatch rdma_assignment_batch_{};
};

}  // namespace slime
