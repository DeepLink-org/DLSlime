#pragma once

#include "engine/assignment.h"

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

typedef struct callback_info {
    callback_info() = default;
    callback_info(OpCode opcode, size_t batch_size, callback_fn_t callback): opcode_(opcode), batch_size_(batch_size)
    {
        if (callback)
            callback_ = std::move(callback);
    }

    callback_fn_t callback_{[this](int code) {
        std::unique_lock<std::mutex> lock(mutex_);
        finished_.fetch_add(1, std::memory_order_relaxed);
        done_cv_.notify_one();
    }};

    OpCode opcode_;

    size_t batch_size_;

    std::atomic<int>        finished_{0};
    std::condition_variable done_cv_;
    std::mutex              mutex_;

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

    json                 dump() const;
    friend std::ostream& operator<<(std::ostream& os, const RDMAAssignment& assignment);

private:
    OpCode opcode_;

    Assignment* batch_{nullptr};
    size_t      batch_size_;

    callback_info_t* callback_info_;
};

class RDMASchedulerAssignment {
    friend class RDMAScheduler;

public:
    RDMASchedulerAssignment(RDMAAssignmentSharedPtrBatch& rdma_assignment_batch):
        rdma_assignment_batch_(std::move(rdma_assignment_batch))
    {
    }
    ~RDMASchedulerAssignment();

    void query();
    void wait();

    json                 dump() const;
    friend std::ostream& operator<<(std::ostream& os, const RDMASchedulerAssignment& assignment);

private:
    RDMAAssignmentSharedPtrBatch rdma_assignment_batch_{};
};

}  // namespace slime
