#pragma once

#include "engine/assignment.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <infiniband/verbs.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "json.hpp"
#include "logging.h"

namespace slime {

using json = nlohmann::json;

class RDMAAssign;
class RDMAAssignHandler;

using callback_fn_t            = std::function<void(int, int)>;
using RDMAAssignSharedPtr      = std::shared_ptr<RDMAAssign>;
using RDMAAssignSharedPtrBatch = std::vector<RDMAAssignSharedPtr>;

// TODO (Jimy): add timeout check
const std::chrono::milliseconds kNoTimeout = std::chrono::milliseconds::zero();

static const std::map<OpCode, ibv_wr_opcode> ASSIGN_OP_2_IBV_WR_OP = {
    {OpCode::READ, ibv_wr_opcode::IBV_WR_RDMA_READ},
    {OpCode::WRITE, ibv_wr_opcode::IBV_WR_RDMA_WRITE},
    {OpCode::SEND, ibv_wr_opcode::IBV_WR_SEND},
    {OpCode::SEND_WITH_IMM, ibv_wr_opcode::IBV_WR_SEND_WITH_IMM},
    {OpCode::WRITE_WITH_IMM, ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM},
};

typedef struct callback_info {
    callback_info() = default;
    callback_info(OpCode opcode, size_t batch_size, callback_fn_t callback, Assignment* batch):
        opcode_(opcode), batch_size_(batch_size), batch_(batch)
    {
        if (callback) {
            callback_ = std::move(callback);
        }
    }

    callback_fn_t callback_{[this](int code, int imm_data) {
        if (code != 0) {
            for (int i = 0; i < batch_size_; ++i) {
                SLIME_LOG_ERROR("ERROR ASSIGNMENT: ", batch_[i].dump());
            }
        }
        std::unique_lock<std::mutex> lock(mutex_);
        finished_.fetch_add(1, std::memory_order_relaxed);
        done_cv_.notify_one();
    }};

    OpCode opcode_;

    size_t batch_size_;

    std::atomic<int>        finished_{0};
    std::condition_variable done_cv_;
    std::mutex              mutex_;
    Assignment*             batch_;

    void wait()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        done_cv_.wait(lock, [this]() { return finished_ > 0; });
        return;
    }

    std::chrono::duration<double> latency()
    {
        return std::chrono::duration<double>::zero();
    }

    bool query()
    {
        return finished_.load() > 0;
    }
} callback_info_t;

class RDMAAssign {
    friend class RDMAContext;
    friend std::ostream& operator<<(std::ostream& os, const RDMAAssign& assignment);

public:
    RDMAAssign(OpCode opcode, AssignmentBatch& batch, callback_fn_t callback = nullptr);

    ~RDMAAssign()
    {
        delete[] batch_;
    }

    inline size_t batch_size()
    {
        return batch_size_;
    };

    void wait();
    bool query();

    std::chrono::duration<double> latency()
    {
        return callback_info_->latency();
    }

    json dump() const;

private:
    OpCode opcode_;

    Assignment* batch_{nullptr};
    size_t      batch_size_;

    int32_t imm_data_{0};
    bool    with_imm_data_{false};

    std::shared_ptr<callback_info_t> callback_info_;
};

class RDMAAssignHandler {
    friend std::ostream& operator<<(std::ostream& os, const RDMAAssignHandler& assignment);

public:
    RDMAAssignHandler(RDMAAssignSharedPtrBatch& rdma_assignment_batch):
        rdma_assignment_batch_(std::move(rdma_assignment_batch))
    {
    }
    ~RDMAAssignHandler();

    bool query();
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
    RDMAAssignSharedPtrBatch rdma_assignment_batch_{};
};

}  // namespace slime
