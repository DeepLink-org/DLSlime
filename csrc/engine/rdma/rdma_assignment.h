#pragma once

#include "engine/assignment.h"

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <emmintrin.h>
#include <infiniband/verbs.h>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "engine/rdma/rdma_env.h"
#include "json.hpp"
#include "logging.h"

namespace slime {

using json = nlohmann::json;

class RDMAAssign;
class RDMAAssignHandler;

using callback_fn_t = std::function<void(int, int)>;

// TODO (Jimy): add timeout check
const std::chrono::milliseconds kNoTimeout = std::chrono::milliseconds::zero();

static const std::map<OpCode, ibv_wr_opcode> ASSIGN_OP_2_IBV_WR_OP = {
    {OpCode::READ, ibv_wr_opcode::IBV_WR_RDMA_READ},
    {OpCode::WRITE, ibv_wr_opcode::IBV_WR_RDMA_WRITE},
    {OpCode::SEND, ibv_wr_opcode::IBV_WR_SEND},
    {OpCode::SEND_WITH_IMM, ibv_wr_opcode::IBV_WR_SEND_WITH_IMM},
    {OpCode::WRITE_WITH_IMM, ibv_wr_opcode::IBV_WR_RDMA_WRITE_WITH_IMM},
};

struct alignas(64) RDMAAssign {
    static constexpr size_t MAX_ASSIGN_CAPACITY = 4096;
    friend class RDMAContext;
    friend class RDMAEndpointV0;
    friend std::ostream& operator<<(std::ostream& os, const RDMAAssign& assignment);

public:
    typedef enum: int {
        SUCCESS                   = 0,
        ASSIGNMENT_BATCH_OVERFLOW = 400,
        UNKNOWN_OPCODE            = 401,
        TIME_OUT                  = 402,
        FAILED                    = 403,
    } CALLBACK_STATUS;

    RDMAAssign() = default;
    RDMAAssign(OpCode opcode, AssignmentBatch& batch, callback_fn_t callback = nullptr, bool is_inline = false);
    void
    reset(OpCode opcode, size_t qpi, AssignmentBatch& batch, callback_fn_t callback = nullptr, bool is_inline = false);

    ~RDMAAssign() {}

    inline size_t batch_size()
    {
        return batch_size_;
    };

    void wait();
    bool query();

    std::chrono::duration<double> latency()
    {
        return std::chrono::duration<double>::zero();
    }

    json dump() const;

private:
    callback_fn_t callback_{};

    size_t qpi_;

    OpCode opcode_;

    size_t     batch_size_{0};
    Assignment batch_[MAX_ASSIGN_CAPACITY];

    int32_t imm_data_{0};
    bool    with_imm_data_{false};

    bool is_inline_;

    std::atomic<bool> in_use_{false};
    std::atomic<int>  finished_{0};
};

class RDMAAssignHandler {
    friend std::ostream& operator<<(std::ostream& os, const RDMAAssignHandler& assignment);

public:
    RDMAAssignHandler(std::vector<RDMAAssign*>& rdma_assignment_batch):
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
    std::vector<RDMAAssign*> rdma_assignment_batch_{};
};

}  // namespace slime
