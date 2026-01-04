#pragma once

#include <emmintrin.h>
#include <infiniband/verbs.h>

#include <algorithm>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>

#include "dlslime/engine/assignment.h"
#include "dlslime/json.hpp"
#include "dlslime/logging.h"
#include "rdma_context.h"
#include "rdma_env.h"

namespace dlslime {

using json = nlohmann::json;

class RDMAAssign;

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
    friend class SendFuture;
    friend class RecvFuture;
    friend class ReadWriteFuture;
    friend class ImmRecvFuture;
    friend class RDMAContext;
    friend class RDMAChannel;
    friend class RDMAIOEndpoint;
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
    void reset(OpCode           opcode,
               size_t           qpi,
               AssignmentBatch& batch,
               callback_fn_t    callback  = nullptr,
               bool             is_inline = false,
               int32_t          imm_data  = 0);

    ~RDMAAssign() {}

    inline size_t batch_size()
    {
        return batch_.size();
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

    AssignmentBatch batch_;

    int32_t imm_data_{0};
    bool    with_imm_data_{false};

    bool is_inline_{false};
};

}  // namespace dlslime
