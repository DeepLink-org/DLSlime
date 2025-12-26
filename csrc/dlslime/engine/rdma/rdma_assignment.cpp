#include "rdma_assignment.h"
#include "dlslime/engine/assignment.h"

#include <cstdint>
#include <stdexcept>

namespace dlslime {

void RDMAAssign::reset(
    OpCode opcode, size_t qpi, AssignmentBatch& batch, callback_fn_t callback, bool is_inline, int32_t imm_data)
{
    opcode_    = opcode;
    qpi_       = qpi;
    is_inline_ = is_inline;
    imm_data_  = imm_data;

    if (callback != nullptr) {
        callback_ = callback;
    }
    else {
        callback_ = [this](int code, int imm_data) {};
    }

    SLIME_ASSERT(callback_, "NULL CALLBACK!!");

    if (batch.size() > MAX_ASSIGN_CAPACITY) {
        SLIME_ABORT("Batch size too large for pooled object!");
    }

    batch_size_ = batch.size();
    std::memcpy(batch_, batch.data(), sizeof(Assignment) * batch_size_);
}

json RDMAAssign::dump() const
{
    json j = {{"opcode", opcode_}};
    for (int i = 0; i < batch_size_; ++i)
        j["rdma_assign"].push_back(batch_[i].dump());
    j["is_inline"] = is_inline_;
    return j;
}

std::ostream& operator<<(std::ostream& os, const RDMAAssign& assignment)
{
    os << assignment.dump().dump(2);
    return os;
}

}  // namespace dlslime
