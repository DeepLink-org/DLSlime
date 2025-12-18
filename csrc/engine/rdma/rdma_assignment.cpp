#include "rdma_assignment.h"

#include <cstdint>
#include <stdexcept>

namespace slime {
void RDMAAssign::reset(OpCode opcode, size_t qpi, AssignmentBatch& batch, callback_fn_t callback, bool is_inline)
{
    opcode_    = opcode;
    qpi_       = qpi;
    is_inline_ = is_inline;

    if (callback != nullptr) {
        callback_ = callback;
    }
    else {
        callback_ = [this](int code, int imm_data) {
            if (code != 0) {
                for (int i = 0; i < batch_size_; ++i) {
                    SLIME_LOG_ERROR("ERROR ASSIGNMENT: Batch: ", batch_[i].dump(), ", OpCode: ", uint64_t(opcode_));
                }
            }
            finished_.fetch_add(1, std::memory_order_release);
        };
    }

    SLIME_ASSERT(callback_, "NULL CALLBACK!!");

    finished_.store(0, std::memory_order_release);

    if (batch.size() > MAX_ASSIGN_CAPACITY) {
        SLIME_ABORT("Batch size too large for pooled object!");
    }

    batch_size_ = batch.size();
    std::memcpy(batch_, batch.data(), sizeof(Assignment) * batch_size_);
}

void RDMAAssign::wait()
{
    while (finished_.load(std::memory_order_acquire) == 0)
        _mm_pause();
}

bool RDMAAssign::query()
{
    return finished_.load() > 0;
}

json RDMAAssign::dump() const
{
    json j = {{"opcode", opcode_}};
    for (int i = 0; i < batch_size_; ++i)
        j["rdma_assign"].push_back(batch_[i].dump().dump());
    return j;
}

std::ostream& operator<<(std::ostream& os, const RDMAAssign& assignment)
{
    os << assignment.dump().dump(2);
    return os;
}

RDMAAssignHandler::~RDMAAssignHandler() {}

void RDMAAssignHandler::wait()
{
    for (RDMAAssign*& rdma_assignment : rdma_assignment_batch_) {
        rdma_assignment->wait();
    }
    return;
}

bool RDMAAssignHandler::query()
{
    for (RDMAAssign*& rdma_assignment : rdma_assignment_batch_) {
        if (rdma_assignment->query())
            return true;
    }
    return false;
}

json RDMAAssignHandler::dump() const
{
    json j;
    for (int i = 0; i < rdma_assignment_batch_.size(); ++i) {
        j["rdma_sch_assign"].push_back(rdma_assignment_batch_[i]->dump());
    }
    return j;
}

std::ostream& operator<<(std::ostream& os, const RDMAAssignHandler& assignment)
{
    os << assignment.dump().dump(2);
    return os;
}

}  // namespace slime
