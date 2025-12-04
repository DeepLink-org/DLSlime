#include "rdma_assignment.h"

#include <stdexcept>

namespace slime {

RDMAAssign::RDMAAssign(OpCode opcode, AssignmentBatch& batch, callback_fn_t callback)
{
    opcode_ = opcode;

    batch_size_ = batch.size();

    batch_      = new Assignment[batch_size_];
    std::move(batch.begin(), batch.end(), batch_);

    callback_info_ = std::make_shared<callback_info_t>(opcode, batch_size_, callback, batch_);
}

void RDMAAssign::wait()
{
    callback_info_->wait();
}

bool RDMAAssign::query()
{
    return callback_info_->query();
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
    for (RDMAAssignSharedPtr& rdma_assignment : rdma_assignment_batch_) {
        rdma_assignment->wait();
    }
    return;
}

bool RDMAAssignHandler::query()
{
    for (RDMAAssignSharedPtr& rdma_assignment : rdma_assignment_batch_) {
        if (rdma_assignment->query()) return true;
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
