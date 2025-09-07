#include "rdma_assignment.h"

#include <stdexcept>

namespace slime {

RDMAAssignment::RDMAAssignment(OpCode opcode, AssignmentBatch& batch, std::shared_ptr<rdma_metrics_t> metrics, callback_fn_t callback)
{
    opcode_ = opcode;

    batch_size_ = batch.size();
    batch_      = new Assignment[batch_size_];

    size_t cnt = 0;
    for (const Assignment& assignment : batch) {
        batch_[cnt].mr_key        = assignment.mr_key;
        batch_[cnt].source_offset = assignment.source_offset;
        batch_[cnt].target_offset = assignment.target_offset;
        batch_[cnt].length        = assignment.length;
        cnt += 1;
    }
    if (metrics == nullptr)
        metrics_ = std::make_shared<rdma_metrics_t>();
    else
        metrics_ = metrics;
    callback_info_ = std::make_shared<callback_info_t>(opcode, batch_size_, metrics, callback);
}

void RDMAAssignment::wait()
{
    callback_info_->wait();
}

bool RDMAAssignment::query()
{
    return callback_info_->query();
}

json RDMAAssignment::dump() const
{
    json j;
    for (int i = 0; i < batch_size_; ++i)
        j["rdma_assign"].push_back(batch_[i].dump().dump());
    return j;
}

std::ostream& operator<<(std::ostream& os, const RDMAAssignment& assignment)
{
    os << assignment.dump().dump(2);
    return os;
}

RDMASchedulerAssignment::~RDMASchedulerAssignment() {}

void RDMASchedulerAssignment::wait()
{
    for (RDMAAssignmentSharedPtr& rdma_assignment : rdma_assignment_batch_) {
        rdma_assignment->wait();
    }
    return;
}

void RDMASchedulerAssignment::query()
{
    throw std::runtime_error("Not Implemented.");
}

json RDMASchedulerAssignment::dump() const
{
    json j;
    for (int i = 0; i < rdma_assignment_batch_.size(); ++i) {
        j["rdma_sch_assign"].push_back(rdma_assignment_batch_[i]->dump());
    }
    return j;
}

std::ostream& operator<<(std::ostream& os, const RDMASchedulerAssignment& assignment)
{
    os << assignment.dump().dump(2);
    return os;
}

}  // namespace slime
