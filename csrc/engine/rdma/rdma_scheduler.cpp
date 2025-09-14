#include "rdma_scheduler.h"

#include <algorithm>
#include <cstddef>
#include <random>
#include <stdexcept>
#include <string>
#include <vector>

#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "utils/logging.h"
#include "utils/utils.h"

namespace slime {

const int64_t RDMAScheduler::SPLIT_ASSIGNMENT_BYTES;
const int64_t RDMAScheduler::SPLIT_ASSIGNMENT_BATCH_SIZE;
const int     RDMAScheduler::PORT_EACH_DEVICE;

RDMAScheduler::RDMAScheduler(const std::vector<std::string>& dev_names_args)
{
    // Get all available RDMA devices
    SLIME_LOG_INFO("Initialize an RDMA Scheduler.");

    std::vector<std::string> dev_names;
    if (dev_names_args.empty()) {
        dev_names = available_nic();
    }
    else {
        dev_names = dev_names_args;
    }

    for (const std::string& dev_name : dev_names) {
        SLIME_LOG_INFO("device name: ", dev_name);
    }

    size_t count = dev_names.size() * PORT_EACH_DEVICE;
    rdma_ctxs_   = std::vector<RDMAContext>(count);
    int index    = 0;
    for (const std::string& name : dev_names) {
        for (int ib = 1; ib <= PORT_EACH_DEVICE; ++ib) {
            rdma_ctxs_[index].init(name, ib, "RoCE");
            ++index;
        }
    }

    std::srand(std::time(nullptr));
}

RDMAScheduler::~RDMAScheduler()
{
    for (RDMAContext& ctx : rdma_ctxs_) {
        ctx.stop_future();
    }
}

int64_t RDMAScheduler::register_memory_region(const std::string& mr_key, uintptr_t data_ptr, uint64_t length)
{
    // Register the memory region in each RDMA context
    for (RDMAContext& rdma_ctx : rdma_ctxs_) {
        rdma_ctx.register_memory_region(mr_key, data_ptr, length);
    }
    return 0;
}

int RDMAScheduler::connect(const json& remote_info)
{
    SLIME_ASSERT_EQ(
        rdma_ctxs_.size(), remote_info.size(), "Currently only support two nodes with same number of RDMA devices");
    for (int i = 0; i < rdma_ctxs_.size(); ++i) {
        rdma_ctxs_[i].connect(remote_info[i]);
        rdma_ctxs_[i].launch_future();
    }
    return 0;
}

RDMASchedulerAssignmentSharedPtr RDMAScheduler::submitAssignment(OpCode opcode, AssignmentBatch& batch)
{
    // size_t batch_size = batch.size();
    // rdma_index_to_assignments_.clear();
    // size_t total_ctxs = rdma_ctxs_.size();

    // for (int i = 0; i < batch_size; ++i) {
    //     rdma_index_to_assignments_[selectRdma()].push_back(batch[i]);
    // }

    // RDMAAssignmentSharedPtrBatch rdma_assignment_batch;
    // for (int i = 0; i < rdma_ctxs_.size(); i++) {
    //     if (!rdma_index_to_assignments_[i].empty()) {
    //         RDMAAssignmentSharedPtr rdma_assignment = rdma_ctxs_[i].submit(opcode, rdma_index_to_assignments_[i]);
    //         rdma_assignment_batch.push_back(rdma_assignment);
    //     }
    // }

    RDMAAssignmentSharedPtrBatch rdma_assignment_batch;
    auto assignment = rdma_ctxs_[selectRdma()].submit(opcode, batch);
    for (int i = 0; i < assignment->rdma_assignment_batch_.size(); ++i)
        rdma_assignment_batch.push_back(assignment->rdma_assignment_batch_[i]);

    return std::make_shared<RDMASchedulerAssignment>(rdma_assignment_batch);
}

int RDMAScheduler::selectRdma()
{
    // Simplest round robin, we could enrich it in the future
    last_rdma_selection_ = (last_rdma_selection_ + 1) % rdma_ctxs_.size();
    return last_rdma_selection_;
}

json RDMAScheduler::scheduler_info()
{
    json json_info = json();
    for (int i = 0; i < rdma_ctxs_.size(); ++i) {
        json_info[i] = rdma_ctxs_[i].endpoint_info();
    }
    return json_info;
}

}  // namespace slime
