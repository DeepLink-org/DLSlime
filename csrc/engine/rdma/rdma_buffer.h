#pragma once
#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_env.h"

#include <condition_variable>
#include <cstring>
#include <infiniband/verbs.h>

namespace slime {
class RDMABuffer {
    static constexpr size_t MAX_BATCH = 8192;
    friend class RDMAEndpoint;

public:
    ibv_mr* mr_list;

    RDMABuffer(AssignmentBatch& batch)
    {
        size_t assign_batch_size = batch_size();
        // Copy AssignmentBatch to RDMABuffer
        assign_batch_.reserve(assign_batch_size);
        for (int i = 0; i < assign_batch_size; ++i) {
            assign_batch_.emplace_back(batch[i]);
        }
        // Allocate MemoryRegion
        mr_list = new ibv_mr[assign_batch_size];
        memset(mr_list, 0, sizeof(ibv_mr) * assign_batch_size);
    }

    ~RDMABuffer()
    {
        delete[] mr_list;
    }

    inline size_t batch_size() const
    {
        return assign_batch_.size();
    }

    void waitSend();
    void waitRecv();

private:
    AssignmentBatch assign_batch_{};

    std::atomic<int64_t>    finished_;
    std::condition_variable finished_cv_;
};
}  // namespace slime
