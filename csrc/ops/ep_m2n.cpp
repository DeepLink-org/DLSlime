#include "ep_m2n.h"
#include "utils/logging.h"

namespace slime {

void EPM2N::connect(std::vector<json> m2n_endpoint_info)  {
    std::vector<json> rdma_info;
    json remote_info = {
        {"mr_info", {}}, {"rdma_info", json::array()}
    };
    for (const auto &info: m2n_endpoint_info) {
        remote_info["mr_info"].update(info["mr_info"]);
        for (int i = 0; i < num_qp_per_rank_; ++i)
            rdma_info.emplace_back(info["rdma_info"][rank_ * num_qp_per_rank_ + i]);
    }
    remote_info["rdma_info"] = rdma_info;
    std::cout << "connecting rank :" << rank_ << std::endl;
    ctx_->connect(remote_info);
    ctx_->launch_future();
}

void EPM2N::registerBuffer(std::vector<std::tuple<size_t, size_t, size_t>> views) {
    for (int i = 0; i < views.size(); ++i) {
        std::string mr_key = "buffer_" + std::to_string(rank_) + "_" + std::to_string(i);
        ctx_->register_memory_region(mr_key, std::get<0>(views[i])  + std::get<1>(views[i]), std::get<2>(views[i]));
    }
}

RDMASchedulerAssignmentSharedPtr EPM2N::m2nSend(std::vector<size_t> length) {
    SLIME_ASSERT(role_ == EPM2NRole::M, "only supported in M Side");
    RDMAAssignmentSharedPtrBatch rdma_assign_batch;
    for (int i = 0; i < length.size(); ++i) {
        for (int j = 0; j < num_qp_per_rank_; ++j) {
            std::string mr_key = "buffer_" + std::to_string(rank_) + "_" + std::to_string(i);
            std::string remote_mr_key = "buffer_" + std::to_string(i) + "_" + std::to_string(rank_);
            AssignmentBatch batch{
                Assignment(mr_key, remote_mr_key, length[i] / num_qp_per_rank_ * j, length[i] / num_qp_per_rank_ * j, length[i] / num_qp_per_rank_)
            };
            RDMAAssignmentSharedPtr rdma_assign = std::make_shared<RDMAAssignment>(OpCode::WRITE_WITH_IMM, batch, length[i]);
            ctx_->post_rc_oneside_batch( i * num_qp_per_rank_ + j, rdma_assign);
            rdma_assign_batch.emplace_back(rdma_assign);
        }
    }
    return std::make_shared<RDMASchedulerAssignment>(rdma_assign_batch);
}

RDMASchedulerAssignmentSharedPtr EPM2N::m2nRecv() {
    SLIME_ASSERT(role_ == EPM2NRole::N, "only supported in N Side");
    RDMAAssignmentSharedPtrBatch rdma_assign_batch;
    for (int i = 0; i < m_size_ ; ++i) {
        for (int j = 0; j < num_qp_per_rank_; ++j) {
            std::string mr_key = "buffer_" + std::to_string(rank_) + "_" + std::to_string(i);
            std::string remote_mr_key = "buffer_" + std::to_string(i) + "_" + std::to_string(rank_);
            AssignmentBatch batch{
                Assignment(mr_key, remote_mr_key, 0, 0, 0)
            };
            RDMAAssignmentSharedPtr rdma_assign = std::make_shared<RDMAAssignment>(OpCode::RECV, batch);
            ctx_->post_recv_batch(i * num_qp_per_rank_ + j, rdma_assign);
            rdma_assign_batch.emplace_back(rdma_assign);
        }
    }
    return std::make_shared<RDMASchedulerAssignment>(rdma_assign_batch);
}

void EPM2N::n2mSend(std::vector<size_t> length) {}

void EPM2N::n2mRecv() {}
}
