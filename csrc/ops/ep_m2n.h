#pragma once

#include <memory>
#include <string>
#include <vector>
#include <tuple>

#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_common.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_scheduler.h"
#include "utils/json.hpp"

namespace slime {

using json = nlohmann::json;

class EPM2N {
public:
    typedef enum: int {
        M = 0,
        N = 1
    } EPM2NRole;

    EPM2N(
        EPM2NRole role,
        int rank,
        size_t m_size,
        size_t n_size,
        size_t max_bs_per_m_rank,
        size_t num_experts,
        size_t num_qp_per_rank,
        std::string device_name,
        std::string link_type = "RoCE"
    ) : role_(role), rank_(rank), m_size_(m_size), n_size_(n_size), max_bs_per_m_rank_(max_bs_per_m_rank), num_experts_(num_experts), num_qp_per_rank_(num_qp_per_rank){
        if (role_ == EPM2NRole::M)
            ctx_ = std::make_unique<RDMAContext>(num_qp_per_rank * n_size, true);
        else
            ctx_ = std::make_unique<RDMAContext>(num_qp_per_rank * m_size, true);
        ctx_->init(device_name, 1, link_type);
    }

    void registerBuffer(std::vector<std::tuple<size_t, size_t, size_t>> views);

    void connect(std::vector<json> m2n_endpoint_info);

    RDMASchedulerAssignmentSharedPtr m2nSend(std::vector<size_t> length);

    RDMASchedulerAssignmentSharedPtr m2nRecv();

    void n2mSend(std::vector<size_t> lencth);

    void n2mRecv();

    const json endpoint_info() const {
        return ctx_->endpoint_info();
    }

private:
    size_t get_buffer_size();

    std::unique_ptr<RDMAContext> ctx_;

    EPM2NRole role_;

    size_t rank_;
    size_t m_size_;
    size_t n_size_;

    size_t num_experts_;
    size_t max_bs_per_m_rank_;

    size_t num_qp_per_rank_;

};
}
