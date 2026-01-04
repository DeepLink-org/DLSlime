#pragma once

#include "dlslime/engine/rdma/memory_pool.h"
#include "dlslime/json.hpp"
#include "rdma_context.h"

namespace dlslime {

enum RDMAChannelState {
    Initialized,
    Connected,
    Destroyed
};

class RDMAChannel {
    inline static constexpr int      UNDEFINED_QPI      = -1;
    inline static constexpr uint32_t UNDEFINED_IMM_DATA = -1;

public:
    RDMAChannel() = delete;
    RDMAChannel(std::shared_ptr<RDMAMemoryPool> pool): memory_pool_(pool) {}

    ~RDMAChannel()
    {
        reset();
    }

    int32_t init(std::shared_ptr<RDMAContext> ctx, size_t num_qp, int32_t inline_size);
    int32_t connect(json channel_info);

    json channelInfo() const;

    /* Async RDMA SendRecv */
    int64_t post_send_batch(int qpi, RDMAAssign* assign);
    int64_t post_recv_batch(int qpi, RDMAAssign* assign);

    /* Async RDMA Read */
    int64_t post_rc_oneside_batch(int qpi, RDMAAssign* assign);

    int32_t reset();

    inline int32_t num_channel()
    {
        return qp_.size();
    }

private:
    int32_t modify_qp_to_r2r();
    int32_t modify_qp_to_r2s();

    std::vector<struct ibv_qp*> qp_{};

    /* RDMA Exchange Information */
    std::vector<rdma_info_t> remote_rdma_info_;
    std::vector<rdma_info_t> local_rdma_info_;

    /* polling pool */
    std::vector<std::vector<ibv_send_wr>> send_wr_pool_;
    std::vector<std::vector<ibv_recv_wr>> recv_wr_pool_;
    std::vector<std::vector<ibv_sge>>     send_sge_pool_;
    std::vector<std::vector<ibv_sge>>     recv_sge_pool_;

    std::shared_ptr<RDMAContext>    ctx_{};
    std::shared_ptr<RDMAMemoryPool> memory_pool_{};

    RDMAChannelState state{RDMAChannelState::Destroyed};
};
}  // namespace dlslime
