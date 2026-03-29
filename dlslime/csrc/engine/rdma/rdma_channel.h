#pragma once

#include <vector>

#include "dlslime/csrc/engine/rdma/memory_pool.h"
#include "dlslime/csrc/engine/rdma/remote_memory_pool.h"
#include "rdma_config.h"
#include "rdma_context.h"

namespace dlslime {

class RDMAMemoryPool;

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
    RDMAChannel(std::shared_ptr<RDMAMemoryPool> local_pool, std::shared_ptr<RDMARemoteMemoryPool> remote_pool):
        local_pool_(local_pool), remote_pool_(remote_pool)
    {
    }

    // Legacy/Convenience constructor for local-only? Or maybe default remote pool?
    // Let's force providing both.

    ~RDMAChannel()
    {
        reset();
    }

    int32_t init(std::shared_ptr<RDMAContext> ctx, size_t num_qp, int32_t inline_size);
    int32_t connect(json channel_info);

    json channelInfo() const;

    /* Async RDMA SendRecv - local_pool injected at call site (meta_pool or user_pool) */
    int64_t post_send_batch(int qpi, RDMAAssign* assign, std::shared_ptr<RDMAMemoryPool> local_pool);
    int64_t post_recv_batch(int qpi, RDMAAssign* assign, std::shared_ptr<RDMAMemoryPool> local_pool);

    /* Async RDMA Read - local_pool injected at call site */
    int64_t post_rc_oneside_batch(int qpi, RDMAAssign* assign, std::shared_ptr<RDMAMemoryPool> local_pool);

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

    std::shared_ptr<RDMAContext>          ctx_{};
    std::shared_ptr<RDMAMemoryPool>       local_pool_{};
    std::shared_ptr<RDMARemoteMemoryPool> remote_pool_{};

    RDMAChannelState state{RDMAChannelState::Destroyed};
};
}  // namespace dlslime
