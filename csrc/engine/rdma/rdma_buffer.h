#pragma once
#include "device/signal.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_endpoint_v0.h"

#include <condition_variable>
#include <cstdint>
#include <infiniband/verbs.h>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "logging.h"
#include "rdma_common.h"

namespace slime {

class RDMAEndpoint;

struct alignas(64) RDMABuffer: public std::enable_shared_from_this<RDMABuffer> {
    friend class RDMAEndpointV0;

public:
    RDMABuffer() = default;

    RDMABuffer(uintptr_t ptr, size_t offset, size_t data_size):
        view_(storage_view_t{ptr + offset, 0, data_size})
    {
    }

    ~RDMABuffer() = default;

    bool waitSend();
    bool waitRecv();

private:
    storage_view_t view_;

    std::atomic<int> send_completed_{0};
    std::atomic<int> recv_completed_{0};

    std::atomic<int32_t> slot_id_{0};

    uint64_t                                     num_pack_{1};
    std::shared_ptr<slime::device::DeviceSignal> signal_;
};

}  // namespace slime
