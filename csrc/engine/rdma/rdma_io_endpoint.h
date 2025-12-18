#pragma once

#include "rdma_context.h"

#include "json.hpp"

namespace slime {

using json = nlohmann::json;

class RDMAIOEndpoint {
public:
    RDMAIOEndpoint() = default;
    ~RDMAIOEndpoint();

    explicit RDMAIOEndpoint(std::shared_ptr<RDMAContext> ctx, size_t qp_nums);

    void connect(const json& remote_endpoint_info);

    inline json endpointInfo() const;

    inline int32_t registerMemoryRegion(uintptr_t mr_key, uintptr_t ptr, size_t length);

    int32_t readBatch() const;

    int32_t writeBatch() const;

    int32_t writeWithImmBatch() const;

    int32_t RecvImm() const;

    int32_t wait(int32_t slot) const;

private:
};

}  // namespace slime