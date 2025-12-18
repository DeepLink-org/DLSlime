#include "rdma_io_endpoint.h"

namespace slime {
RDMAIOEndpoint::RDMAIOEndpoint(std::shared_ptr<RDMAContext> ctx, size_t qp_nums) {}

void RDMAIOEndpoint::connect(const json& remote_endpoint_info){};

inline json RDMAIOEndpoint::endpointInfo() const {};

inline int32_t RDMAIOEndpoint::registerMemoryRegion(uintptr_t mr_key, uintptr_t ptr, size_t length){};

int32_t RDMAIOEndpoint::readBatch() const {};

int32_t RDMAIOEndpoint::writeBatch() const {};

int32_t RDMAIOEndpoint::writeWithImmBatch() const {};

int32_t RDMAIOEndpoint::RecvImm() const {};
}  // namespace slime
