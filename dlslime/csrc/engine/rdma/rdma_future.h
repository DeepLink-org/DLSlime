#pragma once

#include <cstdint>
#include <memory>

#include "dlslime/csrc/device/device_future.h"

namespace dlslime {

struct EndpointOpState;

/**
 * @brief Base class for RDMA futures, inherits from DeviceFuture
 *
 * Provides consistent interface with other device types (Ascend, NVLink)
 */
class RDMAFuture: public DeviceFuture {
public:
    virtual ~RDMAFuture() = default;

    virtual int32_t wait() const = 0;
};

class SendFuture;
class RecvFuture;
class ImmRecvFuture;
class ReadWriteFuture;

// Futures hold a shared_ptr<EndpointOpState> so they track a specific logical
// operation, not whatever happens to occupy a reused slot at wait() time.
class SendFuture: public RDMAFuture {
public:
    explicit SendFuture(std::shared_ptr<EndpointOpState> op_state);

    int32_t wait() const override;

private:
    std::shared_ptr<EndpointOpState> op_state_;
};

class RecvFuture: public RDMAFuture {
public:
    explicit RecvFuture(std::shared_ptr<EndpointOpState> op_state);

    int32_t wait() const override;

private:
    std::shared_ptr<EndpointOpState> op_state_;
};

class ReadWriteFuture: public RDMAFuture {
public:
    explicit ReadWriteFuture(std::shared_ptr<EndpointOpState> op_state);

    int32_t wait() const override;

private:
    std::shared_ptr<EndpointOpState> op_state_;
};

class ImmRecvFuture: public RDMAFuture {
public:
    explicit ImmRecvFuture(std::shared_ptr<EndpointOpState> op_state);

    int32_t wait() const override;

    int32_t  immData() const;
    bool     timeTraceEnabled() const;
    uint64_t timeTraceStartNs() const;
    uint64_t timeTraceEndNs() const;
    uint64_t timeTraceElapsedNs() const;

private:
    std::shared_ptr<EndpointOpState> op_state_;
};

}  // namespace dlslime
