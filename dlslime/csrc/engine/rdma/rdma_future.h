#pragma once

#include <cstdint>
#include <memory>

#include "dlslime/csrc/device/device_future.h"

namespace dlslime {

struct SendContext;
struct RecvContext;
struct ReadWriteContext;
struct ImmRecvContext;
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

    int32_t immData() const;

private:
    std::shared_ptr<EndpointOpState> op_state_;
};

}  // namespace dlslime
