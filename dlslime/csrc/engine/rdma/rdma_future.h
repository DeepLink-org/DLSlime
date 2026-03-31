#pragma once

#include <cstdint>
#include <memory>

#include "dlslime/csrc/device/device_future.h"

namespace dlslime {

struct SendContext;
struct RecvContext;
struct ReadWriteContext;
struct ImmRecvContext;

/**
 * @brief Base class for RDMA futures, inherits from DeviceFuture
 *
 * Provides consistent interface with other device types (Ascend, NVLink)
 */
class RDMAFuture : public DeviceFuture {
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
    explicit SendFuture(SendContext* ctx);

    int32_t wait() const override;

private:
    SendContext* ctx_;
};

class RecvFuture: public RDMAFuture {
public:
    explicit RecvFuture(RecvContext* ctx);

    int32_t wait() const override;

private:
    RecvContext* ctx_;
};

class ReadWriteFuture: public RDMAFuture {
public:
    explicit ReadWriteFuture(ReadWriteContext* ctx);

    int32_t wait() const override;

private:
    ReadWriteContext* ctx_;
};

class ImmRecvFuture: public RDMAFuture {
public:
    explicit ImmRecvFuture(ImmRecvContext* ctx);

    int32_t wait() const override;

    int32_t immData() const;

private:
    ImmRecvContext* ctx_;
};

}  // namespace dlslime
