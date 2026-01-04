#pragma once
#include <cstdint>

namespace dlslime {
namespace device {

class alignas(64) DeviceSignal {
public:
    virtual ~DeviceSignal() = default;

    virtual void init() = 0;

    virtual void record_gpu_ready() = 0;

    virtual bool is_gpu_ready() = 0;

    virtual uint32_t get_comm_done_mask()        = 0;
    virtual void     set_comm_done(int qp_index) = 0;

    virtual void bind_stream(void* stream_handle)               = 0;
    virtual void wait_comm_done_on_stream(uint32_t target_mask) = 0;

    virtual void wait_comm_done_cpu(uint32_t target_mask) = 0;

    virtual void force_complete() = 0;
    virtual void reset_all()      = 0;
};

}  // namespace device
}  // namespace dlslime
