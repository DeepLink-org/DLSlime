#pragma once
#include <immintrin.h>

#include <atomic>

#include "dlslime/device/signal.h"
#include "dlslime/logging.h"

namespace dlslime {
namespace device {

class alignas(64) HostOnlySignal: public DeviceSignal {
public:
    struct Flags {
        std::atomic<int>      gpu_ready{0};
        std::atomic<uint32_t> comm_done{0};
    };

    HostOnlySignal()
    {
        init();
    };
    ~HostOnlySignal()
    {
        if (flags_)
            delete flags_;
    }

    void init() override
    {
        flags_ = new Flags();
    }

    void record_gpu_ready() override
    {
        flags_->gpu_ready.store(1, std::memory_order_release);
    }

    bool is_gpu_ready() override
    {
        return (flags_->gpu_ready.load(std::memory_order_acquire) == 1);
    }

    uint32_t get_comm_done_mask() override
    {
        return flags_->comm_done.load(std::memory_order_acquire);
    }

    void set_comm_done(int qp_index) override
    {
        uint32_t bit = (1 << qp_index);
        SLIME_LOG_INFO("set comm done: ", bit);
        flags_->comm_done.fetch_or(bit, std::memory_order_release);
    }

    // Force wake all waiters regardless of their expected_mask
    void force_complete() override
    {
        flags_->comm_done.store(0xFFFFFFFF, std::memory_order_release);
    }

    void bind_stream(void*) override {};
    void wait_comm_done_on_stream(uint32_t) override {}

    void wait_comm_done_cpu(uint32_t target_mask) override
    {
        while ((flags_->comm_done.load(std::memory_order_acquire) & target_mask) != target_mask) {
            _mm_pause();
        }
    }

    void reset_all() override
    {
        flags_->gpu_ready.store(0, std::memory_order_release);
        flags_->comm_done.store(0, std::memory_order_release);
    }

private:
    Flags* flags_          = nullptr;
    void*  stream_handler_ = nullptr;
};

}  // namespace device
}  // namespace dlslime
