#pragma once
#include <cuda.h>
#include <cuda_runtime.h>
#include <immintrin.h>  // for _mm_pause

#include <atomic>
#include <stdexcept>

#include "dlslime/device/signal.h"
#include "dlslime/engine/rdma/rdma_env.h"
#include "dlslime/logging.h"
#include "nvtx_helper.h"

namespace dlslime {
namespace device {

class alignas(64) CudaDeviceSignal: public DeviceSignal {
public:
    struct Flags {
        volatile int      gpu_ready;
        volatile uint32_t comm_done;
    };

    CudaDeviceSignal()
    {
        init();
    }

    ~CudaDeviceSignal()
    {
        if (host_ptr_)
            cudaFreeHost((void*)host_ptr_);
    }

    void init() override
    {
        cudaError_t err = cudaHostAlloc((void**)&host_ptr_, sizeof(Flags), cudaHostAllocMapped | cudaHostAllocPortable);
        if (err != cudaSuccess)
            throw std::runtime_error("cudaHostAlloc failed");

        err = cudaHostGetDevicePointer(&dev_ptr_, (void*)host_ptr_, 0);
        if (err != cudaSuccess)
            throw std::runtime_error("cudaHostGetDevicePointer failed");

        reset_all();
    }

    void record_gpu_ready() override
    {
        NVTX_RANGE("Signal: Record GPU Ready", COLOR_GREEN);
        cudaStream_t stream = static_cast<cudaStream_t>(stream_handle_);
        cuStreamWriteValue32(stream, (CUdeviceptr)&dev_ptr_->gpu_ready, 1, 0);
    }

    bool is_gpu_ready() override
    {
        int val = __atomic_load_n(&host_ptr_->gpu_ready, __ATOMIC_ACQUIRE);
        return (val == 1);
    }

    void set_comm_done(int qp_index) override
    {
        NVTX_RANGE("Signal: Set Comm Done", COLOR_PURPLE);
        uint32_t bit = (1 << qp_index);
        __atomic_fetch_or(&host_ptr_->comm_done, bit, __ATOMIC_RELEASE);
    }

    uint32_t get_comm_done_mask() override
    {
        return __atomic_load_n(&host_ptr_->comm_done, __ATOMIC_ACQUIRE);
    }

    void bind_stream(void* stream_handle) override
    {
        stream_handle_ = stream_handle;
    };

    void wait_comm_done_on_stream(uint32_t target_mask) override
    {
        cudaStream_t stream = static_cast<cudaStream_t>(stream_handle_);

        NVTX_RANGE("Signal: Stream Wait Comm", COLOR_RED);
        cuStreamWaitValue32(stream, (CUdeviceptr)&dev_ptr_->comm_done, target_mask, CU_STREAM_WAIT_VALUE_EQ);
    }

    void wait_comm_done_cpu(uint32_t target_mask) override
    {
        if (SLIME_BYPASS_DEVICE_SIGNAL) {
            while (true) {
                uint32_t val = __atomic_load_n(&host_ptr_->comm_done, __ATOMIC_ACQUIRE);
                if (val == target_mask)
                    break;
                _mm_pause();
            }
        }
        else {
            wait_comm_done_on_stream(target_mask);
        }
    }

    void force_complete() override
    {
        NVTX_RANGE("Signal: Force Complete", COLOR_PURPLE);
        __atomic_store_n(&host_ptr_->comm_done, 0xFFFFFFFF, __ATOMIC_RELEASE);
    }

    void reset_all() override
    {
        __atomic_store_n(&host_ptr_->gpu_ready, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&host_ptr_->comm_done, 0, __ATOMIC_RELAXED);
        __sync_synchronize();  // Memory barrier
    }

private:
    Flags* host_ptr_      = nullptr;
    Flags* dev_ptr_       = nullptr;
    void*  stream_handle_ = nullptr;
};

}  // namespace device
}  // namespace dlslime
