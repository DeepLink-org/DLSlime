#pragma once
#include "../signal.h"
#include "logging.h"

#include <atomic>
#include <cuda.h>
#include <cuda_runtime.h>
#include <immintrin.h>  // for _mm_pause
#include <stdexcept>

namespace slime {
namespace device {

class CudaDeviceSignal: public DeviceSignal {
public:
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
        cudaError_t err = cudaHostAlloc((void**)&host_ptr_, sizeof(int), cudaHostAllocMapped | cudaHostAllocPortable);
        if (err != cudaSuccess)
            throw std::runtime_error("cudaHostAlloc failed");

        err = cudaHostGetDevicePointer(&dev_ptr_, (void*)host_ptr_, 0);
        if (err != cudaSuccess)
            throw std::runtime_error("cudaHostGetDevicePointer failed");

        *host_ptr_ = 0;
    }

    void record_on_stream(void* stream_handle) override
    {
        cudaStream_t stream = static_cast<cudaStream_t>(stream_handle);

        *host_ptr_ = 0;

        cuStreamWriteValue32(stream, (CUdeviceptr)dev_ptr_, 1, 0); 
    }

    void wait_on_stream(void* stream_handle, uint32_t target_val = 1) override {
        cudaStream_t stream = static_cast<cudaStream_t>(stream_handle);
        cuStreamWaitValue32(stream, (CUdeviceptr)dev_ptr_, target_val, 0);
    }

    void set_signal_from_cpu(int val = 1) override {
        __atomic_store_n(host_ptr_, val, __ATOMIC_RELEASE);
    }

    bool is_busy() override
    {
        int val = __atomic_load_n(host_ptr_, __ATOMIC_ACQUIRE);
        SLIME_LOG_INFO(val);
        return (val == 0);
    }

    void reset() override
    {
        *host_ptr_ = 0;
    }

private:
    volatile int* host_ptr_ = nullptr;  // CPU 读的地址
    void*         dev_ptr_  = nullptr;  // GPU 写的地址
};

}  // namespace device
}  // namespace slime
