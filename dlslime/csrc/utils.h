#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <string>

#include "dlslime/csrc/common/jring.h"

namespace dlslime {

// ============================================================
// SpinLock — lightweight alternative to std::mutex for short
// critical sections (e.g. the imm-recv matching path).
// ============================================================

class SpinLock {
    std::atomic_flag flag_ = ATOMIC_FLAG_INIT;

public:
    void lock() noexcept
    {
        while (flag_.test_and_set(std::memory_order_acquire)) {
            // Spin. On x86 this compiles to a PAUSE-based loop.
        }
    }
    void unlock() noexcept { flag_.clear(std::memory_order_release); }
};

inline jring_t* createRing(const char* name, size_t count)
{
    size_t ring_sz = jring_get_buf_ring_size(sizeof(void*), count);

    void* mem = nullptr;
    // Align to 64 bytes to match cache line size, preventing false sharing.
    if (posix_memalign(&mem, 64, ring_sz) != 0) {
        throw std::runtime_error(std::string("Failed to allocate ring memory: ") + name);
    }

    jring_t* r = (jring_t*)mem;

    // Initialize ring: MP=1 (Multi-Producer safe), MC=1 (Multi-Consumer safe).
    // This allows multiple threads to enqueue requests if needed.
    if (jring_init(r, count, sizeof(void*), 1, 1) < 0) {
        free(mem);
        throw std::runtime_error(std::string("Failed to init ring: ") + name);
    }
    return r;
}

inline void freeRing(jring_t* ring)
{
    if (ring) {
        free(ring);
    }
}

inline uint64_t monotonic_time_ns()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now().time_since_epoch())
            .count());
}

}  // namespace dlslime
