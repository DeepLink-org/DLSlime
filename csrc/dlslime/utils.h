#pragma once

#include <atomic>
#include <stdexcept>
#include <string>

#include "dlslime/jring.h"

namespace dlslime {

class SpinLock {
    std::atomic_flag flag_ = ATOMIC_FLAG_INIT;

public:
    void lock() noexcept
    {
        while (flag_.test_and_set(std::memory_order_acquire)) {
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

}  // namespace dlslime
