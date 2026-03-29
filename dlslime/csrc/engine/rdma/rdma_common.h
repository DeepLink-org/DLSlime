#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

namespace dlslime {

typedef struct remote_mr {
    remote_mr() = default;
    remote_mr(uintptr_t addr, size_t length, uint32_t rkey): addr(addr), length(length), rkey(rkey) {}

    uintptr_t addr{(uintptr_t) nullptr};
    size_t    length{0};
    uint32_t  rkey{0};
} remote_mr_t;

typedef struct StorageView {
    uintptr_t data_ptr;
    size_t    storage_offset;
    size_t    length;
} storage_view_t;

using storage_view_batch_t = std::vector<storage_view_t>;

}  // namespace dlslime
