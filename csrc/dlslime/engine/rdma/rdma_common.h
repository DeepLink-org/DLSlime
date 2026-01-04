#pragma once

#include <cstdint>
#include <cstdlib>
#include <vector>

namespace dlslime {

typedef struct StorageView {
    uintptr_t data_ptr;
    size_t    storage_offset;
    size_t    length;
} storage_view_t;

using storage_view_batch_t = std::vector<storage_view_t>;

}  // namespace dlslime
