#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace slime {
namespace nvshmem_engine {
namespace internode {

std::vector<uint8_t> get_unique_id();

int init(const std::vector<uint8_t>& root_unique_id_val, int rank, int num_ranks);

void* alloc(size_t size, size_t alignment);

void free(void *ptr);

void barrier();

}  // namespace internode
}  // namespace nvshmem_engine
}  // namespace slime
