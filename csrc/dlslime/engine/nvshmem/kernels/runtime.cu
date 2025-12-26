#include <cstddef>
#include <cstdint>
#include <cstring>
#include <vector>

#include "configs.cuh"
#include "exception.cuh"
#include "ibgda_device.cuh"
#include "launch.cuh"
#include "utils.cuh"

namespace dlslime {
namespace nvshmem_engine {
namespace internode {

int init(const std::vector<uint8_t>& root_unique_id_val, int rank, int num_ranks)
{
    nvshmemx_uniqueid_t  root_unique_id;
    nvshmemx_init_attr_t attr;
    std::memcpy(&root_unique_id, root_unique_id_val.data(), sizeof(nvshmemx_uniqueid_t));
    nvshmemx_set_attr_uniqueid_args(rank, num_ranks, &root_unique_id, &attr);
    nvshmemx_init_attr(NVSHMEMX_INIT_WITH_UNIQUEID, &attr);
    nvshmem_barrier_all();
    return nvshmem_my_pe();
}

std::vector<uint8_t> get_unique_id()
{
    nvshmemx_uniqueid_t unique_id;
    nvshmemx_get_uniqueid(&unique_id);
    std::vector<uint8_t> result(sizeof(nvshmemx_uniqueid_t));
    std::memcpy(result.data(), &unique_id, sizeof(nvshmemx_uniqueid_t));
    return result;
}

void* alloc(size_t size, size_t alignment)
{
    return nvshmem_align(alignment, size);
}

void free(void* ptr) {
    nvshmem_free(ptr);
}

void barrier() {
    nvshmem_barrier_all();
}

}  // namespace internode
}  // namespace nvshmem_engine
}  // namespace dlslime
