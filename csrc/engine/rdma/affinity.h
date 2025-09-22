#pragma once

#include "env.h"
#include "logging.h"

#include <numa.h>
#include <thread>

namespace slime {

#ifndef likely
#define likely(x) __glibc_likely(x)
#define unlikely(x) __glibc_unlikely(x)
#endif

#define ERR_NUMA (-300)

static inline int bindToSocket(int socket_id)
{
    // Adapted from https://github.com/kvcache-ai/Mooncake.git
    if (unlikely(numa_available() < 0)) {
        SLIME_LOG_WARN("The platform does not support NUMA");
        return ERR_NUMA;
    }
    cpu_set_t cpu_set;
    CPU_ZERO(&cpu_set);
    if (socket_id < 0 || socket_id >= numa_num_configured_nodes())
        socket_id = 0;
    struct bitmask* cpu_list = numa_allocate_cpumask();
    numa_node_to_cpus(socket_id, cpu_list);
    int nr_possible_cpus = numa_num_possible_cpus();
    int nr_cpus          = 0;
    for (int cpu = 0; cpu < nr_possible_cpus; ++cpu) {
        if (numa_bitmask_isbitset(cpu_list, cpu) && numa_bitmask_isbitset(numa_all_cpus_ptr, cpu)) {
            CPU_SET(cpu, &cpu_set);
            nr_cpus++;
        }
    }
    numa_free_cpumask(cpu_list);
    if (nr_cpus == 0)
        return 0;
    if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpu_set)) {
        SLIME_LOG_ERROR("bindToSocket: pthread_setaffinity_np failed");
        return ERR_NUMA;
    }
    return 0;
}

}  // namespace slime
