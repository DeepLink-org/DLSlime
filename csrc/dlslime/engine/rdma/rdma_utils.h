#pragma once

#include <immintrin.h>
#include <infiniband/verbs.h>
#include <numa.h>

#include <cstdlib>
#include <functional>
#include <iostream>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "dlslime/engine/rdma/ibv_helper.h"
#include "dlslime/engine/rdma/rdma_env.h"
#include "dlslime/logging.h"

namespace dlslime {

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

inline void cpu_relax()
{
    _mm_pause();
}

inline std::vector<std::string> available_nic()
{
    int                 num_devices;
    struct ibv_device** dev_list;

    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        SLIME_LOG_DEBUG("No RDMA devices");
        return {};
    }

    std::vector<std::string> available_devices;
    for (int i = 0; i < num_devices; ++i) {
        std::string dev_name = (char*)ibv_get_device_name(dev_list[i]);
        if (SLIME_VISIBLE_DEVICES.empty()
            || std::find(SLIME_VISIBLE_DEVICES.begin(), SLIME_VISIBLE_DEVICES.end(), dev_name)
                   != SLIME_VISIBLE_DEVICES.end())
            available_devices.push_back(dev_name);
    }
    return available_devices;
}

inline int get_gid_index(std::string dev_name)
{
    int                 num_devices;
    struct ibv_device** dev_list;

    dev_list = ibv_get_device_list(&num_devices);
    if (!dev_list) {
        SLIME_LOG_DEBUG("No RDMA devices");
        return {};
    }

    std::vector<std::string> available_devices;
    for (int i = 0; i < num_devices; ++i) {
        std::string dev_name_i = (char*)ibv_get_device_name(dev_list[i]);
        if (strcmp(dev_name_i.c_str(), dev_name.c_str()) == 0) {
            struct ibv_context* ib_ctx = ibv_open_device(dev_list[i]);
            int gidx = ibv_find_sgid_type(ib_ctx, 1, ibv_gid_type_custom::IBV_GID_TYPE_ROCE_V2, AF_INET);
            ibv_close_device(ib_ctx);
            return gidx;
        }
    }
    return -1;
}

inline int32_t socketId(const std::string& device_name)
{
    // Adapted from https://github.com/kvcache-ai/Mooncake.git
    std::string   path = "/sys/class/infiniband/" + device_name + "/device/numa_node";
    std::ifstream file(path);
    if (file.is_open()) {
        int socket_id;
        file >> socket_id;
        file.close();
        return (socket_id < 0) ? 0 : socket_id;
    }
    else {
        return 0;
    }
}

}  // namespace dlslime
