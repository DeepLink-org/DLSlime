#include "infiniband/verbs.h"

#include <functional>

#include "engine/rdma/rdma_env.h"
#include "utils/ibv_helper.h"
#include "logging.h"
#include "utils/utils.h"

namespace slime {
std::vector<std::string> available_nic()
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

int get_gid_index(std::string dev_name)
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

}  // namespace slime
