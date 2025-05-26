#include "infiniband/verbs.h"

#include <functional>

#include "engine/rdma/rdma_env.h"
#include "utils/logging.h"
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
}  // namespace slime
