#pragma once

#define XXH_INLINE_ALL

#include "xxhash.h"
#include <iostream>
#include <fstream>
#include <string>
#include <string_view>

namespace slime {
inline int64_t get_xxhash(std::string_view str)
{
    // 使用 xxHash3 (目前最快的版本)，生成 64 位 hash
    // 注意：seed 设为 0 或者任意你喜欢的数字
    return (int64_t)XXH3_64bits(str.data(), str.size());
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

}  // namespace slime
