#pragma once

#include <thread>
#include <cstdlib>
#include <sstream>
#include <string>
#include <vector>

namespace slime {

template<typename T>
T get_env(const char* name, T default_value)
{
    const char* val = std::getenv(name);
    if (!val)
        return default_value;

    if constexpr (std::is_same_v<T, int>) {
        return std::stoi(val);
    }
    else if constexpr (std::is_same_v<T, std::vector<std::string>>) {
        std::vector<std::string> result;
        std::stringstream        ss(val);
        std::string              item;
        while (std::getline(ss, item, ',')) {
            result.emplace_back(item);
        }
        return result;
    }
    else {
        static_assert(sizeof(T) == 0, "Unsupported type for get_env");
    }
}

std::vector<std::string> available_nic();

int get_gid_index(std::string dev_name);

// 设置线程亲和性：将线程绑定到指定的 CPU 核心（core_id 从 0 开始）
bool set_thread_affinity(std::thread& thread, int core_id);

}  // namespace slime
