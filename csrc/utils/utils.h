#pragma once

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

}  // namespace slime
