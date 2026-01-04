#pragma once

#include <numa.h>

#include <cstdlib>
#include <iostream>
#include <sstream>
#include <string>
#include <type_traits>
#include <vector>

namespace dlslime {

#ifndef likely
#define likely(x) __glibc_likely(x)
#define unlikely(x) __glibc_unlikely(x)
#endif

template<typename T>
T get_env(const char* name, T default_value)
{
    const char* val = std::getenv(name);
    if (!val)
        return default_value;

    if constexpr (std::is_same_v<T, std::vector<std::string>>) {
        std::vector<std::string> result;
        std::stringstream        ss(val);
        std::string              item;
        while (std::getline(ss, item, ',')) {
            result.emplace_back(item);
        }
        return result;
    }
    else {
        return std::stoi(val);
    }
}

inline const int SLIME_LOG_LEVEL = get_env<int>("SLIME_LOG_LEVEL", 0);
inline const int SLIME_LOG_MUTEX = get_env<int>("SLIME_LOG_MUTEX", 0);

}  // namespace dlslime
