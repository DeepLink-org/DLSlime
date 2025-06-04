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

inline const std::vector<std::string> SLIME_VISIBLE_DEVICES =
    get_env<std::vector<std::string>>("SLIME_VISIBLE_DEVICES", {});
inline const int SLIME_MAX_SEND_WR        = get_env<int>("SLIME_MAX_SEND_WR", 4096);
inline const int SLIME_MAX_RECV_WR        = get_env<int>("SLIME_MAX_RECV_WR", 4096);
inline const int SLIME_POLL_COUNT         = get_env<int>("SLIME_POLL_COUNT", 256);
inline const int SLIME_MAX_RD_ATOMIC      = get_env<int>("SLIME_MAX_RD_ATOMIC", 16);
inline const int SLIME_MAX_DEST_RD_ATOMIC = get_env<int>("SLIME_MAX_DEST_RD_ATOMIC", 16);
inline const int SLIME_SERVICE_LEVEL      = get_env<int>("SLIME_SERVICE_LEVEL", 0);
inline const int SLIME_GID_INDEX          = get_env<int>("SLIME_GID_INDEX", -1);
inline const int SLIME_QP_NUM             = get_env<int>("SLIME_QP_NUM", 4);
inline const int SLIME_CQ_NUM             = get_env<int>("SLIME_CQ_NUM", 1);
inline const int SLIME_MAX_CQ_DEPTH       = get_env<int>("SLIME_MAX_CQ_DEPTH", 8192);

}  // namespace slime
