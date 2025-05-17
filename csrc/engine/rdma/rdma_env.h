#pragma once
#include <cstdlib>
#include <string>

namespace slime {

inline int get_env_int(const char* name, int default_value)
{
    const char* val = std::getenv(name);
    return val ? std::stoi(val) : default_value;
}

inline const int MAX_SEND_WR        = get_env_int("SLIME_MAX_SEND_WR", 8192);
inline const int MAX_RECV_WR        = get_env_int("SLIME_MAX_RECV_WR", 8192);
inline const int POLL_COUNT         = get_env_int("SLIME_POLL_COUNT", 256);
inline const int MAX_DEST_RD_ATOMIC = get_env_int("SLIME_MAX_DEST_RD_ATOMIC", 16);
inline const int SERVICE_LEVEL      = get_env_int("SLIME_SERVICE_LEVEL", 0);
inline const int GID_INDEX          = get_env_int("SLIME_GID_INDEX", -1);
inline const int QP_NUM             = get_env_int("SLIME_QP_NUM", 4);
inline const int CQ_NUM             = get_env_int("SLIME_CQ_NUM", 1);

}  // namespace slime
