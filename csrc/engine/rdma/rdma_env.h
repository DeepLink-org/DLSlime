#pragma once

#include "env.h"

#include <string>
#include <vector>

namespace slime {

inline const std::vector<std::string> SLIME_VISIBLE_DEVICES =
    get_env<std::vector<std::string>>("SLIME_VISIBLE_DEVICES", {});
inline const int SLIME_MAX_LENGTH_PER_ASSIGNMENT = get_env<int>("SLIME_MAX_LENGTH_PER_ASSIGNMENT", 1 << 30);
inline const int SLIME_MAX_SEND_WR               = get_env<int>("SLIME_MAX_SEND_WR", 8192);
inline const int SLIME_MAX_RECV_WR               = get_env<int>("SLIME_MAX_RECV_WR", 8192);
inline const int SLIME_POLL_COUNT                = get_env<int>("SLIME_POLL_COUNT", 256);
inline const int SLIME_MAX_RD_ATOMIC             = get_env<int>("SLIME_MAX_RD_ATOMIC", 16);
inline const int SLIME_MAX_DEST_RD_ATOMIC        = get_env<int>("SLIME_MAX_DEST_RD_ATOMIC", 16);
inline const int SLIME_SERVICE_LEVEL             = get_env<int>("SLIME_SERVICE_LEVEL", 0);
inline const int SLIME_GID_INDEX                 = get_env<int>("SLIME_GID_INDEX", -1);
inline const int SLIME_QP_NUM                    = get_env<int>("SLIME_QP_NUM", 1);
inline const int SLIME_CQ_NUM                    = get_env<int>("SLIME_CQ_NUM", 1);
inline const int SLIME_MAX_CQ_DEPTH              = get_env<int>("SLIME_MAX_CQ_DEPTH", 8192);
inline const int SLIME_AGG_QP_NUM                = get_env<int>("SLIME_AGG_QP_NUM", 1);
inline const int SLIME_BYPASS_DEVICE_SIGNAL      = get_env<int>("SLIME_BYPASS_DEVICE_SIGNAL", 1);
inline const int SLIME_MAX_SLOT_FIFO_DEPTH       = get_env<int>("SLIME_MAX_SLOT_FIFO_DEPTH", 4096);
}  // namespace slime
