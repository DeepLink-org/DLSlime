#pragma once


namespace dlslime {

namespace internode {

void send_ll(int8_t* data, int8_t* buffer, int8_t* signal_buffer, size_t length, size_t msg_size_per_warp, size_t num_warps_per_sm, int rank, int dst_rank);

void recv_ll(int8_t* data, int8_t* buffer, int8_t* signal_buffer, size_t length, size_t msg_size_per_warp, size_t num_warps_per_sm, int rank, int src_rank);

}

}
