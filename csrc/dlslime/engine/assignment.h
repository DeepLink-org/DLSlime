#pragma once

#include <algorithm>
#include <cmath>
#include <cstdint>
#include <cstring>
#include <functional>
#include <string>
#include <vector>

#include "dlslime/json.hpp"
#include "dlslime/logging.h"

namespace dlslime {

struct Assignment;

using assign_tuple_t = std::tuple<uintptr_t, uintptr_t, uint64_t, uint64_t, size_t>;
using chunk_tuple_t  = std::tuple<uintptr_t, uint64_t, size_t>;

using json            = nlohmann::json;
using AssignmentBatch = std::vector<Assignment>;

enum class OpCode : uint8_t {
    READ,
    WRITE,
    SEND,
    RECV,
    SEND_WITH_IMM,
    WRITE_WITH_IMM
};

struct alignas(64) Assignment {
    friend std::ostream& operator<<(std::ostream& os, const Assignment& assignment);
    Assignment() = default;

    Assignment(const assign_tuple_t& assign):
        mr_key(std::get<0>(assign)),
        remote_mr_key(std::get<1>(assign)),
        target_offset(std::get<2>(assign)),
        source_offset(std::get<3>(assign)),
        length(std::get<4>(assign))
    {
    }

    Assignment(const uintptr_t& mr_key, uint64_t target_offset, uint64_t source_offset, uint64_t length):
        mr_key(mr_key),
        remote_mr_key(mr_key),
        target_offset(target_offset),
        source_offset(source_offset),
        length(length)
    {
    }

    Assignment(const uintptr_t& mr_key,
               const uintptr_t& remote_mr_key,
               uint64_t         target_offset,
               uint64_t         source_offset,
               uint64_t         length):
        mr_key(mr_key),
        remote_mr_key(remote_mr_key),
        target_offset(target_offset),
        source_offset(source_offset),
        length(length)
    {
    }

    json dump() const;

    uintptr_t mr_key{};
    uintptr_t remote_mr_key{};
    uint64_t  target_offset{};
    uint64_t  source_offset{};
    uint64_t  length{};
};

inline void split_assign_by_max_length(OpCode, const AssignmentBatch& batch, AssignmentBatch& output, size_t max_length)
{
    if (batch.empty())
        return;

    size_t total_count = 0;
    for (const auto& item : batch) {
        if (item.length == 0)
            continue;
        total_count += (item.length + max_length - 1) / max_length;
    }

    output.reserve(output.size() + total_count);

    for (const auto& item : batch) {
        if (item.length <= max_length) {
            output.emplace_back(item);
        }
        else {
            for (uint64_t offset = 0; offset < item.length; offset += max_length) {
                uint64_t chunk_len = std::min(static_cast<uint64_t>(max_length), item.length - offset);
                output.emplace_back(item.mr_key,
                                    item.remote_mr_key,
                                    item.target_offset + offset,
                                    item.source_offset + offset,
                                    chunk_len);
            }
        }
    }
}

inline void
split_assign_by_step(OpCode, const AssignmentBatch& batch, std::vector<AssignmentBatch>& batch_split, size_t step)
{
    if (batch.empty() || step == 0)
        return;

    size_t num_chunks = (batch.size() + step - 1) / step;
    batch_split.reserve(num_chunks);

    for (size_t i = 0; i < batch.size(); i += step) {
        size_t current_chunk_size = std::min(step, batch.size() - i);

        AssignmentBatch split_batch;
        split_batch.reserve(current_chunk_size);

        split_batch.insert(split_batch.end(), batch.begin() + i, batch.begin() + i + current_chunk_size);

        batch_split.emplace_back(std::move(split_batch));
    }
}

inline void nsplit_assign_by_step(OpCode                        opcode,
                                  const AssignmentBatch&        batch,
                                  std::vector<AssignmentBatch>& batch_nsplit,
                                  size_t                        nstep)
{
    if (nstep == 0)
        return;
    size_t bsize = batch.size();
    size_t step  = (bsize + nstep - 1) / nstep;
    split_assign_by_step(opcode, batch, batch_nsplit, step);
}

}  // namespace dlslime
