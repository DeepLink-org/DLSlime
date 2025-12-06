#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "json.hpp"
#include "logging.h"

namespace slime {

struct Assignment;

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

typedef struct Assignment {
    friend std::ostream& operator<<(std::ostream& os, const Assignment& assignment);
    Assignment() = default;
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

    /* dump */
    json dump() const;

    uintptr_t mr_key{};
    uintptr_t remote_mr_key{};
    uint64_t  source_offset{};
    uint64_t  target_offset{};
    uint64_t  length{};

} assignment_t;

inline void split_assign_by_max_length(OpCode           opcode,
                                       AssignmentBatch& batch,
                                       AssignmentBatch& batch_split_after_max_length,
                                       size_t           max_length)
{
    for (size_t i = 0; i < batch.size(); ++i) {
        if (batch[i].length < max_length) {
            batch_split_after_max_length.push_back(std::move(batch[i]));
        }
        else {
            for (size_t j = 0; j < batch[i].length; j += max_length) {
                batch_split_after_max_length.push_back(
                    Assignment(batch[i].mr_key,
                               batch[i].remote_mr_key,
                               batch[i].target_offset + j,
                               batch[i].source_offset + j,
                               std::min(static_cast<size_t>(max_length), batch[i].length - j)));
            }
        }
    }
}

inline void
split_assign_by_step(OpCode opcode, AssignmentBatch& batch, std::vector<AssignmentBatch>& batch_split, size_t step)
{
    // split assignment by step
    for (int i = 0; i < batch.size(); i += step) {
        AssignmentBatch split_batch;
        std::move(batch.begin() + i, std::min(batch.end(), batch.begin() + i + step), std::back_inserter(split_batch));
        batch_split.push_back(split_batch);
    }
}

inline void
nsplit_assign_by_step(OpCode opcode, AssignmentBatch& batch, std::vector<AssignmentBatch>& batch_nsplit, size_t nstep)
{
    // split assignment by nstep
    size_t bsize = batch.size();
    int    step  = (bsize + nstep - 1) / nstep;
    split_assign_by_step(opcode, batch, batch_nsplit, step);
}

}  // namespace slime
