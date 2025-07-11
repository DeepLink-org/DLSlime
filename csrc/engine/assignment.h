#pragma once

#include <cstdint>
#include <functional>
#include <string>
#include <vector>

#include "utils/logging.h"
#include "utils/json.hpp"

namespace slime {

struct Assignment;

using json = nlohmann::json;
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
    Assignment(std::string mr_key, uint64_t target_offset, uint64_t source_offset, uint64_t length):
        mr_key(mr_key), target_offset(target_offset), source_offset(source_offset), length(length)
    {
    }

    Assignment(std::string mr_key, uint64_t target_offset, uint64_t source_offset, uint64_t length, uint64_t r_addr, uint32_t r_key):
        mr_key(mr_key), target_offset(target_offset), source_offset(source_offset), length(length), remote_addr(r_addr), remote_rkey(r_key)
    {
    }

    /* dump */
    json dump() const;

    std::string mr_key{};
    uint64_t    source_offset{};
    uint64_t    target_offset{};
    uint64_t    length{};

    uint64_t remote_addr{};
    uint32_t remote_rkey{};
    uint32_t slot_id;

} assignment_t;

}  // namespace slime
