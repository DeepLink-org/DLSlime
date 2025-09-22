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
    Assignment(std::string mr_key, uint64_t target_offset, uint64_t source_offset, uint64_t length):
        mr_key(mr_key), target_offset(target_offset), source_offset(source_offset), length(length)
    {
    }

    /* dump */
    json dump() const;

    std::string mr_key{};
    uint64_t    source_offset{};
    uint64_t    target_offset{};
    uint64_t    length{};

} assignment_t;

}  // namespace slime
