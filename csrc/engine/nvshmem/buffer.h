#pragma once

#include "utils/json.hpp"

using json = nlohmann::json;

namespace slime {

class Buffer {
    void sync(const std::vector<int>& device_ids, json buffer_info);
}