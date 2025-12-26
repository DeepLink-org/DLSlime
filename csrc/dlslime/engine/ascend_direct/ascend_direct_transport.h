#pragma once

#include <map>
#include <memory>
#include <string>
#include <unordered_set>

#include "adxl/adxl_engine.h"
#include "adxl/adxl_types.h"
#include "dlslime/engine/assignment.h"
#include "dlslime/logging.h"

namespace dlslime {

class AscendDirectContext {
public:
    AscendDirectContext() = default;
    ~AscendDirectContext();

    int init(const std::string& host, int port);

    int read_batch(AssignmentBatch& batch, const std::string& host, int port);

    int register_memory_region(const std::string& location, uintptr_t addr, size_t length);

    int unregister_memory_region(uintptr_t addr);

    int connect(const std::string& host, int port);

    int disconnect(const std::string& adxl_engine_name);

    int disconnect(const std::string& host, int port);

private:
    const static int                     CONNECT_TIMEOUT_MILLIS = 60 * 1000;
    std::unique_ptr<adxl::AdxlEngine>    adxl_                  = nullptr;
    std::map<uintptr_t, adxl::MemHandle> addr_to_memhandle_;
    std::unordered_set<std::string>      connected_engines_;
};

}  // namespace dlslime
