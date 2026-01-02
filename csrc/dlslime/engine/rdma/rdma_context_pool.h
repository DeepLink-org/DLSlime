#pragma once

#include <cstddef>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "dlslime/engine/rdma/rdma_utils.h"
#include "dlslime/logging.h"
#include "rdma_context.h"

namespace dlslime {

class GlobalContextManager {
public:
    static GlobalContextManager& instance()
    {
        static GlobalContextManager instance;
        return instance;
    }

    std::shared_ptr<RDMAContext>
    get_context(const std::string& dev_name = "", uint8_t ib_port = 1, const std::string& link_type = "RoCE")
    {
        std::lock_guard<std::mutex> lock(mutex_);

        std::string key  = dev_name;
        auto        nics = available_nic();
        if (nics.empty()) {
            SLIME_LOG_WARN("No Available nics");
            return nullptr;
        }
        if (std::find(nics.begin(), nics.end(), key) == nics.end() or dev_name.empty()) {
            key = nics[0];
        }

        if (contexts_.find(key) == contexts_.end()) {
            SLIME_LOG_INFO("Initializing new RDMAContext for device: ", key);

            auto context = std::make_shared<RDMAContext>();

            if (context->init(key, ib_port, link_type) != 0) {
                SLIME_LOG_ERROR("Failed to init RDMAContext for {}", key);
                return nullptr;
            }

            contexts_[key] = context;

            if (!default_context_) {
                default_context_ = context;
            }
        }

        return contexts_[key];
    }

    std::shared_ptr<RDMAContext> get_default_context()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return default_context_;
    }

private:
    GlobalContextManager() = default;

    GlobalContextManager(const GlobalContextManager&)            = delete;
    GlobalContextManager& operator=(const GlobalContextManager&) = delete;

    std::mutex mutex_;

    std::unordered_map<std::string, std::shared_ptr<RDMAContext>> contexts_;

    std::shared_ptr<RDMAContext> default_context_ = nullptr;
};

}  // namespace dlslime
