#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

#include "engine/rdma/rdma_utils.h"
#include "logging.h"
#include "rdma_context.h"

namespace slime {

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

        std::string key = dev_name;

        if (contexts_.find(key) == contexts_.end()) {
            SLIME_LOG_INFO("Initializing new RDMAContext for device: ", dev_name);

            auto context = std::make_shared<RDMAContext>();

            if (context->init(dev_name, ib_port, link_type) != 0) {
                SLIME_LOG_ERROR("Failed to init RDMAContext for {}", dev_name);
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

}  // namespace slime
