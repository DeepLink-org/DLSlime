#include "ascend_direct_endpoint.h"

#include <memory>
#include <sstream>
#include <string>

#include "adxl/adxl_engine.h"
#include "ascend_future.h"
#include "ascend_local_memory_pool.h"
#include "ascend_remote_memory_pool.h"
#include "dlslime/csrc/engine/assignment.h"

namespace dlslime {

AscendDirectEndpoint::AscendDirectEndpoint()
{
    // Create new pools internally
    local_pool_  = std::make_shared<AscendLocalMemoryPool>();
    remote_pool_ = std::make_shared<AscendRemoteMemoryPool>();
    SLIME_LOG_DEBUG("AscendDirectEndpoint created with new pools");
}

AscendDirectEndpoint::AscendDirectEndpoint(std::shared_ptr<AscendLocalMemoryPool>  local_pool,
                                           std::shared_ptr<AscendRemoteMemoryPool> remote_pool):
    local_pool_(local_pool), remote_pool_(remote_pool)
{
    if (!local_pool_ || !remote_pool_) {
        SLIME_LOG_ERROR("AscendDirectEndpoint constructed with null pool(s)");
    }
    SLIME_LOG_DEBUG("AscendDirectEndpoint created with shared pools");
}

AscendDirectEndpoint::~AscendDirectEndpoint() {}

int AscendDirectEndpoint::init(const std::string& host, int port)
{
    local_host_ = host;
    local_port_ = port;

    adxl_ = std::make_unique<adxl::AdxlEngine>();
    if (adxl_ == nullptr) {
        SLIME_LOG_ERROR("Failed to create AdxlEngine instance");
        return -1;
    }

    // Black box: AdxlEngine initialization
    std::string                                      adxl_engine_name = host + ":" + std::to_string(port);
    std::map<adxl::AscendString, adxl::AscendString> options;

    auto status = adxl_->Initialize(adxl::AscendString(adxl_engine_name.c_str()), options);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("Failed to initialize AdxlEngine, status: ", status);
        return -1;
    }

    SLIME_LOG_INFO("Initialized AscendDirectEndpoint at ", adxl_engine_name);
    return 0;
}

std::shared_ptr<AscendFuture> AscendDirectEndpoint::read(std::vector<assign_tuple_t>& assign, void* stream_handle)
{
    if (assign.empty()) {
        SLIME_LOG_WARN("AscendDirectEndpoint::read() called with empty assignment");
        return nullptr;
    }

    // Create context for this operation
    auto ctx     = std::make_unique<AscendContext>();
    ctx->state_  = AscendIOContextState::PENDING;
    ctx->signal  = nullptr;  // No device signal for now (synchronous API)
    ctx->slot_id = -1;

    // Convert assign_tuple_t to AdxlEngine operations
    std::vector<adxl::TransferOpDesc> op_descs;
    op_descs.reserve(assign.size());

    for (auto& assign_tuple : assign) {
        auto mr_key        = std::get<0>(assign_tuple);
        auto remote_mr_key = std::get<1>(assign_tuple);
        auto target_offset = std::get<2>(assign_tuple);
        auto source_offset = std::get<3>(assign_tuple);
        auto length        = std::get<4>(assign_tuple);

        // Get memory region descriptors from pools
        ascend_local_mr_t  local_mr  = local_pool_->get_mr(mr_key);
        ascend_remote_mr_t remote_mr = remote_pool_->get_remote_mr(remote_mr_key);

        if (local_mr.mr_key == 0 || remote_mr.mr_key == 0) {
            SLIME_LOG_ERROR("Memory region not found: local_key=", mr_key, " remote_key=", remote_mr_key);
            return nullptr;
        }

        // Build operation descriptor
        // AdxlEngine requires ABSOLUTE addresses (base + offset)
        adxl::TransferOpDesc op_desc;
        op_desc.local_addr  = local_mr.addr + source_offset;   // Local absolute address
        op_desc.remote_addr = remote_mr.addr + target_offset;  // Remote absolute address
        op_desc.len         = length;

        op_descs.push_back(op_desc);

        // Store assignment for tracking
        ctx->assigns_.emplace_back(mr_key, remote_mr_key, target_offset, source_offset, length);
    }

    // Determine remote engine name (assume first assignment's remote info)
    // Note: In production, this should be passed explicitly or cached
    // For now, we assume connect() was already called with the remote endpoint
    if (connected_engines_.empty()) {
        SLIME_LOG_ERROR("No connected remote engines for read operation");
        return nullptr;
    }

    // Use the first connected engine (simplified - assumes single remote peer)
    std::string remote_engine_name = *connected_engines_.begin();

    // Execute synchronous transfer (black box AdxlEngine API)
    ctx->state_ = AscendIOContextState::POSTED;
    auto status = adxl_->TransferSync(remote_engine_name.c_str(), adxl::READ, op_descs, CONNECT_TIMEOUT_MILLIS);

    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("TransferSync failed in AscendDirectEndpoint::read, status: ", status);
        ctx->state_ = AscendIOContextState::FREE;
        return nullptr;
    }

    // Mark as done (synchronous completion)
    ctx->state_ = AscendIOContextState::DONE;
    ctx->completed.store(true, std::memory_order_release);

    // Create and return future (will be trivially complete for sync API)
    auto ctx_raw = ctx.release();  // Transfer ownership to future
    return std::make_shared<AscendFuture>(ctx_raw);
}

void AscendDirectEndpoint::register_memory_region(uint64_t mr_key, uintptr_t addr, size_t offset, size_t length)
{
    // Prepare memory descriptor (black box AdxlEngine API)
    adxl::MemType mem_type = adxl::MEM_DEVICE;

    adxl::MemDesc mem_desc{};
    mem_desc.addr = static_cast<uint64_t>(addr + offset);  // Apply offset to base address
    mem_desc.len  = length;

    // Register with AdxlEngine
    adxl::MemHandle mem_handle;
    auto            status = adxl_->RegisterMem(mem_desc, mem_type, mem_handle);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("adxl RegisterMem failed for key ", mr_key, ", addr ", addr, ", status: ", status);
        return;
    }

    // Store in LOCAL memory pool
    local_pool_->register_memory_region(mr_key, addr, offset, length, mem_handle);

    // Track for cleanup in destructor
    registered_mr_keys_.insert(mr_key);
}

void AscendDirectEndpoint::unregister_memory_region(uint64_t mr_key)
{
    // Get memory region handle from LOCAL pool
    ascend_local_mr_t mr = local_pool_->get_mr(mr_key);
    if (mr.mr_key == 0) {
        SLIME_LOG_WARN("Attempted to unregister non-existent memory region key: ", mr_key);
        return;
    }

    // Deregister from AdxlEngine
    adxl_->DeregisterMem(mr.handle);

    // Remove from LOCAL memory pool
    local_pool_->unregister_memory_region(mr_key);

    // Remove from tracking set
    registered_mr_keys_.erase(mr_key);

    SLIME_LOG_INFO("Unregistered memory region: key=", mr_key);
}

void AscendDirectEndpoint::register_remote_memory_region(uint64_t           remote_mr_key,
                                                         const std::string& name,
                                                         const json&        mr_info)
{
    // For Ascend, we don't use the 'name' parameter (kept for API compatibility)
    // Register remote memory region in the REMOTE pool (metadata only)
    remote_pool_->register_remote_memory_region(remote_mr_key, mr_info);
}

json AscendDirectEndpoint::endpoint_info() const
{
    json info;
    info["host"] = local_host_;
    info["port"] = local_port_;
    return info;
}

void AscendDirectEndpoint::connect(const json& remote_info)
{
    if (!remote_info.contains("host") || !remote_info.contains("port")) {
        SLIME_LOG_ERROR("Invalid remote_info JSON: missing host or port");
        return;
    }

    std::string remote_host = remote_info["host"];
    int         remote_port = remote_info["port"];

    connect_internal(remote_host, remote_port);
}

void AscendDirectEndpoint::disconnect(const std::string& host, int port)
{
    std::string adxl_engine_name = host + ":" + std::to_string(port);
    disconnect_internal(adxl_engine_name);
}

int AscendDirectEndpoint::connect_internal(const std::string& host, int port)
{
    std::string adxl_engine_name = host + ":" + std::to_string(port);

    if (connected_engines_.count(adxl_engine_name)) {
        SLIME_LOG_INFO("Already connected to ", adxl_engine_name);
        return 0;
    }

    // Black box: AdxlEngine connection
    auto status = adxl_->Connect(adxl_engine_name.c_str(), CONNECT_TIMEOUT_MILLIS);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("Failed to connect to AdxlEngine: ", adxl_engine_name, " status: ", status);
        return -1;
    }

    connected_engines_.insert(adxl_engine_name);
    SLIME_LOG_INFO("Connected to AdxlEngine: ", adxl_engine_name);
    return 0;
}

int AscendDirectEndpoint::disconnect_internal(const std::string& adxl_engine_name)
{
    if (!connected_engines_.count(adxl_engine_name)) {
        SLIME_LOG_WARN("Not connected to ", adxl_engine_name, ", but calling disconnect");
        return -1;
    }

    // Black box: AdxlEngine disconnection
    auto status = adxl_->Disconnect(adxl_engine_name.c_str(), CONNECT_TIMEOUT_MILLIS);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("Failed to disconnect from AdxlEngine: ", adxl_engine_name, " status: ", status);
        return -1;
    }

    connected_engines_.erase(adxl_engine_name);
    SLIME_LOG_INFO("Disconnected from AdxlEngine: ", adxl_engine_name);
    return 0;
}

}  // namespace dlslime
