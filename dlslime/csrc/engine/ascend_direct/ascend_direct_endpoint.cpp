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

    auto ctx     = std::make_unique<AscendContext>();
    ctx->state_  = AscendIOContextState::PENDING;
    ctx->signal  = nullptr;
    ctx->slot_id = -1;

    std::vector<adxl::TransferOpDesc> op_descs;
    op_descs.reserve(assign.size());

    for (auto& assign_tuple : assign) {
        auto local_handle  = static_cast<int32_t>(std::get<0>(assign_tuple));
        auto remote_handle = static_cast<int32_t>(std::get<1>(assign_tuple));
        auto target_offset = std::get<2>(assign_tuple);
        auto source_offset = std::get<3>(assign_tuple);
        auto length        = std::get<4>(assign_tuple);

        ascend_local_mr_t  local_mr  = local_pool_->get_mr_fast(local_handle);
        ascend_remote_mr_t remote_mr = remote_pool_->get_remote_mr_fast(remote_handle);

        if (local_mr.addr == 0 || remote_mr.addr == 0) {
            SLIME_LOG_ERROR("Memory region not found: local_handle=", local_handle, " remote_handle=", remote_handle);
            return nullptr;
        }

        adxl::TransferOpDesc op_desc;
        op_desc.local_addr  = local_mr.addr + source_offset;
        op_desc.remote_addr = remote_mr.addr + target_offset;
        op_desc.len         = length;

        op_descs.push_back(op_desc);

        ctx->assigns_.emplace_back(static_cast<uintptr_t>(local_handle),
                                   static_cast<uintptr_t>(remote_handle),
                                   target_offset,
                                   source_offset,
                                   length);
    }

    if (connected_engines_.empty()) {
        SLIME_LOG_ERROR("No connected remote engines for read operation");
        return nullptr;
    }

    std::string remote_engine_name = *connected_engines_.begin();

    ctx->state_ = AscendIOContextState::POSTED;
    auto status = adxl_->TransferSync(remote_engine_name.c_str(), adxl::READ, op_descs, CONNECT_TIMEOUT_MILLIS);

    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("TransferSync failed in AscendDirectEndpoint::read, status: ", status);
        ctx->state_ = AscendIOContextState::FREE;
        return nullptr;
    }

    ctx->state_ = AscendIOContextState::DONE;
    ctx->completed.store(true, std::memory_order_release);

    auto ctx_raw = ctx.release();
    return std::make_shared<AscendFuture>(ctx_raw);
}

std::shared_ptr<AscendFuture> AscendDirectEndpoint::read(std::vector<named_assign_tuple_t>& named_assign,
                                                         void*                              stream_handle)
{
    std::vector<assign_tuple_t> resolved;
    resolved.reserve(named_assign.size());

    for (auto& na : named_assign) {
        int32_t local_handle  = local_pool_->get_mr_handle(std::get<0>(na));
        int32_t remote_handle = remote_pool_->get_mr_handle(std::get<1>(na));

        if (local_handle < 0 || remote_handle < 0) {
            SLIME_LOG_ERROR("Named read: MR not found, local=", std::get<0>(na), " remote=", std::get<1>(na));
            return nullptr;
        }

        resolved.emplace_back(static_cast<uintptr_t>(local_handle),
                              static_cast<uintptr_t>(remote_handle),
                              std::get<2>(na),
                              std::get<3>(na),
                              std::get<4>(na));
    }
    return read(resolved, stream_handle);
}

int32_t AscendDirectEndpoint::register_memory_region(uintptr_t                  addr,
                                                     size_t                     offset,
                                                     size_t                     length,
                                                     std::optional<std::string> name)
{
    adxl::MemType mem_type = adxl::MEM_DEVICE;

    adxl::MemDesc mem_desc{};
    mem_desc.addr = static_cast<uint64_t>(addr + offset);
    mem_desc.len  = length;

    adxl::MemHandle mem_handle;
    auto            status = adxl_->RegisterMem(mem_desc, mem_type, mem_handle);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("adxl RegisterMem failed for addr ", addr, ", status: ", status);
        return -1;
    }

    int32_t handle = local_pool_->register_memory_region(addr, offset, length, mem_handle, name);

    registered_mr_handles_.insert(handle);
    return handle;
}

void AscendDirectEndpoint::unregister_memory_region(int32_t handle)
{
    if (!local_pool_->has_mr(handle)) {
        SLIME_LOG_WARN("Attempted to unregister non-existent memory region handle: ", handle);
        return;
    }

    adxl::MemHandle adxl_handle = local_pool_->get_adxl_handle(handle);
    adxl_->DeregisterMem(adxl_handle);

    local_pool_->unregister_memory_region(handle);
    registered_mr_handles_.erase(handle);

    SLIME_LOG_INFO("Unregistered memory region: handle=", handle);
}

int32_t AscendDirectEndpoint::register_remote_memory_region(const json& mr_info, std::optional<std::string> name)
{
    return remote_pool_->register_remote_memory_region(mr_info, name);
}

int32_t AscendDirectEndpoint::get_mr_handle(const std::string& name)
{
    return local_pool_->get_mr_handle(name);
}

int32_t AscendDirectEndpoint::get_remote_mr_handle(const std::string& name)
{
    return remote_pool_->get_mr_handle(name);
}

json AscendDirectEndpoint::endpoint_info() const
{
    json info;
    info["host"]    = local_host_;
    info["port"]    = local_port_;
    info["mr_info"] = local_pool_->mr_info();
    return info;
}

json AscendDirectEndpoint::mr_info() const
{
    return local_pool_->mr_info();
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

    if (remote_info.contains("mr_info")) {
        for (auto& item : remote_info["mr_info"].items()) {
            register_remote_memory_region(item.value(), item.key());
        }
    }
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
