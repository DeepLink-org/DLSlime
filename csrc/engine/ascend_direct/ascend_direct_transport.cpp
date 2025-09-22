#include <memory>
#include <string>

#include "adxl/adxl_engine.h"
#include "ascend_direct_transport.h"
#include "engine/assignment.h"

#include "ascend_direct_transport.h"

namespace slime {

AscendDirectContext::~AscendDirectContext()
{
    std::vector<uintptr_t> addrs;
    for (auto& p : addr_to_memhandle_) {
        addrs.push_back(p.first);
    }
    for (uintptr_t a : addrs) {
        unregister_memory_region(a);
    }

    std::vector<std::string> adxl_names;
    for (const std::string name : connected_engines_) {
        adxl_names.push_back(name);
    }
    for (const std::string& name : adxl_names) {
        disconnect(name);
    }

    adxl_.reset();
}

int AscendDirectContext::init(const std::string& host, int host_port)
{
    adxl_ = std::make_unique<adxl::AdxlEngine>();
    if (adxl_ == nullptr) {
        SLIME_LOG_ERROR("Failed to create AdxlEngine instance");
        return -1;
    }

    std::string                                      adxl_engine_name = host + ":" + std::to_string(host_port);
    std::map<adxl::AscendString, adxl::AscendString> options;

    auto status = adxl_->Initialize(adxl::AscendString(adxl_engine_name.c_str()), options);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("Failed to initialize AdxlEngine, status: ", status);
        return -1;
    }

    SLIME_LOG_INFO("Initialized AdxlEngine at ", adxl_engine_name);
    return 0;
}

int AscendDirectContext::read_batch(AssignmentBatch& batch, const std::string& host, int port)
{
    int conn_state = connect(host, port);
    if (conn_state != 0) {
        SLIME_LOG_ERROR("Failed to connect to remote when read_batch ", host, ":", port);
        return -1;
    }

    adxl::TransferOp                  operation = adxl::READ;
    std::vector<adxl::TransferOpDesc> op_descs;
    for (Assignment& assign : batch) {
        adxl::TransferOpDesc op_des;
        op_des.local_addr  = static_cast<uintptr_t>(assign.source_offset);
        op_des.remote_addr = static_cast<uintptr_t>(assign.target_offset);
        op_des.len         = assign.length;
        op_descs.push_back(op_des);
    }

    std::string adxl_engine_name = host + ":" + std::to_string(port);
    auto        status = adxl_->TransferSync(adxl_engine_name.c_str(), operation, op_descs, CONNECT_TIMEOUT_MILLIS);
    if (status != 0) {
        SLIME_LOG_ERROR("TransferSync failed in AscendDirectContext::read_batch, status code = ", status);
    }

    return 0;
}

int AscendDirectContext::register_memory_region(const std::string& location, uintptr_t addr, size_t length)
{
    adxl::MemType mem_type;
    if (location.substr(0, 3) == "cpu") {
        mem_type = adxl::MEM_HOST;
    }
    else if (location.substr(0, 3) == "npu") {
        mem_type = adxl::MEM_DEVICE;
    }
    else {
        SLIME_LOG_ERROR("Unsupported adxl memory location type");
        return -1;
    }

    adxl::MemDesc mem_desc{};
    mem_desc.addr = static_cast<uint64_t>(addr);
    mem_desc.len  = length;

    adxl::MemHandle mem_handle;
    auto            adxl_ret = adxl_->RegisterMem(mem_desc, mem_type, mem_handle);
    if (adxl_ret != adxl::SUCCESS) {
        SLIME_LOG_ERROR("adxl RegisterMem ", addr, " failed with code: ", adxl_ret, ".");
        return -1;
    }

    addr_to_memhandle_[addr] = mem_handle;
    return 0;
}

int AscendDirectContext::unregister_memory_region(uintptr_t addr)
{
    auto iter = addr_to_memhandle_.find(addr);
    if (iter == addr_to_memhandle_.end()) {
        SLIME_LOG_WARN("Unregister a non-exist memory ", addr);
        return -1;
    }
    adxl_->DeregisterMem(addr_to_memhandle_[addr]);
    addr_to_memhandle_.erase(addr);
    return 0;
}

int AscendDirectContext::connect(const std::string& host, int port)
{
    std::string adxl_engine_name = host + ":" + std::to_string(port);
    if (connected_engines_.count(adxl_engine_name)) {
        SLIME_LOG_INFO("Already connected to ", adxl_engine_name);
        return 0;
    }

    auto status = adxl_->Connect(adxl_engine_name.c_str(), CONNECT_TIMEOUT_MILLIS);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("Failed to connect to AdxlEngine: ", adxl_engine_name);
        return -1;
    }
    connected_engines_.insert(adxl_engine_name);
    SLIME_LOG_INFO("Connected to AdxlEngine: ", adxl_engine_name);
    return 0;
}

int AscendDirectContext::disconnect(const std::string& adxl_engine_name)
{
    if (!connected_engines_.count(adxl_engine_name)) {
        SLIME_LOG_WARN("Have NOT connected to ", adxl_engine_name, ", but calling disconnect");
        return -1;
    }

    auto status = adxl_->Disconnect(adxl_engine_name.c_str(), CONNECT_TIMEOUT_MILLIS);
    if (status != adxl::SUCCESS) {
        SLIME_LOG_ERROR("Failed to disconnect to AdxlEngine", adxl_engine_name);
        return -1;
    }

    connected_engines_.erase(adxl_engine_name);
    SLIME_LOG_INFO("Disconnected to AdxlEngine: ", adxl_engine_name);
    return 0;
}

int AscendDirectContext::disconnect(const std::string& host, int port)
{
    std::string adxl_engine_name = host + ":" + std::to_string(port);
    return disconnect(adxl_engine_name);
}

}  // namespace slime
