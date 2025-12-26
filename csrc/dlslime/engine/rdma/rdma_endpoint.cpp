#include "rdma_endpoint.h"

#include "rdma_context_pool.h"
#include "rdma_io_endpoint.h"
#include "rdma_utils.h"
#include "rdma_worker.h"
#include "rdma_worker_pool.h"

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>

namespace dlslime {

// ============================================================
// Constructor & Setup
// ============================================================

RDMAEndpoint::RDMAEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp, std::shared_ptr<RDMAWorker> worker)
{
    ctx_    = ctx ? ctx : GlobalContextManager::instance().get_context();
    worker_ = worker ? worker : GlobalWorkerManager::instance().get_default_worker(socketId(ctx_->device_name_));

    io_endpoint_  = std::make_shared<RDMAIOEndpoint>(ctx_, num_qp);
    msg_endpoint_ = std::make_shared<RDMAMsgEndpoint>(ctx_, num_qp);
}

RDMAEndpoint::RDMAEndpoint(
    std::string dev_name, int32_t ib_port, std::string link_type, size_t num_qp, std::shared_ptr<RDMAWorker> worker)
{
    ctx_    = GlobalContextManager::instance().get_context(dev_name, ib_port, link_type);
    worker_ = worker ? worker : GlobalWorkerManager::instance().get_default_worker(socketId(ctx_->device_name_));

    io_endpoint_  = std::make_shared<RDMAIOEndpoint>(ctx_, num_qp);
    msg_endpoint_ = std::make_shared<RDMAMsgEndpoint>(ctx_, num_qp);
}

void RDMAEndpoint::connect(const json& remote_endpoint_info)
{
    if (remote_endpoint_info.contains("io_info")) {
        json info{};
        info["mr_info"] = remote_endpoint_info["mr_info"];
        info.update(remote_endpoint_info["io_info"]);
        io_endpoint_->connect(info);
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'io_info' in remote info");
    }

    if (remote_endpoint_info.contains("msg_info")) {
        json info{};
        info["mr_info"] = remote_endpoint_info["mr_info"];
        info.update(remote_endpoint_info["msg_info"]);
        msg_endpoint_->connect(info);
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'msg_info' in remote info");
    }
    connected_.store(true, std::memory_order_release);

    worker_->addEndpoint(shared_from_this());
}

json RDMAEndpoint::endpointInfo() const
{
    // 2. 聚合连接信息：将两个子 Endpoint 的信息打包
    return json{{"mr_info", ctx_->memory_pool_->mr_info()},
                {"io_info", io_endpoint_->endpointInfo()},
                {"msg_info", msg_endpoint_->endpointInfo()}};
}

// ============================================================
// Memory Management
// ============================================================

int32_t RDMAEndpoint::registerOrAccessMemoryRegion(uintptr_t mr_key, uintptr_t ptr, size_t length)
{
    return ctx_->registerOrAccessMemoryRegion(mr_key, ptr, length);
}

int32_t RDMAEndpoint::registerOrAccessRemoteMemoryRegion(uintptr_t ptr, json mr_info)
{
    return ctx_->registerOrAccessRemoteMemoryRegion(ptr, mr_info);
}

// ============================================================
// Two-Sided Primitives (Message Passing) -> MsgEndpoint
// ============================================================

std::shared_ptr<SendFuture> RDMAEndpoint::send(uintptr_t data_ptr, size_t offset, size_t length, void* stream_handler)
{
    return msg_endpoint_->send(data_ptr, offset, length, stream_handler);
}

std::shared_ptr<RecvFuture> RDMAEndpoint::recv(uintptr_t data_ptr, size_t offset, size_t length, void* stream_handler)
{
    return msg_endpoint_->recv(data_ptr, offset, length, stream_handler);
}

// ============================================================
// One-Sided Primitives (RDMA IO) -> IOEndpoint
// ============================================================

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::read(std::vector<uintptr_t>& local_ptr,
                                                    std::vector<uintptr_t>& remote_ptr,
                                                    std::vector<uintptr_t>& target_offset,
                                                    std::vector<uintptr_t>& source_offset,
                                                    std::vector<size_t>&    length,
                                                    void*                   stream)

{
    return io_endpoint_->read(local_ptr, remote_ptr, target_offset, source_offset, length, stream);
};

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::write(std::vector<uintptr_t>& local_ptr,
                                                     std::vector<uintptr_t>& remote_ptr,
                                                     std::vector<uintptr_t>& target_offset,
                                                     std::vector<uintptr_t>& source_offset,
                                                     std::vector<size_t>&    length,
                                                     void*                   stream)
{
    return io_endpoint_->write(local_ptr, remote_ptr, target_offset, source_offset, length, stream);
}

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::writeWithImm(std::vector<uintptr_t>& local_ptr,
                                                            std::vector<uintptr_t>& remote_ptr,
                                                            std::vector<uintptr_t>& target_offset,
                                                            std::vector<uintptr_t>& source_offset,
                                                            std::vector<size_t>&    length,
                                                            int32_t                 imm_data,
                                                            void*                   stream)
{
    return io_endpoint_->writeWithImm(local_ptr, remote_ptr, target_offset, source_offset, length, imm_data, stream);
}

std::shared_ptr<ImmRecvFuture> RDMAEndpoint::immRecv(void* stream)
{
    return io_endpoint_->immRecv(stream);
}

int32_t RDMAEndpoint::process()
{
    if (connected_.load(std::memory_order_acquire)) {
        return msg_endpoint_->process() + io_endpoint_->process();
    }
    return 0;
}

}  // namespace dlslime
