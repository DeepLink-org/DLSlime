#include "rdma_endpoint.h"

#include <atomic>
#include <memory>
#include <stdexcept>
#include <string>

#include "engine/rdma/rdma_channel.h"
#include "rdma_context_pool.h"
#include "rdma_io_endpoint.h"
#include "rdma_utils.h"
#include "rdma_worker.h"
#include "rdma_worker_pool.h"

namespace dlslime {

// ============================================================
// Constructor & Setup
// ============================================================

RDMAEndpoint::RDMAEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp, std::shared_ptr<RDMAWorker> worker)
{
    ctx_ = ctx;
    if (not ctx_)
        SLIME_ABORT("No NIC Resources");
    memory_pool_ = std::make_shared<RDMAMemoryPool>(ctx);
    worker_      = worker ? worker : GlobalWorkerManager::instance().get_default_worker(socketId(ctx_->device_name_));

    io_endpoint_  = std::make_shared<RDMAIOEndpoint>(ctx_, memory_pool_, num_qp);
    msg_endpoint_ = std::make_shared<RDMAMsgEndpoint>(ctx_, memory_pool_, num_qp);
}

RDMAEndpoint::RDMAEndpoint(
    std::string dev_name, int32_t ib_port, std::string link_type, size_t num_qp, std::shared_ptr<RDMAWorker> worker):
    RDMAEndpoint(GlobalContextManager::instance().get_context(dev_name, ib_port, link_type), num_qp, worker)
{
}

void RDMAEndpoint::connect(const json& remote_endpoint_info)
{
    for (auto& item : remote_endpoint_info["mr_info"].items()) {
        memory_pool_->registerRemoteMemoryRegion(item.value()["mr_key"].get<uintptr_t>(), item.value());
    }

    if (remote_endpoint_info.contains("io_info")) {
        json info{};
        io_endpoint_->connect(remote_endpoint_info["io_info"]);
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'io_info' in remote info");
    }

    if (remote_endpoint_info.contains("msg_info")) {
        msg_endpoint_->connect(remote_endpoint_info["msg_info"]);
    }
    else {
        SLIME_LOG_WARN("UnifiedConnect: Missing 'msg_info' in remote info");
    }
    connected_.store(true, std::memory_order_release);

    worker_->addEndpoint(shared_from_this());
}

json RDMAEndpoint::endpointInfo() const
{
    return json{{"mr_info", memory_pool_->mr_info()},
                {"io_info", io_endpoint_->endpointInfo()},
                {"msg_info", msg_endpoint_->endpointInfo()}};
}

void RDMAEndpoint::shutdown()
{
    connected_.store(false, std::memory_order_release);

    // Manually cancel all pending futures to unblock waiting threads (e.g. Client destructor)
    if (io_endpoint_) {
        io_endpoint_->cancelAll();
    }
    if (msg_endpoint_) {
        msg_endpoint_->cancelAll();
    }

    if (worker_) {
        worker_->removeEndpoint(shared_from_this());
    }

    // Do NOT reset endpoints here.
    // Worker loop might still be accessing them until removed.
    // Let shared_ptr reference counting handle destruction naturally.
}

// ============================================================
// Memory Management
// ============================================================

int32_t RDMAEndpoint::registerOrAccessMemoryRegion(uintptr_t mr_key, uintptr_t ptr, uintptr_t offset, size_t length)
{
    return memory_pool_->registerMemoryRegion(mr_key, ptr + offset, length);
}

int32_t RDMAEndpoint::registerOrAccessRemoteMemoryRegion(uintptr_t ptr, json mr_info)
{
    return memory_pool_->registerRemoteMemoryRegion(ptr, mr_info);
}

// ============================================================
// Two-Sided Primitives (Message Passing) -> MsgEndpoint
// ============================================================

std::shared_ptr<SendFuture> RDMAEndpoint::send(const chunk_tuple_t& chunk, void* stream_handler)
{
    return msg_endpoint_->send(chunk, stream_handler);
}

std::shared_ptr<RecvFuture> RDMAEndpoint::recv(const chunk_tuple_t& chunk, void* stream_handler)
{
    return msg_endpoint_->recv(chunk, stream_handler);
}

// ============================================================
// One-Sided Primitives (RDMA IO) -> IOEndpoint
// ============================================================

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::read(const std::vector<assign_tuple_t>& assign, void* stream)

{
    return io_endpoint_->read(assign, stream);
};

std::shared_ptr<ReadWriteFuture> RDMAEndpoint::write(const std::vector<assign_tuple_t>& assign, void* stream)
{
    return io_endpoint_->write(assign, stream);
}

std::shared_ptr<ReadWriteFuture>
RDMAEndpoint::writeWithImm(const std::vector<assign_tuple_t>& assign, int32_t imm_data, void* stream)
{
    return io_endpoint_->writeWithImm(assign, imm_data, stream);
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
