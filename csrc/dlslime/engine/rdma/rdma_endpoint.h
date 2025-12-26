#pragma once

#include "rdma_context.h"
#include "rdma_io_endpoint.h"
#include "rdma_msg_endpoint.h"

#include "dlslime/json.hpp"

#include <memory>

namespace dlslime {

using json = nlohmann::json;

class SendFuture;
class RecvFuture;
class ReadWriteFuture;
class ImmRecvFuture;

class RDMAWorker;

class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {
public:
    RDMAEndpoint(std::shared_ptr<RDMAContext> ctx, size_t num_qp, std::shared_ptr<RDMAWorker> worker = nullptr);

    RDMAEndpoint(std::string                 dev_name  = "",
                 int32_t                     ib_port   = 1,
                 std::string                 link_type = "RoCE",
                 size_t                      num_qp    = 1,
                 std::shared_ptr<RDMAWorker> worker    = nullptr);

    void connect(const json& remote_endpoint_info);

    json endpointInfo() const;

    int32_t registerOrAccessMemoryRegion(uintptr_t mr_key, uintptr_t ptr, size_t length);
    int32_t registerOrAccessRemoteMemoryRegion(uintptr_t ptr, json mr_info);

    // TwoSide Primitive
    std::shared_ptr<SendFuture> send(uintptr_t data_ptr, size_t offset, size_t length, void* stream_handler);
    std::shared_ptr<RecvFuture> recv(uintptr_t data_ptr, size_t offset, size_t length, void* stream_handler);

    // OneSide Primitive
    std::shared_ptr<ReadWriteFuture> read(std::vector<uintptr_t>& local_ptr,
                                          std::vector<uintptr_t>& remote_ptr,
                                          std::vector<uintptr_t>& target_offset,
                                          std::vector<uintptr_t>& source_offset,
                                          std::vector<size_t>&    length,
                                          void*                   stream);
    std::shared_ptr<ReadWriteFuture> write(std::vector<uintptr_t>& local_ptr,
                                           std::vector<uintptr_t>& remote_ptr,
                                           std::vector<uintptr_t>& target_offset,
                                           std::vector<uintptr_t>& source_offset,
                                           std::vector<size_t>&    length,
                                           void*                   stream);
    std::shared_ptr<ReadWriteFuture> writeWithImm(std::vector<uintptr_t>& local_ptr,
                                                  std::vector<uintptr_t>& remote_ptr,
                                                  std::vector<uintptr_t>& target_offset,
                                                  std::vector<uintptr_t>& source_offset,
                                                  std::vector<size_t>&    length,
                                                  int32_t                 imm_data,
                                                  void*                   stream);

    std::shared_ptr<ImmRecvFuture> immRecv(void* stream = nullptr);

    int32_t process();

    void setId(int64_t id) { id_.store(id, std::memory_order_relaxed); }
    int64_t getId() const { return id_.load(std::memory_order_relaxed); }

private:
    std::atomic<int64_t>             id_{-1};
    std::atomic<bool>                connected_{false};
    std::shared_ptr<RDMAContext>     ctx_;
    std::shared_ptr<RDMAWorker>      worker_;
    std::shared_ptr<RDMAIOEndpoint>  io_endpoint_;
    std::shared_ptr<RDMAMsgEndpoint> msg_endpoint_;
};

}  // namespace dlslime
