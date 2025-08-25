#pragma once
#include "engine/assignment.h"
#include "engine/rdma/rdma_context.h"

#include <atomic>
#include <condition_variable>
#include <infiniband/verbs.h>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "rdma_common.h"

#define MAX_META_BATCH_SIZE 8
#define MAX_META_BUFFER_SIZE 64

namespace slime {

class RDMABuffer;
class RDMAEndpoint;

using callback_t = std::function<void()>;
using json       = nlohmann::json;

typedef struct MetaData {

    uint64_t mr_addr[MAX_META_BATCH_SIZE];
    uint32_t mr_rkey[MAX_META_BATCH_SIZE];
    uint32_t mr_size[MAX_META_BATCH_SIZE];
    uint32_t mr_slot;
    uint32_t mr_qpidx;

} __attribute__((packed)) meta_data_t;

typedef struct RDMATask {
    explicit RDMATask(std::shared_ptr<RDMAEndpoint> endpoint,
                      uint32_t                      task_id,
                      OpCode                        opcode,
                      std::shared_ptr<RDMABuffer>   buffer);
    ~RDMATask();

    std::string getMetaKey();
    std::string getDataKey(int32_t idx);

    AssignmentBatch getMetaAssignmentBatch();
    AssignmentBatch getDataAssignmentBatch();

    int registerMetaMemoryRegion();
    int registerDataMemoryRegion();
    int registerRemoteDataMemoryRegion();

    void fillBuffer();

    int targetQPI();

    uint32_t                    slot_id_;
    OpCode                      opcode_;
    std::shared_ptr<RDMABuffer> buffer_;

    std::shared_ptr<RDMAEndpoint> endpoint_;
} rdma_task_t;

class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {
    friend class RDMABuffer;

public:
    explicit RDMAEndpoint(const std::string& dev_name, uint8_t ib_port, const std::string& link_type, size_t qp_num);

    ~RDMAEndpoint();

    void connect(const json& data_ctx_info, const json& meta_ctx_info);

    std::shared_ptr<RDMABuffer> createRDMABuffer(storage_view_batch_t batch);

    void addRecvTask(std::shared_ptr<RDMABuffer> buffer);
    void addSendTask(std::shared_ptr<RDMABuffer> buffer);

    json getDataContextInfo() const
    {
        return data_ctx_->endpoint_info();
    }

    json getMetaContextInfo() const
    {
        return meta_ctx_->endpoint_info();
    }

    std::shared_ptr<RDMAContext> dataCtx()
    {
        return data_ctx_;
    }

    std::shared_ptr<RDMAContext> MetaCtx()
    {
        return meta_ctx_;
    }

    int dataCtxQPNum()
    {
        return data_ctx_qp_num_;
    }

    std::vector<meta_data_t>& getMetaBuffer()
    {
        return meta_buffer_;
    }

private:
    void waitandPopTask(std::chrono::milliseconds timeout);

    void asyncRecvData(std::shared_ptr<rdma_task_t> task);
    void asyncSendData(std::shared_ptr<rdma_task_t> task);

    size_t data_ctx_qp_num_;
    size_t meta_ctx_qp_num_;

    std::atomic<uint32_t> send_slot_id_{RDMAContext::UNDEFINED_IMM_DATA};
    std::atomic<uint32_t> recv_slot_id_{RDMAContext::UNDEFINED_IMM_DATA};

    std::vector<meta_data_t> meta_buffer_;

    uint32_t batch_size_;

    std::map<uint32_t, std::shared_ptr<rdma_task_t>> send_batch_slot_;
    std::map<uint32_t, std::shared_ptr<rdma_task_t>> recv_batch_slot_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;

    std::queue<std::shared_ptr<rdma_task_t>> rdma_tasks_queue_;
    std::thread                              rdma_tasks_threads_;

    std::condition_variable rdma_tasks_cv_;
    std::mutex              rdma_tasks_mutex_;

    std::map<uint32_t, std::function<void()>> imm_data_callback_;

    bool RDMA_tasks_threads_running_;
};

}  // namespace slime
