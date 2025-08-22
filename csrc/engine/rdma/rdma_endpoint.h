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

#define MAX_BATCH_SIZE 8
#define MAX_META_SIZE 64

namespace slime {

class RDMABuffer;
class RDMAEndpoint;
class SimpleBuffer;
class RDMAMetaBufferManager;

using callback_t = std::function<void()>;
using json       = nlohmann::json;

struct __attribute__((packed)) metaBuffer {

    uint64_t mr_addr[MAX_BATCH_SIZE];
    uint32_t mr_rkey[MAX_BATCH_SIZE];
    uint32_t mr_size[MAX_BATCH_SIZE];

    void cleanBuffer()
    {
        std::fill(std::begin(mr_addr), std::end(mr_addr), 0);
        std::fill(std::begin(mr_rkey), std::end(mr_rkey), 0);
        std::fill(std::begin(mr_size), std::end(mr_size), 0);
    }

    bool fillBuffer(int idx, uint64_t addr, uint32_t rkey, uint32_t size)
    {
        if (idx < 0 || idx >= MAX_BATCH_SIZE) {
            throw std::out_of_range("Index out of range for metaBuffer");
        }
        mr_addr[idx] = addr;
        mr_rkey[idx] = rkey;
        mr_size[idx] = size;
        return true;
    }
};

struct __attribute__((packed)) remoteBufferInfo {
    uint64_t buffer_addr[2];
    uint32_t buffer_rkey[2];
    uint32_t buffer_size[2];
};

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

    void fillMetaInfo();
    void fillBuffer();

    int targetQPI();

    uint32_t                    slot_id_;
    OpCode                      opcode_;
    std::shared_ptr<RDMABuffer> buffer_;

    // meta_data_t* meta_data_buf_{nullptr};

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

    std::shared_ptr<RDMAContext> metaCtx()
    {
        return meta_ctx_;
    }

    int dataCtxQPNum()
    {
        return data_ctx_qp_num_;
    }

    // std::vector<meta_data_t*> send_meta_pool_;
    // std::vector<meta_data_t*> recv_meta_pool_;

private:
    void endPointQueue(std::chrono::milliseconds timeout);

    void asyncRecvData(std::shared_ptr<rdma_task_t> task);
    void asyncSendData(std::shared_ptr<rdma_task_t> task);

    size_t data_ctx_qp_num_;
    size_t meta_ctx_qp_num_;

    std::unique_ptr<SimpleBuffer> s_send_buffer_;
    std::unique_ptr<SimpleBuffer> s_recv_buffer_;

    std::unique_ptr<RDMAMetaBufferManager> meta_buffer_manager_;

    std::atomic<uint32_t> send_slot_id_{RDMAContext::UNDEFINED_IMM_DATA};
    std::atomic<uint32_t> recv_slot_id_{RDMAContext::UNDEFINED_IMM_DATA};

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

    remoteBufferInfo* remote_buffer_info_{nullptr};
    remoteBufferInfo* local_buffer_info_{nullptr};

    bool RDMA_tasks_threads_running_;
};

class SimpleBuffer {

public:
    SimpleBuffer(size_t num_buffer, size_t element_size, size_t batch_size, std::shared_ptr<RDMAEndpoint> end_point):
        num_buffer_(num_buffer), element_size_(element_size), batch_size_(batch_size), end_point_(end_point)
    {

        element_buffer_size_ = element_size * batch_size;
        total_size_          = num_buffer * element_buffer_size_;
        simple_buffer_ptr_   = new uint8_t[total_size_];
        cur_buffer_idx_      = 0;
        buffer_status_.resize(num_buffer);
        for (auto& buffer_status : buffer_status_) {
            buffer_status.store(false);
        }

        std::cout << "SimpleBuffer created with " << num_buffer_ << " buffers of size " << element_buffer_size_
                  << "Bytes" << std::endl;

        end_point_->dataCtx()->register_memory_region(
            "SIMPLE_BUFFER", reinterpret_cast<uintptr_t>(simple_buffer_ptr_), total_size_);
    }

    ~SimpleBuffer()
    {
        end_point_->dataCtx()->unregister_memory_region("SIMPLE_BUFFER");
        delete[] simple_buffer_ptr_;
    }

    size_t getNumBuffer() const
    {
        return num_buffer_;
    }

    uint32_t getSimpleBufferRkey()
    {
        return end_point_->dataCtx()->get_mr("SIMPLE_BUFFER")->rkey;
    }

    bool
    copyTensorToBuffer(size_t idx, std::vector<uintptr_t> ptrs, std::vector<size_t> offset, std::vector<size_t> size)
    {
        if (idx < 0 || idx >= num_buffer_) {
            throw std::out_of_range("Index out of range for SimpleBuffer");
        }
        if (size[0] > element_buffer_size_) {
            throw std::runtime_error("Data size exceeds buffer size");
        }

        for (size_t i = 0; i < ptrs.size(); ++i) {
            if (offset[i] + size[i] > element_size_) {
                throw std::runtime_error("Offset and size exceed element size");
            }
            void* data = reinterpret_cast<void*>(ptrs[i] + offset[i]);
            std::memcpy(simple_buffer_ptr_ + idx * element_buffer_size_ + element_buffer_size_ * i, data, size[i]);
        }
        buffer_status_[idx].store(true);
        return true;
    }

    bool
    copyTensorFromBuffer(size_t idx, std::vector<uintptr_t> ptrs, std::vector<size_t> offset, std::vector<size_t> size)
    {
        if (idx < 0 || idx >= num_buffer_) {
            throw std::out_of_range("Index out of range for SimpleBuffer");
        }
        if (size[0] > element_buffer_size_) {
            throw std::runtime_error("Data size exceeds buffer size");
        }
        if (!buffer_status_[idx].load()) {
            throw std::runtime_error("Buffer not ready for reading");
        }

        for (size_t i = 0; i < ptrs.size(); ++i) {
            if (offset[i] + size[i] > element_size_) {
                throw std::runtime_error("Offset and size exceed element size");
            }
            void* data = reinterpret_cast<void*>(ptrs[i] + offset[i]);
            std::memcpy(data, simple_buffer_ptr_ + idx * element_buffer_size_ + element_buffer_size_ * i, size[i]);
        }
        buffer_status_[idx].store(false);
        return true;
    }

    void* getIdxBufferPtr(size_t idx)
    {
        if (idx < 0 || idx >= num_buffer_) {
            throw std::out_of_range("Index out of range for SimpleBuffer");
        }
        return static_cast<void*>(simple_buffer_ptr_ + idx * element_buffer_size_);
    }

    void* getCurrentBufferPtr()
    {
        return static_cast<void*>(simple_buffer_ptr_ + cur_buffer_idx_ * element_buffer_size_);
    }

private:
    uint8_t* simple_buffer_ptr_;

    std::shared_ptr<RDMAEndpoint>  end_point_;
    std::vector<std::atomic<bool>> buffer_status_;
    std::atomic<size_t>            cur_buffer_idx_{0};

    size_t num_buffer_;
    size_t total_size_;
    size_t batch_size_;
    size_t element_size_;
    size_t element_buffer_size_;
};

class RDMAMetaBufferManager {

public:
    RDMAMetaBufferManager() = default;
    RDMAMetaBufferManager(const int MAX_META_BUFFER_SIZE, std::shared_ptr<RDMAEndpoint> endPoint): endPoint_(endPoint)
    {
        meta_tx_buffer_.resize(MAX_META_BUFFER_SIZE);
        meta_rx_buffer_.resize(MAX_META_BUFFER_SIZE);

        for (int idx = 0; idx < MAX_META_BUFFER_SIZE; ++idx) {
            meta_tx_buffer_[idx].cleanBuffer();
            meta_rx_buffer_[idx].cleanBuffer();
        }

        endPoint_->metaCtx()->register_memory_region("META_SEND_BUFFER",
                                                     reinterpret_cast<uintptr_t>(meta_tx_buffer_.data()),
                                                     sizeof(metaBuffer) * MAX_META_BUFFER_SIZE);
        endPoint_->metaCtx()->register_memory_region("META_RECV_BUFFER",
                                                     reinterpret_cast<uintptr_t>(meta_rx_buffer_.data()),
                                                     sizeof(metaBuffer) * MAX_META_BUFFER_SIZE);

        SLIME_LOG_INFO("RDMA Meta Buffer Manager created.");
    }
    ~RDMAMetaBufferManager()
    {

        for (auto& buffer : meta_tx_buffer_) {
            buffer.cleanBuffer();
        }
        for (auto& buffer : meta_rx_buffer_) {
            buffer.cleanBuffer();
        }

        endPoint_->metaCtx()->unregister_memory_region("META_SEND_BUFFER");
        endPoint_->metaCtx()->unregister_memory_region("META_RECV_BUFFER");
    }

    uint32_t getSendMetaBufferRkey()
    {
        return endPoint_->metaCtx()->get_mr("META_SEND_BUFFER")->rkey;
    }

    uint32_t getRecvMetaBufferRkey()
    {
        return endPoint_->metaCtx()->get_mr("META_RECV_BUFFER")->rkey;
    }

    uint64_t getSendMetaBufferAddr()
    {
        return reinterpret_cast<uint64_t>(meta_tx_buffer_.data());
    }
    uint64_t getRecvMetaBufferAddr()
    {
        return reinterpret_cast<uint64_t>(meta_rx_buffer_.data());
    }

    metaBuffer& getSendMetaBuffer()
    {
        if (cur_send_idx_ < 0 || cur_send_idx_ >= meta_tx_buffer_.size()) {
            SLIME_LOG_ERROR("Index out of range for send meta buffer.")
            throw std::out_of_range("Index out of range for send meta buffer");
        }
        cur_send_idx_++;
        return meta_tx_buffer_[cur_send_idx_];
    }

    metaBuffer& getRecvMetaBuffer()
    {
        if (cur_recv_idx_ < 0 || cur_recv_idx_ >= meta_rx_buffer_.size()) {
            SLIME_LOG_ERROR("Index out of range for receive meta buffer.");
            throw std::out_of_range("Index out of range for receive meta buffer");
        }
        cur_recv_idx_++;
        return meta_rx_buffer_[cur_recv_idx_];
    }

    void cleanBuffer(size_t idx, bool isSend)
    {
        if (idx < 0 || idx >= meta_tx_buffer_.size()) {
            SLIME_LOG_ERROR("Index out of range for send meta buffer.")
            throw std::out_of_range("Index out of range for send meta buffer");
        }
        if (idx < 0 || idx >= meta_rx_buffer_.size()) {
            SLIME_LOG_ERROR("Index out of range for receive meta buffer.");
            throw std::out_of_range("Index out of range for receive meta buffer");
        }

        if (isSend)
            meta_tx_buffer_[idx].cleanBuffer();
        else
            meta_rx_buffer_[idx].cleanBuffer();
    }

    RDMAMetaBufferManager(const RDMAMetaBufferManager&)            = delete;
    RDMAMetaBufferManager& operator=(const RDMAMetaBufferManager&) = delete;

private:
    std::vector<metaBuffer> meta_tx_buffer_;
    std::vector<metaBuffer> meta_rx_buffer_;

    std::shared_ptr<RDMAEndpoint> endPoint_;

    std::atomic<size_t> cur_send_idx_{0};
    std::atomic<size_t> cur_recv_idx_{0};
};

}  // namespace slime
