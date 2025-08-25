#pragma once
#include "engine/assignment.h"
#include "engine/rdma/rdma_buffer.h"
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
#include <zmq.hpp>

#include "rdma_common.h"

#define MAX_BATCH_SIZE 8
#define MAX_META_SIZE 64
#define MAX_ELEMENT_SIZE 8192
#define MAX_BUFFER_BATCH_SIZE 8



namespace slime {

class RDMABuffer;
class RDMAEndpoint;
class MetaBufferManager;

using callback_t = std::function<void()>;
using json       = nlohmann::json;

struct __attribute__((packed)) metaBuffer {

    uint64_t mr_addr[MAX_BATCH_SIZE];
    uint32_t mr_rkey[MAX_BATCH_SIZE];
    uint32_t mr_size[MAX_BATCH_SIZE];

    void metaBufferReset()
    {
        std::fill(std::begin(mr_addr), std::end(mr_addr), 0);
        std::fill(std::begin(mr_rkey), std::end(mr_rkey), 0);
        std::fill(std::begin(mr_size), std::end(mr_size), 0);
    }

    bool metaBufferRenew(int idx, uint64_t addr, uint32_t rkey, uint32_t size)
    {
        if (idx < 0 || idx >= MAX_BATCH_SIZE) {
            SLIME_LOG_ERROR("Index out of range for metaBuffer");
            return false;
        }
        mr_addr[idx] = addr;
        mr_rkey[idx] = rkey;
        mr_size[idx] = size;
        return true;
    }
};

// for short data transmit
struct LowLatencyBuffer {

    LowLatencyBuffer(size_t num_buffer, size_t element_size, size_t batch_size)
    {
        ptr          = new uint8_t[num_buffer * element_size * batch_size];
        buffer_size  = num_buffer * element_size * batch_size;
        segment_size = element_size;
        segment_num  = batch_size;
    }

    ~LowLatencyBuffer()
    {
        delete[] ptr;
    }

    bool cToBuffer(size_t idx, std::vector<uintptr_t> ptrs, std::vector<size_t> offset, std::vector<size_t> size)
    {
        if (status[idx] == true) {
            std::cout << "Buffer is already in use, please wait for the next round." << std::endl;
            return false;
        }

        uint8_t* base_ptr = ptr + idx * (segment_size * segment_num);

        for (size_t i = 0; i < ptrs.size(); ++i) {
            std::memcpy(base_ptr + i * segment_size, reinterpret_cast<uint8_t*>(ptrs[i] + offset[i]), size[i]);
        }

        status[idx].store(true);
        return true;
    }

    bool cToTensor(size_t idx, std::vector<uintptr_t> ptrs, std::vector<size_t> offset, std::vector<size_t> size)
    {
        if (status[idx] == false) {
            std::cout << "Buffer is not ready, please prepare the buffer first." << std::endl;
            return false;
        }

        uint8_t* base_ptr = ptr + idx * (segment_size * segment_num);

        for (size_t i = 0; i < ptrs.size(); ++i) {
            std::memcpy(reinterpret_cast<uint8_t*>(ptrs[i] + offset[i]), base_ptr + i * segment_size, size[i]);
        }

        status[idx].store(false);
        return true;
    }

    uint8_t* getPtr() const
    {
        return ptr;
    }

    size_t getBufferSize() const
    {
        return buffer_size;
    }

    size_t getIndex() const
    {
        size_t idx = buffer_index.load();
        if (!status[idx].load()) {
            return idx;
        }
        else {
            return -1;
        }
    }

    // LightWeightMessageBuffer 分为 num_buffer 个 buffer，每次传输只选择其中一个 buffer 进行传输
    // 通过 atomic<bool> 来表示每个 buffer 的使用状态
    std::vector<std::atomic<bool>> status;

    size_t              segment_size;
    size_t              segment_num;
    size_t              buffer_size;
    std::atomic<size_t> buffer_index{0};
    uint8_t*            ptr;
};

typedef struct endPointAssignment {

    endPointAssignment(std::shared_ptr<RDMABuffer> buf,
                       size_t                      slot_idx,
                       OpCode                      opcode,
                       bool                        is_low_latency_enabled = false);

    ~endPointAssignment();
    
    

    AssignmentBatch getMetaAssignmentBatch();
    AssignmentBatch getDataAssignmentBatch();

    AssignmentBatch meta_batch;
    AssignmentBatch data_batch;

    OpCode opcode_;

    std::shared_ptr<RDMAEndpoint> endPoint_;
    std::shared_ptr<RDMABuffer>   buf_;
    size_t                        idx_;
    bool                          is_low_latency_enabled_;
} endPointAssignment_t;

// typedef struct RDMATask {
//     explicit RDMATask(std::shared_ptr<RDMAEndpoint> endpoint,
//                       uint32_t                      task_id,
//                       OpCode                        opcode,
//                       std::shared_ptr<RDMABuffer>   buffer);
//     ~RDMATask();

//     std::string getMetaKey();
//     std::string getDataKey(int32_t idx);

//     AssignmentBatch getMetaAssignmentBatch();
//     AssignmentBatch getDataAssignmentBatch();

//     int registerMetaMemoryRegion();
//     int registerDataMemoryRegion();
//     int registerRemoteDataMemoryRegion();

//     void fillMetaInfo();
//     void fillBuffer();

//     int targetQPI();

//     uint32_t                    slot_id_;
//     OpCode                      opcode_;
//     std::shared_ptr<RDMABuffer> buffer_;

//     // meta_data_t* meta_data_buf_{nullptr};

//     std::shared_ptr<RDMAEndpoint> endpoint_;
// } rdma_task_t;

// EndPoint 负责管理所有的context，包括data context和meta context
// meta context负责传输meta信息，data context负责传输实际的数据

class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {
    friend class RDMABuffer;

public:
    explicit RDMAEndpoint(const std::string& dev_name, uint8_t ib_port, const std::string& link_type, size_t qp_num);

    ~RDMAEndpoint();

    void endPointConnect(const json& data_ctx_info, const json& meta_ctx_info);

    void addSendEndPointAssignment(std::shared_ptr<RDMABuffer> buf);
    void addRecvEndPointAssignment(std::shared_ptr<RDMABuffer> buf);

    // json getDataContextInfo() const
    // {
    //     return data_ctx_->endpoint_info();
    // }

    // json getMetaContextInfo() const
    // {
    //     return meta_ctx_->endpoint_info();
    // }

    // std::shared_ptr<RDMAContext> dataCtx()
    // {
    //     return data_ctx_;
    // }

    // std::shared_ptr<RDMAContext> metaCtx()
    // {
    //     return meta_ctx_;
    // }

    // int dataCtxQPNum()
    // {
    //     return data_ctx_qp_num_;
    // }

private:
    void endPointQueue(std::chrono::milliseconds timeout);
    void asyncRecvData(std::shared_ptr<endPointAssignment> assignment);
    void asyncSendData(std::shared_ptr<endPointAssignment> assignment);

    // context
    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;
    size_t                       data_ctx_qp_num_;
    size_t                       meta_ctx_qp_num_;

    // meta data
    std::vector<metaBuffer> meta_SEND_buffer_;
    std::vector<metaBuffer> meta_RECV_buffer_;

    std::atomic<size_t> cur_meta_send_idx_{0};
    std::atomic<size_t> cur_meta_recv_idx_{0};

    // low latency buffer
    std::unique_ptr<LowLatencyBuffer> ll_SEND_buffer_;
    std::unique_ptr<LowLatencyBuffer> ll_RECV_buffer_;

    std::atomic<size_t> cur_ll_send_idx_{0};
    std::atomic<size_t> cur_ll_recv_idx_{0};

    // normal send recv
    std::atomic<size_t> send_slot_idx_{RDMAContext::UNDEFINED_IMM_DATA};
    std::atomic<size_t> recv_slot_idx_{RDMAContext::UNDEFINED_IMM_DATA};

    std::map<uint32_t, std::shared_ptr<endPointAssignment>> send_assignment_slot_;
    std::map<uint32_t, std::shared_ptr<endPointAssignment>> recv_assignment_slot_;

    // endpoint queue
    std::queue<std::shared_ptr<endPointAssignment>> endPoint_queue_;
    std::thread                                     endPoint_queue_thread_;

    std::condition_variable endPoint_queue_cv_;
    std::mutex              endPoint_queue_mutex_;
    bool                    endPoint_queue_running_;

    // std::map<uint32_t, std::function<void()>> imm_data_callback_;
};

class MetaBufferManager {

public:
    MetaBufferManager() = default;
    MetaBufferManager(const int MAX_META_BUFFER_SIZE)
    {
        max_meta_size = MAX_META_BUFFER_SIZE;
        meta_SEND_buffer.resize(max_meta_size);
        meta_RECV_buffer.resize(max_meta_size);

        for (size_t idx = 0; idx < max_meta_size; ++idx) {
            meta_SEND_buffer[idx].metaBufferReset();
            meta_RECV_buffer[idx].metaBufferReset();
        }
    }
    ~MetaBufferManager()
    {

        // for (auto& buffer : meta_tx_buffer_) {
        //     buffer.cleanBuffer();
        // }
        // for (auto& buffer : meta_rx_buffer_) {
        //     buffer.cleanBuffer();
        // }

        // endPoint_->metaCtx()->unregister_memory_region("META_SEND_BUFFER");
        // endPoint_->metaCtx()->unregister_memory_region("META_RECV_BUFFER");
    }

    // uint64_t getSendMetaBufferAddr()
    // {
    //     return reinterpret_cast<uint64_t>(meta_SEND_buffer.data());
    // }
    // uint64_t getRecvMetaBufferAddr()
    // {
    //     return reinterpret_cast<uint64_t>(meta_RECV_buffer.data());
    // }

    metaBuffer& getSendMetaBuffer()
    {
        if (cur_send_idx_ < 0 || cur_send_idx_ >= meta_SEND_buffer.size()) {
            SLIME_LOG_ERROR("Index out of range for send meta buffer.")
            throw std::out_of_range("Index out of range for send meta buffer");
        }
        cur_send_idx_++;
        return meta_SEND_buffer[cur_send_idx_ % max_meta_size];
    }

    metaBuffer& getRecvMetaBuffer()
    {
        if (cur_recv_idx_ < 0 || cur_recv_idx_ >= meta_RECV_buffer.size()) {
            SLIME_LOG_ERROR("Index out of range for receive meta buffer.");
            throw std::out_of_range("Index out of range for receive meta buffer");
        }
        cur_recv_idx_++;
        return meta_RECV_buffer[cur_recv_idx_ % max_meta_size];
    }

    void metaBufferReset(size_t idx, bool isSend)
    {
        if (idx < 0 || idx >= meta_SEND_buffer.size()) {
            SLIME_LOG_ERROR("Index out of range for send meta buffer.")
            throw std::out_of_range("Index out of range for send meta buffer");
        }
        if (idx < 0 || idx >= meta_RECV_buffer.size()) {
            SLIME_LOG_ERROR("Index out of range for receive meta buffer.");
            throw std::out_of_range("Index out of range for receive meta buffer");
        }

        if (isSend)
            meta_SEND_buffer[idx].metaBufferReset();
        else
            meta_RECV_buffer[idx].metaBufferReset();
    }

    MetaBufferManager(const MetaBufferManager&)            = delete;
    MetaBufferManager& operator=(const MetaBufferManager&) = delete;

private:
    std::vector<metaBuffer> meta_SEND_buffer;
    std::vector<metaBuffer> meta_RECV_buffer;

    size_t              max_meta_size;
    std::atomic<size_t> cur_send_idx_{0};
    std::atomic<size_t> cur_recv_idx_{0};
};

}  // namespace slime
