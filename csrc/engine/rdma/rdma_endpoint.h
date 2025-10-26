#pragma once
#include "engine/assignment.h"
#include "engine/rdma/rdma_context.h"

#include <atomic>
#include <bits/stdint-uintn.h>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <infiniband/verbs.h>
#include <shared_mutex>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "rdma_common.h"

using JSON = nlohmann::json;

#define MAX_META_BATCH_SIZE 64
#define MAX_META_BUFFER_SIZE 64

namespace slime {

class RDMABuffer;
class RDMAEndpoint;

// typedef struct MetaData {

//     uint64_t mr_addr[MAX_META_BATCH_SIZE];
//     uint32_t mr_rkey[MAX_META_BATCH_SIZE];
//     uint32_t mr_size[MAX_META_BATCH_SIZE];
//     uint32_t mr_slot;
//     uint32_t mr_qpidx;

// } meta_data_t;





struct alignas(64) MetaElement 
{
    uint64_t mr_addr[MAX_META_BATCH_SIZE];
    uint32_t mr_rkey[MAX_META_BATCH_SIZE];
    uint32_t mr_size[MAX_META_BATCH_SIZE];
    uint64_t mr_slot;
    
    MetaElement() 
    {
        std::memset(mr_addr, 0, sizeof(mr_addr));
        std::memset(mr_rkey, 0, sizeof(mr_rkey));
        std::memset(mr_size, 0, sizeof(mr_size));
        mr_slot = 0;
    }
};


template<typename T>
class MetaBuffer 
{
public:
    MetaBuffer(size_t size) : size_(size), storage_(size) {}
    

    void setMeta(int id, const T& meta) 
    {
        std::unique_lock<std::shared_mutex> lock(mutexes_[id % size_]);
        storage_[id % size_] = meta;
    }
    
  
    T getMeta(int id) const 
    {
        std::shared_lock<std::shared_mutex> lock(mutexes_[id % size_]);
        return storage_[id % size_];
    }
    
  
    size_t getSize() const { return size_; }
    

    const T* data() const { return storage_.data(); }
    T* data() { return storage_.data(); }
    
private:
    std::vector<T> storage_;  
    mutable std::vector<std::shared_mutex> mutexes_;  
    size_t size_;
};



struct RDMABufferQueueElement
{
    RDMABufferQueueElement() = delete;
    RDMABufferQueueElement(const RDMABufferQueueElement&) = delete;
    RDMABufferQueueElement(uint32_t unique_slot_id, rdma_opcode, std::shared_ptr<RDMABuffer> rdma_buffer_ = nullptr);

    uint32_t                        unique_slot_id_;
    OpCode                          rdma_opcode_; 
    std::shared_ptr<RDMABuffer>     rdma_buffer_;
    std::atomic<bool>               is_finished_;
    std::function<void(int, int)>   callback_;
};


struct RDMAPrePostQueueElement
{
    RDMAPrePostQueueElement() = delete;
    RDMAPrePostQueueElement(const RDMAPrePostQueueElement&) = delete;
    RDMAPrePostQueueElement(uint32_t task_id, OpCode rdma_opcode, std::shared_ptr<RDMAEndpoint> rdma_endpoint = nullptr);

    uint32_t            task_id_;
    OpCode              rdma_opcode_;
    AssignmentBatch     assignment_batch_;


    std::shared_ptr<RDMAEndpoint>   rdma_endpoint_;
    std::atomic<bool>               is_finished_;
    std::function<void(int, int)>   callback_;

}


template<typename T>
class ProxyQueue
{
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;

public:

    ProxyQueue() = default;
    ProxyQueue(const ProxyQueue&) = delete;
    ProxyQueue& operator=(const ProxyQueue&) = delete;


    void enqueue(T element)
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(element));
        }
        cv_.notify_one();
    }

    T dequeue()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [this] { return !queue_.empty();});

        T element = std::move(queue_.front());
        queue_.pop();
        return element;
    }


    template<typename M>
    bool peekQueue(T& element, M&& m)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!queue_.empty() && m(queue_.front())) {
            element = std::move(queue_.front());
            queue_.pop();
            return true;
        }
        return false;
    }


    bool fetchQueue(T& element)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;
            
        element = std::move(queue_.front());
        queue_.pop();
        return true;
    }


    template<typename a, typename p>
    bool fetchQueue(T& element, const std::chrono::duration<a, p> &time_out)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, time_out, [this] { return !queue_.empty(); }))
        {
            return false;
        }
        element = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    size_t size() const 
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
};




class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {

    friend class RDMABuffer;

public:
    RDMAEndpoint(const std::string& dev_name,  size_t  ib_port,
                 const std::string& link_type, size_t  qp_nums);

    // TODO: 设计聚合多网卡传输的Send Recv

    ~RDMAEndpoint();

    void addRDMABuffer(std::shared_ptr<RDMABuffer> rdma_buffer)


private:

    void SendMetaQueueThread(std::chrono::milliseconds timeout);
    void RecvDataQueueThread(std::chrono::milliseconds timeout);
    
    void SendBufferQueueThread(std::chrono::milliseconds timeout);
    
    void WaitSendFinishQueueThread(std::chrono::milliseconds timeout);
    void WaitRecvFinishQueueThread(std::chrono::milliseconds timeout);


    void postRDMAAssignment(Opcode rdma_opcode);

    ProxyQueue<RDMAPrePostQueueElement> meta_recv_queue_;
    ProxyQueue<RDMAPrePostQueueElement> data_recv_queue_;
    
    ProxyQueue<RDMABufferQueueElement> send_buffer_queue_;

    ProxyQueue<RDMABufferQueueElement> send_finish_queue_;
    ProxyQueue<RDMABufferQueueElement> recv_finish_queue_;

    std::thread meta_recv_thread_;
    std::thread data_recv_thread_;

    std::thread send_buffer_thread_;

    std::thread send_finish_thread_;
    std::thread recv_finish_thread_;

    size_t data_ctx_qp_num_;
    size_t meta_ctx_qp_num_;

    std::atomic<uint32_t> unique_SEND_SLOT_ID_{RDMAContext::UNDEFINED_IMM_DATA};
    std::atomic<uint32_t> unique_RECV_SLOT_ID_{RDMAContext::UNDEFINED_IMM_DATA};

    std::vector<uint32_t>    dum_meta_buffer_;
    std::vector<uint32_t>    dum_data_buffer_;

    std::vector<meta_data_t> meta_buffer_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;


};


// struct RDMASendRecvTask {

//     RDMASendRecvTask(std::shared_ptr<RDMAEndpoint> rdma_endpoint, uint32_t task_id);
//     RDMASendRecvTask(std::shared_ptr<RDMAEndpoint> rdma_endpoint, OpCode rdma_opcode, uint32_t task_id);

//     ~RDMASendRecvTask();

//     int makeAssignmentBatch();
//     int makeMetaMR();
//     int makeDataMR();
//     int makeRemoteDataMR();
//     int makeDummyAssignmentBatch();

//     void configurationTask(std::shared_ptr<RDMABuffer> rdma_buffer, uint32_t unique_slot_id, OpCode rmda_opcode);

//     uint32_t task_id_;
//     uint32_t unique_slot_id_;
//     OpCode   rdma_operation_;

//     std::shared_ptr<RDMABuffer>   rdma_buffer_;
//     std::shared_ptr<RDMAEndpoint> rdma_endpoint_;
//     AssignmentBatch               meta_assignment_batch_;
//     AssignmentBatch               dum_meta_assignment_batch_;
//     AssignmentBatch               data_assignment_batch_;
//     AssignmentBatch               dum_data_assignment_batch_;

// };

    // json dataCtxInfo() const
    // {
    //     return data_ctx_->endpoint_info();
    // }

    // json metaCtxInfo() const
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

    // std::vector<meta_data_t>& metaBuffer()
    // {
    //     return meta_buffer_;
    // }

    // int metaCtxQPNum()
    // {
    //     return meta_ctx_qp_num_;
    // }
    // int dataCtxQPNum()
    // {
    //     return data_ctx_qp_num_;
    // }

}  // namespace slime
