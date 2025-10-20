#pragma once
#include "engine/assignment.h"
#include "engine/rdma/rdma_context.h"

#include <atomic>
#include <bits/stdint-uintn.h>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <infiniband/verbs.h>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "rdma_common.h"

#define MAX_META_BATCH_SIZE 64
#define MAX_META_BUFFER_SIZE 64

namespace slime {

class RDMABuffer;
class RDMAEndpoint;

typedef struct MetaData {

    uint64_t mr_addr[MAX_META_BATCH_SIZE];
    uint32_t mr_rkey[MAX_META_BATCH_SIZE];
    uint32_t mr_size[MAX_META_BATCH_SIZE];
    uint32_t mr_slot;
    uint32_t mr_qpidx;

} __attribute__((packed)) meta_data_t;



// 管理收发两端的RDMABuffer
// Send: 接收RDMABuffer构造一个RDMABufferQueueElement并入队SendWaitQueue。

//       查询其对应的meta是否准备好，如果准备好则弹出执行RDMA WRITE WITH IMM DATA

//       弹出的RDMABuffer入队SendCompletionQueue，等待其回调函数执行完成并修改完成标志位


// Recv: 接收RDMABuffer构造一个RDMABufferQueueElement并入队RecvWaitQueue。

//       查询其对应的预提交队列是否已有对应的元素，如果有则弹出，没有则循环等待，同时直接执行meta的 RDMA WRITE WITH IMM DATA
 
//       弹出的RDMABuffer入队RecvCompletionQueue，等待其回调函数执行完成并修改完成标志位


class RDMABufferQueueElement
{
private:

    std::shared_ptr<RDMABuffer>   rdma_buffer_{nullptr};
    std::function<void()>         callback_;
    OpCode                        rdma_opcode_; 
    uint32_t                      unique_slot_id_;
    std::atomic<bool>             is_finished_;


public:

    RDMABufferQueueElement() = delete;
    RDMABufferQueueElement(const RDMABufferQueueElement&) = delete;
    RDMABufferQueueElement(uint32_t unique_slot_id, OpCode rdma_opcode);


}



// 预提交队列

// Send: 生成metaRecvPrePostQueue，并预提交一定数量的RDMAPrePostQueueElement，所有元素使用dummy assignmentbatch 
//       检查队头标志位，如果为true，则将该元素弹出并送入metaQueue? 同时补充新的RDMAPrePostQueueElement

// Recv: 生成dataRecvPrePostQueue，并预提交一定数量的RDMAPrePostQueueElement，所有元素使用dummy assignmentbatch
//       检查队头标志位，如果为true，同时修改RDMABuffer中的标志位，并补充新的RDMAPrePostQueueElement


/**
 * RDMAPrePostQueueElement - RDMA endpoint task management
 * 
 * Manages RDMA "pre" (RECV) operations from submission to completion.
 * 
 * Workflow:
 * 1. RDMAPrePostQueueElement enqueued --> initiates meta_recv or data_recv (based on operation type)
 * 2. Poll queue head's is_finished status repeatedly
 * 3. When is_finished == true --> pop RDMAPrePostQueueElement from queue and 
 *    forward information to meta queue
 * 
 * Components:
 * 1. rdma_endpoint   - For submit operations
 * 2. rdma_buffer     - For DATA RECV pre post
 * 3. AssignmentBatch - 
 * 4. callback        - Callback function in RDMAContext for handling is_finished status
 * 5. is_finished     - Flag indicating whether the task is completed
 */
class RDMAPrePostQueueElement
{

private:

    // set the indicator of the task
    void setStatus(bool status);


    void postRecvAssignment(std::shared_ptr<RDMAEndpoint> rdma_endpoint);




    std::shared_ptr<RDMAEndpoint> rdma_endpoint_{nullptr}; // Used for submit the assignment_batch_ to RDMAContext
    std::shared_ptr<RDMABuffer>   rdma_buffer_{nullptr}; // Used for the data pre post recv
    std::function<void()>         callback_; // Store the callback
    OpCode                        rdma_opcode_; 
    AssignmentBatch               assignment_batch_; 
    std::atomic<bool>             is_finished_; // The indicator of the if the recv is finished
    mutable std::mutex            task_mutex_;

public:

    // Delete default and copy constructors to enforce specific construction
    RDMAPrePostQueueElement() = delete;
    RDMAPrePostQueueElement(const RDMAPrePostQueueElement&) = delete;

    /**
     * Constructor for RECV operations
     * @param task_id Unique task identifier
     * @param rdma_opcode RDMA operation code 
     * @param rdma_endpoint RDMA endpoint for operation submission
     **/
    RDMAPrePostQueueElement(uint32_t task_id, 
                            OpCode rdma_opcode = OpCode::RECV, 
                            std::shared_ptr<RDMAEndpoint> rdma_endpoint = nullptr);

     /**
     * Constructor for SEND operations
     * @param task_id Unique task identifier
     * @param rdma_opcode RDMA operation code (default: SEND)
     * @param rdma_endpoint RDMA endpoint for operation submission
     * @param rdma_buffer RDMA buffer for data transmission
     */
    RDMAPrePostQueueElement(uint32_t task_id, 
                            OpCode rdma_opcode = OpCode::SEND, 
                            std::shared_ptr<RDMAEndpoint> rdma_endpoint = nullptr, 
                            std::shared_ptr<RDMABuffer> rdma_buffer = nullptr);

    ~RDMAEndPointTask();

}














/**
 * ProxyQueue - The Queue template which is used in the proxy
 *
 * Workflow:
 *
 * Components:
 *
 */
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











class RDMASendRecvTaskPool {

public:
    RDMASendRecvTaskPool(std::shared_ptr<RDMAEndpoint> rdma_endpoint);
    ~RDMASendRecvTaskPool();

    std::shared_ptr<RDMASendRecvTask>
    fetchSendRecvTask(std::shared_ptr<RDMABuffer> rdma_buffer, uint32_t unique_slot_id, OpCode rdma_operation);

    int releaseSendRecvTask(std::shared_ptr<RDMASendRecvTask> rdma_task);

private:
    std::shared_ptr<RDMAEndpoint>                  rdma_endpoint_;
    std::vector<std::shared_ptr<RDMASendRecvTask>> rdma_send_task_pool_;
    std::vector<std::shared_ptr<RDMASendRecvTask>> rdma_recv_task_pool_;
    std::vector<bool>                              rdma_send_task_in_use_;
    std::vector<bool>                              rdma_recv_task_in_use_;

    mutable std::mutex      pool_mutex_;
    std::condition_variable task_available_cv_;

    size_t pool_size_;
};

class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {

    friend class RDMABuffer;

public:
    explicit RDMAEndpoint(const std::string& dev_name,
                          uint8_t            ib_port,
                          const std::string& link_type,
                          size_t             qp_num = 1);

    explicit RDMAEndpoint(const std::string& data_dev_name,
                          const std::string& meta_dev_name,
                          uint8_t            ib_port,
                          const std::string& link_type,
                          size_t             qp_num);

    // TODO: 设计聚合多网卡传输的Send Recv

    ~RDMAEndpoint();

    void connect(const json& data_ctx_info, const json& meta_ctx_info);

    void addRecvTask(std::shared_ptr<RDMABuffer> rdma_buffer);
    void addSendTask(std::shared_ptr<RDMABuffer> rdma_buffer);

    void postSendTask(std::shared_ptr<RDMASendRecvTask> task);

    json dataCtxInfo() const
    {
        return data_ctx_->endpoint_info();
    }

    json metaCtxInfo() const
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

    std::vector<meta_data_t>& metaBuffer()
    {
        return meta_buffer_;
    }

    int metaCtxQPNum()
    {
        return meta_ctx_qp_num_;
    }
    int dataCtxQPNum()
    {
        return data_ctx_qp_num_;
    }



private:
    void mainQueueThread(std::chrono::milliseconds timeout);

    void asyncRecvData(std::shared_ptr<RDMASendRecvTask> task);
    void asyncSendData(std::shared_ptr<RDMASendRecvTask> task);


    ProxyQueue<RDMAEndPointTask> meta_recv_queue_;



    std::queue<AssignmentBatch> meta_recv_queue;
    std::queue<meta_data_t> meta_queue;
    std::queue<RDMASendRecvTask> send_queue;


    mutable std::mutex mutex_proxy
   

    



    std::shared_ptr<RDMASendRecvTaskPool> rdma_task_pool_;

    size_t data_ctx_qp_num_;
    size_t meta_ctx_qp_num_;

    std::atomic<uint32_t> unique_SEND_SLOT_ID_{RDMAContext::UNDEFINED_IMM_DATA};
    std::atomic<uint32_t> unique_RECV_SLOT_ID_{RDMAContext::UNDEFINED_IMM_DATA};

    std::vector<uint32_t>    dum_meta_buffer_;
    std::vector<uint32_t>    dum_data_buffer_;
    std::vector<meta_data_t> meta_buffer_;

    std::unordered_map<uint32_t, std::shared_ptr<RDMASendRecvTask>> send_task_;
    std::unordered_map<uint32_t, std::shared_ptr<RDMASendRecvTask>> recv_task_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;

    std::queue<std::shared_ptr<RDMASendRecvTask>> rdma_tasks_queue_;
    std::thread                                   rdma_tasks_threads_;

    std::condition_variable rdma_tasks_cv_;
    std::mutex              rdma_tasks_mutex_;

    bool RDMA_tasks_threads_running_;
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

}  // namespace slime
