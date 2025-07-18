#pragma once
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_context.h"

#include <condition_variable>
#include <infiniband/verbs.h>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <vector>

namespace slime {

constexpr uint8_t batch_sizezzzz = 64;

using callback_t = std::function<void()>;

typedef struct meta_data {

    uint64_t mr_addr;
    uint32_t mr_rkey;
    uint32_t mr_size;
    uint32_t mr_slot;
    uint32_t padding;

} __attribute__((packed)) meta_data_t;

typedef struct RDMA_task {

    uint8_t     task_id;
    uint32_t    batch_size;
    OpCode      op_code;
    callback_t  callback;

}RDMA_task_t;


class RDMAEndpoint {



public:

    RDMAEndpoint(const std::string& dev_name, uint8_t ib_port, const std::string& link_type, size_t buffer_size)
    {
        SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");
        data_ctx_ = std::make_shared<RDMAContext>();
        meta_ctx_ = std::make_shared<RDMAContext>();

        data_ctx_->init(dev_name, ib_port, link_type);
        meta_ctx_->init(dev_name, ib_port, link_type);

        send_slot_id_ = 0;
        recv_slot_id_ = 0;
        SLIME_LOG_INFO("RDMA Endpoint Init Success and Launch the RDMA Endpoint Task Threads...");
        RDMA_tasks_threads_running_ = true;
        RDMA_tasks_threads_ = std::thread([this] {this->WaitandPopTask(std::chrono::milliseconds(100));});
    }

    ~RDMAEndpoint()
    {
        //std::cout << "RDMAEndpoint destroyed! this=" << this << std::endl;
        {
            std::unique_lock<std::mutex> lock(RDMA_tasks_mutex_);
            RDMA_tasks_threads_running_ = false;
        }

        RDMA_tasks_cv_.notify_all();

        if (RDMA_tasks_threads_.joinable())
            RDMA_tasks_threads_.join();


    }

    void ContextConnect(const json& data_ctx_info, const json& meta_ctx_info)
    {
        data_ctx_->connect(data_ctx_info);
        meta_ctx_->connect(meta_ctx_info);

        data_ctx_->launch_future();
        meta_ctx_->launch_future();
    }


    void AddRDMARecvTask(std::vector<uintptr_t> &ptrs, std::vector<size_t> &data_size, uint32_t batch_size, callback_t callback)
    {

        RDMA_task_t task;
        task.task_id    = GenerateRECVAssignmentBatch(ptrs, data_size, batch_size);
        task.batch_size = batch_size;
        task.op_code    = OpCode::RECV;
        task.callback   = callback;
        {
            std::unique_lock<std::mutex> lock(RDMA_tasks_mutex_);
            RDMA_tasks_queue_.push(std::move(task));
        }
        RDMA_tasks_cv_.notify_one();
    }

    void AddRDMASendTask(std::vector<uintptr_t> &ptrs, std::vector<size_t> &data_size, uint32_t batch_size, callback_t callback)
    {

        RDMA_task_t task;
        task.task_id    = GenerateSENDAssignmentBatch(ptrs, data_size, batch_size);
        task.batch_size = batch_size;
        task.op_code    = OpCode::SEND;
        task.callback   = callback;

        {
            std::unique_lock<std::mutex> lock(RDMA_tasks_mutex_);
            RDMA_tasks_queue_.push(std::move(task));
        }
        RDMA_tasks_cv_.notify_one();
    }

    json GetDataContextInfo() const
    {
        return data_ctx_->endpoint_info();
    }

    json GetMetaContextInfo() const
    {
        return meta_ctx_->endpoint_info();
    }

private:


    void RegisterMemRegion(std::string str, uintptr_t ptr, size_t data_size)
    {
        data_ctx_->register_memory_region(str, ptr, data_size);
    }

    void RegisterRecvMemRegionBatch(std::string           str,
                                    std::vector<uintptr_t> ptrs,
                                    std::vector<size_t>    data_size,
                                    uint32_t               batch_size);

    void RegisterSendMemRegionBatch(std::string           str,
                                    std::vector<uintptr_t> ptrs,
                                    std::vector<size_t>    data_size,
                                    uint32_t               batch_size);

    void UnregisterDataMemRegionBatch(std::string str, uint32_t batch_size);

    void UnregisterMetaMemRegionBatch(std::string str)
    {
        meta_ctx_->unregister_memory_region(str);
    }

    uint8_t GenerateRECVAssignmentBatch(std::vector<uintptr_t> &ptrs, std::vector<size_t> &data_size, uint32_t batch_size);
    uint8_t GenerateSENDAssignmentBatch(std::vector<uintptr_t> &ptrs, std::vector<size_t> &data_size, uint32_t batch_size);

    void WaitandPopTask(std::chrono::milliseconds timeout);

    void AsyncRecvData(RDMA_task_t &task);
    void AsyncSendData(RDMA_task_t &task);

    // void SyncRecvData();
    // void SyncSendData();

    std::atomic<uint8_t> send_slot_id_;
    std::atomic<uint8_t> recv_slot_id_;

    uint32_t batch_size_;

    std::unordered_map<uint32_t,  AssignmentBatch> send_batch_slot_;
    std::unordered_map<uint32_t,  AssignmentBatch> recv_batch_slot_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;

    std::mutex meta_data_mutex;

    std::queue<RDMA_task_t> RDMA_tasks_queue_;
    std::thread RDMA_tasks_threads_;
    std::condition_variable RDMA_tasks_cv_;
    std::mutex RDMA_tasks_mutex_;

    bool RDMA_tasks_threads_running_;
};

}  // namespace slime
