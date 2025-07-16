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

using callback_t = std::function<void()>;

typedef struct meta_data {

    uint64_t mr_addr;
    uint32_t mr_rkey;
    uint32_t mr_size;
    uint32_t mr_slot;
    uint32_t padding;

} __attribute__((packed)) meta_data_t;


typedef struct RDMA_task {

    uint8_t    task_id;
    uint8_t    op_code;
    uint32_t    batch_size;
    callback_t  callback;

}RDMA_task_t;

class RingBuffer {

public:
    class BufferFullException: public std::runtime_error {
    public:
        BufferFullException(): std::runtime_error("Ring buffer is full") {}
    };

    explicit RingBuffer(size_t _max_buffer_size = 64):
        max_buffer_size_(_max_buffer_size),
        assignmentbatch_queue_(_max_buffer_size),
        head_(0),
        tail_(0),
        current_size_(0)
    {
        if (max_buffer_size_ == 0) {
            throw std::invalid_argument("Buffer capacity cannot be zero");
        }
    }

    ~RingBuffer() = default;

    void PushAssignmentBatch(AssignmentBatch& assignment_batch)
    {

        std::lock_guard<std::mutex> lock(queue_mutex_);

        if (current_size_ == max_buffer_size_) {
            throw BufferFullException();
        }

        assignmentbatch_queue_[tail_] = std::move(assignment_batch);
        tail_                         = (tail_ + 1) % max_buffer_size_;
        current_size_ += 1;
        queue_cv_.notify_one();
    }

    void WaitAndPop(AssignmentBatch& assignment_batch)
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { return current_size_ > 0; });

        assignment_batch = std::move(assignmentbatch_queue_[head_]);
        head_            = (head_ + 1) % max_buffer_size_;
        current_size_--;
    }

    bool WaitAndPop(AssignmentBatch& assignment_batch, std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (!queue_cv_.wait_for(lock, timeout, [this] { return current_size_ > 0; })) {
            return false;
        }

        assignment_batch = std::move(assignmentbatch_queue_[head_]);
        head_            = (head_ + 1) % max_buffer_size_;
        current_size_--;
        return true;
    }

    std::shared_ptr<AssignmentBatch> wait_and_pop()
    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        queue_cv_.wait(lock, [this] { return current_size_ > 0; });

        auto result = std::make_shared<AssignmentBatch>(std::move(assignmentbatch_queue_[head_]));
        head_       = (head_ + 1) % max_buffer_size_;
        current_size_--;
        return result;
    }

    bool IsEmpty() const
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return current_size_ == 0;
    }

    size_t Size() const
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        return current_size_;
    }

    size_t MaxBufferSize() const
    {
        return max_buffer_size_;
    }

    void NotifyAll()
    {
        std::lock_guard<std::mutex> lock(queue_mutex_);
        queue_cv_.notify_all();
    }

private:
    const size_t                 max_buffer_size_;
    std::vector<AssignmentBatch> assignmentbatch_queue_;

    size_t head_;
    size_t tail_;
    size_t current_size_;

    mutable std::mutex      queue_mutex_;
    std::condition_variable queue_cv_;
};

class RDMAEndpoint {

public:

    RDMAEndpoint(const std::string& dev_name, uint8_t ib_port, const std::string& link_type, size_t buffer_size)
    {

        std::cout << "Init the Contexts and RDMA Devices..." << std::endl;
        SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");
        data_ctx_ = std::make_shared<RDMAContext>();
        meta_ctx_ = std::make_shared<RDMAContext>();

        data_ctx_->init(dev_name, ib_port, link_type);
        meta_ctx_->init(dev_name, ib_port, link_type);

        std::cout << "Init the SEND/RECV ring buffer..." << std::endl;
        SLIME_LOG_INFO("Init the SEND/RECV ring buffer...");

        send_buffer_ = std::make_unique<RingBuffer>(buffer_size);
        recv_buffer_ = std::make_unique<RingBuffer>(buffer_size);

        send_slot_id_ = 0;
        recv_slot_id_ = 0;

        std::cout << "RDMA Endpoint Init Success..." << std::endl;
        SLIME_LOG_INFO("RDMA Endpoint Init Success...");

        RDMA_tasks_threads_running_ = true;
        RDMA_tasks_threads_ = std::thread([this] {this->AsyncSendRecvData(std::chrono::milliseconds(100));});


    }

    ~RDMAEndpoint()
    {
        // 析构所有对象

        {
            std::unique_lock<std::mutex> lock(RDMA_tasks_mutex_);
            RDMA_tasks_threads_running_ = false;
        }
        RDMA_tasks_cv_.notify_all();
        if (RDMA_tasks_threads_.joinable())
        {
            RDMA_tasks_threads_.join();
        }

    }

    void ContextConnect(const json& data_ctx_info, const json& meta_ctx_info)
    {
        data_ctx_->connect(data_ctx_info);
        meta_ctx_->connect(meta_ctx_info);

        data_ctx_->launch_future();
        meta_ctx_->launch_future();
    }

    void LaunchSend(int max_threads = 1);
    void LaunchRecv(int max_threads = 1);
    void WaitRecv();
    void WaitSend();
    void Stop();

    /*
        支持buffer的Engine
    */
    void AddRDMARecvTask(std::vector<uintptr_t> &ptrs, std::vector<size_t> &data_size, uint32_t batch_size, callback_t callback)
    {

        RDMA_task_t task;
        task.task_id = GenerateRECVAssignmentBatch(ptrs, data_size, batch_size);
        task.op_code = 1;
        task.batch_size = batch_size;
        task.callback = callback;
        {
            std::unique_lock<std::mutex> lock(RDMA_tasks_mutex_);
            RDMA_tasks_queue_.push(std::move(task));
        }
        RDMA_tasks_cv_.notify_one();
    }

    void AddRDMASendTask(std::vector<uintptr_t> &ptrs, std::vector<size_t> &data_size, uint32_t batch_size, callback_t callback)
    {


        RDMA_task_t task;
        task.task_id = GenerateSENDAssignmentBatch(ptrs, data_size, batch_size);
        task.op_code = 0;
        task.batch_size = batch_size;
        task.callback = callback;

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

    void cRecv(std::vector<uintptr_t> ptrs, std::vector<size_t> data_size, uint32_t batch_size)
    {
        recv_slot_id_++;
        std::string cur_key = "RECV_KEY_" + std::to_string(recv_slot_id_);

        RegisterRecvMemRegionBatch(cur_key, ptrs, data_size, batch_size);

        AssignmentBatch recv_data_batch;
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = cur_key + "_" + std::to_string(i);
            recv_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
            recv_data_batch[i].slot_id = recv_slot_id_;
        }

        recv_buffer_->PushAssignmentBatch(recv_data_batch);
    }

    void cSend(std::vector<uintptr_t> ptrs, std::vector<size_t> data_size, uint32_t batch_size)
    {
        send_slot_id_++;
        std::string cur_key = "SEND_KEY_" + std::to_string(send_slot_id_);

        RegisterSendMemRegionBatch(cur_key, ptrs, data_size, batch_size);

        AssignmentBatch send_data_batch;
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = cur_key + "_" + std::to_string(i);  // 生成和注册内存区域相同的KEY
            send_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
            send_data_batch[i].slot_id = send_slot_id_;
        }

        send_buffer_->PushAssignmentBatch(send_data_batch);
    }

    template<typename T>
    void Recv(std::vector<T*> ptrs, std::vector<size_t> data_size, uint32_t batch_size)
    {
        recv_slot_id_++;
        std::string cur_key = "RECV_KEY_" + std::to_string(recv_slot_id_);

        // change to uintptr_t type
        std::vector<uintptr_t> uint_ptrs;
        for (auto ptr : ptrs) {
            uint_ptrs.push_back(reinterpret_cast<uintptr_t>(ptr));
        }

        RegisterRecvMemRegionBatch(cur_key, uint_ptrs, data_size, batch_size);

        AssignmentBatch recv_data_batch;
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = cur_key + "_" + std::to_string(i);
            recv_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
            recv_data_batch[i].slot_id = recv_slot_id_;
        }

        recv_buffer_->PushAssignmentBatch(recv_data_batch);
    }

    template<typename T>
    void Send(std::vector<T*> ptrs, std::vector<size_t> data_size, uint32_t batch_size)
    {
        send_slot_id_++;
        std::string cur_key = "SEND_KEY_" + std::to_string(send_slot_id_);

        std::vector<uintptr_t> uint_ptrs;
        for (auto ptr : ptrs) {
            uint_ptrs.push_back(reinterpret_cast<uintptr_t>(ptr));
        }

        RegisterSendMemRegionBatch(cur_key, uint_ptrs, data_size, batch_size);

        AssignmentBatch send_data_batch;
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = cur_key + "_" + std::to_string(i);  // 生成和注册内存区域相同的KEY
            send_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
            send_data_batch[i].slot_id = send_slot_id_;
        }

        send_buffer_->PushAssignmentBatch(send_data_batch);
    }

private:
    void RegisterMemRegion(std::string& str, uintptr_t ptr, size_t data_size)
    {
        data_ctx_->register_memory_region(str, ptr, data_size);
    }

    // The mr key is followed by the form: str+"i"+"RECV"
    void RegisterRecvMemRegionBatch(std::string&           str,
                                    std::vector<uintptr_t> ptrs,
                                    std::vector<size_t>    data_size,
                                    uint32_t               batch_size)
    {
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = str + "_" + std::to_string(i);
            RegisterMemRegion(KEY, ptrs[i], data_size[i]);
        }
    }
    // The mr key is followed by the form: str+"i"+"SEND"
    void RegisterSendMemRegionBatch(std::string&           str,
                                    std::vector<uintptr_t> ptrs,
                                    std::vector<size_t>    data_size,
                                    uint32_t               batch_size)
    {
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = str + "_" + std::to_string(i);
            RegisterMemRegion(KEY, ptrs[i], data_size[i]);
        }
    }

    void UnregisterMemRegionBatch(std::string& str, uint32_t batch_size)
    {
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = str + "_" + std::to_string(i);
            data_ctx_->unregister_memory_region(KEY);
        }
    }

    uint8_t GenerateRECVAssignmentBatch(std::vector<uintptr_t> &ptrs, std::vector<size_t> &data_size, uint32_t batch_size)
    {

        recv_slot_id_++;
        std::string cur_key = "RECV_KEY_" + std::to_string(recv_slot_id_);
        RegisterRecvMemRegionBatch(cur_key, ptrs, data_size, batch_size);
        AssignmentBatch recv_data_batch;
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = cur_key + "_" + std::to_string(i);
            recv_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
            recv_data_batch[i].slot_id = recv_slot_id_;
        }
        recv_batch_slot_.emplace(recv_slot_id_, recv_data_batch);
        return recv_slot_id_;
    }

    uint8_t GenerateSENDAssignmentBatch(std::vector<uintptr_t> ptrs, std::vector<size_t> data_size, uint32_t batch_size)
    {
        send_slot_id_++;
        std::string cur_key = "SEND_KEY_" + std::to_string(send_slot_id_);
        RegisterSendMemRegionBatch(cur_key, ptrs, data_size, batch_size);
        AssignmentBatch send_data_batch;
        for (uint32_t i = 0; i < batch_size; ++i) {
            std::string KEY = cur_key + "_" + std::to_string(i);
            send_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
            send_data_batch[i].slot_id = send_slot_id_;
        }
        send_batch_slot_.emplace(send_slot_id_, send_data_batch);
        return send_slot_id_;
    }

    //No matter SEND or RECV, all these tasks are viewed as the RDMATasks.
    void AsyncSendRecvData(std::chrono::milliseconds timeout)
    {
        while (true)
        {
            RDMA_task_t task;
            {
                std::unique_lock<std::mutex> lock(RDMA_tasks_mutex_);
                //RDMA_tasks_cv_.wait(lock, [this] { return !RDMA_tasks_queue_.empty() || !RDMA_tasks_threads_running_;});
                bool timeout_occurred = !RDMA_tasks_cv_.wait_for(lock, std::chrono::milliseconds(100), [this]
                {
                    return !RDMA_tasks_queue_.empty() || !RDMA_tasks_threads_running_;
                });
                if (timeout_occurred)
                    continue;

                if (!RDMA_tasks_threads_running_ && RDMA_tasks_queue_.empty())
                    break;

                if (!RDMA_tasks_queue_.empty())
                {
                    task = std::move(RDMA_tasks_queue_.front());
                    RDMA_tasks_queue_.pop();
                }

                if (task.op_code == 0)
                    AsyncSendData(task);
                if (task.op_code == 1)
                    AsyncRecvData(task);

            }
        }
    }

    void AsyncRecvData(RDMA_task_t &task)
    {
        std::cout<<"AsyncRecvData"<< std::endl;
        size_t      batch_size    = task.batch_size;
        //auto        meta_data_buf = std::make_shared<meta_data_t[]>(batch_size);
        auto meta_data_buf = std::shared_ptr<meta_data_t[]>(new meta_data_t[batch_size], std::default_delete<meta_data_t[]>());
        uint8_t     slot_id       = task.task_id;
        std::string META_KEY      = "RECV_META_" + std::to_string(slot_id);
        auto it = recv_batch_slot_.find(slot_id);
        if (it != recv_batch_slot_.end())
        {
            auto  recv_data = it->second;

            auto data_callback = [this, slot_id, batch_size, task] (int status)
            {
                std::string cur_key  = "RECV_KEY_" + std::to_string(slot_id);
                std::string META_KEY = "RECV_META_" + std::to_string(slot_id);
                UnregisterMemRegionBatch(cur_key, batch_size);
                meta_ctx_->unregister_memory_region(META_KEY);
                std::cout << "The slot_id: " << slot_id << "has been successfully received" << std::endl;
                task.callback();
            };

            auto meta_callback = [&] (int status)
            {
                //if (status != 0) return;
                std::cout<< "The meta data has been transmitted, post the recv..." << std::endl;
                auto data_atx = data_ctx_->submit(OpCode::RECV, recv_data, data_callback);

            };

            {
                std::lock_guard<std::mutex> lock(meta_data_mutex);
                for (size_t i = 0; i < batch_size; ++i)
                {
                    meta_data_buf[i].mr_addr =
                        reinterpret_cast<uint64_t>(data_ctx_->memory_pool_->get_mr(recv_data[i].mr_key)->addr);
                    meta_data_buf[i].mr_rkey = data_ctx_->memory_pool_->get_mr(recv_data[i].mr_key)->rkey;
                    meta_data_buf[i].mr_size = data_ctx_->memory_pool_->get_mr(recv_data[i].mr_key)->length;
                    meta_data_buf[i].mr_slot = recv_data[i].slot_id;
                }

                meta_ctx_->register_memory_region(
                    META_KEY, reinterpret_cast<uintptr_t>(meta_data_buf.get()), batch_size * sizeof(meta_data_t));

                AssignmentBatch meta_data(batch_size);
                for (size_t i = 0; i < batch_size; ++i)
                {
                    meta_data[i] = Assignment(META_KEY, 0, i * sizeof(meta_data_t), sizeof(meta_data_t));
                }
                std::cout<<"STSSSSSSSSSSSSSSS"<< std::endl;
                auto meta_atx = meta_ctx_->submit(OpCode::SEND, meta_data, meta_callback);
            }


        }
        else
        {
            std::cout << "The data in slot " << slot_id << "is not prepared" << std::endl;
            std::cout << "There must be some bugs..." << std::endl;
            return;
        }

    }


    void AsyncSendData(RDMA_task_t &task)
    {
         std::cout << "AsyncSendData" << std::endl;
        size_t      batch_size    = task.batch_size;
        std::cout<<"batch_size: " << batch_size << std::endl;
        //auto        meta_data_buf = std::make_shared<meta_data_t[]>(batch_size);
        // auto meta_data_buf = std::shared_ptr<meta_data_t[]>(new meta_data_t[batch_size], std::default_delete<meta_data_t[]>());
        auto meta_data_buf = (meta_data_t*)malloc(sizeof(meta_data_t) * batch_size);
        std::cout << meta_data_buf[0].mr_slot << std::endl;
        std::string META_KEY      = "SEND_META_" + std::to_string(task.task_id);

        std::cout << "SEND_META_" << std::endl;

        auto data_callback = [this, META_KEY, batch_size, task] (int status)
        {
            //std::string cur_key = "SEND_KEY_" + std::to_string(slot_id);
            //UnregisterMemRegionBatch(cur_key, batch_size);
            meta_ctx_->unregister_memory_region(META_KEY);
            task.callback();
            std::cout<<"Data has been successfully Send" << std::endl;
        };

        auto meta_callback = [&] (int status) mutable
        {
            std::cout << "submit SSSSSS" << std::endl;
            std::cout << "submit SSSSSSsubmit SSSSSSsubmit SSSSSSsubmit SSSSSS"<< std::endl;
            //if (status != 0) return;
            uint8_t slot_id = 1;
            std::cout << "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABBBBBBBBBBBBBBB" << &send_batch_slot_ << std::endl;
            auto    it      = this->send_batch_slot_.find(0);
            // std::cout << meta_data_buf[0].mr_slot << std::endl;
            // if (it != send_batch_slot_.end())
            // {
            //     std::cout << "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA" << std::endl;
            //     std::cout << meta_data_buf[0].mr_addr<< std::endl;
            //     auto send_data = it->second;
            //     for (size_t i = 0; i < batch_size; ++i)
            //     {
            //         send_data[i].remote_addr   = meta_data_buf[i].mr_addr;
            //         send_data[i].remote_rkey   = meta_data_buf[i].mr_rkey;
            //         send_data[i].length        = meta_data_buf[i].mr_size;
            //         send_data[i].target_offset = 0;
            //     }
            //      std::cout << "submit SSSSSS" << std::endl;
            //      std::cout << send_data[0].remote_addr << std::endl;
            //     auto data_atx = data_ctx_->submit(OpCode::WRITE_WITH_IMM, send_data, data_callback);

            // }
            // else
            // {
            //     std::cout << "The data in slot " << meta_data_buf[0].mr_slot << "is not prepared" << std::endl;
            //     std::cout << "There must be some bugs..." << std::endl;
            // }
        };


        {
            std::lock_guard<std::mutex> lock(meta_data_mutex);
            std::cout << META_KEY << std::endl;
            //std::cout << reinterpret_cast<uintptr_t>(meta_data_buf.get()) << std::endl;
            meta_ctx_->register_memory_region(
                META_KEY, reinterpret_cast<uintptr_t>(meta_data_buf), batch_size * sizeof(meta_data_t));

            AssignmentBatch meta_data(batch_size);
            for (size_t i = 0; i < batch_size; ++i)
            {
                meta_data[i] = Assignment(META_KEY, 0, i * sizeof(meta_data_t), sizeof(meta_data_t));
            }
            std::cout << "submit metaCCCCCCCCCCcc" << std::endl;
            auto  meta_atx = meta_ctx_->submit(OpCode::RECV, meta_data, meta_callback);
            std::cout << "submit meta" << std::endl;
        }

    }

    void SyncRecvData();
    void SyncSendData();

    std::vector<std::future<void>> recv_futures_;
    std::vector<std::future<void>> send_futures_;

    std::atomic<uint8_t> send_slot_id_;
    std::atomic<uint8_t> recv_slot_id_;

    uint32_t batch_size_;

    std::unordered_map<uint32_t,  AssignmentBatch> send_batch_slot_;
    std::unordered_map<uint32_t,  AssignmentBatch> recv_batch_slot_;

    std::atomic<bool>           RECV_RUN{true};
    std::unique_ptr<RingBuffer> recv_buffer_;

    std::atomic<bool>           SEND_RUN{true};
    std::unique_ptr<RingBuffer> send_buffer_;

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
