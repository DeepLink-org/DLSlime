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

typedef struct meta_data {

    uint64_t mr_addr;
    uint32_t mr_rkey;
    uint32_t mr_size;
    uint32_t mr_slot;
    uint32_t padding;

} __attribute__((packed)) meta_data_t;

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
    }

    ~RDMAEndpoint() = default;

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

    void AsyncRecvData();
    void AsyncSendData();

    std::vector<std::future<void>> recv_futures_;
    std::vector<std::future<void>> send_futures_;

    std::atomic<uint32_t> send_slot_id_;
    std::atomic<uint32_t> recv_slot_id_;

    uint32_t batch_size_;

    std::atomic<bool>           RECV_RUN{true};
    std::unique_ptr<RingBuffer> recv_buffer_;

    std::atomic<bool>           SEND_RUN{true};
    std::unique_ptr<RingBuffer> send_buffer_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;

    std::mutex meta_data_mutex;
};

}  // namespace slime
