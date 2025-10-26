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
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <vector>

#include "logging.h"
#include "rdma_common.h"

using JSON = const nlohmann::json;

#define MAX_META_BATCH_SIZE 64
#define MAX_META_BUFFER_SIZE 64
#define MAX_QUEUE_SIZE 64

namespace slime {

class RDMABuffer;
class RDMAEndpoint;

typedef struct MetaData {

    uint64_t mr_addr[MAX_META_BATCH_SIZE];
    uint32_t mr_rkey[MAX_META_BATCH_SIZE];
    uint32_t mr_size[MAX_META_BATCH_SIZE];
    uint32_t mr_slot;
    uint32_t mr_qpidx;

} meta_data_t;

struct alignas(64) MetaElement {
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
class MetaBuffer {
public:
    MetaBuffer(size_t size): size_(size), storage_(size) {}

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

    size_t getSize() const
    {
        return size_;
    }

    const T* data() const
    {
        return storage_.data();
    }
    T* data()
    {
        return storage_.data();
    }

private:
    std::vector<T>                         storage_;
    mutable std::vector<std::shared_mutex> mutexes_;
    size_t                                 size_;
};

struct RDMABufferQueueElement {

    RDMABufferQueueElement();
    RDMABufferQueueElement(uint32_t                    unique_slot_id,
                           OpCode                      rdma_opcode,
                           std::shared_ptr<RDMABuffer> rdma_buffer = nullptr):
        unique_slot_id_(unique_slot_id), rdma_opcode_(rdma_opcode), rdma_buffer_(rdma_buffer)

    {
        is_finished_  = false;
        auto callback = [this](int status, int slot_id) mutable { this->is_finished_ = true; };
        callback_     = std::move(callback);
    }

    RDMABufferQueueElement& operator=(RDMABufferQueueElement&& other) noexcept
    {
        if (this != &other) {
            unique_slot_id_ = other.unique_slot_id_;
            rdma_opcode_    = other.rdma_opcode_;
            rdma_buffer_    = std::move(other.rdma_buffer_);
            is_finished_.store(other.is_finished_.load(std::memory_order_relaxed));
            callback_ = std::move(other.callback_);
        }
        return *this;
    }

    uint32_t                      unique_slot_id_;
    OpCode                        rdma_opcode_;
    std::shared_ptr<RDMABuffer>   rdma_buffer_;
    std::atomic<bool>             is_finished_;
    std::function<void(int, int)> callback_;
};

struct RDMAPrePostQueueElement {

    RDMAPrePostQueueElement();
    RDMAPrePostQueueElement(uint32_t                      unique_task_id,
                            OpCode                        rdma_opcode,
                            std::shared_ptr<RDMAEndpoint> rdma_endpoint = nullptr):
        unique_task_id_(unique_task_id), rdma_opcode_(rdma_opcode), rdma_endpoint_(rdma_endpoint)
    {

        assignment_batch_ = rdma_opcode_ == OpCode::SEND ?
                                AssignmentBatch{Assignment("dum_meta_buffer_", 0, 0, 16 * sizeof(uint32_t))} :
                                AssignmentBatch{Assignment("dum_data_buffer_", 0, 0, 16 * sizeof(uint32_t))};

        is_finished_  = false;
        auto callback = [this](int status, int slot_id) mutable { this->is_finished_ = true; };
        callback_     = std::move(callback);
    }

    RDMAPrePostQueueElement& operator=(RDMAPrePostQueueElement&& other) noexcept
    {
        if (this != &other) {
            unique_task_id_   = other.unique_task_id_;
            rdma_opcode_      = other.rdma_opcode_;
            assignment_batch_ = std::move(other.assignment_batch_);
            rdma_endpoint_    = std::move(other.rdma_endpoint_);
            is_finished_.store(other.is_finished_.load(std::memory_order_relaxed));
            callback_ = std::move(other.callback_);
        }
        return *this;
    }

    uint32_t        unique_task_id_;
    OpCode          rdma_opcode_;
    AssignmentBatch assignment_batch_;

    std::shared_ptr<RDMAEndpoint> rdma_endpoint_;
    std::atomic<bool>             is_finished_;
    std::function<void(int, int)> callback_;
};

class RingBufferReadyManager {
public:
    explicit RingBufferReadyManager(size_t size): buffer_size_(size), slots_(size)
    {

        for (size_t i = 0; i < buffer_size_; ++i) {
            slots_[i].store(false, std::memory_order_relaxed);
        }
    }

    ~RingBufferReadyManager() = default;

    bool writeSlot(uint32_t idx, bool value, bool wait_if_false = false, int max_wait_attempts = 100)
    {
        size_t index = idx % buffer_size_;

        if (wait_if_false) {
            return writeWithWait(index, value, max_wait_attempts);
        }
        else {
            slots_[index].store(value, std::memory_order_release);
            return true;
        }
    }

    bool readSlot(uint32_t idx) const
    {
        size_t index = idx % buffer_size_;
        return slots_[index].load(std::memory_order_acquire);
    }

    bool modifySlot(uint32_t idx, bool new_value, bool expected_value)
    {
        size_t index = idx % buffer_size_;

        bool expected = expected_value;
        return slots_[index].compare_exchange_strong(expected, new_value, std::memory_order_acq_rel);
    }

    bool waitForSlot(uint32_t idx, bool desired_value, int max_attempts = 100, int sleep_ms = 1)
    {
        size_t index = idx % buffer_size_;

        for (int attempt = 0; attempt < max_attempts; ++attempt) {
            if (slots_[index].load(std::memory_order_acquire) == desired_value) {
                return true;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
        }
        return false;
    }

    bool setSlotTrueWithWait(uint32_t idx, int max_wait_attempts = 10)
    {
        return writeSlot(idx, true, true, max_wait_attempts);
    }

    bool setSlotFalseWithWait(uint32_t task_id, int max_wait_attempts = 10)
    {
        return writeSlot(task_id, false, true, max_wait_attempts);
    }

    size_t getBufferSize() const
    {
        return buffer_size_;
    }

    size_t getTrueCount() const
    {
        size_t count = 0;
        for (size_t i = 0; i < buffer_size_; ++i) {
            if (slots_[i].load(std::memory_order_acquire)) {
                ++count;
            }
        }
        return count;
    }

    void resetAll()
    {
        for (size_t i = 0; i < buffer_size_; ++i) {
            slots_[i].store(false, std::memory_order_relaxed);
        }
    }

private:
    bool writeWithWait(size_t index, bool new_value, int max_attempts)
    {
        bool opposite_value = !new_value;

        for (int attempt = 0; attempt < max_attempts; ++attempt) {
            bool current_value = slots_[index].load(std::memory_order_acquire);

            if (current_value == opposite_value) {
                slots_[index].store(new_value, std::memory_order_release);
                return true;
            }
            else {
                std::this_thread::yield();
            }
        }
        return false;
    }

private:
    const size_t                   buffer_size_;
    std::vector<std::atomic<bool>> slots_;
};

template<typename T>
class ProxyQueue {
private:
    std::queue<T>           queue_;
    mutable std::mutex      mutex_;
    std::condition_variable cv_;

    std::atomic<bool> stop_flag_{false};

public:
    ProxyQueue()                             = default;
    ProxyQueue(const ProxyQueue&)            = delete;
    ProxyQueue& operator=(const ProxyQueue&) = delete;

    void enqueue(T&& element)
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
        cv_.wait(lock, [this] { return !queue_.empty() || stop_flag_.load(); });

        if (stop_flag_.load()) {
            SLIME_LOG_ERROR("STOP");
        }

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
    bool fetchQueue(T& element, const std::chrono::duration<a, p>& time_out)
    {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, time_out, [this] { return !queue_.empty() || stop_flag_.load(); })) {
            return false;
        }

        if (stop_flag_.load()) {
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

    void notifyAll()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            stop_flag_.store(true);
        }
        cv_.notify_all();
    }

    void stop()
    {
        stop_flag_.store(true);
        cv_.notify_all();
    }

    void restart()
    {
        stop_flag_.store(false);
    }
};

class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {

    friend class RDMABuffer;

public:
    RDMAEndpoint(const std::string& dev_name, size_t ib_port, const std::string& link_type, size_t qp_nums);

    // TODO: 设计聚合多网卡传输的Send Recv

    void connect(JSON& data_ctx_info, JSON& meta_ctx_info);

    ~RDMAEndpoint();

    void startAllThreads();
    void stopAllThreads();

    void addRDMABuffer(std::shared_ptr<RDMABuffer> rdma_buffer);

private:
    void metaRecvQueueThread(std::chrono::milliseconds timeout);
    void dataRecvQueueThread(std::chrono::milliseconds timeout);

    void SendBufferQueueThread(std::chrono::milliseconds timeout);

    void WaitSendFinishQueueThread(std::chrono::milliseconds timeout);
    void WaitRecvFinishQueueThread(std::chrono::milliseconds timeout);

    void addPreQueueElement(OpCode rdma_opcode);
    void addRDMABuffer(OpCode rdma_opcode, std::shared_ptr<RDMABuffer> rdma_buffer);
    void postMetaWrite(std::shared_ptr<RDMABuffer> rdma_buffer);
    void postDataWrite(RDMABufferQueueElement& element, std::shared_ptr<RDMABuffer> rdma_buffer);
    void postRDMAAssignment(OpCode rdma_opcode);

    ProxyQueue<RDMAPrePostQueueElement> meta_recv_queue_;
    ProxyQueue<RDMAPrePostQueueElement> data_recv_queue_;

    ProxyQueue<RDMABufferQueueElement> send_buffer_queue_;

    ProxyQueue<RDMABufferQueueElement> send_finish_queue_;
    ProxyQueue<RDMABufferQueueElement> recv_finish_queue_;

    ProxyQueue<meta_data_t> meta_recv_store_queue_;

    std::thread meta_recv_thread_;
    std::thread data_recv_thread_;

    std::thread send_buffer_thread_;

    std::thread send_finish_thread_;
    std::thread recv_finish_thread_;

    size_t data_ctx_qp_num_;
    size_t meta_ctx_qp_num_;

    std::atomic<uint32_t> unique_SEND_SLOT_ID_{0};
    std::atomic<uint32_t> unique_RECV_SLOT_ID_{0};

    std::vector<uint32_t> dum_meta_buffer_;
    std::vector<uint32_t> dum_data_buffer_;

    std::vector<meta_data_t> meta_buffer_;

    std::unordered_map<uint32_t, bool> meta_buffer_is_ready_;
    mutable std::shared_mutex          meta_buffer_mutex_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;

    std::atomic<uint32_t> unique_meta_recv_id_{0};
    std::atomic<uint32_t> unique_data_recv_id_{0};

    std::atomic<bool>                       stop_SendMetaQueueThread_{false};
    std::atomic<bool>                       stop_dataRecvQueueThread_{false};
    std::atomic<bool>                       stop_SendBufferQueueThread_{false};
    std::atomic<bool>                       stop_WaitSendFinishQueueThread_{false};
    std::atomic<bool>                       stop_WaitRecvFinishQueueThread_{false};
    std::unique_ptr<RingBufferReadyManager> meta_buffer_manager_;
    std::unique_ptr<RingBufferReadyManager> data_buffer_manager_;
};

}  // namespace slime
