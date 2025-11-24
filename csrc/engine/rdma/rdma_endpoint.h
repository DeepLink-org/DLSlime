#pragma once
#include "engine/assignment.h"
#include "engine/rdma/rdma_context.h"

#include <atomic>
#include <bits/stdint-uintn.h>
#include <condition_variable>
#include <cstddef>
#include <deque>
#include <functional>
#include <infiniband/verbs.h>
#include <memory>
#include <mutex>
#include <queue>
#include <shared_mutex>
#include <stdexcept>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "logging.h"
#include "rdma_common.h"

namespace slime {

class RDMABuffer;
class RDMAEndpoint;

typedef struct MetaData {

    uint64_t mr_addr;
    uint32_t mr_rkey;
    uint32_t mr_size;
    uint32_t mr_slot;
    uint32_t mr_qpidx;

} meta_data_t;

struct RDMABufferQueueElement {

    RDMABufferQueueElement() = default;

    RDMABufferQueueElement(uint32_t unique_id, OpCode rdma_opcode, std::shared_ptr<RDMABuffer> rdma_buffer = nullptr):
        unique_id_(unique_id), rdma_opcode_(rdma_opcode), rdma_buffer_(rdma_buffer)
    {
        is_finished_ptr_ = std::make_shared<std::atomic<bool>>(false);
    }

    RDMABufferQueueElement(const RDMABufferQueueElement& other) = default;

    RDMABufferQueueElement(RDMABufferQueueElement&& other) noexcept = default;
    RDMABufferQueueElement& operator=(RDMABufferQueueElement&& other) noexcept
    {
        if (this != &other) {
            unique_id_             = other.unique_id_;
            rdma_opcode_           = other.rdma_opcode_;
            rdma_buffer_           = std::move(other.rdma_buffer_);
            is_finished_ptr_       = std::move(other.is_finished_ptr_);
            other.rdma_buffer_     = nullptr;
            other.is_finished_ptr_ = nullptr;
        }
        return *this;
    }
    uint32_t                           unique_id_{0};
    OpCode                             rdma_opcode_{OpCode::SEND};
    std::shared_ptr<RDMABuffer>        rdma_buffer_{nullptr};
    std::shared_ptr<std::atomic<bool>> is_finished_ptr_{nullptr};
};

struct RDMAPrePostQueueElement {

    RDMAPrePostQueueElement() = default;
    RDMAPrePostQueueElement(uint32_t unique_id, OpCode rdma_opcode): unique_id_(unique_id), rdma_opcode_(rdma_opcode)
    {
        is_finished_ptr_ = std::make_shared<std::atomic<bool>>(false);
    }

    RDMAPrePostQueueElement(const RDMAPrePostQueueElement& other)     = default;
    RDMAPrePostQueueElement(RDMAPrePostQueueElement&& other) noexcept = default;

    RDMAPrePostQueueElement& operator=(RDMAPrePostQueueElement&& other) noexcept
    {
        if (this != &other) {
            unique_id_             = other.unique_id_;
            rdma_opcode_           = other.rdma_opcode_;
            is_finished_ptr_       = std::move(other.is_finished_ptr_);
            other.is_finished_ptr_ = nullptr;
        }
        return *this;
    }
    uint32_t                           unique_id_{0};
    OpCode                             rdma_opcode_{OpCode::SEND};
    std::shared_ptr<std::atomic<bool>> is_finished_ptr_{nullptr};
};

class RingSlotsManager {

private:
    const size_t buffer_size_;

    struct Slot {
        std::atomic<bool>     status{false};
        std::atomic<uint32_t> layers{0};
    };

    std::vector<Slot> slots_;

public:
    explicit RingSlotsManager(size_t size): buffer_size_(size), slots_(size)
    {

        for (size_t i = 0; i < buffer_size_; ++i) {
            slots_[i].status.store(false, std::memory_order_relaxed);
            slots_[i].layers.store(0, std::memory_order_relaxed);
        }
    }

    RingSlotsManager(const RingSlotsManager&)            = delete;
    RingSlotsManager& operator=(const RingSlotsManager&) = delete;
    RingSlotsManager(RingSlotsManager&&)                 = delete;
    RingSlotsManager& operator=(RingSlotsManager&&)      = delete;

    bool releaseSlot(uint32_t idx, int max_wait_attempts = 5)
    {
        size_t   index        = idx % buffer_size_;
        uint32_t target_layer = idx / buffer_size_;

        for (int attempt = 0; attempt < max_wait_attempts; ++attempt) {
            bool     cur_status = slots_[index].status.load(std::memory_order_acquire);
            uint32_t cur_layers = slots_[index].layers.load(std::memory_order_acquire);

            if (!cur_status || cur_layers != target_layer) {
                return false;
            }

            bool expected_status = true;
            if (slots_[index].status.compare_exchange_strong(
                    expected_status, false, std::memory_order_release, std::memory_order_relaxed)) {
                uint32_t next_layer = cur_layers + 1;
                if (slots_[index].layers.compare_exchange_strong(
                        cur_layers, next_layer, std::memory_order_release, std::memory_order_relaxed)) {
                    return true;
                }
                slots_[index].status.store(true, std::memory_order_release);
                return false;
            }
            std::this_thread::yield();
        }
        return false;
    }

    bool acquireSlot(uint32_t idx, int max_wait_attempts = 5)
    {
        size_t   index         = idx % buffer_size_;
        uint32_t target_layers = idx / buffer_size_;

        for (int attempt = 0; attempt < max_wait_attempts; ++attempt) {
            bool     cur_status = slots_[index].status.load(std::memory_order_acquire);
            uint32_t cur_layers = slots_[index].layers.load(std::memory_order_acquire);

            if (cur_status || cur_layers != target_layers) {
                std::this_thread::yield();
                continue;
            }

            bool expected = false;
            if (slots_[index].status.compare_exchange_strong(
                    expected, true, std::memory_order_release, std::memory_order_relaxed)) {
                return true;
            }

            std::this_thread::yield();
        }

        return false;
    }

    bool checkSlotAvailable(uint32_t idx) const
    {
        size_t   index         = idx % buffer_size_;
        uint32_t target_layers = idx / buffer_size_;

        bool     status = slots_[index].status.load(std::memory_order_acquire);
        uint32_t layers = slots_[index].layers.load(std::memory_order_acquire);
        return !status && (layers == target_layers);
    }

    bool checkSlotReady(uint32_t idx) const
    {
        size_t   index         = idx % buffer_size_;
        uint32_t target_layers = idx / buffer_size_;

        bool     status = slots_[index].status.load(std::memory_order_acquire);
        uint32_t layers = slots_[index].layers.load(std::memory_order_acquire);

        return status && (layers == target_layers);
    }

    void printSlots() const
    {
        std::cout << "=== RingSlotsManager Status (size=" << buffer_size_ << ") ===" << std::endl;
        for (size_t i = 0; i < buffer_size_; ++i) {
            bool     status = slots_[i].status.load(std::memory_order_relaxed);
            uint32_t layers = slots_[i].layers.load(std::memory_order_relaxed);

            std::string state = status ? "occupied" : "free";

            std::cout << "  [" << std::setw(3) << i << "] layer=" << std::setw(2) << layers << " : " << state
                      << std::endl;
        }
        std::cout << "========================================" << std::endl;
    }
};

template<typename T>
class ProxyQueue {
private:
    std::queue<T>      queue_;
    mutable std::mutex mutex_;

public:
    void enqueue(T element)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(element));
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

    bool checkQueue(const T*& element)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        element = &queue_.front();
        return true;
    }
    bool popQueue()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (queue_.empty())
            return false;

        queue_.pop();
        return true;
    }

    template<typename M>
    bool peekQueue(uint32_t& task_id, M&& matcher)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!queue_.empty() && std::forward<M>(matcher)(queue_.front())) {
            task_id = queue_.front().unique_id_;
            // queue_.pop();
            return true;
        }
        return false;
    }

    template<typename M>
    bool peekQueue(T& element, M&& matcher)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!queue_.empty() && std::forward<M>(matcher)(queue_.front())) {
            element = std::move(queue_.front());
            queue_.pop();
            return true;
        }
        return false;
    }

    bool getFrontTaskId(uint32_t& task_id)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!queue_.empty()) {
            task_id = queue_.front().unique_id_;
            return true;
        }
        return false;
    }

    size_t size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    bool empty() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }
};

class RDMAEndpoint: public std::enable_shared_from_this<RDMAEndpoint> {

    friend class RDMABuffer;

public:
    explicit RDMAEndpoint(const std::string& dev_name, size_t ib_port, const std::string& link_type, size_t qp_nums);


    json dataCtxInfo() const
    {
        return data_ctx_->endpoint_info();
    }

    json metaCtxInfo() const
    {
        return meta_ctx_->endpoint_info();
    }

    void connect(const json& data_ctx_info, const json& meta_ctx_info);

    ~RDMAEndpoint();

    void proxyInit();
    void proxyDestroy();

    void addRDMABuffer(std::shared_ptr<RDMABuffer> rdma_buffer);

private:
    void metaRecvQueueThread(std::chrono::milliseconds timeout);
    void dataRecvQueueThread(std::chrono::milliseconds timeout);

    void SendBufferQueueThread(std::chrono::milliseconds timeout);
    void RecvBufferQueueThread(std::chrono::milliseconds timeout);

    void SendFinishQueueThread(std::chrono::milliseconds timeout);
    void RecvFinishQueueThread(std::chrono::milliseconds timeout);

    void addPreQueueElement(OpCode rdma_opcode);
    void addRDMABuffer(OpCode rdma_opcode, std::shared_ptr<RDMABuffer> rdma_buffer);
    void postMetaWrite(uint32_t idx, std::shared_ptr<RDMABuffer> rdma_buffer);
    void postDataWrite(RDMABufferQueueElement& element, std::shared_ptr<RDMABuffer> rdma_buffer);
    void postRDMAAssignment(OpCode rdma_opcode);

    ProxyQueue<RDMAPrePostQueueElement> meta_recv_queue_;
    ProxyQueue<RDMAPrePostQueueElement> data_recv_queue_;

    ProxyQueue<RDMABufferQueueElement> send_buffer_queue_;
    ProxyQueue<RDMABufferQueueElement> recv_buffer_queue_;

    ProxyQueue<RDMABufferQueueElement> send_finish_queue_;
    ProxyQueue<RDMABufferQueueElement> recv_finish_queue_;

    std::thread meta_recv_thread_;
    std::thread data_recv_thread_;

    std::thread send_buffer_thread_;
    std::thread recv_buffer_thread_;

    std::thread send_finish_thread_;
    std::thread recv_finish_thread_;

    size_t data_ctx_qp_num_;
    size_t meta_ctx_qp_num_;

    std::atomic<uint32_t> unique_SEND_SLOT_ID_{0};
    std::atomic<uint32_t> unique_RECV_SLOT_ID_{0};

    std::vector<uint32_t> dum_meta_buffer_;
    std::vector<uint32_t> dum_data_buffer_;

    std::vector<meta_data_t> meta_buffer_;

    std::shared_ptr<RDMAContext> data_ctx_;
    std::shared_ptr<RDMAContext> meta_ctx_;

    std::atomic<uint32_t> unique_meta_recv_id_{0};
    std::atomic<uint32_t> unique_data_recv_id_{0};

    std::atomic<bool>                 stop_meta_recv_queue_thread_{false};
    std::atomic<bool>                 stop_data_recv_queue_thread_{false};
    std::atomic<bool>                 stop_send_buffer_queue_thread_{false};
    std::atomic<bool>                 stop_recv_buffer_queue_thread_{false};
    std::atomic<bool>                 stop_wait_send_finish_queue_thread_{false};
    std::atomic<bool>                 stop_wait_recv_finish_queue_thread_{false};
    std::unique_ptr<RingSlotsManager> meta_slots_manager_;
    std::unique_ptr<RingSlotsManager> data_slots_manager_;

    std::unordered_map<uint32_t, RDMABufferQueueElement> recv_buffer_mapping_;
};
}  // namespace slime