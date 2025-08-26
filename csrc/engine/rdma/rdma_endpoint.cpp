#include "engine/rdma/rdma_endpoint.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_common.h"
#include "engine/rdma/rdma_context.h"
#include <atomic>
#include <mutex>
#include <string>

namespace slime {

RDMATask::RDMATask(std::shared_ptr<RDMAEndpoint> endpoint,
                   uint32_t                      task_id,
                   OpCode                        opcode,
                   std::shared_ptr<RDMABuffer>   buffer):
    endpoint_(endpoint), slot_id_(task_id), opcode_(opcode), buffer_(buffer)
{
    // meta_data_buf_ = new meta_data_t[buffer->batchSize()];
    // registerMetaMemoryRegion();
    registerDataMemoryRegion();
    fillBuffer();
}

RDMATask::~RDMATask() {}

std::string RDMATask::getDataKey(int32_t idx)
{
    std::string          data_prefix        = opcode_ == OpCode::SEND ? "DATA_SEND" : "DATA_RECV";
    storage_view_batch_t storage_view_batch = buffer_->storageViewBatch();
    return data_prefix + "@" + std::to_string(storage_view_batch[idx].data_ptr) + "_"
           + std::to_string(storage_view_batch[idx].length) + "_" + std::to_string(idx);
}

AssignmentBatch RDMATask::getMetaAssignmentBatch()
{
    size_t meta_buffer_idx = slot_id_ % MAX_META_BUFFER_SIZE;
    return AssignmentBatch{Assignment("meta_buffer",
                                      meta_buffer_idx * sizeof(meta_data_t),
                                      meta_buffer_idx * sizeof(meta_data_t),
                                      sizeof(meta_data_t))};
}

AssignmentBatch RDMATask::getDataAssignmentBatch()
{
    AssignmentBatch      batch;
    storage_view_batch_t storage_view_batch = buffer_->storageViewBatch();
    for (int i = 0; i < buffer_->batchSize(); ++i) {
        batch.push_back(Assignment(getDataKey(i), 0, 0, storage_view_batch[i].length));
    }
    return batch;
}

int RDMATask::registerDataMemoryRegion()
{
    // std::cout << getDataKey(0) << std::endl;
    auto mr_is_exist = endpoint_->dataCtx()->get_mr_for_endpoint(getDataKey(0));

    if (mr_is_exist != nullptr) {
        //已经注册过, 直接使用
        std::cout << "已经注册过, 直接使用" << std::endl;
        return 0;
    }
    else {
        //未注册
        std::cout << "未注册" << std::endl;
        std::cout << getDataKey(0) << std::endl;
        storage_view_batch_t storage_view_batch = buffer_->storageViewBatch();
        for (int i = 0; i < buffer_->batchSize(); ++i) {
            endpoint_->dataCtx()->register_memory_region(
                getDataKey(i), storage_view_batch[i].data_ptr, storage_view_batch[i].length);
        }
        return 0;
    }
}

void RDMATask::fillBuffer()
{
    if (opcode_ == OpCode::SEND) {
        std::vector<meta_data_t>& meta_buf = endpoint_->getMetaBuffer();
        // memset(meta_buf.data() + (slot_id_ % MAX_META_BUFFER_SIZE) * sizeof(meta_data_t), 0, sizeof(meta_data_t));
    }
    else if (opcode_ == OpCode::RECV) {
        std::vector<meta_data_t>& meta_buf = endpoint_->getMetaBuffer();
        for (size_t i = 0; i < buffer_->batchSize(); ++i) {
            auto mr                                              = endpoint_->dataCtx()->get_mr(getDataKey(i));
            meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_addr[i] = reinterpret_cast<uint64_t>(mr->addr);
            meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_rkey[i] = mr->rkey;
            meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_size[i] = mr->length;
            meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_slot    = slot_id_;
            meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_qpidx   = slot_id_ % endpoint_->dataCtxQPNum();
        }
    }
    else {
        SLIME_LOG_ERROR("Unsupported opcode in RDMATask::fillBuffer()");
    }
}

// int RDMATask::targetQPI()
// {
//     return
//     // return meta_data_buf_[0].mr_qpidx;
// }

int RDMATask::registerRemoteDataMemoryRegion()
{
    auto mr_is_exist = endpoint_->dataCtx()->get_remote_mr_for_endpoint(getDataKey(0));
    if (mr_is_exist == 0) {
        std::vector<meta_data_t>& meta_buf = endpoint_->getMetaBuffer();
        for (size_t i = 0; i < buffer_->batchSize(); ++i) {
            uint64_t addr   = meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_addr[i];
            uint32_t length = meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_size[i];
            uint32_t rkey   = meta_buf[slot_id_ % MAX_META_BUFFER_SIZE].mr_rkey[i];
            std::cout << slot_id_ << "slot_id_" << std::endl;
            std::cout << getDataKey(i) << std::endl;
            std::cout << addr << " " << length << " " << rkey << std::endl;
            endpoint_->dataCtx()->register_remote_memory_region(getDataKey(i), addr, length, rkey);
        }
        return 0;
    }
    else {
        return 0;
    }
}

RDMAEndpoint::RDMAEndpoint(const std::string& dev_name, uint8_t ib_port, const std::string& link_type, size_t qp_num)
{
    SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");
    data_ctx_ = std::make_shared<RDMAContext>(qp_num);
    meta_ctx_ = std::make_shared<RDMAContext>(1);

    data_ctx_->init(dev_name, ib_port, link_type);
    meta_ctx_->init(dev_name, ib_port, link_type);

    data_ctx_qp_num_ = data_ctx_->qp_list_len_;
    meta_ctx_qp_num_ = meta_ctx_->qp_list_len_;
    SLIME_LOG_INFO("The QP number of data plane is: ", data_ctx_qp_num_);
    SLIME_LOG_INFO("The QP number of control plane is: ", meta_ctx_qp_num_);
    SLIME_LOG_INFO("RDMA Endpoint Init Success and Launch the RDMA Endpoint Task Threads...");

    const size_t max_meta_buffer_size = 64;
    meta_buffer_.reserve(max_meta_buffer_size);
    memset(meta_buffer_.data(), 0, meta_buffer_.size() * sizeof(meta_data_t));
    meta_ctx_->register_memory_region(
        "meta_buffer", reinterpret_cast<uintptr_t>(meta_buffer_.data()), sizeof(meta_data_t) * max_meta_buffer_size);
}

RDMAEndpoint::~RDMAEndpoint()
{
    {
        std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
        RDMA_tasks_threads_running_ = false;
    }

    rdma_tasks_cv_.notify_all();

    if (rdma_tasks_threads_.joinable())
        rdma_tasks_threads_.join();
}

void RDMAEndpoint::connect(const json& data_ctx_info, const json& meta_ctx_info)
{
    data_ctx_->connect(data_ctx_info);
    meta_ctx_->connect(meta_ctx_info);

    data_ctx_->launch_future();
    meta_ctx_->launch_future();

    RDMA_tasks_threads_running_ = true;
    rdma_tasks_threads_         = std::thread([this] { this->waitandPopTask(std::chrono::milliseconds(100)); });
}

// std::shared_ptr<RDMABuffer> RDMAEndpoint::createRDMABuffer(storage_view_batch_t batch)
// {
//     std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
//     return std::make_shared<RDMABuffer>(shared_from_this(), batch);
// }

void RDMAEndpoint::addSendTask(std::shared_ptr<RDMABuffer> buffer)
{
    std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    ++send_slot_id_;
    auto task = std::make_shared<rdma_task_t>(shared_from_this(), send_slot_id_, OpCode::SEND, buffer);
    send_batch_slot_[send_slot_id_] = task;
    rdma_tasks_queue_.push(task);
    rdma_tasks_cv_.notify_one();
}

void RDMAEndpoint::addRecvTask(std::shared_ptr<RDMABuffer> buffer)
{
    std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    ++recv_slot_id_;
    auto task = std::make_shared<rdma_task_t>(shared_from_this(), recv_slot_id_, OpCode::RECV, buffer);
    recv_batch_slot_[recv_slot_id_] = task;
    rdma_tasks_queue_.push(task);
    rdma_tasks_cv_.notify_one();
}

void RDMAEndpoint::waitandPopTask(std::chrono::milliseconds timeout)
{
    while (true) {
        std::shared_ptr<rdma_task_t> task;
        {
            std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
            rdma_tasks_cv_.wait(lock, [this] { return !rdma_tasks_queue_.empty() || !RDMA_tasks_threads_running_; });

            if (!RDMA_tasks_threads_running_ && rdma_tasks_queue_.empty())
                break;

            if (!rdma_tasks_queue_.empty()) {
                task = std::move(rdma_tasks_queue_.front());
                rdma_tasks_queue_.pop();
            }

            switch (task->opcode_) {
                case OpCode::SEND:
                    asyncSendData(task);
                    break;
                case OpCode::RECV:
                    asyncRecvData(task);
                    break;
                default:
                    SLIME_LOG_ERROR("Unknown OpCode in WaitandPopTask");
                    break;
            }
        }
    }
}

void RDMAEndpoint::asyncSendData(std::shared_ptr<rdma_task_t> task)
{
    size_t   batch_size = task->buffer_->batchSize();
    uint32_t slot_id    = task->slot_id_;

    auto data_callback = [this, task](int status, int _) mutable {
        std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
        this->send_batch_slot_[task->slot_id_]->buffer_->send_done_callback();
    };

    auto meta_callback = [this, task, data_callback](int status, int slot_id) mutable {
        std::unique_lock<std::mutex> lock(this->rdma_tasks_mutex_);
        task->registerRemoteDataMemoryRegion();
        AssignmentBatch data_assign_batch = task->getDataAssignmentBatch();
        std::cout << task->slot_id_ << "mr_addr: " << (uintptr_t)meta_buffer_[task->slot_id_].mr_addr[0]
                  << " rkey: " << meta_buffer_[task->slot_id_].mr_rkey[0] << std::endl;
        auto data_atx = this->data_ctx_->submit(
            OpCode::WRITE_WITH_IMM, data_assign_batch, data_callback, RDMAContext::UNDEFINED_QPI, slot_id);
    };

    {
        AssignmentBatch meta_data = task->getMetaAssignmentBatch();
        meta_ctx_->submit(OpCode::RECV, meta_data, meta_callback);
    }
}

void RDMAEndpoint::asyncRecvData(std::shared_ptr<rdma_task_t> task)
{
    auto data_callback = [this, task](int status, int slot_id) mutable {
        std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
        recv_batch_slot_[slot_id]->buffer_->recv_done_callback();
    };

    auto meta_callback = [this, task, data_callback](int status, int _) mutable {
        std::unique_lock<std::mutex> lock(this->rdma_tasks_mutex_);
        AssignmentBatch              assign_batch = task->getDataAssignmentBatch();
        auto data_atx = this->data_ctx_->submit(OpCode::RECV, assign_batch, data_callback, RDMAContext::UNDEFINED_QPI);
    };

    {
        AssignmentBatch meta_data = task->getMetaAssignmentBatch();
        std::cout << ".mr_addr[0]: " << (uintptr_t)meta_buffer_[task->slot_id_].mr_addr[0]
                  << " rkey: " << meta_buffer_[task->slot_id_].mr_rkey[0] << std::endl;
        meta_ctx_->submit(OpCode::WRITE_WITH_IMM, meta_data, meta_callback, RDMAContext::UNDEFINED_QPI, task->slot_id_);
    }
}

}  // namespace slime
