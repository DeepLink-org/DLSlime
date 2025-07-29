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
    meta_data_buf_ = new meta_data_t[buffer->batchSize()];
    registerMetaMemoryRegion();
    registerDataMemoryRegion();
    fillBuffer();
}

RDMATask::~RDMATask()
{
    delete[] meta_data_buf_;
}

std::string RDMATask::getMetaKey()
{
    std::string meta_key = opcode_ == OpCode::SEND ? "META_SEND" : "META_RECV";
    return meta_key + "@" + std::to_string(slot_id_);
}

std::string RDMATask::getDataKey(int32_t idx)
{
    std::string data_key = opcode_ == OpCode::SEND ? "DATA_SEND" : "DATA_RECV";
    return data_key + "@" + std::to_string(slot_id_) + "_" + std::to_string(idx);
}

AssignmentBatch RDMATask::getMetaAssignmentBatch()
{
    return AssignmentBatch{Assignment(getMetaKey(), 0, 0, sizeof(meta_data_t) * buffer_->batchSize())};
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

int RDMATask::registerMetaMemoryRegion()
{
    endpoint_->MetaCtx()->register_memory_region(
        getMetaKey(), (uintptr_t)meta_data_buf_, buffer_->batchSize() * sizeof(meta_data_t));
    return 0;
}

int RDMATask::registerDataMemoryRegion()
{
    storage_view_batch_t storage_view_batch = buffer_->storageViewBatch();
    for (int i = 0; i < buffer_->batchSize(); ++i) {
        endpoint_->dataCtx()->register_memory_region(
            getDataKey(i), storage_view_batch[i].data_ptr, storage_view_batch[i].length);
    }
    return 0;
}

void RDMATask::fillBuffer()
{
    for (size_t i = 0; i < buffer_->batchSize(); ++i) {
        auto mr                    = endpoint_->dataCtx()->get_mr(getDataKey(i));
        meta_data_buf_[i].mr_addr  = reinterpret_cast<uint64_t>(mr->addr);
        meta_data_buf_[i].mr_rkey  = mr->rkey;
        meta_data_buf_[i].mr_size  = mr->length;
        meta_data_buf_[i].mr_slot  = slot_id_;
        meta_data_buf_[i].mr_qpidx = slot_id_ % endpoint_->dataCtxQPNum();
    }
}

int RDMATask::targetQPI()
{
    return meta_data_buf_[0].mr_qpidx;
}

int RDMATask::registerRemoteDataMemoryRegion()
{
    for (size_t i = 0; i < buffer_->batchSize(); ++i) {
        json     mr_info;
        uint64_t addr     = meta_data_buf_[i].mr_addr;
        uint32_t length   = meta_data_buf_[i].mr_size;
        uint32_t rkey     = meta_data_buf_[i].mr_rkey;
        mr_info["addr"]   = addr;
        mr_info["length"] = length;
        mr_info["rkey"]   = rkey;
        endpoint_->dataCtx()->register_remote_memory_region(getDataKey(i), mr_info);
    }
    return 0;
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

std::shared_ptr<RDMABuffer> RDMAEndpoint::createRDMABuffer(storage_view_batch_t batch)
{
    std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    return std::make_shared<RDMABuffer>(shared_from_this(), batch);
}

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
        auto            data_atx          = this->data_ctx_->submit(
            OpCode::WRITE_WITH_IMM, data_assign_batch, data_callback, task->targetQPI(), slot_id);
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
        auto data_atx = this->data_ctx_->submit(OpCode::RECV, assign_batch, data_callback, task->targetQPI());
    };

    {
        auto            batch_size = task->buffer_->batchSize();
        AssignmentBatch meta_data  = task->getMetaAssignmentBatch();
        meta_ctx_->submit(OpCode::SEND_WITH_IMM, meta_data, meta_callback, RDMAContext::UNDEFINED_QPI, task->slot_id_);
    }
}

}  // namespace slime
