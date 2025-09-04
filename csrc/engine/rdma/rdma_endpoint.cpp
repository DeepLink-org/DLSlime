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

endPointAssignment::endPointAssignment(std::shared_ptr<RDMABuffer> buf,
                                       size_t                      slot_idx,
                                       OpCode                      opcode,
                                       bool                        is_low_latency_enabled)
{
    // 生成一个endPoint任务
    // 两种模式，low latency模式直接将RDMAbuffer的数据复制到lowlatencybuffer然后生成RDMAAssignment；
    // normal模式则是注册memory region然后生成RDMAAssignment
    buf_                    = buf;
    idx_                    = slot_idx;
    opcode_                 = opcode;
    is_low_latency_enabled_ = is_low_latency_enabled;

    if (is_low_latency_enabled_) {

        storage_view_batch_t storage_view_batch = buf_->storageViewBatch();
        size_t               batch_size         = buf_->batchSize();
        std::string          mem_region_name    = (opcode == OpCode::SEND) ? "LL_SEND_BUFFER" : "LL_RECV_BUFFER";
        for (size_t i = 0; i < batch_size; ++i) {
            data_batch.push_back(Assignment(mem_region_name,
                                            0,
                                            idx_ * MAX_ELEMENT_SIZE * MAX_BUFFER_BATCH_SIZE + i * MAX_ELEMENT_SIZE,
                                            storage_view_batch[i].length));
        }
    }
    else {
        storage_view_batch_t storage_view_batch = buf_->storageViewBatch();
        size_t               batch_size         = buf_->batchSize();
        // 注册data的memory region

        // std::string          mem_region_name    = (opcode == OpCode::SEND) ? "LL_SEND_BUFFER" : "LL_RECV_BUFFER";
        for (size_t i = 0; i < batch_size; ++i) {
            // register the data memory region

            data_batch.push_back(Assignment("BUFFER", i * 256, 0, storage_view_batch[i].length));
        }
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
    SLIME_LOG_INFO("RDMA Endpoint Init Success!");

    SLIME_LOG_INFO("register the meta buffer...");

    const size_t max_meta_buffer_size = 64;
    meta_ctx_->register_memory_region("META_SEND_BUFFER",
                                      reinterpret_cast<uintptr_t>(meta_SEND_buffer_.data()),
                                      sizeof(metaBuffer) * max_meta_buffer_size);

    meta_ctx_->register_memory_region("META_RECV_BUFFER",
                                      reinterpret_cast<uintptr_t>(meta_RECV_buffer_.data()),
                                      sizeof(metaBuffer) * max_meta_buffer_size);

    SLIME_ASSERT_EQ(cur_meta_send_idx_, 0, "meta data idx must be set to zero at first");
    SLIME_ASSERT_EQ(cur_meta_recv_idx_, 0, "meta data idx must be set to zero at first");
    SLIME_LOG_INFO("register the meta buffer success!");

    SLIME_LOG_INFO("register the low latency buffer...");

    const size_t max_buffer_size       = 64;
    const size_t max_element_size      = 8192;
    const size_t max_buffer_batch_size = 8;

    ll_SEND_buffer_ = std::make_unique<LowLatencyBuffer>(max_buffer_size, max_element_size, max_buffer_batch_size);
    ll_RECV_buffer_ = std::make_unique<LowLatencyBuffer>(max_buffer_size, max_element_size, max_buffer_batch_size);
    SLIME_ASSERT(ll_SEND_buffer_, "ll_SEND_buffer_ is nullptr");
    SLIME_ASSERT(ll_RECV_buffer_, "ll_RECV_buffer_ is nullptr");
    data_ctx_->register_memory_region(
        "LL_SEND_BUFFER", reinterpret_cast<uintptr_t>(ll_SEND_buffer_->getPtr()), ll_SEND_buffer_->getBufferSize());

    data_ctx_->register_memory_region(
        "LL_RECV_BUFFER", reinterpret_cast<uintptr_t>(ll_RECV_buffer_->getPtr()), ll_RECV_buffer_->getBufferSize());

    SLIME_ASSERT_EQ(cur_ll_send_idx_, 0, "meta data idx must be set to zero at first");
    SLIME_ASSERT_EQ(cur_ll_recv_idx_, 0, "meta data idx must be set to zero at first");
    SLIME_LOG_INFO("register the meta buffer success!");
}

RDMAEndpoint::~RDMAEndpoint()
{
    SLIME_LOG_INFO("stop the endpoint thread...");
    {
        std::unique_lock<std::mutex> lock(endPoint_queue_mutex_);
        endPoint_queue_running_ = false;
    }

    endPoint_queue_cv_.notify_all();

    if (endPoint_queue_thread_.joinable())
        endPoint_queue_thread_.join();

    SLIME_LOG_INFO("stop the endpoint thread success!");
    SLIME_LOG_INFO("unregister the memory region in endpoint...");
    meta_ctx_->unregister_memory_region("META_SEND_BUFFER");
    meta_ctx_->unregister_memory_region("META_RECV_BUFFER");

    data_ctx_->unregister_memory_region("LL_SEND_BUFFER");
    data_ctx_->unregister_memory_region("LL_RECV_BUFFER");
    SLIME_LOG_INFO("unregister the memory region in endpoint success!");
}

void RDMAEndpoint::endPointConnect(const json& data_ctx_info, const json& meta_ctx_info)
{
    SLIME_LOG_INFO("connect the endpoint...");
    data_ctx_->connect(data_ctx_info);
    meta_ctx_->connect(meta_ctx_info);

    data_ctx_->launch_future();
    meta_ctx_->launch_future();
    SLIME_LOG_INFO("connect the endpoint success!");

    SLIME_LOG_INFO("start the endpoint queue...");
    endPoint_queue_running_ = true;
    endPoint_queue_thread_  = std::thread([this] { this->endPointQueue(std::chrono::milliseconds(100)); });
    SLIME_LOG_INFO("start the endpoint queue success!");
}

void RDMAEndpoint::addSendEndPointAssignment(std::shared_ptr<RDMABuffer> buf)
{
    std::unique_lock<std::mutex> lock(endPoint_queue_mutex_);
    send_slot_idx_++;
    auto send_endpoint_assignment         = std::make_shared<endPointAssignment>(buf, send_slot_idx_);
    send_assignment_slot_[send_slot_idx_] = send_endpoint_assignment;
    endPoint_queue_.push(send_endpoint_assignment);
    endPoint_queue_cv_.notify_one();
}

void RDMAEndpoint::endPointQueue(std::chrono::milliseconds timeout)
{
    while (true) {
        std::shared_ptr<endPointAssignment> assignment;
        {
            std::unique_lock<std::mutex> lock(endPoint_queue_mutex_);
            endPoint_queue_cv_.wait(lock, [this] { return !endPoint_queue_.empty() || !endPoint_queue_running_; });

            if (!endPoint_queue_running_ && endPoint_queue_.empty())
                break;

            if (!endPoint_queue_.empty()) {
                assignment = std::move(endPoint_queue_.front());
                endPoint_queue_.pop();
            }

            switch (assignment->opcode_) {
                case OpCode::SEND:
                    asyncSendData(assignment);
                    break;
                case OpCode::RECV:
                    asyncRecvData(assignment);
                    break;
                default:
                    SLIME_LOG_ERROR("Unknown OpCode in endPointQueue");
                    break;
            }
        }
    }
}

void RDMAEndpoint::asyncSendData(std::shared_ptr<endPointAssignment> assignment)
{
    // size_t   batch_size = task->buffer_->batchSize();
    // uint32_t slot_id    = task->slot_id_;

    // bool is_low_latency_mode = assignment->low_latency_mode_enabled_;

    // auto data_callback = [this, assignment](int status, int slot_id) mutable {
    //     // std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    //     // this->send_batch_slot_[slot_id]->buffer_->send_done_callback();
    // };

    // if (is_low_latency_mode) {
    //     // 在这里生成RDMA的Assignments
    //     AssignmentBatch data_assign;
    //     auto            data_atx = data_ctx_->submit(OpCode::WRITE_WITH_IMM, data_assign, data_callback);
    // }
    // else {
    //     int j = 0;
    // }

    // auto data_callback = [this, task](int status, int _) mutable {
    //     std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    //     this->send_batch_slot_[task->slot_id_]->buffer_->send_done_callback();
    // };

    // auto meta_callback = [this, task, data_callback](int status, int slot_id) mutable {
    //     std::unique_lock<std::mutex> lock(this->rdma_tasks_mutex_);
    //     task->registerRemoteDataMemoryRegion();
    //     AssignmentBatch data_assign_batch = task->getDataAssignmentBatch();
    //     auto            data_atx          = this->data_ctx_->submit(
    //         OpCode::WRITE_WITH_IMM, data_assign_batch, data_callback, task->targetQPI(), slot_id);
    // };

    // {
    //     AssignmentBatch meta_data = task->getMetaAssignmentBatch();
    //     meta_ctx_->submit(OpCode::RECV, meta_data, meta_callback);
    // }
}

void RDMAEndpoint::asyncRecvData(std::shared_ptr<endPointAssignment> assignment)
{
    // bool is_low_latency_mode = assignment->low_latency_mode_enabled_;
    // auto data_callback       = [this, assignment](int status, int slot_id) mutable {};

    // if (is_low_latency_mode) {
    //     AssignmentBatch data_assign;
    //     auto            data_atx = data_ctx_->submit(OpCode::RECV, data_assign, data_callback);
    // }
    // else {
    //     int j = 0;
    // }
    //     auto data_callback = [this, task](int status, int slot_id) mutable {
    //         std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    //         recv_batch_slot_[slot_id]->buffer_->recv_done_callback();
    //     };

    // auto meta_callback = [this, task, data_callback](int status, int _) mutable {
    //     std::unique_lock<std::mutex> lock(this->rdma_tasks_mutex_);
    //     AssignmentBatch              assign_batch = task->getDataAssignmentBatch();
    //     auto data_atx = this->data_ctx_->submit(OpCode::RECV, assign_batch, data_callback, task->targetQPI());
    // };

    // {
    //     auto            batch_size = task->buffer_->batchSize();
    //     AssignmentBatch meta_data  = task->getMetaAssignmentBatch();
    //     meta_ctx_->submit(OpCode::SEND_WITH_IMM, meta_data, meta_callback, RDMAContext::UNDEFINED_QPI,
    //     task->slot_id_);
    // }
}

}  // namespace slime
