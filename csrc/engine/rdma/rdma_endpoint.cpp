#include "engine/rdma/rdma_endpoint.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_common.h"
#include "engine/rdma/rdma_context.h"

#include "logging.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>

namespace slime {

RDMAEndpoint::RDMAEndpoint(const std::string& dev_name, size_t ib_port, const std::string& link_type, size_t qp_nums)
{
    SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");
    data_ctx_ = std::make_shared<RDMAContext>(qp_nums, 0);
    meta_ctx_ = std::make_shared<RDMAContext>(1, 0);

    data_ctx_->init(dev_name, ib_port, link_type);
    meta_ctx_->init(dev_name, ib_port, link_type);

    data_ctx_qp_num_ = data_ctx_->qp_list_len_;
    meta_ctx_qp_num_ = meta_ctx_->qp_list_len_;

    SLIME_LOG_INFO("The QP number of data plane is: ", data_ctx_qp_num_);
    SLIME_LOG_INFO("The QP number of control plane is: ", meta_ctx_qp_num_);
    SLIME_LOG_INFO("RDMA Endpoint Init Success...");

    SLIME_LOG_INFO("Allocate The MR Buffer and Dummy Buffer");
    constexpr size_t max_meta_buffer_size = MAX_META_BUFFER_SIZE * 2;
    meta_buffer_.reserve(max_meta_buffer_size);
    memset(meta_buffer_.data(), 0, meta_buffer_.size() * sizeof(meta_data_t));
    meta_ctx_->register_memory_region(
        "meta_buffer", reinterpret_cast<uintptr_t>(meta_buffer_.data()), sizeof(meta_data_t) * max_meta_buffer_size);

    constexpr size_t max_dum_buffer_size = 16;
    dum_meta_buffer_.reserve(max_dum_buffer_size);
    meta_ctx_->register_memory_region("dum_meta_buffer_",
                                      reinterpret_cast<uintptr_t>(dum_meta_buffer_.data()),
                                      sizeof(uint32_t) * dum_meta_buffer_.size());
    dum_data_buffer_.reserve(max_dum_buffer_size);
    data_ctx_->register_memory_region("dum_data_buffer_",
                                      reinterpret_cast<uintptr_t>(dum_data_buffer_.data()),
                                      sizeof(uint32_t) * dum_data_buffer_.size());

    SLIME_LOG_INFO("The MR Buffer and Dummy Buffer Construct Success...");

    SLIME_LOG_INFO("Construct the pre-submit queue");

    for (size_t i = 0; i < MAX_QUEUE_SIZE; i += 1) {
        addPreQueueElement(OpCode::SEND);
        addPreQueueElement(OpCode::RECV);
    }

    meta_buffer_manager_ = std::make_unique<RingBufferReadyManager>(MAX_META_BUFFER_SIZE);
    data_buffer_manager_ = std::make_unique<RingBufferReadyManager>(MAX_META_BUFFER_SIZE);
}

void RDMAEndpoint::connect(JSON& data_ctx_info, JSON& meta_ctx_info)
{

    data_ctx_->connect(data_ctx_info);
    meta_ctx_->connect(meta_ctx_info);

    data_ctx_->launch_future();
    meta_ctx_->launch_future();
}

void RDMAEndpoint::startAllThreads()
{
    SLIME_LOG_INFO("Starting all RDMA endpoint threads...");

    stop_SendMetaQueueThread_.store(false, std::memory_order_release);
    stop_dataRecvQueueThread_.store(false, std::memory_order_release);
    stop_SendBufferQueueThread_.store(false, std::memory_order_release);
    stop_WaitSendFinishQueueThread_.store(false, std::memory_order_release);
    stop_WaitRecvFinishQueueThread_.store(false, std::memory_order_release);

    // 时间根据需要再调整
    meta_recv_thread_ = std::thread([this]() { this->metaRecvQueueThread(std::chrono::milliseconds(1)); });

    data_recv_thread_ = std::thread([this]() { this->dataRecvQueueThread(std::chrono::milliseconds(1)); });

    send_buffer_thread_ = std::thread([this]() { this->SendBufferQueueThread(std::chrono::milliseconds(1)); });

    send_finish_thread_ = std::thread([this]() { this->WaitSendFinishQueueThread(std::chrono::milliseconds(1)); });

    recv_finish_thread_ = std::thread([this]() { this->WaitRecvFinishQueueThread(std::chrono::milliseconds(1)); });

    SLIME_LOG_INFO("All 5 RDMA endpoint threads started successfully");
}

void RDMAEndpoint::stopAllThreads()
{
    SLIME_LOG_INFO("Stopping all RDMA endpoint threads...");

    stop_SendMetaQueueThread_.store(true, std::memory_order_release);
    stop_dataRecvQueueThread_.store(true, std::memory_order_release);
    stop_SendBufferQueueThread_.store(true, std::memory_order_release);
    stop_WaitSendFinishQueueThread_.store(true, std::memory_order_release);
    stop_WaitRecvFinishQueueThread_.store(true, std::memory_order_release);

    meta_recv_queue_.notifyAll();
    data_recv_queue_.notifyAll();
    send_buffer_queue_.notifyAll();
    send_finish_queue_.notifyAll();
    recv_finish_queue_.notifyAll();

    if (meta_recv_thread_.joinable()) {
        meta_recv_thread_.join();
        SLIME_LOG_DEBUG("Meta recv thread stopped");
    }

    if (data_recv_thread_.joinable()) {
        data_recv_thread_.join();
        SLIME_LOG_DEBUG("Data recv thread stopped");
    }

    if (send_buffer_thread_.joinable()) {
        send_buffer_thread_.join();
        SLIME_LOG_DEBUG("Send buffer thread stopped");
    }

    if (send_finish_thread_.joinable()) {
        send_finish_thread_.join();
        SLIME_LOG_DEBUG("Send finish thread stopped");
    }

    if (recv_finish_thread_.joinable()) {
        recv_finish_thread_.join();
        SLIME_LOG_DEBUG("Recv finish thread stopped");
    }

    SLIME_LOG_INFO("All RDMA endpoint threads stopped successfully");
}

RDMAEndpoint::~RDMAEndpoint()
{
    try {
        stopAllThreads();
        SLIME_LOG_INFO("RDMAEndpoint destroyed successfully");
    }
    catch (const std::exception& e) {
        SLIME_LOG_ERROR("Exception in RDMAEndpoint destructor: ", e.what());
    }
}

void RDMAEndpoint::addPreQueueElement(OpCode rdma_opcode)
{
    if (rdma_opcode == OpCode::SEND) {

        RDMAPrePostQueueElement meta_recv_queue_element = RDMAPrePostQueueElement(
            unique_meta_recv_id_.fetch_add(1, std::memory_order_relaxed), OpCode::SEND, shared_from_this());

        meta_ctx_->submit(OpCode::RECV,
                          meta_recv_queue_element.assignment_batch_,
                          meta_recv_queue_element.callback_,
                          RDMAContext::UNDEFINED_QPI,
                          meta_recv_queue_element.unique_task_id_);

        meta_recv_queue_.enqueue(std::move(meta_recv_queue_element));
    }
    else if (rdma_opcode == OpCode::RECV) {

        RDMAPrePostQueueElement data_recv_queue_element = RDMAPrePostQueueElement(
            unique_data_recv_id_.fetch_add(1, std::memory_order_relaxed), OpCode::RECV, shared_from_this());

        data_ctx_->submit(OpCode::RECV,
                          data_recv_queue_element.assignment_batch_,
                          data_recv_queue_element.callback_,
                          RDMAContext::UNDEFINED_QPI,
                          data_recv_queue_element.unique_task_id_);

        data_recv_queue_.enqueue(std::move(data_recv_queue_element));
    }
    else {
        SLIME_LOG_ERROR("Undefined OPCODE IN RDMAEndpoint::addPreQueueElement");
    }
}

void RDMAEndpoint::addRDMABuffer(OpCode rdma_opcode, std::shared_ptr<RDMABuffer> rdma_buffer)
{
    if (rdma_opcode == OpCode::SEND) {
        RDMABufferQueueElement buffer_element = RDMABufferQueueElement(
            unique_SEND_SLOT_ID_.fetch_add(1, std::memory_order_relaxed), OpCode::SEND, rdma_buffer);

        send_buffer_queue_.enqueue(std::move(buffer_element));
    }
    else if (rdma_opcode == OpCode::RECV) {
        postMetaWrite(rdma_buffer);

        RDMABufferQueueElement buffer_element = RDMABufferQueueElement(
            unique_RECV_SLOT_ID_.fetch_add(1, std::memory_order_relaxed), OpCode::RECV, rdma_buffer);

        recv_finish_queue_.enqueue(std::move(buffer_element));
    }
    else {
        SLIME_LOG_ERROR("Undefined OPCODE IN RDMAEndpoint::addRDMABuffer");
    }
}

void RDMAEndpoint::postMetaWrite(std::shared_ptr<RDMABuffer> rdma_buffer)
{

    std::string prefix = "DATA_RECV_";
    std::string MR_KEY = prefix + std::to_string(unique_RECV_SLOT_ID_) + "_";
    auto        mr     = data_ctx_->get_mr(MR_KEY + std::to_string(0));

    if (mr != nullptr) {
        SLIME_LOG_DEBUG("The DATA MR has been  REGISTERED! The SLOT_ID is: ", unique_RECV_SLOT_ID_);
    }
    else {
        auto view_batch = rdma_buffer->view_batch();
        for (size_t i = 0; i < rdma_buffer->batch_size(); ++i) {
            data_ctx_->register_memory_region(MR_KEY + std::to_string(i), view_batch[i].data_ptr, view_batch[i].length);
        }
    }

    for (size_t i = 0; i < rdma_buffer->batch_size(); i += 1) {
        auto mr = data_ctx_->get_mr(MR_KEY + std::to_string(i));
        meta_buffer_[MAX_META_BUFFER_SIZE + unique_RECV_SLOT_ID_ % MAX_META_BUFFER_SIZE].mr_addr[i] =
            reinterpret_cast<uint64_t>(mr->addr);
        meta_buffer_[MAX_META_BUFFER_SIZE + unique_RECV_SLOT_ID_ % MAX_META_BUFFER_SIZE].mr_rkey[i] = mr->rkey;
        meta_buffer_[MAX_META_BUFFER_SIZE + unique_RECV_SLOT_ID_ % MAX_META_BUFFER_SIZE].mr_size[i] = mr->length;
        meta_buffer_[MAX_META_BUFFER_SIZE + unique_RECV_SLOT_ID_ % MAX_META_BUFFER_SIZE].mr_slot = unique_RECV_SLOT_ID_;
    }

    size_t          meta_buffer_idx = unique_RECV_SLOT_ID_ % MAX_META_BUFFER_SIZE;
    AssignmentBatch meta_assignment_batch_ =
        AssignmentBatch{Assignment("meta_buffer",
                                   meta_buffer_idx * sizeof(meta_data_t),
                                   MAX_META_BUFFER_SIZE * sizeof(meta_data_t) + meta_buffer_idx * sizeof(meta_data_t),
                                   sizeof(meta_data_t))};

    auto meta_callback = [this](int status, int slot_id) mutable { std::cout << "META WRITE SUCCESS"; };
    meta_ctx_->submit(OpCode::WRITE_WITH_IMM,
                      meta_assignment_batch_,
                      meta_callback,
                      RDMAContext::UNDEFINED_QPI,
                      unique_RECV_SLOT_ID_);
}

void RDMAEndpoint::metaRecvQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("metaRecvQueueThread started");

    while (!stop_SendMetaQueueThread_.load(std::memory_order_acquire)) {

        RDMAPrePostQueueElement element;

        bool found = meta_recv_queue_.peekQueue(element, [](const RDMAPrePostQueueElement& elem) -> bool {
            return elem.is_finished_.load(std::memory_order_acquire);
        });

        if (found) {
            if (meta_buffer_manager_->setSlotTrueWithWait(element.unique_task_id_)) {
                SLIME_LOG_DEBUG("Set slot {} to true for task {}",
                                element.unique_task_id_ % MAX_META_BUFFER_SIZE,
                                element.unique_task_id_);
            }
            else {
                SLIME_LOG_WARN("Failed to set slot for task {} - timeout", element.unique_task_id_);
            }
        }
        else {
            if (timeout.count() > 0) {
                std::this_thread::sleep_for(timeout);
            }
            else {
                std::this_thread::yield();
            }
        }
    }

    SLIME_LOG_INFO("metaRecvQueueThread stopped");
}

void RDMAEndpoint::dataRecvQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("RecvDataQueueThread started");
    while (!stop_dataRecvQueueThread_.load(std::memory_order_acquire)) {

        RDMAPrePostQueueElement element;

        bool found = data_recv_queue_.peekQueue(element, [](const RDMAPrePostQueueElement& elem) -> bool {
            return elem.is_finished_.load(std::memory_order_acquire);
        });

        if (found) {

            if (data_buffer_manager_->setSlotTrueWithWait(element.unique_task_id_)) {
                SLIME_LOG_DEBUG("Set slot {} to true for task {}",
                                element.unique_task_id_ % MAX_META_BUFFER_SIZE,
                                element.unique_task_id_);
            }
            else {
                SLIME_LOG_WARN("Failed to set slot for task {} - timeout", element.unique_task_id_);
            }
        }
        else {
            if (timeout.count() > 0) {
                std::this_thread::sleep_for(timeout);
            }
            else {
                std::this_thread::yield();
            }
        }
    }
    SLIME_LOG_INFO("RecvDataQueueThread stopped");
}

void RDMAEndpoint::SendBufferQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("SendBufferQueueThread started");

    while (!stop_SendBufferQueueThread_.load(std::memory_order_acquire)) {
        RDMABufferQueueElement element;

        bool has_element = send_buffer_queue_.fetchQueue(element);

        if (has_element) {

            bool is_ready = meta_buffer_manager_->readSlot(element.unique_slot_id_);

            if (is_ready) {

                postDataWrite(element, element.rdma_buffer_);
                send_finish_queue_.enqueue(std::move(element));
                SLIME_LOG_DEBUG("Processing send buffer element {}, meta buffer is ready", element.unique_slot_id_);
            }
            else {

                SLIME_LOG_DEBUG("Meta buffer not ready for element {}, re-enqueueing", element.unique_slot_id_);

                send_buffer_queue_.enqueue(std::move(element));

                if (timeout.count() > 0) {
                    std::this_thread::sleep_for(timeout);
                }
                else {
                    std::this_thread::yield();
                }
            }
        }
        else {
            if (timeout.count() > 0) {
                std::this_thread::sleep_for(timeout);
            }
            else {
                std::this_thread::yield();
            }
        }
    }

    SLIME_LOG_INFO("SendBufferQueueThread stopped");
}

void RDMAEndpoint::postDataWrite(RDMABufferQueueElement& element, std::shared_ptr<RDMABuffer> rdma_buffer)
{
    std::string prefix = "DATA_SEND_";
    std::string MR_KEY = prefix + std::to_string(unique_SEND_SLOT_ID_) + "_";
    auto        mr     = data_ctx_->get_mr(MR_KEY + std::to_string(0));
    if (mr != nullptr) {
        SLIME_LOG_DEBUG("The DATA MR has been  REGISTERED! The SLOT_ID is: ", unique_SEND_SLOT_ID_);
    }
    else {
        auto view_batch = rdma_buffer->view_batch();
        for (size_t i = 0; i < rdma_buffer->batch_size(); ++i) {
            std::cout << view_batch[i].data_ptr << std::endl;
            data_ctx_->register_memory_region(MR_KEY + std::to_string(i), view_batch[i].data_ptr, view_batch[i].length);
        }
    }

    auto mr_remote = data_ctx_->get_remote_mr(MR_KEY + std::to_string(0));
    if (mr_remote.addr == 0) {

        for (size_t i = 0; i < rdma_buffer->batch_size(); ++i) {
            uint64_t addr = meta_buffer_[unique_SEND_SLOT_ID_ % MAX_META_BUFFER_SIZE].mr_addr[i];
            uint32_t size = meta_buffer_[unique_SEND_SLOT_ID_ % MAX_META_BUFFER_SIZE].mr_size[i];
            uint32_t rkey = meta_buffer_[unique_SEND_SLOT_ID_ % MAX_META_BUFFER_SIZE].mr_rkey[i];
            data_ctx_->register_remote_memory_region(MR_KEY + std::to_string(i), addr, size, rkey);
        }
    }

    else {
        SLIME_LOG_DEBUG("The REMOTE DATA MR has been REGISTERED! The SLOT_ID is: ", unique_SEND_SLOT_ID_);
    }

    AssignmentBatch batch;

    for (size_t i = 0; i < rdma_buffer->batch_size(); ++i) {
        batch.push_back(Assignment(MR_KEY + std::to_string(i), 0, 0, rdma_buffer->view_batch()[i].length));
    }

    auto data_atx = this->data_ctx_->submit(
        OpCode::WRITE_WITH_IMM, batch, element.callback_, RDMAContext::UNDEFINED_QPI, unique_SEND_SLOT_ID_);
}

void RDMAEndpoint::WaitSendFinishQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("WaitSendFinishQueueThread started");

    while (!stop_WaitSendFinishQueueThread_.load(std::memory_order_acquire)) {
        bool processed_any = false;

        while (true) {
            RDMABufferQueueElement element;

            bool found = send_finish_queue_.peekQueue(element, [](const RDMABufferQueueElement& elem) -> bool {
                return elem.is_finished_.load(std::memory_order_acquire);
            });

            if (found) {
                SLIME_LOG_DEBUG("Processing completed send element {}", element.unique_slot_id_);

                processed_any = true;
            }
            else {
                break;
            }
        }

        if (!processed_any) {
            if (timeout.count() > 0) {
                std::this_thread::sleep_for(timeout);
            }
            else {
                std::this_thread::yield();
            }
        }
    }

    SLIME_LOG_INFO("WaitSendFinishQueueThread stopped");
}

void RDMAEndpoint::WaitRecvFinishQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("WaitRecvFinishQueueThread started");

    while (!stop_WaitRecvFinishQueueThread_.load(std::memory_order_acquire)) {
        bool processed_any = false;

        while (true) {
            RDMABufferQueueElement element;

            bool found = recv_finish_queue_.peekQueue(element, [](const RDMABufferQueueElement& elem) -> bool {
                return elem.is_finished_.load(std::memory_order_acquire);
            });

            if (found) {

                SLIME_LOG_DEBUG("Processing completed recv element {}", element.unique_slot_id_);
                processed_any = true;
            }
            else {

                break;
            }
        }
        if (!processed_any) {
            if (timeout.count() > 0) {
                std::this_thread::sleep_for(timeout);
            }
            else {
                std::this_thread::yield();
            }
        }
    }

    SLIME_LOG_INFO("WaitRecvFinishQueueThread stopped");
}

// int RDMASendRecvTask::makeRemoteDataMR()
// {
//     if (!rdma_buffer_ || !rdma_endpoint_) {
//         SLIME_LOG_ERROR("Null pointer detected: rdma_buffer_=" << rdma_buffer_
//                                                                << ", rdma_endpoint_=" << rdma_endpoint_);
//         return -1;
//     }

//     std::string prefix = (rdma_operation_ == OpCode::SEND) ? "DATA_SEND_" : "DATA_RECV_";
//     std::string MR_KEY = prefix + std::to_string(unique_slot_id_) + "_";
//     auto        mr     = rdma_endpoint_->dataCtx()->get_remote_mr(MR_KEY + std::to_string(0));

//     if (mr.addr == 0) {
//         auto& meta_buffer = rdma_endpoint_->metaBuffer();
//         for (size_t i = 0; i < rdma_buffer_->batch_size(); ++i) {
//             uint64_t addr = meta_buffer[unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_addr[i];
//             uint32_t size = meta_buffer[unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_size[i];
//             uint32_t rkey = meta_buffer[unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_rkey[i];
//             rdma_endpoint_->dataCtx()->register_remote_memory_region(MR_KEY + std::to_string(i), addr, size,
//             rkey);
//         }
//     }

//     else {
//         SLIME_LOG_DEBUG("The REMOTE DATA MR has been REGISTERED! The SLOT_ID is: ", unique_slot_id_);
//     }

//     return 0;
// }

// RDMAEndpoint::~RDMAEndpoint()
// {
//     {
//         std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
//         RDMA_tasks_threads_running_ = false;
//     }

//     rdma_tasks_cv_.notify_all();

//     if (rdma_tasks_threads_.joinable())
//         rdma_tasks_threads_.join();
// }

// void RDMAEndpoint::connect(const json& data_ctx_info, const json& meta_ctx_info)
// {
//     std::cout << "DDD" << std::endl;
//     data_ctx_->connect(data_ctx_info);
//     meta_ctx_->connect(meta_ctx_info);
//     std::cout << "DDDDDD" << std::endl;
//     data_ctx_->launch_future();
//     meta_ctx_->launch_future();
//     std::cout << "DDDDDDDDDDDD" << std::endl;
//     RDMA_tasks_threads_running_ = true;
//     rdma_tasks_threads_         = std::thread([this] { this->mainQueueThread(std::chrono::milliseconds(100)); });
//     std::cout << "DDDDDDDDDDDDDDDDDD" << std::endl;
//     rdma_task_pool_ = std::make_shared<RDMASendRecvTaskPool>(shared_from_this());
//     std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
// }

// RDMASendRecvTask::RDMASendRecvTask(std::shared_ptr<RDMAEndpoint> rdma_endpoint, uint32_t task_id)
// {
//     rdma_endpoint_ = rdma_endpoint;
//     task_id_       = task_id;
//     makeDummyAssignmentBatch();
// }

// RDMASendRecvTask::RDMASendRecvTask(std::shared_ptr<RDMAEndpoint> rdma_endpoint, OpCode rmda_opcode, uint32_t
// task_id):
//     rdma_endpoint_(rdma_endpoint), rdma_operation_(rmda_opcode), task_id_(task_id)
// {
//     unique_slot_id_ = -1;
//     rdma_buffer_    = nullptr;
//     makeDummyAssignmentBatch();
// }

// RDMASendRecvTask::~RDMASendRecvTask()
// {
//     SLIME_LOG_INFO("Destroy the RDMASendRecvTask: ", task_id_);
// }

// // 生成meta Assignment 和 data Assignment
// int RDMASendRecvTask::makeAssignmentBatch()
// {
//     size_t meta_buffer_idx = unique_slot_id_ % MAX_META_BUFFER_SIZE;
//     if (rdma_operation_ == OpCode::SEND) {
//         AssignmentBatch batch;
//         std::string     prefix = "DATA_SEND_";
//         std::cout << "rdma_buffer_->batch_size()" << rdma_buffer_->batch_size() << std::endl;
//         for (size_t i = 0; i < rdma_buffer_->batch_size(); ++i) {
//             batch.push_back(Assignment(prefix + std::to_string(unique_slot_id_) + "_" + std::to_string(i),
//                                        0,
//                                        0,
//                                        rdma_buffer_->view_batch()[i].length));
//         }
//         data_assignment_batch_ = batch;
//     }
//     if (rdma_operation_ == OpCode::RECV) {
//         meta_assignment_batch_ = AssignmentBatch{
//             Assignment("meta_buffer",
//                        meta_buffer_idx * sizeof(meta_data_t),
//                        MAX_META_BUFFER_SIZE * sizeof(meta_data_t) + meta_buffer_idx * sizeof(meta_data_t),
//                        sizeof(meta_data_t))};
//     }
//     return 0;
// }

// int RDMASendRecvTask::makeDummyAssignmentBatch()
// {

//     dum_meta_assignment_batch_ = AssignmentBatch{Assignment("dum_meta_buffer_", 0, 0, 16 * sizeof(uint32_t))};
//     dum_data_assignment_batch_ = AssignmentBatch{Assignment("dum_data_buffer_", 0, 0, 16 * sizeof(uint32_t))};

//     return 0;
// }

// int RDMASendRecvTask::makeMetaMR()
// {
//     auto& meta_buffer = rdma_endpoint_->metaBuffer();

//     std::string prefix = (rdma_operation_ == OpCode::SEND) ? "DATA_SEND_" : "DATA_RECV_";
//     for (size_t i = 0; i < rdma_buffer_->batch_size(); ++i) {
//         auto mr = rdma_endpoint_->dataCtx()->get_mr(prefix + std::to_string(unique_slot_id_) + "_" +
//         std::to_string(i)); if (rdma_operation_ == OpCode::RECV) {
//             meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_addr[i] =
//                 reinterpret_cast<uint64_t>(mr->addr);
//             meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_rkey[i] = mr->rkey;
//             meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_size[i] = mr->length;
//             meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_slot    = unique_slot_id_;
//         }
//     }

//     return 0;
// }

// void RDMAEndpoint::mainQueueThread(std::chrono::milliseconds timeout)
// {
//     while (RDMA_tasks_threads_running_) {
//         std::shared_ptr<RDMASendRecvTask> task;

//         {
//             std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);

//             bool has_task = rdma_tasks_cv_.wait_for(
//                 lock, timeout, [this] { return !rdma_tasks_queue_.empty() || !RDMA_tasks_threads_running_; });

//             if (!RDMA_tasks_threads_running_)
//                 break;
//             if (!has_task)
//                 continue;

//             task = std::move(rdma_tasks_queue_.front());
//             rdma_tasks_queue_.pop();
//         }

//         if (task) {
//             switch (task->rdma_operation_) {
//                 case OpCode::SEND:
//                     asyncSendData(task);
//                     break;
//                 case OpCode::RECV:
//                     asyncRecvData(task);
//                     break;
//                 default:
//                     SLIME_LOG_ERROR("Unknown OpCode in WaitandPopTask");
//                     break;
//             }
//         }
//     }
// }

// void RDMAEndpoint::asyncSendData(std::shared_ptr<RDMASendRecvTask> task)
// {
//     std::cout << "The send task has been pre post" << std::endl;
// }

// void RDMAEndpoint::asyncRecvData(std::shared_ptr<RDMASendRecvTask> task)
// {
//     auto data_callback = [this, task](int status, int slot_id) mutable {
//         this->rdma_task_pool_->releaseSendRecvTask(task);
//         task->rdma_buffer_->recv_done_callback();
//     };

//     auto meta_callback = [this, task](int status, int slot_id) mutable {
//         std::cout << "The recv task has been pre post" << std::endl;
//     };

//     {
//         auto data_atx = this->data_ctx_->submit(OpCode::RECV,
//                                                 task->dum_data_assignment_batch_,
//                                                 data_callback,
//                                                 RDMAContext::UNDEFINED_QPI,
//                                                 task->unique_slot_id_);

//         AssignmentBatch meta_assignment_batch = task->meta_assignment_batch_;
//         meta_ctx_->submit(OpCode::WRITE_WITH_IMM,
//                           meta_assignment_batch,
//                           meta_callback,
//                           RDMAContext::UNDEFINED_QPI,
//                           task->unique_slot_id_);
//     }
// }

// }
// void RDMATask::fillBuffer()
// {
//     if (opcode_ == OpCode::SEND) {
//         std::vector<meta_data_t>& meta_buf = endpoint_->getMetaBuffer();
//     }
//     else if (opcode_ == OpCode::RECV) {
//         std::vector<meta_data_t>& meta_buf = endpoint_->getMetaBuffer();
//         for (size_t i = 0; i < buffer_->batchSize(); ++i) {
//             auto mr = endpoint_->dataCtx()->get_mr(getDataKey(i));
//             meta_buf[MAX_META_BUFFER_SIZE + slot_id_ % MAX_META_BUFFER_SIZE].mr_addr[i] =
//                 reinterpret_cast<uint64_t>(mr->addr);
//             meta_buf[MAX_META_BUFFER_SIZE + slot_id_ % MAX_META_BUFFER_SIZE].mr_rkey[i] = mr->rkey;
//             meta_buf[MAX_META_BUFFER_SIZE + slot_id_ % MAX_META_BUFFER_SIZE].mr_size[i] = mr->length;
//         }
//         meta_buf[MAX_META_BUFFER_SIZE + slot_id_ % MAX_META_BUFFER_SIZE].mr_slot = slot_id_;
//         meta_buf[MAX_META_BUFFER_SIZE + slot_id_ % MAX_META_BUFFER_SIZE].mr_qpidx =
//             slot_id_ % endpoint_->dataCtxQPNum();
//     }
//     else {
//         SLIME_LOG_ERROR("Unsupported opcode in RDMATask::fillBuffer()");
//     }
// }

// RDMAEndpoint::RDMAEndpoint(const std::string& data_dev_name,
//                            const std::string& meta_dev_name,
//                            uint8_t            ib_port,
//                            const std::string& link_type,
//                            size_t             qp_num)
// {
//     SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");
//     data_ctx_ = std::make_shared<RDMAContext>(qp_num, 0);
//     meta_ctx_ = std::make_shared<RDMAContext>(1, 0);

//     data_ctx_->init(data_dev_name, ib_port, link_type);
//     meta_ctx_->init(meta_dev_name, ib_port, link_type);

//     data_ctx_qp_num_ = data_ctx_->qp_list_len_;
//     meta_ctx_qp_num_ = meta_ctx_->qp_list_len_;
//     SLIME_LOG_INFO("The QP number of data plane is: ", data_ctx_qp_num_);
//     SLIME_LOG_INFO("The QP number of control plane is: ", meta_ctx_qp_num_);
//     SLIME_LOG_INFO("RDMA Endpoint Init Success and Launch the RDMA Endpoint Task Threads...");

//     const size_t max_meta_buffer_size = MAX_META_BUFFER_SIZE * 2;
//     meta_buffer_.reserve(max_meta_buffer_size);
//     memset(meta_buffer_.data(), 0, meta_buffer_.size() * sizeof(meta_data_t));
//     meta_ctx_->register_memory_region(
//         "meta_buffer", reinterpret_cast<uintptr_t>(meta_buffer_.data()), sizeof(meta_data_t) *
//         max_meta_buffer_size);
// }

// RDMAEndpoint::~RDMAEndpoint()
// {
//     {
//         std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
//         RDMA_tasks_threads_running_ = false;
//     }

//     rdma_tasks_cv_.notify_all();

//     if (rdma_tasks_threads_.joinable())
//         rdma_tasks_threads_.join();
// }

}  // namespace slime
