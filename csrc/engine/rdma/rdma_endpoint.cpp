#include "engine/rdma/rdma_endpoint.h"
#include "engine/assignment.h"
#include "engine/rdma/rdma_assignment.h"
#include "engine/rdma/rdma_buffer.h"
#include "engine/rdma/rdma_common.h"
#include "engine/rdma/rdma_context.h"

#include "logging.h"

#include <atomic>
#include <mutex>
#include <stdexcept>
#include <string>

namespace slime {

RDMASendRecvTask::RDMASendRecvTask(std::shared_ptr<RDMAEndpoint> rdma_endpoint, uint32_t task_id)
{
    rdma_endpoint_ = rdma_endpoint;
    task_id_       = task_id;
    makeDummyAssignmentBatch();
}

RDMASendRecvTask::RDMASendRecvTask(std::shared_ptr<RDMAEndpoint> rdma_endpoint, OpCode rmda_opcode, uint32_t task_id):
    rdma_endpoint_(rdma_endpoint), rdma_operation_(rmda_opcode), task_id_(task_id)
{
    unique_slot_id_ = -1;
    rdma_buffer_    = nullptr;
    makeDummyAssignmentBatch();
}

RDMASendRecvTask::~RDMASendRecvTask()
{
    SLIME_LOG_INFO("Destroy the RDMASendRecvTask: ", task_id_);
}

// 生成meta Assignment 和 data Assignment
int RDMASendRecvTask::makeAssignmentBatch()
{
    size_t meta_buffer_idx = unique_slot_id_ % MAX_META_BUFFER_SIZE;
    if (rdma_operation_ == OpCode::SEND) {
        AssignmentBatch batch;
        std::string     prefix = "DATA_SEND_";
        std::cout << "rdma_buffer_->batch_size()" << rdma_buffer_->batch_size() << std::endl;
        for (size_t i = 0; i < rdma_buffer_->batch_size(); ++i) {
            batch.push_back(Assignment(prefix + std::to_string(unique_slot_id_) + "_" + std::to_string(i),
                                       0,
                                       0,
                                       rdma_buffer_->view_batch()[i].length));
        }
        data_assignment_batch_ = batch;
    }
    if (rdma_operation_ == OpCode::RECV) {
        meta_assignment_batch_ = AssignmentBatch{
            Assignment("meta_buffer",
                       meta_buffer_idx * sizeof(meta_data_t),
                       MAX_META_BUFFER_SIZE * sizeof(meta_data_t) + meta_buffer_idx * sizeof(meta_data_t),
                       sizeof(meta_data_t))};
    }
    return 0;
}

int RDMASendRecvTask::makeDummyAssignmentBatch()
{

    dum_meta_assignment_batch_ = AssignmentBatch{Assignment("dum_meta_buffer_", 0, 0, 16 * sizeof(uint32_t))};
    dum_data_assignment_batch_ = AssignmentBatch{Assignment("dum_data_buffer_", 0, 0, 16 * sizeof(uint32_t))};

    return 0;
}

int RDMASendRecvTask::makeMetaMR()
{
    auto& meta_buffer = rdma_endpoint_->metaBuffer();

    std::string prefix = (rdma_operation_ == OpCode::SEND) ? "DATA_SEND_" : "DATA_RECV_";
    for (size_t i = 0; i < rdma_buffer_->batch_size(); ++i) {
        auto mr = rdma_endpoint_->dataCtx()->get_mr(prefix + std::to_string(unique_slot_id_) + "_" + std::to_string(i));
        if (rdma_operation_ == OpCode::RECV) {
            meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_addr[i] =
                reinterpret_cast<uint64_t>(mr->addr);
            meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_rkey[i] = mr->rkey;
            meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_size[i] = mr->length;
            meta_buffer[MAX_META_BUFFER_SIZE + unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_slot    = unique_slot_id_;
        }
    }

    return 0;
}

int RDMASendRecvTask::makeDataMR()
{

    std::string prefix = (rdma_operation_ == OpCode::SEND) ? "DATA_SEND_" : "DATA_RECV_";
    std::string MR_KEY = prefix + std::to_string(unique_slot_id_) + "_";
    auto        mr     = rdma_endpoint_->dataCtx()->get_mr(MR_KEY + std::to_string(0));
    if (mr != nullptr) {
        SLIME_LOG_DEBUG("The DATA MR has been  REGISTERED! The SLOT_ID is: ", unique_slot_id_);
    }
    else {
        auto view_batch = rdma_buffer_->view_batch();
        for (size_t i = 0; i < rdma_buffer_->batch_size(); ++i) {
            std::cout << view_batch[i].data_ptr << std::endl;
            rdma_endpoint_->dataCtx()->register_memory_region(
                MR_KEY + std::to_string(i), view_batch[i].data_ptr, view_batch[i].length);
        }
    }

    return 0;
}
int RDMASendRecvTask::makeRemoteDataMR()
{
    if (!rdma_buffer_ || !rdma_endpoint_) {
        SLIME_LOG_ERROR("Null pointer detected: rdma_buffer_=" << rdma_buffer_
                                                               << ", rdma_endpoint_=" << rdma_endpoint_);
        return -1;
    }

    std::string prefix = (rdma_operation_ == OpCode::SEND) ? "DATA_SEND_" : "DATA_RECV_";
    std::string MR_KEY = prefix + std::to_string(unique_slot_id_) + "_";
    auto        mr     = rdma_endpoint_->dataCtx()->get_remote_mr(MR_KEY + std::to_string(0));

    if (mr.addr == 0) {
        auto& meta_buffer = rdma_endpoint_->metaBuffer();
        for (size_t i = 0; i < rdma_buffer_->batch_size(); ++i) {
            uint64_t addr = meta_buffer[unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_addr[i];
            uint32_t size = meta_buffer[unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_size[i];
            uint32_t rkey = meta_buffer[unique_slot_id_ % MAX_META_BUFFER_SIZE].mr_rkey[i];
            rdma_endpoint_->dataCtx()->register_remote_memory_region(MR_KEY + std::to_string(i), addr, size, rkey);
        }
    }

    else {
        SLIME_LOG_DEBUG("The REMOTE DATA MR has been REGISTERED! The SLOT_ID is: ", unique_slot_id_);
    }

    return 0;
}

void RDMASendRecvTask::configurationTask(std::shared_ptr<RDMABuffer> rdma_buffer,
                                         uint32_t                    unique_slot_id,
                                         OpCode                      rmda_opcode)
{
    rdma_buffer_    = rdma_buffer;
    unique_slot_id_ = unique_slot_id;
    rdma_operation_ = rmda_opcode;
    makeAssignmentBatch();
    makeDataMR();
    makeMetaMR();
}

RDMASendRecvTaskPool::RDMASendRecvTaskPool(std::shared_ptr<RDMAEndpoint> rdma_endpoint)
{
    rdma_endpoint_ = rdma_endpoint;
    pool_size_     = 128;

    for (size_t i = 0; i < pool_size_; ++i) {
        auto task = std::make_shared<RDMASendRecvTask>(rdma_endpoint_, i);
        rdma_send_task_pool_.push_back(task);
        // rdma_endpoint_->postSendTask(task);
        rdma_send_task_in_use_.push_back(false);
    }

    for (size_t i = 0; i < pool_size_; ++i) {
        rdma_recv_task_pool_.push_back(std::make_shared<RDMASendRecvTask>(rdma_endpoint_, i));
        rdma_recv_task_in_use_.push_back(false);
    }
}

RDMASendRecvTaskPool::~RDMASendRecvTaskPool()
{
    SLIME_LOG_INFO("Destroy the RDMASendRecvTaskPool");
}

std::shared_ptr<RDMASendRecvTask> RDMASendRecvTaskPool::fetchSendRecvTask(std::shared_ptr<RDMABuffer> rdma_buffer,
                                                                          uint32_t                    unique_slot_id,
                                                                          OpCode                      rdma_operation)
{
    if (rdma_operation == OpCode::SEND) {

        std::unique_lock<std::mutex> lock(pool_mutex_);

        task_available_cv_.wait(lock, [this]() {
            return std::find(this->rdma_send_task_in_use_.begin(), this->rdma_send_task_in_use_.end(), false)
                   != this->rdma_send_task_in_use_.end();
        });

        for (size_t i = 0; i < rdma_send_task_in_use_.size(); ++i) {
            if (!rdma_send_task_in_use_[i]) {
                rdma_send_task_in_use_[i] = true;
                auto task                 = rdma_send_task_pool_[i];
                task->configurationTask(rdma_buffer, unique_slot_id, rdma_operation);
                rdma_endpoint_->postSendTask(task);
                return task;
            }
        }
    }
    else if (rdma_operation == OpCode::RECV) {

        std::unique_lock<std::mutex> lock(pool_mutex_);

        task_available_cv_.wait(lock, [this]() {
            return std::find(this->rdma_recv_task_in_use_.begin(), this->rdma_recv_task_in_use_.end(), false)
                   != this->rdma_recv_task_in_use_.end();
        });

        for (size_t i = 0; i < rdma_recv_task_in_use_.size(); ++i) {
            if (!rdma_recv_task_in_use_[i]) {
                rdma_recv_task_in_use_[i] = true;
                auto task                 = rdma_recv_task_pool_[i];
                task->configurationTask(rdma_buffer, unique_slot_id, rdma_operation);
                return task;
            }
        }
    }
    else {
        SLIME_LOG_ERROR("Unsupported RDMA operation in fetchSendRecvTask!");
        return nullptr;
    }

    return nullptr;
}

int RDMASendRecvTaskPool::releaseSendRecvTask(std::shared_ptr<RDMASendRecvTask> rdma_task)
{

    if (rdma_task->rdma_operation_ == OpCode::SEND) {
        std::unique_lock<std::mutex> lock(pool_mutex_);
        rdma_send_task_in_use_[rdma_task->task_id_] = false;
        rdma_endpoint_->postSendTask(rdma_task);
        task_available_cv_.notify_one();
        return 0;
    }
    else if (rdma_task->rdma_operation_ == OpCode::RECV) {
        std::unique_lock<std::mutex> lock(pool_mutex_);
        rdma_recv_task_in_use_[rdma_task->task_id_] = false;
        task_available_cv_.notify_one();
        return 0;
    }
    else {
        SLIME_LOG_ERROR("Unsupported RDMA operation in releaseSendRecvTask!");
        return -1;
    }
}

RDMAEndpoint::RDMAEndpoint(const std::string& dev_name, uint8_t ib_port, const std::string& link_type, size_t qp_num)
{
    SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");

    data_ctx_ = std::make_shared<RDMAContext>(qp_num, 0);
    meta_ctx_ = std::make_shared<RDMAContext>(1, 0);

    data_ctx_->init(dev_name, ib_port, link_type);
    meta_ctx_->init(dev_name, ib_port, link_type);

    data_ctx_qp_num_ = data_ctx_->qp_list_len_;
    meta_ctx_qp_num_ = meta_ctx_->qp_list_len_;
    SLIME_LOG_INFO("The QP number of data plane is: ", data_ctx_qp_num_);
    SLIME_LOG_INFO("The QP number of control plane is: ", meta_ctx_qp_num_);
    SLIME_LOG_INFO("RDMA Endpoint Init Success and Launch the RDMA Endpoint Task Threads...");

    const size_t max_meta_buffer_size = MAX_META_BUFFER_SIZE * 2;
    meta_buffer_.reserve(max_meta_buffer_size);
    memset(meta_buffer_.data(), 0, meta_buffer_.size() * sizeof(meta_data_t));
    meta_ctx_->register_memory_region(
        "meta_buffer", reinterpret_cast<uintptr_t>(meta_buffer_.data()), sizeof(meta_data_t) * max_meta_buffer_size);

    std::cout << meta_buffer_[0].mr_addr[0] << std::endl;

    dum_meta_buffer_.reserve(16);
    memset(dum_meta_buffer_.data(), 0, dum_meta_buffer_.size() * sizeof(uint32_t));
    meta_ctx_->register_memory_region("dum_meta_buffer_",
                                      reinterpret_cast<uintptr_t>(dum_meta_buffer_.data()),
                                      sizeof(uint32_t) * dum_meta_buffer_.size());

    dum_data_buffer_.reserve(16);
    memset(dum_data_buffer_.data(), 1048, dum_data_buffer_.size() * sizeof(uint32_t));
    data_ctx_->register_memory_region("dum_data_buffer_",
                                      reinterpret_cast<uintptr_t>(dum_data_buffer_.data()),
                                      sizeof(uint32_t) * dum_data_buffer_.size());
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
    std::cout << "DDD" << std::endl;
    data_ctx_->connect(data_ctx_info);
    meta_ctx_->connect(meta_ctx_info);
    std::cout << "DDDDDD" << std::endl;
    data_ctx_->launch_future();
    meta_ctx_->launch_future();
    std::cout << "DDDDDDDDDDDD" << std::endl;
    RDMA_tasks_threads_running_ = true;
    rdma_tasks_threads_         = std::thread([this] { this->mainQueueThread(std::chrono::milliseconds(100)); });
    std::cout << "DDDDDDDDDDDDDDDDDD" << std::endl;
    rdma_task_pool_ = std::make_shared<RDMASendRecvTaskPool>(shared_from_this());
    std::cout << "!!!!!!!!!!!!!!!!!!!!!!!!" << std::endl;
}

void RDMAEndpoint::addSendTask(std::shared_ptr<RDMABuffer> rdma_buffer)
{
    std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    ++unique_SEND_SLOT_ID_;
    auto task = rdma_task_pool_->fetchSendRecvTask(rdma_buffer, unique_SEND_SLOT_ID_, OpCode::SEND);
    send_task_[unique_SEND_SLOT_ID_] = task;
    rdma_tasks_queue_.push(task);
    rdma_tasks_cv_.notify_one();
}

void RDMAEndpoint::addRecvTask(std::shared_ptr<RDMABuffer> rdma_buffer)
{
    std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
    ++unique_RECV_SLOT_ID_;
    auto task = rdma_task_pool_->fetchSendRecvTask(rdma_buffer, unique_RECV_SLOT_ID_, OpCode::RECV);
    recv_task_[unique_RECV_SLOT_ID_] = task;
    rdma_tasks_queue_.push(task);
    rdma_tasks_cv_.notify_one();
}

void RDMAEndpoint::mainQueueThread(std::chrono::milliseconds timeout)
{
    while (RDMA_tasks_threads_running_) {
        std::shared_ptr<RDMASendRecvTask> task;

        {
            std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);

            bool has_task = rdma_tasks_cv_.wait_for(
                lock, timeout, [this] { return !rdma_tasks_queue_.empty() || !RDMA_tasks_threads_running_; });

            if (!RDMA_tasks_threads_running_)
                break;
            if (!has_task)
                continue;

            task = std::move(rdma_tasks_queue_.front());
            rdma_tasks_queue_.pop();
        }

        if (task) {
            switch (task->rdma_operation_) {
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

void RDMAEndpoint::postSendTask(std::shared_ptr<RDMASendRecvTask> task)
{
    auto data_callback = [this, task](int status, int _) mutable {
        this->rdma_task_pool_->releaseSendRecvTask(task);
        task->rdma_buffer_->send_done_callback();
    };

    auto meta_callback = [this, task, data_callback](int status, int slot_id) mutable {
        task->makeRemoteDataMR();
        AssignmentBatch data_assign_batch = task->data_assignment_batch_;
        auto            data_atx          = this->data_ctx_->submit(OpCode::WRITE_WITH_IMM,
                                                data_assign_batch,
                                                data_callback,
                                                RDMAContext::UNDEFINED_QPI,
                                                task->unique_slot_id_);
    };

    {
        AssignmentBatch meta_assignment_batch = task->dum_meta_assignment_batch_;
        meta_ctx_->submit(OpCode::RECV, meta_assignment_batch, meta_callback);
    }
}

void RDMAEndpoint::asyncSendData(std::shared_ptr<RDMASendRecvTask> task)
{
    std::cout << "The send task has been pre post" << std::endl;
}

void RDMAEndpoint::asyncRecvData(std::shared_ptr<RDMASendRecvTask> task)
{
    auto data_callback = [this, task](int status, int slot_id) mutable {
        this->rdma_task_pool_->releaseSendRecvTask(task);
        task->rdma_buffer_->recv_done_callback();
    };

    auto meta_callback = [this, task](int status, int slot_id) mutable {
        std::cout << "The recv task has been pre post" << std::endl;
    };

    {
        auto data_atx = this->data_ctx_->submit(OpCode::RECV,
                                                task->dum_data_assignment_batch_,
                                                data_callback,
                                                RDMAContext::UNDEFINED_QPI,
                                                task->unique_slot_id_);

        AssignmentBatch meta_assignment_batch = task->meta_assignment_batch_;
        meta_ctx_->submit(OpCode::WRITE_WITH_IMM,
                          meta_assignment_batch,
                          meta_callback,
                          RDMAContext::UNDEFINED_QPI,
                          task->unique_slot_id_);
    }
}

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

// RDMAEndpoint::RDMAEndpoint(const std::string& dev_name, uint8_t ib_port, const std::string& link_type, size_t qp_num)
// {
//     SLIME_LOG_INFO("Init the Contexts and RDMA Devices...");

//     data_ctx_ = std::make_shared<RDMAContext>(qp_num, 0);
//     meta_ctx_ = std::make_shared<RDMAContext>(1, 0);

//     data_ctx_->init(dev_name, ib_port, link_type);
//     meta_ctx_->init(dev_name, ib_port, link_type);

//     data_ctx_qp_num_ = data_ctx_->qp_list_len_;
//     meta_ctx_qp_num_ = meta_ctx_->qp_list_len_;
//     SLIME_LOG_INFO("The QP number of data plane is: ", data_ctx_qp_num_);
//     SLIME_LOG_INFO("The QP number of control plane is: ", meta_ctx_qp_num_);
//     SLIME_LOG_INFO("RDMA Endpoint Init Success and Launch the RDMA Endpoint Task Threads...");

//     const size_t max_meta_buffer_size = MAX_META_BUFFER_SIZE * 2;
//     meta_buffer_.reserve(max_meta_buffer_size);
//     memset(meta_buffer_.data(), 0, meta_buffer_.size() * sizeof(meta_data_t));
//     meta_ctx_->register_memory_region(
//         "meta_buffer", reinterpret_cast<uintptr_t>(meta_buffer_.data()), sizeof(meta_data_t) * max_meta_buffer_size);
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
//         "meta_buffer", reinterpret_cast<uintptr_t>(meta_buffer_.data()), sizeof(meta_data_t) * max_meta_buffer_size);
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
//     data_ctx_->connect(data_ctx_info);
//     meta_ctx_->connect(meta_ctx_info);

//     data_ctx_->launch_future();
//     meta_ctx_->launch_future();

//     RDMA_tasks_threads_running_ = true;
//     rdma_tasks_threads_         = std::thread([this] { this->waitandPopTask(std::chrono::milliseconds(100)); });
// }

// void RDMAEndpoint::addSendTask(std::shared_ptr<RDMABuffer> buffer)
// {
//     std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
//     ++send_slot_id_;
//     auto task = std::make_shared<rdma_task_t>(shared_from_this(), send_slot_id_, OpCode::SEND, buffer);
//     send_batch_slot_[send_slot_id_] = task;
//     rdma_tasks_queue_.push(task);
//     rdma_tasks_cv_.notify_one();
// }

// void RDMAEndpoint::addRecvTask(std::shared_ptr<RDMABuffer> buffer)
// {
//     std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
//     ++recv_slot_id_;
//     auto task = std::make_shared<rdma_task_t>(shared_from_this(), recv_slot_id_, OpCode::RECV, buffer);
//     recv_batch_slot_[recv_slot_id_] = task;
//     rdma_tasks_queue_.push(task);
//     rdma_tasks_cv_.notify_one();
// }

// void RDMAEndpoint::asyncSendData(std::shared_ptr<rdma_task_t> task)
// {
//     // size_t   batch_size = task->buffer_->batchSize();
//     // uint32_t slot_id    = task->slot_id_;

//     auto data_callback = [this, task](int status, int _) mutable {
//         std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);

//         task->buffer_->send_done_callback();
//     };

//     auto meta_callback = [this, task, data_callback](int status, int slot_id) mutable {
//         std::unique_lock<std::mutex> lock(this->rdma_tasks_mutex_);
//         // Assert
//         task->registerRemoteDataMemoryRegion();
//         AssignmentBatch data_assign_batch = task->getDataAssignmentBatch();
//         auto            data_atx          = this->data_ctx_->submit(
//             OpCode::WRITE_WITH_IMM, data_assign_batch, data_callback, RDMAContext::UNDEFINED_QPI, slot_id);
//     };

//     {
//         AssignmentBatch meta_data = task->getMetaAssignmentBatch();
//         meta_ctx_->submit(OpCode::RECV, meta_data, meta_callback);
//     }
// }

// void RDMAEndpoint::asyncRecvData(std::shared_ptr<rdma_task_t> task)
// {
//     auto data_callback = [this, task](int status, int slot_id) mutable {
//         std::unique_lock<std::mutex> lock(rdma_tasks_mutex_);
//         recv_batch_slot_[slot_id]->buffer_->recv_done_callback();
//     };

//     auto meta_callback = [this, task, data_callback](int status, int _) mutable {
//         std::unique_lock<std::mutex> lock(this->rdma_tasks_mutex_);
//         AssignmentBatch              assign_batch = task->getDataAssignmentBatch();

//     };

//     {
//         // post recv
//           auto data_atx = this->data_ctx_->submit(OpCode::RECV, assign_batch, data_callback,
//           RDMAContext::UNDEFINED_QPI);
//         AssignmentBatch meta_data = task->getMetaAssignmentBatch();
//         meta_ctx_->submit(OpCode::WRITE_WITH_IMM, meta_data, meta_callback, RDMAContext::UNDEFINED_QPI,
//         task->slot_id_);
//     }
// }

}  // namespace slime
