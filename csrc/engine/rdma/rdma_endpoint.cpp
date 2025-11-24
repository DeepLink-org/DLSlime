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
#include <iostream>
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

    SLIME_LOG_INFO("Allocate MR Buffer and Dummy Buffer");
    meta_buffer_.resize(SLIME_META_BUFFER_SIZE * 2);
    memset(meta_buffer_.data(), 0, meta_buffer_.size() * sizeof(meta_data_t));
    meta_ctx_->register_memory_region(
        "meta_buffer", reinterpret_cast<uintptr_t>(meta_buffer_.data()), sizeof(meta_data_t) * meta_buffer_.size());

    dum_meta_buffer_.resize(SLIME_DUMMY_BUFFER_SIZE);
    memset(dum_meta_buffer_.data(), 0, dum_meta_buffer_.size() * sizeof(uint32_t));
    meta_ctx_->register_memory_region("dum_meta_buffer",
                                      reinterpret_cast<uintptr_t>(dum_meta_buffer_.data()),
                                      sizeof(uint32_t) * dum_meta_buffer_.size());

    dum_data_buffer_.resize(SLIME_DUMMY_BUFFER_SIZE);
    memset(dum_data_buffer_.data(), 0, dum_data_buffer_.size() * sizeof(uint32_t));
    data_ctx_->register_memory_region("dum_data_buffer",
                                      reinterpret_cast<uintptr_t>(dum_data_buffer_.data()),
                                      sizeof(uint32_t) * dum_data_buffer_.size());

    SLIME_LOG_INFO("The MR Buffer and Dummy Buffer Allocate Success...");

    SLIME_LOG_INFO("Construct the pre-submit queue");
    meta_slots_manager_ = std::make_unique<RingSlotsManager>(SLIME_STATUS_SLOT_SIZE);
    data_slots_manager_ = std::make_unique<RingSlotsManager>(SLIME_STATUS_SLOT_SIZE);
}

void RDMAEndpoint::connect(const json& data_ctx_info, const json& meta_ctx_info)
{

    SLIME_LOG_INFO("Lauch the RDMAConstex for DATA and META")
    data_ctx_->connect(data_ctx_info);
    meta_ctx_->connect(meta_ctx_info);

    data_ctx_->launch_future();
    meta_ctx_->launch_future();

    for (size_t i = 0; i < SLIME_STATUS_SLOT_SIZE; i += 1) {
        addPreQueueElement(OpCode::SEND);
        addPreQueueElement(OpCode::RECV);
    }
    proxyInit();
}

void RDMAEndpoint::proxyInit()
{
    SLIME_LOG_INFO("Starting all RDMA endpoint threads...");

    stop_meta_recv_queue_thread_.store(false, std::memory_order_release);
    stop_data_recv_queue_thread_.store(false, std::memory_order_release);
    stop_send_buffer_queue_thread_.store(false, std::memory_order_release);
    stop_recv_buffer_queue_thread_.store(false, std::memory_order_release);
    stop_wait_send_finish_queue_thread_.store(false, std::memory_order_release);

    meta_recv_thread_   = std::thread([this]() { this->metaRecvQueueThread(std::chrono::milliseconds(0)); });
    data_recv_thread_   = std::thread([this]() { this->dataRecvQueueThread(std::chrono::milliseconds(0)); });
    send_buffer_thread_ = std::thread([this]() { this->SendBufferQueueThread(std::chrono::milliseconds(0)); });
    recv_buffer_thread_ = std::thread([this]() { this->RecvBufferQueueThread(std::chrono::milliseconds(0)); });
    send_finish_thread_ = std::thread([this]() { this->SendFinishQueueThread(std::chrono::milliseconds(0)); });

    SLIME_LOG_INFO("All Proxy Threads Started Successfully");
}

void RDMAEndpoint::proxyDestroy()
{
    SLIME_LOG_INFO("Stopping all RDMA endpoint threads...");

    stop_meta_recv_queue_thread_.store(true, std::memory_order_release);
    stop_data_recv_queue_thread_.store(true, std::memory_order_release);
    stop_send_buffer_queue_thread_.store(true, std::memory_order_release);
    stop_recv_buffer_queue_thread_.store(true, std::memory_order_release);
    stop_wait_send_finish_queue_thread_.store(true, std::memory_order_release);

    if (meta_recv_thread_.joinable()) {
        meta_recv_thread_.join();
        SLIME_LOG_INFO("Meta recv thread stopped");
    }

    if (data_recv_thread_.joinable()) {
        data_recv_thread_.join();
        SLIME_LOG_INFO("Data recv thread stopped");
    }

    if (send_buffer_thread_.joinable()) {
        send_buffer_thread_.join();
        SLIME_LOG_INFO("Send buffer thread stopped");
    }

    if (recv_buffer_thread_.joinable()) {
        recv_buffer_thread_.join();
        SLIME_LOG_INFO("Recv buffer thread stopped");
    }

    if (send_finish_thread_.joinable()) {
        send_finish_thread_.join();
        SLIME_LOG_INFO("Send finish thread stopped");
    }

    SLIME_LOG_INFO("All Proxy Threads Stopped Successfully");
}

RDMAEndpoint::~RDMAEndpoint()
{
    try {
        proxyDestroy();
        data_ctx_->stop_future();
        meta_ctx_->stop_future();
        SLIME_LOG_INFO("RDMAEndpoint destroyed successfully");
    }
    catch (const std::exception& e) {
        SLIME_LOG_ERROR("Exception in RDMAEndpoint destructor: ", e.what());
    }
}

void RDMAEndpoint::addPreQueueElement(OpCode rdma_opcode)
{
    if (rdma_opcode == OpCode::SEND) {
        RDMAPrePostQueueElement meta_recv_queue_element =
            RDMAPrePostQueueElement(unique_meta_recv_id_.load(std::memory_order_relaxed), OpCode::SEND);
        uint32_t idx           = meta_recv_queue_element.unique_id_;
        auto     is_finish_ptr = meta_recv_queue_element.is_finished_ptr_;

        auto meta_recv_callback = [idx, is_finish_ptr](int status, int slot_id) mutable {
            is_finish_ptr->store(true, std::memory_order_release);
        };

        AssignmentBatch assignment_batch_ = AssignmentBatch{Assignment("dum_meta_buffer", 0, 0, 16 * sizeof(uint32_t))};
        meta_ctx_->submit(OpCode::RECV, assignment_batch_, meta_recv_callback, RDMAContext::UNDEFINED_QPI, idx);

        meta_recv_queue_.enqueue(meta_recv_queue_element);
        unique_meta_recv_id_.fetch_add(1, std::memory_order_relaxed);
    }
    else if (rdma_opcode == OpCode::RECV) {

        RDMAPrePostQueueElement data_recv_queue_element =
            RDMAPrePostQueueElement(unique_data_recv_id_.load(std::memory_order_relaxed), OpCode::RECV);

        uint32_t idx                = data_recv_queue_element.unique_id_;
        auto     is_finish_ptr      = data_recv_queue_element.is_finished_ptr_;
        auto     data_recv_callback = [idx, is_finish_ptr](int status, int slot_id) mutable {
            is_finish_ptr->store(true, std::memory_order_release);
        };
        AssignmentBatch assignment_batch_ = AssignmentBatch{Assignment("dum_data_buffer", 0, 0, 16 * sizeof(uint32_t))};
        data_ctx_->submit(OpCode::RECV, assignment_batch_, data_recv_callback, RDMAContext::UNDEFINED_QPI, idx);

        data_recv_queue_.enqueue(data_recv_queue_element);
        data_slots_manager_->acquireSlot(idx);
        unique_data_recv_id_.fetch_add(1, std::memory_order_relaxed);
    }
    else {
        SLIME_LOG_ERROR("Undefined OPCODE IN RDMAEndpoint::addPreQueueElement");
    }
}

void RDMAEndpoint::addRDMABuffer(OpCode rdma_opcode, std::shared_ptr<RDMABuffer> rdma_buffer)
{

    if (rdma_opcode == OpCode::SEND) {
        uint32_t               cur_idx        = unique_SEND_SLOT_ID_.load(std::memory_order_relaxed);
        RDMABufferQueueElement buffer_element = RDMABufferQueueElement(cur_idx, OpCode::SEND, rdma_buffer);
        send_buffer_queue_.enqueue(buffer_element);
        unique_SEND_SLOT_ID_.fetch_add(1, std::memory_order_relaxed);
    }
    else if (rdma_opcode == OpCode::RECV) {
        uint32_t               cur_idx        = unique_RECV_SLOT_ID_.load(std::memory_order_relaxed);
        RDMABufferQueueElement buffer_element = RDMABufferQueueElement(cur_idx, OpCode::RECV, rdma_buffer);
        recv_buffer_queue_.enqueue(buffer_element);
        unique_RECV_SLOT_ID_.fetch_add(1, std::memory_order_relaxed);
    }
    else {
        SLIME_LOG_ERROR("Undefined OPCODE IN RDMAEndpoint::addRDMABuffer");
    }
}

void RDMAEndpoint::postMetaWrite(uint32_t idx, std::shared_ptr<RDMABuffer> rdma_buffer)
{

    std::string prefix      = "DATA_RECV_";
    std::string MR_KEY      = prefix + std::to_string(idx);
    auto        mr_is_exist = data_ctx_->get_mr(MR_KEY);

    if (mr_is_exist != nullptr) {
        SLIME_LOG_DEBUG("The RECV DATA MR has been REGISTERED! The SLOT_ID is: ", idx);
    }
    else {
        data_ctx_->register_memory_region(MR_KEY, rdma_buffer->ptr_, rdma_buffer->data_size_);
    }

    auto mr = data_ctx_->get_mr(MR_KEY);

    meta_buffer_[SLIME_META_BUFFER_SIZE + idx % SLIME_META_BUFFER_SIZE].mr_addr = reinterpret_cast<uint64_t>(mr->addr);
    meta_buffer_[SLIME_META_BUFFER_SIZE + idx % SLIME_META_BUFFER_SIZE].mr_rkey = mr->rkey;
    meta_buffer_[SLIME_META_BUFFER_SIZE + idx % SLIME_META_BUFFER_SIZE].mr_size = mr->length;
    meta_buffer_[SLIME_META_BUFFER_SIZE + idx % SLIME_META_BUFFER_SIZE].mr_slot = idx;

    size_t          meta_buffer_idx = idx % SLIME_META_BUFFER_SIZE;
    AssignmentBatch meta_assignment_batch_ =
        AssignmentBatch{Assignment("meta_buffer",
                                   meta_buffer_idx * sizeof(meta_data_t),
                                   SLIME_META_BUFFER_SIZE * sizeof(meta_data_t) + meta_buffer_idx * sizeof(meta_data_t),
                                   sizeof(meta_data_t))};

    auto meta_callback = [idx](int status, int slot_id) {
        SLIME_LOG_DEBUG("The META RECV IS SUCCESS FOR SLOT: ", idx);
    };
    meta_ctx_->submit(OpCode::WRITE_WITH_IMM, meta_assignment_batch_, meta_callback, RDMAContext::UNDEFINED_QPI, idx);
}

void RDMAEndpoint::postDataWrite(RDMABufferQueueElement& element, std::shared_ptr<RDMABuffer> rdma_buffer)
{
    uint64_t    addr;
    uint32_t    size;
    uint32_t    rkey;
    uint32_t    idx         = element.unique_id_;
    std::string prefix      = "DATA_SEND_";
    std::string MR_KEY      = prefix + std::to_string(idx);
    auto        mr_is_exist = data_ctx_->get_mr(MR_KEY);
    if (mr_is_exist != nullptr) {
        SLIME_LOG_DEBUG("The SEND DATA MR has been REGISTERED! The SLOT_ID is: ", unique_SEND_SLOT_ID_);
    }
    else {
        data_ctx_->register_memory_region(MR_KEY, rdma_buffer->ptr_, rdma_buffer->data_size_);
    }

    auto mr_remote = data_ctx_->get_remote_mr(MR_KEY);
    if (mr_remote.rkey == 0) {

        addr = meta_buffer_[idx % SLIME_META_BUFFER_SIZE].mr_addr;
        size = meta_buffer_[idx % SLIME_META_BUFFER_SIZE].mr_size;
        rkey = meta_buffer_[idx % SLIME_META_BUFFER_SIZE].mr_rkey;
        data_ctx_->register_remote_memory_region(MR_KEY, addr, size, rkey);
    }

    else {
        SLIME_LOG_DEBUG("The REMOTE DATA MR has been REGISTERED! The SLOT_ID is: ", unique_SEND_SLOT_ID_);
    }

    AssignmentBatch batch = AssignmentBatch{Assignment(MR_KEY, 0, 0, size)};

    auto is_finish_ptr = element.is_finished_ptr_;
    is_finish_ptr->store(false, std::memory_order_release);
    auto data_write_callback = [idx, is_finish_ptr](int status, int slot_id) mutable {
        is_finish_ptr->store(true, std::memory_order_release);
    };

    data_ctx_->submit(OpCode::WRITE_WITH_IMM, batch, data_write_callback, RDMAContext::UNDEFINED_QPI, idx);
}

void RDMAEndpoint::metaRecvQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("metaRecvQueueThread started (timeout={}ms)", timeout.count());

    while (!stop_meta_recv_queue_thread_.load(std::memory_order_acquire)) {
        uint32_t idx;
        if (meta_recv_queue_.getFrontTaskId(idx)) {
            if (meta_slots_manager_->checkSlotAvailable(idx)) {
                bool found = meta_recv_queue_.peekQueue(
                    idx, [](const auto& e) { return e.is_finished_ptr_->load(std::memory_order_acquire); });
                if (found) {
                    if (meta_slots_manager_->acquireSlot(idx)) {
                        if (meta_recv_queue_.popQueue()) {
                            SLIME_LOG_DEBUG("SUCCESS to set META RECV RING SLOT status of task id ",
                                            idx,
                                            " and the slot id ",
                                            idx % SLIME_STATUS_SLOT_SIZE);
                        }
                        else {
                            SLIME_LOG_ERROR("The META Queue Has no element");
                        }
                    }
                    else {
                        SLIME_LOG_ERROR("FAIL to set META RECV RING SLOT status of task id ",
                                        idx,
                                        " and the slot id ",
                                        idx % SLIME_STATUS_SLOT_SIZE);
                    }
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
    SLIME_LOG_INFO("metaRecvQueueThread stopped");
}

void RDMAEndpoint::dataRecvQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("RecvDataQueueThread started (timeout={}ms)", timeout.count());

    while (!stop_data_recv_queue_thread_.load(std::memory_order_acquire)) {
        uint32_t idx;
        if (data_recv_queue_.getFrontTaskId(idx)) {
            bool found = data_recv_queue_.peekQueue(
                idx, [](const auto& e) { return e.is_finished_ptr_->load(std::memory_order_acquire); });
            if (found) {
                RDMABufferQueueElement element = recv_buffer_mapping_[idx];
                element.rdma_buffer_->recvDoneCallback();

                if (data_recv_queue_.popQueue()) {
                    recv_buffer_mapping_.erase(idx);
                    addPreQueueElement(OpCode::RECV);
                    SLIME_LOG_DEBUG("SUCCESS to set DATA RECV RING SLOT status of task id ",
                                    idx,
                                    " and the slot id ",
                                    idx % SLIME_STATUS_SLOT_SIZE);
                }
                else {
                    SLIME_LOG_ERROR("The DATA RECV Queue Has no element");
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
    SLIME_LOG_INFO("RecvDataQueueThread stopped");
}

void RDMAEndpoint::SendBufferQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("SendBufferQueueThread started (timeout={}ms)", timeout.count());

    while (!stop_send_buffer_queue_thread_.load(std::memory_order_acquire)) {

        uint32_t idx;
        if (send_buffer_queue_.getFrontTaskId(idx)) {
            if (meta_slots_manager_->checkSlotReady(idx)) {
                RDMABufferQueueElement element;
                if (send_buffer_queue_.fetchQueue(element)) {
                    postDataWrite(element, element.rdma_buffer_);
                    send_finish_queue_.enqueue(element);
                    if (meta_slots_manager_->releaseSlot(idx)) {
                        addPreQueueElement(OpCode::SEND);
                    }
                    else {
                        SLIME_LOG_ERROR("FAIL to release meta_slots_manager_");
                    }
                }
                else {
                    SLIME_LOG_ERROR("FAIL to fetch the element in  send_buffer_queue_");
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

void RDMAEndpoint::RecvBufferQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("RecvBufferQueueThread started (timeout={}ms)", timeout.count());

    while (!stop_recv_buffer_queue_thread_.load(std::memory_order_acquire)) {
        uint32_t idx;
        if (recv_buffer_queue_.getFrontTaskId(idx)) {
            if (data_slots_manager_->checkSlotReady(idx)) {
                RDMABufferQueueElement element;
                if (recv_buffer_queue_.fetchQueue(element)) {
                    postMetaWrite(element.unique_id_, element.rdma_buffer_);
                    recv_buffer_mapping_.emplace(idx, element);
                    data_slots_manager_->releaseSlot(idx);
                }
                else {
                    SLIME_LOG_ERROR("FAIL to fetchQueue recv_buffer_queue_");
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
    SLIME_LOG_INFO("RecvBufferQueueThread stopped");
}

void RDMAEndpoint::SendFinishQueueThread(std::chrono::milliseconds timeout)
{
    SLIME_LOG_INFO("SendFinishQueueThread started (timeout={}ms)", timeout.count());

    while (!stop_wait_send_finish_queue_thread_.load(std::memory_order_acquire)) {
        uint32_t idx;
        if (send_finish_queue_.getFrontTaskId(idx)) {
            bool found = send_finish_queue_.peekQueue(
                idx, [](const auto& e) { return e.is_finished_ptr_->load(std::memory_order_acquire); });
            if (found) {
                RDMABufferQueueElement element;
                if (send_finish_queue_.fetchQueue(element)) {
                    element.rdma_buffer_->sendDoneCallback();
                    SLIME_LOG_DEBUG(
                        "SUCCESS to send_finish_queue_ ", idx, " and the slot id ", idx % SLIME_STATUS_SLOT_SIZE);
                }
                else {
                    SLIME_LOG_ERROR("The Msend_finish_queue_ no element");
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
    SLIME_LOG_INFO("SendFinishQueueThread stopped");
}

}  // namespace slime
