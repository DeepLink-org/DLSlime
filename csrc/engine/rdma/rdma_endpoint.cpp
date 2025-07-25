#include "engine/rdma/rdma_endpoint.h"

namespace slime {

void RDMAEndpoint::registerDataMemRegion(std::string str, uintptr_t ptr, size_t data_size)
{
    data_ctx_->register_memory_region(str, ptr, data_size);
}

void RDMAEndpoint::registerMetaMemRegion(std::string str, uintptr_t ptr, size_t data_size)
{
    meta_ctx_->register_memory_region(str, ptr, data_size);
}

void RDMAEndpoint::registerRemoteMemoryRegion(std::string mr_key, uintptr_t addr, size_t length, uint32_t rkey)
{
    json mr_info;
    mr_info["addr"]   = addr;
    mr_info["length"] = length;
    mr_info["rkey"]   = rkey;
    data_ctx_->register_remote_memory_region(mr_key, mr_info);
}

void RDMAEndpoint::unregisterDataMemRegionBatch(std::string str, uint32_t batch_size)
{
    for (uint32_t i = 0; i < batch_size; ++i) {
        std::string KEY = str + "_" + std::to_string(i);
        data_ctx_->unregister_memory_region(KEY);
    }
}

void RDMAEndpoint::unregisterMetaMemRegionBatch(std::string str, uint32_t batch_size)
{
    for (uint32_t i = 0; i < batch_size; ++i) {
        std::string KEY = str + "_" + std::to_string(i);
        meta_ctx_->unregister_memory_region(KEY);
    }
}

void RDMAEndpoint::registerRecvMemRegionBatch(std::string str, std::vector<buffer_data_info_t> data_info)
{
    // The mr key is followed by the form: str_ +"i"
    for (size_t i = 0; i < data_info.size(); ++i) {
        std::string KEY = str + "_" + std::to_string(i);
        registerDataMemRegion(KEY, std::get<0>(data_info[i]), std::get<1>(data_info[i]));
    }
}

void RDMAEndpoint::registerSendMemRegionBatch(std::string str, std::vector<buffer_data_info_t> data_info)
{
    // The mr key is followed by the form: str_ +"i"
    for (uint32_t i = 0; i < data_info.size(); ++i) {
        std::string KEY = str + "_" + std::to_string(i);
        registerDataMemRegion(KEY, std::get<0>(data_info[i]), std::get<1>(data_info[i]));
    }
}

void RDMAEndpoint::waitandPopTask(std::chrono::milliseconds timeout)
{
    while (true) {
        RDMA_task_t task;
        {
            std::unique_lock<std::mutex> lock(RDMA_tasks_mutex_);
            bool timeout_occurred = !RDMA_tasks_cv_.wait_for(lock, std::chrono::milliseconds(100), [this] {
                return !RDMA_tasks_queue_.empty() || !RDMA_tasks_threads_running_;
            });
            if (timeout_occurred)
                continue;

            if (!RDMA_tasks_threads_running_ && RDMA_tasks_queue_.empty())
                break;

            if (!RDMA_tasks_queue_.empty()) {
                task = std::move(RDMA_tasks_queue_.front());
                RDMA_tasks_queue_.pop();
            }
            switch (task.op_code) {
                case OpCode::SEND:
                    asyncSendData(task);
                    break;
                case OpCode::RECV:
                    asyncRecvData(task);
                    imm_data_callback_[task.task_id] = task.callback;
                    break;
                default:
                    SLIME_LOG_ERROR("Unknown OpCode in WaitandPopTask");
                    break;
            }
        }
    }
}

uint32_t RDMAEndpoint::generateSendAssignmentBatch(std::vector<buffer_data_info_t> data_info)
{
    send_slot_id_++;
    std::string cur_key = "SEND_KEY_" + std::to_string(send_slot_id_);
    registerSendMemRegionBatch(cur_key, data_info);
    AssignmentBatch send_data_batch;
    for (size_t i = 0; i < data_info.size(); ++i) {
        std::string KEY = cur_key + "_" + std::to_string(i);
        send_data_batch.push_back(Assignment(KEY, std::get<2>(data_info[i]), 0, std::get<1>(data_info[i])));
    }
    send_batch_slot_.emplace(send_slot_id_, send_data_batch);
    return send_slot_id_;
}

uint32_t RDMAEndpoint::generateRecvAssignmentBatch(std::vector<buffer_data_info_t> data_info)
{
    recv_slot_id_++;
    std::string cur_key = "RECV_KEY_" + std::to_string(recv_slot_id_);
    registerRecvMemRegionBatch(cur_key, data_info);
    AssignmentBatch recv_data_batch;
    for (size_t i = 0; i < data_info.size(); ++i) {
        std::string KEY = cur_key + "_" + std::to_string(i);
        recv_data_batch.push_back(Assignment(KEY, std::get<2>(data_info[i]), 0, std::get<1>(data_info[i])));
    }
    recv_batch_slot_.emplace(recv_slot_id_, recv_data_batch);
    return recv_slot_id_;
}

void RDMAEndpoint::asyncSendData(RDMA_task_t& task)
{
    size_t   batch_size    = task.batch_size;
    uint32_t slot_id       = task.task_id;
    auto     task_callback = task.callback;

    std::string META_KEY = "SEND_META_" + std::to_string(slot_id);
    std::string SEND_KEY = "SEND_DATA_" + std::to_string(slot_id);
    auto meta_buf = std::shared_ptr<meta_data_t[]>(new meta_data_t[batch_size], std::default_delete<meta_data_t[]>());

    auto data_callback = [this, SEND_KEY, META_KEY, batch_size, task_callback](int status, int _) { task_callback(); };

    auto meta_callback = [this, meta_buf, slot_id, batch_size, data_callback](int status, int _) {
        auto it = this->send_batch_slot_.find(slot_id);
        if (it != this->send_batch_slot_.end()) {
            auto send_data_batch = it->second;
            for (size_t i = 0; i < batch_size; ++i) {
                this->registerRemoteMemoryRegion(
                    send_data_batch[i].mr_key, meta_buf[i].mr_addr, meta_buf[i].mr_size, meta_buf[i].mr_rkey);
            }
            uint32_t target_qpi = meta_buf[0].mr_qpidx;
            auto     data_atx =
                this->data_ctx_->submit(OpCode::WRITE_WITH_IMM, send_data_batch, data_callback, target_qpi, slot_id);
        }
        else {
            std::cout << "The data in slot " << slot_id << "is not prepared" << std::endl;
            std::cout << "There must be some bugs..." << std::endl;
        }
    };

    {
        std::lock_guard<std::mutex> lock(meta_data_mutex);
        meta_ctx_->register_memory_region(
            META_KEY, reinterpret_cast<uintptr_t>(meta_buf.get()), batch_size * sizeof(meta_data_t));

        AssignmentBatch meta_data(1);
        meta_data[0]  = Assignment(META_KEY, 0, 0, batch_size * sizeof(meta_data_t));
        auto meta_atx = meta_ctx_->submit(OpCode::RECV, meta_data, meta_callback);
    }
}

void RDMAEndpoint::asyncRecvData(RDMA_task_t& task)
{

    size_t   batch_size    = task.batch_size;
    uint32_t slot_id       = task.task_id;
    auto     task_callback = task.callback;

    std::string META_KEY = "RECV_META_" + std::to_string(slot_id);
    std::string RECV_KEY = "RECV_DATA_" + std::to_string(slot_id);
    auto meta_buf = std::shared_ptr<meta_data_t[]>(new meta_data_t[batch_size], std::default_delete<meta_data_t[]>());

    auto data_callback = [this, RECV_KEY, META_KEY, batch_size, task_callback](int status, int imm_data) {
        if (this->imm_data_callback_.find(imm_data) != this->imm_data_callback_.end()) {
            this->imm_data_callback_[imm_data]();
            this->imm_data_callback_.erase(imm_data);
        }
    };

    auto meta_callback = [this, meta_buf, slot_id, batch_size, data_callback](int status, int _) {
        auto it = this->recv_batch_slot_.find(slot_id);
        if (it != this->send_batch_slot_.end()) {
            auto     recv_data_batch = it->second;
            uint32_t target_qpi      = meta_buf[0].mr_qpidx;
            auto     data_atx = this->data_ctx_->submit(OpCode::RECV, recv_data_batch, data_callback, target_qpi);
        }
    };

    {

        std::lock_guard<std::mutex> lock(meta_data_mutex);
        auto                        recv_data_t = recv_batch_slot_[slot_id];
        for (size_t i = 0; i < batch_size; ++i) {
            meta_buf[i].mr_addr =
                reinterpret_cast<uint64_t>(data_ctx_->memory_pool_->get_mr(recv_data_t[i].mr_key)->addr);
            meta_buf[i].mr_rkey  = data_ctx_->memory_pool_->get_mr(recv_data_t[i].mr_key)->rkey;
            meta_buf[i].mr_size  = data_ctx_->memory_pool_->get_mr(recv_data_t[i].mr_key)->length;
            meta_buf[i].mr_slot  = slot_id;
            meta_buf[i].mr_qpidx = slot_id % data_ctx_qp_num_;
        }

        meta_ctx_->register_memory_region(
            META_KEY, reinterpret_cast<uintptr_t>(meta_buf.get()), batch_size * sizeof(meta_data_t));

        AssignmentBatch meta_data(1);
        meta_data[0] = Assignment(META_KEY, 0, 0, batch_size * sizeof(meta_data_t));

        auto meta_atx = meta_ctx_->submit(OpCode::SEND, meta_data, meta_callback);
    }
}

}  // namespace slime
