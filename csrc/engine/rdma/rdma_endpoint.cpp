#include "engine/rdma/rdma_endpoint.h"

namespace slime {

void RDMAEndpoint::RegisterRecvMemRegionBatch(std::string            str,
                                              std::vector<uintptr_t> ptrs,
                                              std::vector<size_t>    data_size,
                                              uint32_t               batch_size)
{
    // The mr key is followed by the form: str_ +"i"
    for (uint32_t i = 0; i < batch_size; ++i) {
        std::string KEY = str + "_" + std::to_string(i);
        RegisterMemRegion(KEY, ptrs[i], data_size[i]);
    }
}

void RDMAEndpoint::RegisterSendMemRegionBatch(std::string            str,
                                              std::vector<uintptr_t> ptrs,
                                              std::vector<size_t>    data_size,
                                              uint32_t               batch_size)
{
    // The mr key is followed by the form: str_ +"i"
    for (uint32_t i = 0; i < batch_size; ++i) {
        std::string KEY = str + "_" + std::to_string(i);
        RegisterMemRegion(KEY, ptrs[i], data_size[i]);
    }
}

void RDMAEndpoint::UnregisterDataMemRegionBatch(std::string str, uint32_t batch_size)
{
    for (uint32_t i = 0; i < batch_size; ++i) {
        std::string KEY = str + "_" + std::to_string(i);
        data_ctx_->unregister_memory_region(KEY);
    }
}

void RDMAEndpoint::WaitandPopTask(std::chrono::milliseconds timeout)
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
                    AsyncSendData(task);
                    break;
                case OpCode::RECV:
                    AsyncRecvData(task);
                    break;
                default:
                    SLIME_LOG_ERROR("Unknown OpCode");
                    break;
            }
        }
    }
}

uint8_t RDMAEndpoint::GenerateSENDAssignmentBatch(std::vector<uintptr_t>& ptrs,
                                                  std::vector<size_t>&    data_size,
                                                  uint32_t                batch_size)
{

    send_slot_id_++;
    std::string cur_key = "SEND_KEY_" + std::to_string(send_slot_id_);
    RegisterSendMemRegionBatch(cur_key, ptrs, data_size, batch_size);
    AssignmentBatch send_data_batch;
    for (uint32_t i = 0; i < batch_size; ++i) {
        std::string KEY = cur_key + "_" + std::to_string(i);
        send_data_batch.push_back(Assignment(KEY, 0, 0, data_size[i]));
        send_data_batch[i].slot_id = send_slot_id_;
    }
    send_batch_slot_.emplace(send_slot_id_, send_data_batch);
    return send_slot_id_;
}

uint8_t RDMAEndpoint::GenerateRECVAssignmentBatch(std::vector<uintptr_t>& ptrs,
                                                  std::vector<size_t>&    data_size,
                                                  uint32_t                batch_size)
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
    recv_batch_slot_.emplace(recv_slot_id_, recv_data_batch);
    return recv_slot_id_;
}

void RDMAEndpoint::AsyncSendData(RDMA_task_t& task)
{
    size_t  batch_size    = task.batch_size;
    uint8_t slot_id       = task.task_id;
    auto    task_callback = task.callback;

    std::string META_KEY = "SEND_META_" + std::to_string(slot_id);
    std::string SEND_KEY = "SEND_DATA_" + std::to_string(slot_id);
    auto meta_buf = std::shared_ptr<meta_data_t[]>(new meta_data_t[batch_size], std::default_delete<meta_data_t[]>());

    auto data_callback = [this, SEND_KEY, META_KEY, batch_size, task_callback](int status) {
        // this->UnregisterDataMemRegionBatch(SEND_KEY, batch_size);
        // this->UnregisterMetaMemRegionBatch(META_KEY);
        task_callback();
    };

    auto meta_callback = [this, meta_buf, slot_id, batch_size, data_callback](int status) {
        auto it = this->send_batch_slot_.find(slot_id);
        if (it != this->send_batch_slot_.end()) {
            auto send_data_batch = it->second;
            for (size_t i = 0; i < batch_size; ++i) {
                send_data_batch[i].remote_addr   = meta_buf[i].mr_addr;
                send_data_batch[i].remote_rkey   = meta_buf[i].mr_rkey;
                send_data_batch[i].length        = meta_buf[i].mr_size;
                send_data_batch[i].target_offset = 0;
            }
            auto data_atx = this->data_ctx_->submit(OpCode::WRITE_WITH_IMM, send_data_batch, data_callback);
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

void RDMAEndpoint::AsyncRecvData(RDMA_task_t& task)
{

    size_t  batch_size    = task.batch_size;
    uint8_t slot_id       = task.task_id;
    auto    task_callback = task.callback;

    std::string META_KEY = "RECV_META_" + std::to_string(slot_id);
    std::string RECV_KEY = "RECV_DATA_" + std::to_string(slot_id);
    auto meta_buf = std::shared_ptr<meta_data_t[]>(new meta_data_t[batch_size], std::default_delete<meta_data_t[]>());

    auto data_callback = [this, RECV_KEY, META_KEY, batch_size, task_callback](int status) {
        // this->UnregisterDataMemRegionBatch(RECV_KEY, batch_size);
        // this->UnregisterMetaMemRegionBatch(META_KEY);
        task_callback();
    };

    auto meta_callback = [this, meta_buf, slot_id, batch_size, data_callback](int status) {
        auto it = this->recv_batch_slot_.find(slot_id);
        if (it != this->send_batch_slot_.end()) {
            auto recv_data_batch = it->second;
            auto data_atx        = this->data_ctx_->submit(OpCode::RECV, recv_data_batch, data_callback);
        }
    };

    {

        std::lock_guard<std::mutex> lock(meta_data_mutex);
        auto                        recv_data_t = recv_batch_slot_[slot_id];
        for (size_t i = 0; i < batch_size; ++i) {
            meta_buf[i].mr_addr =
                reinterpret_cast<uint64_t>(data_ctx_->memory_pool_->get_mr(recv_data_t[i].mr_key)->addr);
            meta_buf[i].mr_rkey = data_ctx_->memory_pool_->get_mr(recv_data_t[i].mr_key)->rkey;
            meta_buf[i].mr_size = data_ctx_->memory_pool_->get_mr(recv_data_t[i].mr_key)->length;
            meta_buf[i].mr_slot = slot_id;
        }

        meta_ctx_->register_memory_region(
            META_KEY, reinterpret_cast<uintptr_t>(meta_buf.get()), batch_size * sizeof(meta_data_t));

        AssignmentBatch meta_data(1);
        meta_data[0] = Assignment(META_KEY, 0, 0, batch_size * sizeof(meta_data_t));

        auto meta_atx = meta_ctx_->submit(OpCode::SEND, meta_data, meta_callback);
    }
}

}  // namespace slime
