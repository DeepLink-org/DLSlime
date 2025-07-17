#include "engine/rdma/rdma_endpoint.h"

namespace slime {

void RDMAEndpoint::LaunchSend(int max_threads)
{
    SEND_RUN = true;
    for (int i = 0; i < max_threads; ++i)
        send_futures_.push_back(std::async(std::launch::async, &RDMAEndpoint::SyncSendData, this));
    std::cout << max_threads << "Threads of SEND are Started..." << std::endl;
}

void RDMAEndpoint::LaunchRecv(int max_threads)
{
    RECV_RUN = true;
    for (int i = 0; i < max_threads; ++i)
        recv_futures_.push_back(std::async(std::launch::async, &RDMAEndpoint::SyncRecvData, this));
    std::cout << max_threads << "Threads of RECV are Started..." << std::endl;
}

void RDMAEndpoint::WaitRecv()
{
    for (auto& future : recv_futures_)
        future.wait();
    std::cout << "All RECV Tasks have finished." << std::endl;
}

void RDMAEndpoint::WaitSend()
{
    for (auto& future : send_futures_)
        future.wait();

    std::cout << "All SEND Tasks have finished." << std::endl;
}

void RDMAEndpoint::Stop()
{
    if (!SEND_RUN && !RECV_RUN) {
        std::cout << "All threads are already stopped." << std::endl;
        return;
    }

    SEND_RUN = false;
    RECV_RUN = false;
    {
        std::lock_guard<std::mutex> lock(meta_data_mutex);
        send_buffer_->NotifyAll();
        recv_buffer_->NotifyAll();
    }

    auto wait_with_timeout = [](auto& futures) {
        for (auto& future : futures) {
            if (future.valid()) {
                auto status = future.wait_for(std::chrono::seconds(2));
                if (status == std::future_status::timeout) {
                    std::cerr << "Warning: Thread termination timeout" << std::endl;
                }
            }
        }
    };

    wait_with_timeout(send_futures_);
    wait_with_timeout(recv_futures_);

    send_futures_.clear();
    recv_futures_.clear();
    std::cout << "All SEND and RECV threads have been terminated." << std::endl;
}



void RDMAEndpoint::AsyncSendData(RDMA_task_t &task)
{

    std::cout<<"AsyncSendDataAsyncSendDataAsyncSendDataAsyncSendData" << std::endl;
    size_t      batch_size    = task.batch_size;
    //std::shared_ptr<meta_data_t[]> meta_data_buf(new meta_data_t[batch_size], std::default_delete<meta_data_t[]>());
    auto meta_data_buf = (meta_data_t *) malloc(sizeof(meta_data_t) * batch_size);
    std::string META_KEY      = "SEND_META_" + std::to_string(task.task_id);
    auto taskcb = task.callback;
    auto data_callback = [&,taskcb] (int status)
    {
        //std::string cur_key = "SEND_KEY_" + std::to_string(slot_id);
        //UnregisterMemRegionBatch(cur_key, batch_size);
       std::cout<<"Send data callback" << std::endl;
       //this->meta_ctx_->unregister_memory_region(META_KEY);
       taskcb();
       std::cout<<"Data has been successfully Send" << std::endl;
    };

    auto meta_callback = [meta_data_buf, batch_size, data_callback, this] (int status)
    {
        std::cout<<"Meta Data RECV" << std::endl;
        uint8_t slot_id = meta_data_buf[0].mr_slot;
        std::cout<<"Meta Data slot_id" << slot_id << std::endl;
        auto    it      = this->send_batch_slot_.find(slot_id);
        if (it != this->send_batch_slot_.end())
        {
            auto send_data = it->second;
            for (size_t i = 0; i < batch_size; ++i)
            {
                send_data[i].remote_addr   = meta_data_buf[i].mr_addr;
                send_data[i].remote_rkey   = meta_data_buf[i].mr_rkey;
                send_data[i].length        = meta_data_buf[i].mr_size;
                std::cout<<meta_data_buf[i].mr_addr << std::endl;
                std::cout<<meta_data_buf[i].mr_rkey << std::endl;
                std::cout<<meta_data_buf[i].mr_size << std::endl;
                send_data[i].target_offset = 0;
            }
            std::cout<<"Meta Data has been successfully RECV" << std::endl;
            auto data_atx = this->data_ctx_->submit(OpCode::WRITE_WITH_IMM, send_data,data_callback);
            //data_atx->wait();

        }
        else
        {
            std::cout << "The data in slot " << meta_data_buf[0].mr_slot << "is not prepared" << std::endl;
            std::cout << "There must be some bugs..." << std::endl;
        }
    };

    {
        std::lock_guard<std::mutex> lock(meta_data_mutex);
        meta_ctx_->register_memory_region(
            META_KEY, reinterpret_cast<uintptr_t>(meta_data_buf), batch_size * sizeof(meta_data_t));

        AssignmentBatch meta_data(batch_size);
        for (size_t i = 0; i < batch_size; ++i)
        {
            meta_data[i] = Assignment(META_KEY, 0, i * sizeof(meta_data_t),sizeof(meta_data_t));
        }

        std::cout << "Meta Data" << meta_data_buf[0].mr_addr << std::endl;
        auto  meta_atx = meta_ctx_->submit(OpCode::RECV, meta_data, meta_callback);
    }

}


void RDMAEndpoint::SyncRecvData()
{
    while (RECV_RUN) {

        AssignmentBatch recv_data_batch;
        if (!recv_buffer_->WaitAndPop(recv_data_batch, std::chrono::milliseconds(500)))
            continue;

        size_t      batch_size    = recv_data_batch.size();
        auto        meta_data_buf = std::make_unique<meta_data_t[]>(batch_size);
        uint8_t     slot_id       = recv_data_batch[0].slot_id;
        std::string META_KEY      = "RECV_META_" + std::to_string(slot_id);

        {
            std::lock_guard<std::mutex> lock(meta_data_mutex);
            for (size_t i = 0; i < batch_size; ++i) {
                meta_data_buf[i].mr_addr =
                    reinterpret_cast<uint64_t>(data_ctx_->memory_pool_->get_mr(recv_data_batch[i].mr_key)->addr);
                meta_data_buf[i].mr_rkey = data_ctx_->memory_pool_->get_mr(recv_data_batch[i].mr_key)->rkey;
                meta_data_buf[i].mr_size = data_ctx_->memory_pool_->get_mr(recv_data_batch[i].mr_key)->length;
                meta_data_buf[i].mr_slot = recv_data_batch[i].slot_id;
            }

            meta_ctx_->register_memory_region(
                META_KEY, reinterpret_cast<uintptr_t>(meta_data_buf.get()), batch_size * sizeof(meta_data_t));
        }
        AssignmentBatch meta_data_assignments(batch_size);
        for (size_t i = 0; i < batch_size; ++i) {
            meta_data_assignments[i] = Assignment(META_KEY, 0, i * sizeof(meta_data_t), sizeof(meta_data_t));
        }
        {
            std::lock_guard<std::mutex> lock(meta_data_mutex);
            auto meta_atx = meta_ctx_->submit(OpCode::SEND, meta_data_assignments);
            meta_atx->wait();

            auto data_atx = data_ctx_->submit(OpCode::RECV, recv_data_batch);
            data_atx->wait();

            std::string cur_key = "RECV_KEY_" + std::to_string(slot_id);
            UnregisterMemRegionBatch(cur_key, batch_size);
            meta_ctx_->unregister_memory_region(META_KEY);
        }
        std::cout << "Data has been successfully Received " << std::endl;
    }
}

void RDMAEndpoint::SyncSendData()
{
    while (SEND_RUN) {

        AssignmentBatch send_data_batch;
        if (!send_buffer_->WaitAndPop(send_data_batch, std::chrono::milliseconds(500)))
            continue;

        size_t      batch_size    = send_data_batch.size();
        auto        meta_data_buf = std::make_unique<meta_data_t[]>(batch_size);
        uint8_t     slot_id       = send_data_batch[0].slot_id;
        std::string META_KEY      = "SEND_META_" + std::to_string(slot_id);
        {
            std::lock_guard<std::mutex> lock(meta_data_mutex);
            meta_ctx_->register_memory_region(
                META_KEY, reinterpret_cast<uintptr_t>(meta_data_buf.get()), batch_size * sizeof(meta_data_t));
        }

        AssignmentBatch meta_data_assignments(batch_size);
        for (size_t i = 0; i < batch_size; ++i) {
            meta_data_assignments[i] = Assignment(META_KEY, 0, i * sizeof(meta_data_t), sizeof(meta_data_t));
        }

        {
            std::lock_guard<std::mutex> lock(meta_data_mutex);
            auto                        meta_atx = meta_ctx_->submit(OpCode::RECV, meta_data_assignments);
            meta_atx->wait();
        }
        {
            std::lock_guard<std::mutex> lock(meta_data_mutex);
            for (size_t i = 0; i < batch_size; ++i) {
                send_data_batch[i].remote_addr   = meta_data_buf[i].mr_addr;
                send_data_batch[i].remote_rkey   = meta_data_buf[i].mr_rkey;
                send_data_batch[i].length        = meta_data_buf[i].mr_size;
                send_data_batch[i].target_offset = 0;
                if (send_data_batch[i].slot_id != meta_data_buf[i].mr_slot)
                    std::cout << "Error in slot it" << std::endl;
            }

            auto data_atx = data_ctx_->submit(OpCode::WRITE_WITH_IMM, send_data_batch);
            data_atx->wait();
            std::string cur_key = "SEND_KEY_" + std::to_string(slot_id);
            UnregisterMemRegionBatch(cur_key, batch_size);
            meta_ctx_->unregister_memory_region(META_KEY);
        }
        std::cout << "Data has been successfully Transmitted " << std::endl;
    }
}

}  // namespace slime
