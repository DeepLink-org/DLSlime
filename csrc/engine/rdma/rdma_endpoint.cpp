#include "engine/rdma/rdma_endpoint.h"

namespace slime {

void RDMAEndpoint::LaunchSend(int max_threads)
{
    SEND_RUN = true;
    for (int i = 0; i < max_threads; ++i)
        send_futures_.push_back(std::async(std::launch::async, &RDMAEndpoint::AsyncSendData, this));
    std::cout << max_threads << "Threads of SEND are Started..." << std::endl;
}

void RDMAEndpoint::LaunchRecv(int max_threads)
{
    RECV_RUN = true;
    for (int i = 0; i < max_threads; ++i)
        recv_futures_.push_back(std::async(std::launch::async, &RDMAEndpoint::AsyncRecvData, this));
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

void RDMAEndpoint::AsyncRecvData()
{
    while (RECV_RUN) {
        // if(recv_buffer_->IsEmpty())
        // {
        //     std::this_thread::sleep_for(std::chrono::milliseconds(100));
        //     continue;
        // }

        AssignmentBatch recv_data_batch;
        // recv_buffer_->WaitAndPop(recv_data_batch);
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
            auto                        meta_atx = meta_ctx_->submit(OpCode::SEND, meta_data_assignments);
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

void RDMAEndpoint::AsyncSendData()
{
    while (SEND_RUN) {
        // if(send_buffer_->IsEmpty())
        // {
        //     std::this_thread::sleep_for(std::chrono::milliseconds(100));
        //     continue;
        // }

        AssignmentBatch send_data_batch;
        if (!send_buffer_->WaitAndPop(send_data_batch, std::chrono::milliseconds(500)))
            continue;
        // send_buffer_->WaitAndPop(send_data_batch);

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
