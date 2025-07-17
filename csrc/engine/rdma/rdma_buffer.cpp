
#include "engine/rdma/rdma_buffer.h"


namespace slime {

    void RDMABuffer::Send()
    {
        send_pending_ = true;
        send_completed_ = false;

        end_point_->AddRDMASendTask(data_ptrs_, data_size_, batch_size_, [this]()
        {
            std::unique_lock<std::mutex> lock(send_mutex_);
            send_completed_ = true;
            send_pending_   = false;
            send_cv_.notify_all();
        });
    }

    void RDMABuffer::Recv()
    {
        recv_pending_ = true;
        recv_completed_ = false;
        end_point_->AddRDMARecvTask(data_ptrs_, data_size_, batch_size_, [this]()
        {
            std::unique_lock<std::mutex> lock(recv_mutex_);
            recv_completed_ = true;
            recv_pending_   = false;
            recv_cv_.notify_all();
        });
    }

    void RDMABuffer::WaitSend()
    {
        std::unique_lock<std::mutex> lock(send_mutex_);

        if(send_completed_)
            return;

        // waiting for the send complete...
        send_cv_.wait(lock, [this] (){ return send_completed_;});
        send_pending_ = false;
        std::cout<<"complete to send the data" << std::endl;

    }

    void RDMABuffer::WaitRecv()
    {
        std::unique_lock<std::mutex> lock(recv_mutex_);

        if(recv_completed_)
            return;

        // waiting for the recv complete...
        recv_cv_.wait(lock, [this] (){ return recv_completed_;});
        recv_pending_ = false;
        std::cout<<"complete to recv the data" << std::endl;
    }





}
