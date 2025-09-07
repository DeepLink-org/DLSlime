
#include "engine/rdma/rdma_buffer.h"

namespace slime {

void RDMABuffer::send()
{
    metrics_->buffer_add_task = std::chrono::steady_clock::now();
    endpoint_->addSendTask(shared_from_this());
}

void RDMABuffer::recv()
{
    metrics_->buffer_add_task = std::chrono::steady_clock::now();
    endpoint_->addRecvTask(shared_from_this());
}

void RDMABuffer::send_done_callback()
{
    std::unique_lock<std::mutex> lock(send_mutex_);
    ++send_completed_;
    metrics_->buffer_callback = std::chrono::steady_clock::now();
    send_cv_.notify_all();
}

void RDMABuffer::recv_done_callback()
{
    std::unique_lock<std::mutex> lock(recv_mutex_);
    ++recv_completed_;
    metrics_->buffer_callback = std::chrono::steady_clock::now();
    recv_cv_.notify_all();
}

bool RDMABuffer::waitSend()
{
    std::unique_lock<std::mutex> lock(send_mutex_);

    if (send_completed_)
        return send_completed_;

    send_cv_.wait(lock, [this]() { return send_completed_ > 0; });
    send_pending_ = false;
    //SLIME_LOG_INFO("complete to send the data.");
    metrics_->buffer_wait_done = std::chrono::steady_clock::now();
    get_time();
    return send_completed_;
}

bool RDMABuffer::waitRecv()
{
    std::unique_lock<std::mutex> lock(recv_mutex_);

    if (recv_completed_)
        return recv_completed_;

    recv_cv_.wait(lock, [this]() { return recv_completed_ > 0; });
    recv_pending_ = false;
    //SLIME_LOG_INFO("complete to recv the data.");
    metrics_->buffer_wait_done = std::chrono::steady_clock::now();
    get_time();
    return recv_completed_;
}


void RDMABuffer::get_time()
{
      std::cout <<"=================  =================  =================  =================   ================="<< std::endl;
    std::cout <<"================= RDMABuffer Metrics ================="<< std::endl;
    std::cout << "buffer add tasks latency: "   << metrics_->get_time_nano(metrics_->buffer_init, metrics_->buffer_add_task) << " us" << std::endl;
    std::cout << "buffer callback  latency: "   << metrics_->get_time_nano(metrics_->buffer_add_task, metrics_->buffer_callback) << " us" << std::endl;
    std::cout << "buffer wait done latency: "   << metrics_->get_time_nano(metrics_->buffer_callback, metrics_->buffer_wait_done) << " us" << std::endl;
    std::cout <<"================= RDMAEndPoint Metrics ================="<< std::endl;
    std::cout << "endpoint queue add tasks latency: " << metrics_->get_time_nano(metrics_->buffer_add_task, metrics_->endpoint_queue_done) << " us" << std::endl;
    // std::cout << "endpoint  add tasks latency: " << metrics_->get_time_nano(metrics_->endpoint_meta_start, metrics_->endpoint_meta_done) << " us" << std::endl;
    // std::cout << "endpoint queue add tasks latency: " << metrics_->get_time_nano(metrics_->endpoint_meta_done, metrics_->endpoint_data_done) << " us" << std::endl;
    std::cout <<"================= RDMAContext Metrics ================="<< std::endl;
    std::cout << "meta ctx submit done latency: " << meta_metrics_->get_time_nano(meta_metrics_->ctx_submit_done, meta_metrics_->ctx_wq_done) << " us" << std::endl;
    std::cout << "meta ctx wq done latency: "     << meta_metrics_->get_time_nano(meta_metrics_->ctx_wq_done, meta_metrics_->ctx_post_done) << " us" << std::endl;
    std::cout << "meta ctx cq done latency: "     << meta_metrics_->get_time_nano(meta_metrics_->ctx_post_done, meta_metrics_->ctx_cq_done) << " us" << std::endl;      
    std::cout << "data ctx submit done latency: " << data_metrics_->get_time_nano(data_metrics_->ctx_submit_done, data_metrics_->ctx_wq_done) << " us" << std::endl;
    std::cout << "data ctx wq done latency: "     << data_metrics_->get_time_nano(data_metrics_->ctx_wq_done, data_metrics_->ctx_post_done) << " us" << std::endl;
    std::cout << "data ctx cq done latency: "     << data_metrics_->get_time_nano(data_metrics_->ctx_post_done, data_metrics_->ctx_cq_done) << " us" << std::endl; 



    // SLIME_LOG_INFO("Buffer add tasks latency: {} ns", metrics_->buffer_add_tasks_latency());
    // SLIME_LOG_INFO("Endpoint add tasks latency: {} ns", metrics_->endpoint_add_tasks_latency());
    // SLIME_LOG_INFO("Endpoint meta start latency: {} ns", metrics_->endpoint_meta_start_latency());
    // SLIME_LOG_INFO("Endpoint meta done latency: {} ns", metrics_->endpoint_meta_done_latency());
    // SLIME_LOG_INFO("Endpoint data start latency: {} ns", metrics_->endpoint_data_start_latency());
    // SLIME_LOG_INFO("Endpoint data done latency: {} ns", metrics_->endpoint_data_done_latency());
}

}  // namespace slime
