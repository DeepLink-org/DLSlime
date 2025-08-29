#pragma once
#include "engine/metrics.h"
#include "engine/rdma/memory_pool.h"
#include "engine/rdma/rdma_context.h"
#include "engine/rdma/rdma_endpoint.h"

#include <condition_variable>
#include <infiniband/verbs.h>
#include <map>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <unordered_map>
#include <vector>

#include "rdma_common.h"

namespace slime {

class RDMAEndpoint;

class RDMABuffer: public std::enable_shared_from_this<RDMABuffer> {
    friend class RDMAEndpoint;

public:
    RDMABuffer(std::shared_ptr<RDMAEndpoint> endpoint, storage_view_batch_t& batch):
        endpoint_(endpoint), storage_view_batch_(std::move(batch))
    {
    }

    RDMABuffer(std::shared_ptr<RDMAEndpoint> endpoint,
               std::vector<uintptr_t>        ptrs,
               std::vector<size_t>           offset,
               std::vector<size_t>           data_size)
    {
        metrics_      = std::make_shared<rdma_metrics_t>();
        meta_metrics_ = std::make_shared<rdma_metrics_t>();
        data_metrics_ = std::make_shared<rdma_metrics_t>();
        batch_size_   = ptrs.size();
        ptrs_         = ptrs;
        offset_       = offset;
        data_size_    = data_size;
        for (uint32_t i = 0; i < batch_size_; ++i) {
            storage_view_t view{.data_ptr = ptrs[i], .storage_offset = offset[i], .length = data_size[i]};
            storage_view_batch_.push_back(view);
        }
        endpoint_             = endpoint;
        metrics_->buffer_init = std::chrono::steady_clock::now();
    }

    ~RDMABuffer() = default;

    const size_t batchSize()
    {
        return storage_view_batch_.size();
    }

    const storage_view_batch_t& storageViewBatch()
    {
        return storage_view_batch_;
    }

    void send();
    void recv();

    bool waitSend();
    bool waitRecv();

    void send_done_callback();
    void recv_done_callback();

    std::shared_ptr<rdma_metrics_t> metrics_;

    std::shared_ptr<rdma_metrics_t> data_metrics_;
    std::shared_ptr<rdma_metrics_t> meta_metrics_;

    std::chrono::duration<double> meta_ctx_submit_latency()
    {
        return (meta_metrics_->ctx_submit_done - metrics_->endpoint_meta_start);
    }

    std::chrono::duration<double> meta_ctx_WQ_latency()
    {
        return (meta_metrics_->ctx_wq_done - meta_metrics_->ctx_submit_done);
    }

    std::chrono::duration<double> meta_ctx_post_latency()
    {
        return (meta_metrics_->ctx_post_done - meta_metrics_->ctx_wq_done);
    }

    std::chrono::duration<double> meta_ctx_cq_latency()
    {
        return (meta_metrics_->ctx_cq_done - meta_metrics_->ctx_post_done);
    }

    std::chrono::duration<double> data_ctx_submit_latency()
    {
        return (data_metrics_->ctx_submit_done - metrics_->endpoint_meta_done);
    }

    std::chrono::duration<double> data_ctx_WQ_latency()
    {
        return (data_metrics_->ctx_wq_done - data_metrics_->ctx_submit_done);
    }

    std::chrono::duration<double> data_ctx_post_latency()
    {
        return (data_metrics_->ctx_post_done - data_metrics_->ctx_wq_done);
    }

    std::chrono::duration<double> data_ctx_cq_latency()
    {
        return (data_metrics_->ctx_cq_done - data_metrics_->ctx_post_done);
    }

    void print_time()
    {

        // std::cout <<"buffer_init:" << metrics_->latency().count() << " ns\n";

        // std::cout <<"add_tasks:" << metrics_->add_tasks.time_since_epoch().count() << " ns\n";
        // std::cout <<"buffer_callback_done:" << metrics_->buffer_callback_done.time_since_epoch().count()  << " ns\n";
        // std::cout <<"buffer_wait_done:" << metrics_->buffer_wait_done.time_since_epoch().count() << " ns\n";

        // std::cout <<"add_endpoint_tasks:" << metrics_->add_endpoint_tasks.time_since_epoch().count()  << " ns\n";
        // std::cout <<"endpoint_queue_done:" << metrics_->endpoint_queue_done.time_since_epoch().count()  << " ns\n";
        // std::cout <<"endpoint_meta_start:" << metrics_->endpoint_meta_start.time_since_epoch().count()  << " ns\n";
        // std::cout <<"endpoint_meta_done:" << metrics_->endpoint_meta_done.time_since_epoch().count()  << " ns\n";
        // std::cout <<"endpoint_data_start:" << metrics_->endpoint_data_start.time_since_epoch().count()  << " ns\n";
        // std::cout <<"endpoint_data_done:" << metrics_->endpoint_data_done.time_since_epoch().count()  << " ns\n";

        //      std::chrono::steady_clock::time_point ctx_submit_done{std::chrono::milliseconds::zero()};
        // std::chrono::steady_clock::time_point ctx_post_done{std::chrono::milliseconds::zero()};
        // std::chrono::steady_clock::time_point ctx_wq_done{std::chrono::milliseconds::zero()};
        // std::chrono::steady_clock::time_point ctx_cq_done{std::chrono::milliseconds::zero()};

        std::cout << "add_tasks_latency: " << metrics_->add_tasks_latency().count() << std::endl;
        std::cout << "add_endpoint_tasks_latency: " << metrics_->add_endpoint_tasks_latency().count() << std::endl;
        std::cout << "queue_done_latency: " << metrics_->queue_done_latency().count() << std::endl;
        std::cout << "meta_start_latency: " << metrics_->meta_start_latency().count() << std::endl;
        std::cout << "meta_done_latency: " << metrics_->meta_done_latency().count() << std::endl;
        std::cout << "data_start_latency: " << metrics_->data_start_latency().count() << std::endl;
        std::cout << "data_done_latency: " << metrics_->data_done_latency().count() << std::endl;
        std::cout << "callback_done_latency: " << metrics_->callback_done_latency().count() << std::endl;
        std::cout << "wait_done_latency: " << metrics_->wait_done_latency().count() << std::endl;
        std::cout << "meta_ctx_submit_latency: " << meta_ctx_submit_latency().count() << std::endl;
        std::cout << "meta_ctx_WQ_latency: " << meta_ctx_WQ_latency().count() << std::endl;
        std::cout << "meta_ctx_post_latency: " << meta_ctx_post_latency().count() << std::endl;
        std::cout << "meta_ctx_cq_latency: " << meta_ctx_cq_latency().count() << std::endl;
        std::cout << "data_ctx_submit_latency: " << data_ctx_submit_latency().count() << std::endl;
        std::cout << "data_ctx_WQ_latency: " << data_ctx_WQ_latency().count() << std::endl;
        std::cout << "data_ctx_post_latency: " << data_ctx_post_latency().count() << std::endl;
        std::cout << "data_ctx_cq_latency: " << data_ctx_cq_latency().count() << std::endl;
    }
    std::shared_ptr<RDMAEndpoint> endpoint_;
    std::vector<uintptr_t>        ptrs_;
    std::vector<size_t>           offset_;
    std::vector<size_t>           data_size_;

private:
    // <tensor_ptrs_, tensor_size_, offset>
    // tensor_ptrs: the pointer of the tensor
    // tensor_size: the length of the tensor
    // offset: the offset of the transmitted tensor
    // std::vector<std::tuple<uintptr_t, size_t, size_t>> data_info;

    storage_view_batch_t storage_view_batch_;

    size_t batch_size_;

    std::atomic<int> send_pending_{0};
    std::atomic<int> recv_pending_{0};

    std::atomic<int> send_completed_{0};
    std::atomic<int> recv_completed_{0};

    std::condition_variable send_cv_;
    std::condition_variable recv_cv_;

    std::mutex send_mutex_;
    std::mutex recv_mutex_;
};

}  // namespace slime
