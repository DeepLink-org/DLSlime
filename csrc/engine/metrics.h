#pragma once

#include <chrono>

namespace slime {
typedef struct RDMAMetrics {

    // buffer
    std::chrono::steady_clock::time_point buffer_init{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point buffer_add_task{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point buffer_callback{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point buffer_wait_done{std::chrono::nanoseconds::zero()};

    // endpoint
    std::chrono::steady_clock::time_point endpoint_queue_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_meta_start{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_meta_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_data_start{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_data_done{std::chrono::nanoseconds::zero()};

    // context
    std::chrono::steady_clock::time_point ctx_submit_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point ctx_wq_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point ctx_post_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point ctx_cq_done{std::chrono::nanoseconds::zero()};

    std::chrono::steady_clock::time_point meta_ctx_submit_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point meta_ctx_wq_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point meta_ctx_post_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point meta_ctx_cq_done{std::chrono::nanoseconds::zero()};

    std::chrono::steady_clock::time_point data_ctx_submit_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point data_ctx_wq_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point data_ctx_post_done{std::chrono::nanoseconds::zero()};
    std::chrono::steady_clock::time_point data_ctx_cq_done{std::chrono::nanoseconds::zero()};

    std::chrono::duration<double> latency()
    {
        return std::chrono::duration<double>(std::chrono::duration_cast<std::chrono::microseconds>(ctx_cq_done - ctx_submit_done).count() / 1e6);
    }


    int64_t get_time_nano(std::chrono::steady_clock::time_point start, std::chrono::steady_clock::time_point end)
    {
        return std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    }

    // Buffer latencies
    int64_t buffer_add_tasks_latency()
    {
        return get_time_nano(buffer_init, buffer_add_task);
    }


    // Endpoint latencies
    int64_t endpoint_add_tasks_latency()
    {
        return get_time_nano(buffer_add_task, endpoint_queue_done);
    }

    int64_t endpoint_meta_start_latency()
    {
        return get_time_nano(endpoint_queue_done, endpoint_meta_start);
    }

    int64_t endpoint_meta_done_latency()
    {
        return get_time_nano(endpoint_meta_start, endpoint_meta_done);
    }

    int64_t endpoint_data_start_latency()
    {
        return get_time_nano(endpoint_meta_done, endpoint_data_start);
    }

    int64_t endpoint_data_done_latency()
    {
        return get_time_nano(endpoint_data_start, endpoint_data_done);
    }

    // Context latencies
    int64_t meta_ctx_sq_latency()
    {
        return get_time_nano(meta_ctx_submit_done, meta_ctx_wq_done);
    }

    int64_t meta_ctx_post_latency()
    {
        return get_time_nano(meta_ctx_wq_done, meta_ctx_post_done);
    }

    int64_t meta_ctx_cq_latency()
    {
        return get_time_nano(meta_ctx_post_done, meta_ctx_cq_done);
    }

    int64_t data_ctx_sq_latency()
    {
        return get_time_nano(data_ctx_submit_done, data_ctx_wq_done);
    }

    int64_t data_ctx_post_latency()
    {
        return get_time_nano(data_ctx_wq_done, data_ctx_post_done);
    }
    
    int64_t data_ctx_cq_latency()
    {
        return get_time_nano(data_ctx_post_done, data_ctx_cq_done);
    }


} rdma_metrics_t;
}

