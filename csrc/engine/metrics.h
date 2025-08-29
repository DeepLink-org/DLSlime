#pragma once

#include <chrono>

namespace slime {
typedef struct RDMAMetrics {


    // buffer
    std::chrono::steady_clock::time_point buffer_init{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point add_tasks{std::chrono::milliseconds::zero()};


    // endpoint
    std::chrono::steady_clock::time_point add_endpoint_tasks{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_queue_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_meta_start{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_meta_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_data_start{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point endpoint_data_done{std::chrono::milliseconds::zero()};

    std::chrono::steady_clock::time_point buffer_callback_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point buffer_wait_done{std::chrono::milliseconds::zero()};

    // context
    std::chrono::steady_clock::time_point ctx_submit_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point ctx_post_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point ctx_wq_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point ctx_cq_before_callback_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point ctx_cq_done{std::chrono::milliseconds::zero()};


    std::chrono::steady_clock::time_point meta_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point data_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point submit_done{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point arrive{std::chrono::milliseconds::zero()};
    std::chrono::steady_clock::time_point done{std::chrono::milliseconds::zero()};

    std::chrono::duration<double> latency()
    {
        return (done - arrive);
    }

    std::chrono::duration<double> add_tasks_latency()
    {
        return (buffer_init - buffer_init);
    }

    std::chrono::duration<double> add_endpoint_tasks_latency()
    {
        return (add_endpoint_tasks - add_tasks);
    }

    std::chrono::duration<double> queue_done_latency()
    {
        return (endpoint_queue_done - add_endpoint_tasks);
    }

    std::chrono::duration<double> meta_start_latency()
    {
        return (endpoint_meta_start - endpoint_queue_done);
    }

    std::chrono::duration<double> meta_done_latency()
    {
        return (endpoint_meta_done - endpoint_meta_start);
    }

    std::chrono::duration<double> data_start_latency()
    {
        return (endpoint_data_start - endpoint_meta_done);
    }

    std::chrono::duration<double> data_done_latency()
    {
        return (endpoint_data_done - endpoint_data_start);
    }

    std::chrono::duration<double> callback_done_latency()
    {
        return (buffer_callback_done - endpoint_meta_start);
    }

    std::chrono::duration<double> wait_done_latency()
    {
        return (buffer_wait_done - buffer_callback_done);
    }


    std::chrono::duration<double> meta_ctx_done_latency()
    {
        return (ctx_submit_done - endpoint_meta_start);
    }


    std::chrono::duration<double> data_ctx_done_latency()
    {
        return (ctx_submit_done - endpoint_data_start);
    }


    std::chrono::duration<double> wq_done_latency()
    {
        return (buffer_wait_done - ctx_submit_done);
    }

    std::chrono::duration<double> post_done_latency()
    {
        return (buffer_wait_done - buffer_callback_done);
    }

    
    std::chrono::duration<double> cq_done_latency()
    {
        return (ctx_cq_done - buffer_callback_done);
    }

} rdma_metrics_t;
}

