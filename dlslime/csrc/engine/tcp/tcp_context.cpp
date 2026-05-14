#include "tcp_context.h"

namespace dlslime {
namespace tcp {

TcpContext::TcpContext() {
    // Keep io_context alive even when there's no work posted yet.
    auto work = asio::make_work_guard(io_ctx_);
    io_thread_ = std::thread([this, w = std::move(work)]() {
        io_ctx_.run();
    });
}

TcpContext::~TcpContext() {
    shutdown();
}

void TcpContext::shutdown() {
    if (!running_) return;
    running_ = false;
    io_ctx_.stop();
    if (io_thread_.joinable()) io_thread_.join();
    conn_pool_.clear();
}

}  // namespace tcp
}  // namespace dlslime
