#pragma once

#include <utility>
#include <asio.hpp>

#include <memory>
#include <thread>

#include "tcp_connection_pool.h"

namespace dlslime {
namespace tcp {

// Process-level shared resource holder.
// Multiple TcpEndpoints can share one TcpContext to run on a single
// io_context thread, reducing thread count.
//
// For sync wrappers: sync_send() = async_send() + future.wait()
// — the io_context drives the I/O, the caller thread just blocks.
class TcpContext {
public:
    TcpContext();
    ~TcpContext();

    TcpContext(const TcpContext&) = delete;
    TcpContext& operator=(const TcpContext&) = delete;

    asio::io_context& io_context() { return io_ctx_; }
    TcpConnectionPool& conn_pool() { return conn_pool_; }

    void shutdown();

private:
    asio::io_context  io_ctx_;
    std::thread       io_thread_;
    TcpConnectionPool conn_pool_{io_ctx_};
    bool              running_{true};
};

}  // namespace tcp
}  // namespace dlslime
