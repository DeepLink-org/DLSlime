#pragma once

#include <asio.hpp>
#include <cstdint>
#include <functional>
#include <memory>

#include "tcp_header.h"
#include "tcp_memory_pool.h"
#include "tcp_op_state.h"

namespace dlslime {
namespace tcp {

class TcpConnectionPool;

struct RecvSlot {
    uintptr_t                   buffer{0};
    size_t                      length{0};
    std::shared_ptr<TcpOpState> op_state;
    std::function<void()>       post_read;
    bool                        exact_size{false};  // reject send size != recv size
};

// ── ServerSession: handles incoming requests on one persistent connection ──
//
// Lifecycle: start() → readHeader → dispatch → readBody/writeBody → readHeader ↻
class ServerSession: public std::enable_shared_from_this<ServerSession> {
public:
    using RecvMatcher = std::function<RecvSlot()>;

    ServerSession(asio::ip::tcp::socket socket, TcpMemoryPool* local_pool, RecvMatcher recv_matcher);

    void start();

private:
    void readHeader();
    void dispatch();
    void readBody(void* dst, size_t len);
    void writeBody(const void* src, size_t len);

    asio::ip::tcp::socket socket_;
    TcpMemoryPool*        local_pool_;
    RecvMatcher           recv_matcher_;
    SessionHeader         header_{};
};

// ── ClientSession: drives one outbound I/O operation ─────
//
// Lifecycle: construct → start_write/start_read → on_done → self-destruct
// Does NOT own OpState or PooledConnection — only drives the I/O and reports ec.
class ClientSession: public std::enable_shared_from_this<ClientSession> {
public:
    using DoneCallback = std::function<void(asio::error_code ec)>;

    ClientSession(asio::ip::tcp::socket sock, DoneCallback on_done);

    // Write header + payload to socket (gather async_write).
    void start_write(const SessionHeader& hdr, const void* payload);

    // Write OP_READ header → read raw response into dst.
    void start_read(const SessionHeader& hdr, void* dst);

private:
    asio::ip::tcp::socket socket_;
    DoneCallback          on_done_;
    SessionHeader         hdr_{};
};

}  // namespace tcp
}  // namespace dlslime
