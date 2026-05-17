#pragma once

#include <utility>
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
};

// ServerSession: handles incoming requests on one persistent connection.
// Lifecycle: start() → readHeader → dispatch → readBody/writeBody → readHeader ↻
class ServerSession : public std::enable_shared_from_this<ServerSession> {
public:
    using RecvMatcher = std::function<RecvSlot()>;

    ServerSession(asio::ip::tcp::socket socket,
                  TcpMemoryPool*         local_pool,
                  RecvMatcher            recv_matcher);

    void start();

private:
    void readHeader();
    void dispatch();
    void readBody(void* dst, size_t len);             // read into caller's buffer
    void writeBody(const void* src, size_t len);      // write from caller's buffer

    asio::ip::tcp::socket socket_;
    TcpMemoryPool*        local_pool_;
    RecvMatcher           recv_matcher_;
    SessionHeader         header_{};
};

}  // namespace tcp
}  // namespace dlslime
