#pragma once

#include <utility>
#include <asio.hpp>

#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "tcp_header.h"
#include "tcp_memory_pool.h"
#include "tcp_op_state.h"

namespace dlslime {
namespace tcp {

class TcpConnectionPool;

constexpr size_t kDefaultChunkSize = 65536;  // 64KB

// ── RecvSlot: returned by RecvMatcher when a SEND matches a pending recv ──
struct RecvSlot {
    uintptr_t                   buffer{0};
    size_t                      length{0};
    std::shared_ptr<TcpOpState> op_state;
};

// ── ServerSession: handles incoming requests on one connection ──
//
// Lifecycle: start() → readHeader → dispatch → readBody/writeBody ↻
// Persistent — one session handles many transfers on the same connection.
// Referenced from Mooncake ServerSession.
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
    void readBody(uint64_t remaining);

    asio::ip::tcp::socket socket_;
    TcpMemoryPool*        local_pool_;
    RecvMatcher           recv_matcher_;
    SessionHeader         header_{};
    uint64_t              transferred_{0};
    std::vector<uint8_t>  chunk_buf_;
};

}  // namespace tcp
}  // namespace dlslime
