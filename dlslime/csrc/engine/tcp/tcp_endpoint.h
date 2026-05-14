#pragma once

#include <utility>
#include <asio.hpp>

#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>

#include "dlslime/csrc/common/json.hpp"
#include "dlslime/csrc/engine/assignment.h"
#include "tcp_connection_pool.h"
#include "tcp_context.h"
#include "tcp_future.h"
#include "tcp_header.h"
#include "tcp_memory_pool.h"
#include "tcp_op_state.h"
#include "tcp_session.h"

namespace dlslime {
namespace tcp {

using json = nlohmann::json;

class TcpEndpoint : public std::enable_shared_from_this<TcpEndpoint> {
public:
    static constexpr int64_t kDefaultTimeoutMs = 30000;

    // 【主构造】自包含 TcpContext (最常用)
    explicit TcpEndpoint(uint16_t port = 0);

    // 【次构造】共享 TcpContext (多 endpoint 复用单 io_context 线程)
    TcpEndpoint(TcpContext& ctx, uint16_t port = 0);

    ~TcpEndpoint();

    TcpEndpoint(const TcpEndpoint&) = delete;
    TcpEndpoint& operator=(const TcpEndpoint&) = delete;

    // ── Connection ──────────────────────────────────────
    json endpoint_info() const;
    void connect(const json& remote_info);
    void shutdown();

    // ── Memory ──────────────────────────────────────────
    int32_t register_memory_region(const std::string& name,
                                   uintptr_t ptr, uintptr_t offset, size_t length);
    int32_t register_remote_memory_region(const std::string& name,
                                          const json& mr_info);
    json mr_info() const;

    // ── Async I/O (all return Future immediately; I/O runs on io_context thread) ──

    // Bilateral send.  timeout_ms controls socket write timeout (SO_SNDTIMEO).
    std::shared_ptr<TcpSendFuture> async_send(
        const chunk_tuple_t& chunk,
        int64_t timeout_ms = kDefaultTimeoutMs);

    // Bilateral recv.  Timeout via future.wait_for().
    std::shared_ptr<TcpRecvFuture> async_recv(
        const chunk_tuple_t& chunk);

    // Unilateral read: request remote to send data from registered buffer.
    std::shared_ptr<TcpReadWriteFuture> async_read(
        const std::vector<assign_tuple_t>& assign,
        int64_t timeout_ms = kDefaultTimeoutMs);

    // Unilateral write: push data to remote registered buffer.
    std::shared_ptr<TcpReadWriteFuture> async_write(
        const std::vector<assign_tuple_t>& assign,
        int64_t timeout_ms = kDefaultTimeoutMs);

    // ── Accessors ───────────────────────────────────────
    void    setId(int64_t id) { id_.store(id, std::memory_order_relaxed); }
    int64_t getId() const    { return id_.load(std::memory_order_relaxed); }
    bool    is_connected() const { return connected_.load(std::memory_order_acquire); }

private:
    void start_io();
    void do_accept();
    ServerSession::RecvMatcher make_recv_matcher();

    bool is_initiator(const std::string& peer_host, uint16_t peer_port) const;
    bool write_message(asio::ip::tcp::socket& sock,
                       const SessionHeader& hdr, const void* payload);

    // ── identity ────────────────────────────────────────
    std::atomic<int64_t> id_{-1};
    std::string local_host_{"0.0.0.0"};
    uint16_t    local_port_{0};
    std::string peer_host_;
    uint16_t    peer_port_{0};
    std::atomic<bool> connected_{false};

    // ── asio core ───────────────────────────────────────
    TcpContext*                    ctx_{nullptr};
    std::unique_ptr<TcpContext>    own_ctx_;
    asio::ip::tcp::acceptor        acceptor_;
    std::atomic<bool>              running_{true};

    // ── memory ──────────────────────────────────────────
    std::shared_ptr<TcpMemoryPool> local_pool_;
    std::shared_ptr<TcpMemoryPool> remote_pool_;

    // ── recv matching ───────────────────────────────────
    struct PendingRecv {
        std::shared_ptr<TcpOpState> op_state;
    };
    std::mutex             recv_mu_;
    std::deque<PendingRecv> pending_recvs_;

    // ── read matching ───────────────────────────────────
    struct PendingRead {
        std::shared_ptr<PooledConnection> conn;
        std::shared_ptr<TcpOpState>       op_state;
    };
    std::mutex read_mu_;
    std::unordered_map<uint64_t, PendingRead> pending_reads_;
    std::atomic<uint64_t> next_req_id_{1};
};

}  // namespace tcp
}  // namespace dlslime
