#pragma once

#include <asio.hpp>
#include <atomic>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
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

// Thrown by transport operations that exist in the abstract endpoint
// surface (for parity with RDMAEndpoint) but have no analogue on this
// transport. Translated to Python ``NotImplementedError`` by the bindings.
struct not_implemented: public std::logic_error {
    using std::logic_error::logic_error;
};

namespace tcp {

using json = nlohmann::json;

class TcpEndpoint: public std::enable_shared_from_this<TcpEndpoint> {
public:
    static constexpr int64_t kDefaultTimeoutMs = 30000;

    explicit TcpEndpoint(const std::string& ip = "0.0.0.0", uint16_t port = 0);

    TcpEndpoint(TcpContext& ctx, const std::string& ip = "0.0.0.0", uint16_t port = 0) = delete;

    ~TcpEndpoint();

    TcpEndpoint(const TcpEndpoint&)            = delete;
    TcpEndpoint& operator=(const TcpEndpoint&) = delete;

    // ── Connection ──────────────────────────────────────
    json endpoint_info() const;
    void connect(const json& remote_endpoint_info);
    void shutdown();

    // ── Memory ──────────────────────────────────────────
    int32_t register_memory_region(const std::string& name, uintptr_t ptr, uintptr_t offset, size_t length);
    int32_t register_remote_memory_region(const std::string& name, const json& mr_info);
    json    mr_info() const;

    // ── Async I/O (all return Future immediately; I/O runs on io_context thread) ──
    // Method names match RDMAEndpoint so PeerAgent can hold either type
    // without an adapter. ``stream`` exists in the bindings for signature
    // parity with RDMA and is ignored on TCP.

    std::shared_ptr<TcpSendFuture> send(const chunk_tuple_t& chunk, int64_t timeout_ms = kDefaultTimeoutMs);

    std::shared_ptr<TcpRecvFuture> recv(const chunk_tuple_t& chunk, bool exact_size = false);

    std::shared_ptr<TcpReadWriteFuture> read(const std::vector<assign_tuple_t>& assign,
                                             int64_t                            timeout_ms = kDefaultTimeoutMs);

    std::shared_ptr<TcpReadWriteFuture> write(const std::vector<assign_tuple_t>& assign,
                                              int64_t                            timeout_ms = kDefaultTimeoutMs);

    // ── RDMA-only ops; provided for interface parity, never succeed on TCP. ──
    [[noreturn]] void write_with_imm(const std::vector<assign_tuple_t>& /*assign*/, uint32_t /*imm_data*/ = 0)
    {
        throw not_implemented("TCP transport does not support write_with_imm; this is RDMA-only.");
    }

    [[noreturn]] void imm_recv()
    {
        throw not_implemented("TCP transport does not support imm_recv; this is RDMA-only.");
    }

    // ── Accessors ───────────────────────────────────────
    void setId(int64_t id)
    {
        id_.store(id, std::memory_order_relaxed);
    }
    int64_t getId() const
    {
        return id_.load(std::memory_order_relaxed);
    }
    bool is_connected() const
    {
        return connected_.load(std::memory_order_acquire);
    }

private:
    void                       start_io();
    void                       do_accept();
    ServerSession::RecvMatcher make_recv_matcher();

    // ── identity ────────────────────────────────────────
    std::atomic<int64_t> id_{-1};
    std::string          local_host_{"0.0.0.0"};
    uint16_t             local_port_{0};
    std::string          peer_host_;
    uint16_t             peer_port_{0};
    std::atomic<bool>    connected_{false};

    // ── asio core ───────────────────────────────────────
    TcpContext*                 ctx_{nullptr};
    std::unique_ptr<TcpContext> own_ctx_;
    asio::ip::tcp::acceptor     acceptor_;
    std::atomic<bool>           running_{true};

    // ── memory ──────────────────────────────────────────
    std::shared_ptr<TcpMemoryPool> local_pool_;
    std::shared_ptr<TcpMemoryPool> remote_pool_;

    // ── recv matching ───────────────────────────────────
    struct PendingRecv {
        std::shared_ptr<TcpOpState> op_state;
        std::unique_ptr<char[]>     staging_buf;
        uintptr_t                   cuda_dst{0};
        bool                        exact_size{false};
    };
    std::mutex              recv_mu_;
    std::deque<PendingRecv> pending_recvs_;
};

}  // namespace tcp
}  // namespace dlslime
