#include "tcp_endpoint.h"

#include <endian.h>
#include <netinet/tcp.h>

#include <cstring>
#include <utility>

#include "dlslime/csrc/logging.h"

namespace dlslime {
namespace tcp {

using tcp = asio::ip::tcp;

// ── helpers ─────────────────────────────────────────────

static void hdr_hton(SessionHeader& h) {
    h.size = htole64(h.size);
    h.addr = htole64(h.addr);
}

// ── RecvMatcher factory ────────────────────────────────

ServerSession::RecvMatcher TcpEndpoint::make_recv_matcher() {
    std::weak_ptr<TcpEndpoint> weak = shared_from_this();
    return [weak]() -> RecvSlot {
        auto self = weak.lock();
        if (!self) return {};
        std::lock_guard<std::mutex> lk(self->recv_mu_);
        if (self->pending_recvs_.empty()) return {};
        auto pr = std::move(self->pending_recvs_.front());
        self->pending_recvs_.pop_front();
        return {pr.op_state->user_buffer, pr.op_state->user_length, pr.op_state};
    };
}

// ── Constructor ────────────────────────────────────────

TcpEndpoint::TcpEndpoint(const std::string& ip, uint16_t port)
    : own_ctx_(std::make_unique<TcpContext>())
    , acceptor_(own_ctx_->io_context())
    , local_pool_(std::make_shared<TcpMemoryPool>())
    , remote_pool_(std::make_shared<TcpMemoryPool>())
    , local_host_(ip) {
    ctx_ = own_ctx_.get();
    local_port_ = port;
    start_io();
}

TcpEndpoint::~TcpEndpoint() {
    shutdown();
}

void TcpEndpoint::start_io() {
    auto addr = asio::ip::make_address(local_host_);
    auto ep   = tcp::endpoint(addr, local_port_);
    acceptor_.open(ep.protocol());
    acceptor_.set_option(tcp::acceptor::reuse_address(true));
    acceptor_.bind(ep);
    acceptor_.listen(64);

    if (local_port_ == 0) {
        asio::error_code ec;
        local_port_ = acceptor_.local_endpoint(ec).port();
    }

    do_accept();
}

// ── do_accept ───────────────────────────────────────────

void TcpEndpoint::do_accept() {
    if (!running_.load(std::memory_order_acquire)) return;
    acceptor_.async_accept(
        [this](asio::error_code ec, tcp::socket sock) {
            if (ec) {
                if (ec != asio::error::operation_aborted)
                    SLIME_LOG_WARN("TcpEndpoint accept: ", ec.message());
                return;
            }
            sock.set_option(tcp::no_delay(true));
            auto session = std::make_shared<ServerSession>(
                std::move(sock), local_pool_.get(), make_recv_matcher());
            session->start();
            do_accept();
        });
}

// ── endpoint_info / connect ─────────────────────────────

json TcpEndpoint::endpoint_info() const {
    return {
        {"host", local_host_},
        {"port", local_port_},
        {"mr_info", local_pool_->mr_info()}
    };
}

json TcpEndpoint::mr_info() const {
    return local_pool_->mr_info();
}

void TcpEndpoint::connect(const json& remote_endpoint_info) {
    auto host = remote_endpoint_info.value("host", "");
    auto port = static_cast<uint16_t>(remote_endpoint_info.value("port", 0));

    // Verify reachability before accepting the peer identity.
    auto conn = ctx_->conn_pool().getConnection(host, port);
    if (!conn) {
        SLIME_LOG_WARN("TcpEndpoint::connect: cannot reach ", host, ":", port);
        return;
    }

    peer_host_ = host;
    peer_port_ = port;
    if (remote_endpoint_info.contains("mr_info")) {
        for (const auto& [name, info] : remote_endpoint_info["mr_info"].items())
            remote_pool_->register_remote_memory_region(info, name);
    }
    connected_.store(true, std::memory_order_release);
    ctx_->conn_pool().returnConnection(std::move(conn));
}

// ── memory registration ─────────────────────────────────

int32_t TcpEndpoint::register_memory_region(const std::string& name,
                                             uintptr_t ptr, size_t length) {
    return local_pool_->register_memory_region(ptr, length, name);
}

int32_t TcpEndpoint::register_remote_memory_region(const std::string& name,
                                                    const json& mr_info) {
    return remote_pool_->register_remote_memory_region(mr_info, name);
}

// ── async_send ──────────────────────────────────────────
// chunk_tuple_t = (src_ptr, offset, length) — raw pointers, no MR lookup.

std::shared_ptr<TcpSendFuture>
TcpEndpoint::async_send(const chunk_tuple_t& chunk, int64_t /*timeout_ms*/) {
    uintptr_t src = std::get<0>(chunk) + std::get<1>(chunk);
    size_t    len = std::get<2>(chunk);

    auto conn = ctx_->conn_pool().getConnection(peer_host_, peer_port_);
    auto op   = TcpOpState::create();
    op->signal->reset_all();

    if (!conn) {
        op->completion_status.store(TCP_FAILED, std::memory_order_release);
        op->signal->force_complete();
        return std::make_shared<TcpSendFuture>(op);
    }

    SessionHeader hdr{len, 0, OP_SEND};
    auto& pool = ctx_->conn_pool();

    auto session = std::make_shared<ClientSession>(
        std::move(conn->socket),
        [op, conn, &pool](asio::error_code ec) {
            op->completion_status.store(
                ec ? TCP_FAILED : TCP_SUCCESS, std::memory_order_release);
            if (op->signal) op->signal->set_comm_done(0);
            pool.returnConnection(conn);
        });
    session->start_write(hdr, reinterpret_cast<const void*>(src));

    return std::make_shared<TcpSendFuture>(op);
}

// ── async_recv ──────────────────────────────────────────
// chunk_tuple_t = (dst_ptr, offset, length) — raw pointers, no MR lookup.

std::shared_ptr<TcpRecvFuture>
TcpEndpoint::async_recv(const chunk_tuple_t& chunk) {
    auto op = TcpOpState::create();
    op->signal->reset_all();
    op->user_buffer = std::get<0>(chunk) + std::get<1>(chunk);
    op->user_length = std::get<2>(chunk);

    {
        std::lock_guard<std::mutex> lk(recv_mu_);
        pending_recvs_.push_back({op});
    }

    return std::make_shared<TcpRecvFuture>(op);
}

// ── async_read ──────────────────────────────────────────

std::shared_ptr<TcpReadWriteFuture>
TcpEndpoint::async_read(const std::vector<assign_tuple_t>& assign,
                         int64_t /*timeout_ms*/) {
    if (assign.empty())
        throw std::runtime_error("TcpEndpoint::async_read: empty assignment");

    const auto& a = assign[0];
    int32_t  local_h   = static_cast<int32_t>(std::get<0>(a));
    int32_t  remote_h  = static_cast<int32_t>(std::get<1>(a));
    uint64_t remote_off = std::get<2>(a);
    uint64_t local_off  = std::get<3>(a);
    size_t   length     = std::get<4>(a);

    auto local  = local_pool_->get_mr_fast(local_h);
    auto remote = remote_pool_->get_remote_mr_fast(remote_h);
    if (local.length == 0 || remote.length == 0)
        throw std::runtime_error("TcpEndpoint::async_read: invalid MR handle");

    auto op = TcpOpState::create();
    op->signal->reset_all();
    op->user_buffer = local.addr + local_off;
    op->user_length = length;

    auto conn = ctx_->conn_pool().getConnection(peer_host_, peer_port_);
    if (!conn) {
        op->completion_status.store(TCP_FAILED, std::memory_order_release);
        op->signal->force_complete();
        return std::make_shared<TcpReadWriteFuture>(op);
    }

    SessionHeader hdr{length, remote.addr + remote_off, OP_READ};
    auto& pool = ctx_->conn_pool();

    auto session = std::make_shared<ClientSession>(
        std::move(conn->socket),
        [op, conn, &pool](asio::error_code ec) {
            op->completion_status.store(
                ec ? TCP_FAILED : TCP_SUCCESS, std::memory_order_release);
            if (op->signal) op->signal->set_comm_done(0);
            pool.returnConnection(conn);
        });
    session->start_read(hdr, reinterpret_cast<void*>(op->user_buffer));

    return std::make_shared<TcpReadWriteFuture>(op);
}

// ── async_write ─────────────────────────────────────────

std::shared_ptr<TcpReadWriteFuture>
TcpEndpoint::async_write(const std::vector<assign_tuple_t>& assign,
                          int64_t /*timeout_ms*/) {
    if (assign.empty())
        throw std::runtime_error("TcpEndpoint::async_write: empty assignment");

    const auto& a = assign[0];
    int32_t  local_h   = static_cast<int32_t>(std::get<0>(a));
    int32_t  remote_h  = static_cast<int32_t>(std::get<1>(a));
    uint64_t remote_off = std::get<2>(a);
    uint64_t local_off  = std::get<3>(a);
    size_t   length     = std::get<4>(a);

    auto local  = local_pool_->get_mr_fast(local_h);
    auto remote = remote_pool_->get_remote_mr_fast(remote_h);
    if (local.length == 0 || remote.length == 0)
        throw std::runtime_error("TcpEndpoint::async_write: invalid MR handle");

    uintptr_t src = local.addr + local_off;

    auto conn = ctx_->conn_pool().getConnection(peer_host_, peer_port_);
    auto op   = TcpOpState::create();
    op->signal->reset_all();

    if (!conn) {
        op->completion_status.store(TCP_FAILED, std::memory_order_release);
        op->signal->force_complete();
        return std::make_shared<TcpReadWriteFuture>(op);
    }

    SessionHeader hdr{length, remote.addr + remote_off, OP_WRITE};
    auto& pool = ctx_->conn_pool();

    auto session = std::make_shared<ClientSession>(
        std::move(conn->socket),
        [op, conn, &pool](asio::error_code ec) {
            op->completion_status.store(
                ec ? TCP_FAILED : TCP_SUCCESS, std::memory_order_release);
            if (op->signal) op->signal->set_comm_done(0);
            pool.returnConnection(conn);
        });
    session->start_write(hdr, reinterpret_cast<const void*>(src));

    return std::make_shared<TcpReadWriteFuture>(op);
}

// ── shutdown ────────────────────────────────────────────

void TcpEndpoint::shutdown() {
    bool expected = true;
    if (!running_.compare_exchange_strong(expected, false))
        return;

    connected_.store(false, std::memory_order_release);
    acceptor_.close();

    {
        std::lock_guard<std::mutex> lk(recv_mu_);
        for (auto& pr : pending_recvs_) {
            if (pr.op_state && pr.op_state->signal) {
                pr.op_state->completion_status.store(TCP_CLOSED, std::memory_order_release);
                pr.op_state->signal->force_complete();
            }
        }
        pending_recvs_.clear();
    }

    if (own_ctx_)
        own_ctx_->shutdown();
}

}  // namespace tcp
}  // namespace dlslime
