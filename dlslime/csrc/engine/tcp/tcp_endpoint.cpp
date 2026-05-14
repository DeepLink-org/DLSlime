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

TcpEndpoint::TcpEndpoint(uint16_t port)
    : own_ctx_(std::make_unique<TcpContext>())
    , acceptor_(own_ctx_->io_context())
    , local_pool_(std::make_shared<TcpMemoryPool>())
    , remote_pool_(std::make_shared<TcpMemoryPool>()) {
    ctx_ = own_ctx_.get();
    local_port_ = port;
    start_io();
}

TcpEndpoint::TcpEndpoint(TcpContext& ctx, uint16_t port)
    : acceptor_(ctx.io_context())
    , local_pool_(std::make_shared<TcpMemoryPool>())
    , remote_pool_(std::make_shared<TcpMemoryPool>()) {
    ctx_ = &ctx;
    local_port_ = port;
    start_io();
}

TcpEndpoint::~TcpEndpoint() {
    shutdown();
}

void TcpEndpoint::start_io() {
    auto ep = tcp::endpoint(tcp::v4(), local_port_);
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

bool TcpEndpoint::is_initiator(const std::string& peer_host,
                                uint16_t peer_port) const {
    int cmp = local_host_.compare(peer_host);
    if (cmp != 0) return cmp > 0;
    return local_port_ > peer_port;
}

void TcpEndpoint::connect(const json& remote_info) {
    if (connected_.load(std::memory_order_acquire)) return;

    peer_host_ = remote_info.value("host", "");
    peer_port_ = static_cast<uint16_t>(remote_info.value("port", 0));

    if (remote_info.contains("mr_info")) {
        for (const auto& [name, info] : remote_info["mr_info"].items())
            remote_pool_->register_remote_memory_region(info, name);
    }

    if (is_initiator(peer_host_, peer_port_)) {
        auto conn = ctx_->conn_pool().getConnection(peer_host_, peer_port_);
        if (conn) ctx_->conn_pool().returnConnection(std::move(conn));
    }

    connected_.store(true, std::memory_order_release);
}

// ── memory registration ─────────────────────────────────

int32_t TcpEndpoint::register_memory_region(const std::string& name,
                                             uintptr_t ptr, uintptr_t offset,
                                             size_t length) {
    return local_pool_->register_memory_region(ptr, offset, length, name);
}

int32_t TcpEndpoint::register_remote_memory_region(const std::string& name,
                                                    const json& mr_info) {
    return remote_pool_->register_remote_memory_region(mr_info, name);
}

// ── write_message ───────────────────────────────────────

bool TcpEndpoint::write_message(tcp::socket& sock,
                                 const SessionHeader& hdr,
                                 const void* payload) {
    asio::error_code ec;
    SessionHeader net = hdr;
    hdr_hton(net);
    std::array<asio::const_buffer, 2> bufs = {
        asio::buffer(&net, sizeof(net)),
        asio::buffer(payload, hdr.size)
    };
    asio::write(sock, bufs, ec);
    return !ec;
}

// ── async_send ──────────────────────────────────────────

std::shared_ptr<TcpSendFuture>
TcpEndpoint::async_send(const chunk_tuple_t& chunk, int64_t timeout_ms) {
    auto mr = local_pool_->get_mr_fast(static_cast<int32_t>(std::get<0>(chunk)));
    if (mr.length == 0)
        throw std::runtime_error("TcpEndpoint::async_send: invalid local MR");

    uintptr_t src = mr.addr + mr.offset + std::get<1>(chunk);
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

    std::weak_ptr<TcpEndpoint> weak = weak_from_this();
    asio::post(ctx_->io_context(), [weak, conn, op, hdr, src, len, &pool]() {
        auto ep = weak.lock();
        if (!ep) {
            op->completion_status.store(TCP_CLOSED, std::memory_order_release);
            if (op->signal) op->signal->force_complete();
            return;
        }

        asio::error_code ec;
        SessionHeader net = hdr;
        hdr_hton(net);
        std::array<asio::const_buffer, 2> bufs = {
            asio::buffer(&net, sizeof(net)),
            asio::buffer(reinterpret_cast<const void*>(src), len)
        };
        asio::async_write(conn->socket, bufs,
            [conn, op, &pool](asio::error_code ec, size_t) {
                op->completion_status.store(
                    ec ? TCP_FAILED : TCP_SUCCESS, std::memory_order_release);
                if (op->signal) op->signal->set_comm_done(0);
                pool.returnConnection(conn);
            });
    });

    return std::make_shared<TcpSendFuture>(op);
}

// ── async_recv ──────────────────────────────────────────

std::shared_ptr<TcpRecvFuture>
TcpEndpoint::async_recv(const chunk_tuple_t& chunk) {
    auto mr = local_pool_->get_mr_fast(static_cast<int32_t>(std::get<0>(chunk)));
    if (mr.length == 0)
        throw std::runtime_error("TcpEndpoint::async_recv: invalid local MR");

    auto op = TcpOpState::create();
    op->signal->reset_all();
    op->user_buffer = mr.addr + mr.offset + std::get<1>(chunk);
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
    int32_t  local_h  = static_cast<int32_t>(std::get<0>(a));
    int32_t  remote_h = static_cast<int32_t>(std::get<1>(a));
    uint64_t remote_off = std::get<2>(a);
    uint64_t local_off  = std::get<3>(a);
    size_t   length     = std::get<4>(a);

    auto local  = local_pool_->get_mr_fast(local_h);
    auto remote = remote_pool_->get_remote_mr_fast(remote_h);
    if (local.length == 0 || remote.length == 0)
        throw std::runtime_error("TcpEndpoint::async_read: invalid MR handle");

    auto op = TcpOpState::create();
    op->signal->reset_all();
    op->user_buffer = local.addr + local.offset + local_off;
    op->user_length = length;

    auto conn = ctx_->conn_pool().getConnection(peer_host_, peer_port_);
    if (!conn) {
        op->completion_status.store(TCP_FAILED, std::memory_order_release);
        op->signal->force_complete();
        return std::make_shared<TcpReadWriteFuture>(op);
    }

    uint64_t req_id = next_req_id_.fetch_add(1, std::memory_order_relaxed);
    {
        std::lock_guard<std::mutex> lk(read_mu_);
        pending_reads_[req_id] = {conn, op};
    }

    SessionHeader hdr{length, remote.addr + remote.offset + remote_off, OP_READ};
    auto& pool = ctx_->conn_pool();

    std::weak_ptr<TcpEndpoint> weak = weak_from_this();
    asio::post(ctx_->io_context(), [weak, conn, op, hdr, req_id, &pool]() {
        auto ep = weak.lock();
        if (!ep) {
            op->completion_status.store(TCP_CLOSED, std::memory_order_release);
            if (op->signal) op->signal->force_complete();
            return;
        }

        SessionHeader net = hdr;
        hdr_hton(net);
        asio::async_write(conn->socket,
            asio::buffer(&net, sizeof(net)),
            [weak, conn, op, req_id, &pool](asio::error_code ec, size_t) {
                if (ec) {
                    op->completion_status.store(TCP_FAILED, std::memory_order_release);
                    if (op->signal) op->signal->set_comm_done(0);
                    pool.returnConnection(conn);
                    auto self = weak.lock();
                    if (self) {
                        std::lock_guard<std::mutex> lk(self->read_mu_);
                        self->pending_reads_.erase(req_id);
                    }
                    return;
                }

                asio::async_read(conn->socket,
                    asio::buffer(reinterpret_cast<void*>(op->user_buffer),
                                 op->user_length),
                    [weak, conn, op, req_id, &pool](asio::error_code ec, size_t n) {
                        op->bytes_copied = n;
                        op->completion_status.store(
                            ec ? TCP_FAILED : TCP_SUCCESS,
                            std::memory_order_release);
                        if (op->signal) op->signal->set_comm_done(0);
                        pool.returnConnection(conn);
                        auto self = weak.lock();
                        if (self) {
                            std::lock_guard<std::mutex> lk(self->read_mu_);
                            self->pending_reads_.erase(req_id);
                        }
                    });
            });
    });

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

    uintptr_t src = local.addr + local.offset + local_off;

    auto conn = ctx_->conn_pool().getConnection(peer_host_, peer_port_);
    auto op   = TcpOpState::create();
    op->signal->reset_all();

    if (!conn) {
        op->completion_status.store(TCP_FAILED, std::memory_order_release);
        op->signal->force_complete();
        return std::make_shared<TcpReadWriteFuture>(op);
    }

    SessionHeader hdr{length, remote.addr + remote.offset + remote_off, OP_WRITE};
    auto& pool = ctx_->conn_pool();

    std::weak_ptr<TcpEndpoint> weak = weak_from_this();
    asio::post(ctx_->io_context(), [weak, conn, op, hdr, src, length, &pool]() {
        auto ep = weak.lock();
        if (!ep) {
            op->completion_status.store(TCP_CLOSED, std::memory_order_release);
            if (op->signal) op->signal->force_complete();
            return;
        }

        asio::error_code ec;
        SessionHeader net = hdr;
        hdr_hton(net);
        std::array<asio::const_buffer, 2> bufs = {
            asio::buffer(&net, sizeof(net)),
            asio::buffer(reinterpret_cast<const void*>(src), length)
        };
        asio::async_write(conn->socket, bufs,
            [conn, op, &pool](asio::error_code ec, size_t) {
                op->completion_status.store(
                    ec ? TCP_FAILED : TCP_SUCCESS, std::memory_order_release);
                if (op->signal) op->signal->set_comm_done(0);
                pool.returnConnection(conn);
            });
    });

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
    {
        std::lock_guard<std::mutex> lk(read_mu_);
        for (auto& [_, pending] : pending_reads_) {
            if (pending.op_state && pending.op_state->signal) {
                pending.op_state->completion_status.store(TCP_CLOSED, std::memory_order_release);
                pending.op_state->signal->force_complete();
            }
        }
        pending_reads_.clear();
    }

    if (own_ctx_)
        own_ctx_->shutdown();
}

}  // namespace tcp
}  // namespace dlslime
