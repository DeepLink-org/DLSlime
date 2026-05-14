#include "tcp_connection_pool.h"

#include <asio/connect.hpp>

#include "dlslime/csrc/logging.h"

namespace dlslime {
namespace tcp {

using tcp = asio::ip::tcp;

std::shared_ptr<PooledConnection>
TcpConnectionPool::getConnection(const std::string& host, uint16_t port) {
    ConnKey key{host, port};

    {
        std::lock_guard<std::mutex> lk(mu_);
        auto it = pool_.find(key);
        if (it != pool_.end()) {
            for (auto& c : it->second) {
                if (!c->in_use && c->socket.is_open()) {
                    c->in_use = true;
                    c->last_used = std::chrono::steady_clock::now();
                    return c;
                }
            }
        }
    }

    tcp::resolver resolver(io_ctx_);
    auto endpoints = resolver.resolve(host, std::to_string(port));
    tcp::socket sock(io_ctx_);
    asio::error_code ec;
    asio::connect(sock, endpoints, ec);
    if (ec) {
        SLIME_LOG_WARN("TcpConnectionPool: connect to ", host, ":", port,
                       " failed: ", ec.message());
        return nullptr;
    }
    sock.set_option(tcp::no_delay(true));

    auto conn = std::make_shared<PooledConnection>(std::move(sock), host, port);
    {
        std::lock_guard<std::mutex> lk(mu_);
        auto& q = pool_[key];
        for (auto& c : q) {
            if (!c->in_use && c->socket.is_open()) {
                asio::error_code ign;
                conn->socket.close(ign);
                c->in_use = true;
                c->last_used = std::chrono::steady_clock::now();
                return c;
            }
        }
        q.push_back(conn);
    }
    return conn;
}

void TcpConnectionPool::returnConnection(
    std::shared_ptr<PooledConnection> conn) {
    if (!conn) return;
    std::lock_guard<std::mutex> lk(mu_);
    if (conn->socket.is_open()) {
        conn->in_use = false;
        conn->last_used = std::chrono::steady_clock::now();
    } else {
        ConnKey key{conn->host, conn->port};
        auto it = pool_.find(key);
        if (it != pool_.end()) {
            auto& q = it->second;
            for (auto qi = q.begin(); qi != q.end(); ++qi)
                if (*qi == conn) { q.erase(qi); break; }
            if (q.empty()) pool_.erase(it);
        }
    }
}

void TcpConnectionPool::cleanupIdleConnections() {
    auto now = std::chrono::steady_clock::now();
    std::lock_guard<std::mutex> lk(mu_);
    for (auto it = pool_.begin(); it != pool_.end(); ) {
        auto& q = it->second;
        while (!q.empty()) {
            auto& c = q.back();
            if (!c->in_use) {
                auto idle = std::chrono::duration_cast<std::chrono::seconds>(
                    now - c->last_used).count();
                if (idle > kIdleTimeout.count()) {
                    asio::error_code ign;
                    c->socket.close(ign);
                    q.pop_back();
                    continue;
                }
            }
            break;
        }
        if (q.empty()) it = pool_.erase(it); else ++it;
    }
}

void TcpConnectionPool::clear() {
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [_, q] : pool_)
        for (auto& c : q) { asio::error_code ign; c->socket.close(ign); }
    pool_.clear();
}

}  // namespace tcp
}  // namespace dlslime
