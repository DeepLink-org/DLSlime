#include "tcp_connection_pool.h"

#include <asio/connect.hpp>

#include "dlslime/csrc/logging.h"

namespace dlslime {
namespace tcp {

using tcp = asio::ip::tcp;

std::shared_ptr<PooledConnection> TcpConnectionPool::getConnection(const std::string& host, uint16_t port)
{
    ConnKey key{host, port};

    {
        std::lock_guard<std::mutex> lk(mu_);
        auto                        it = pool_.find(key);
        if (it != pool_.end()) {
            for (auto& c : it->second) {
                if (!c->in_use && c->socket.is_open()) {
                    c->in_use    = true;
                    c->last_used = std::chrono::steady_clock::now();
                    return c;
                }
            }
        }
    }

    tcp::resolver    resolver(io_ctx_);
    auto             endpoints = resolver.resolve(host, std::to_string(port));
    tcp::socket      sock(io_ctx_);
    asio::error_code ec;
    asio::connect(sock, endpoints, ec);
    if (ec) {
        SLIME_LOG_WARN("TcpConnectionPool: connect to ", host, ":", port, " failed: ", ec.message());
        return nullptr;
    }
    sock.set_option(tcp::no_delay(true));

    auto conn = std::make_shared<PooledConnection>(std::move(sock), host, port);
    {
        std::lock_guard<std::mutex> lk(mu_);
        // Remove idle connection
        cleanupIdleConnections(false);

        auto& q = pool_[key];
        for (auto q_i = q.begin(); q_i != q.end();) {
            auto& c = *q_i;
            if (!c->in_use) {
                if (c->socket.is_open()) {
                    c->in_use    = true;
                    c->last_used = std::chrono::steady_clock::now();
                    asio::error_code ign;
                    conn->socket.close(ign);
                    return c;
                }
                else {
                    // Remove dead connection
                    q_i = q.erase(q_i);
                    continue;
                }
            }
            q_i++;
        }
        q.push_back(conn);
    }
    return conn;
}

void TcpConnectionPool::returnConnection(std::shared_ptr<PooledConnection> conn)
{
    if (!conn)
        return;
    ConnKey key{conn->host, conn->port};

    std::lock_guard<std::mutex> lk(mu_);
    auto                        it = pool_.find(key);
    if (it != pool_.end()) {
        auto& q = it->second;
        for (auto qi = q.begin(); qi != q.end(); ++qi)
            if (*qi == conn) {
                if (conn->socket.is_open()) {
                    conn->in_use    = false;
                    conn->last_used = std::chrono::steady_clock::now();
                }
                else {
                    q.erase(qi);
                }
                break;
            }
        if (q.empty())
            pool_.erase(it);
        return;
    }

    // Connection not found in pool (temporary), close it.
    if (conn->socket.is_open()) {
        asio::error_code ec;
        conn->socket.close(ec);
        if (ec)
            SLIME_LOG_WARN(
                "TcpConnectionPool: close temp conn ", conn->host, ":", conn->port, " failed: ", ec.message());
    }
}

void TcpConnectionPool::cleanupIdleConnections(bool lock)
{
    auto now = std::chrono::steady_clock::now();
    if (lock)
        std::lock_guard<std::mutex> lk(mu_);
    for (auto it = pool_.begin(); it != pool_.end();) {
        auto& q = it->second;
        while (!q.empty()) {
            auto& c = q.back();
            if (!c->in_use) {
                auto idle = std::chrono::duration_cast<std::chrono::seconds>(now - c->last_used).count();
                if (idle > kIdleTimeout.count()) {
                    asio::error_code ign;
                    c->socket.close(ign);
                    q.pop_back();
                    continue;
                }
            }
            break;
        }
        if (q.empty())
            it = pool_.erase(it);
        else
            ++it;
    }
}

void TcpConnectionPool::clear()
{
    std::lock_guard<std::mutex> lk(mu_);
    for (auto& [_, q] : pool_)
        // force close
        for (auto& c : q) {
            c->in_use = false;
            asio::error_code ign;
            c->socket.close(ign);
        }
    pool_.clear();
}

}  // namespace tcp
}  // namespace dlslime
