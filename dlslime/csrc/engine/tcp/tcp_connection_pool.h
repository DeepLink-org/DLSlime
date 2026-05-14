#pragma once

#include <utility>
#include <asio.hpp>

#include <chrono>
#include <cstdint>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>

namespace dlslime {
namespace tcp {

struct PooledConnection {
    asio::ip::tcp::socket socket;
    std::string           host;
    uint16_t              port{0};
    std::chrono::steady_clock::time_point last_used;
    bool                  in_use{true};

    PooledConnection(asio::ip::tcp::socket s, std::string h, uint16_t p)
        : socket(std::move(s)), host(std::move(h)), port(p),
          last_used(std::chrono::steady_clock::now()) {}
};

// Keyed by (host, port).  Thread-safe.
// States: IDLE (in deque, in_use=false) / ACTIVE (checked out) / RESERVED
class TcpConnectionPool {
public:
    static constexpr std::chrono::seconds kIdleTimeout{60};

    explicit TcpConnectionPool(asio::io_context& io_ctx) : io_ctx_(io_ctx) {}

    std::shared_ptr<PooledConnection> getConnection(
        const std::string& host, uint16_t port);

    void returnConnection(std::shared_ptr<PooledConnection> conn);

    void cleanupIdleConnections();
    void clear();

private:
    struct ConnKey {
        std::string host;
        uint16_t    port;
        bool operator==(const ConnKey& o) const {
            return host == o.host && port == o.port;
        }
    };
    struct ConnKeyHash {
        size_t operator()(const ConnKey& k) const {
            return std::hash<std::string>{}(k.host)
                 ^ (std::hash<uint16_t>{}(k.port) << 1);
        }
    };

    asio::io_context& io_ctx_;
    std::mutex mu_;
    std::unordered_map<ConnKey,
                       std::deque<std::shared_ptr<PooledConnection>>,
                       ConnKeyHash> pool_;
};

}  // namespace tcp
}  // namespace dlslime
