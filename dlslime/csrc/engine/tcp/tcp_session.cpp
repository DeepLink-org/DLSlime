#include "tcp_session.h"

#include <endian.h>

#include <asio/error.hpp>
#include <asio/read.hpp>
#include <asio/write.hpp>
#include <cstring>

#include "dlslime/csrc/logging.h"

#ifdef USE_CUDA
#include <cuda_runtime.h>
#endif

namespace dlslime {
namespace tcp {

// ── helpers ─────────────────────────────────────────────

static void hdr_to_net(SessionHeader& hdr)
{
    hdr.size = htole64(hdr.size);
    hdr.addr = htole64(hdr.addr);
}

static void hdr_to_host(SessionHeader& hdr)
{
    hdr.size = le64toh(hdr.size);
    hdr.addr = le64toh(hdr.addr);
}

static bool is_fatal(asio::error_code ec)
{
    return ec && ec != asio::error::eof;
}

#ifdef USE_CUDA
static bool is_cuda_memory(const void* addr)
{
    cudaPointerAttributes attr;
    auto                  st = cudaPointerGetAttributes(&attr, addr);
    return (st == cudaSuccess && attr.type == cudaMemoryTypeDevice);
}
#endif

// ── ServerSession ───────────────────────────────────────

ServerSession::ServerSession(asio::ip::tcp::socket socket, TcpMemoryPool* local_pool, RecvMatcher recv_matcher):
    socket_(std::move(socket)), local_pool_(local_pool), recv_matcher_(std::move(recv_matcher))
{
}

void ServerSession::start()
{
    readHeader();
}

void ServerSession::readHeader()
{
    auto self = shared_from_this();
    header_   = {};
    asio::async_read(socket_, asio::buffer(&header_, sizeof(header_)), [this, self](asio::error_code ec, size_t /*n*/) {
        if (ec) {
            if (is_fatal(ec))
                SLIME_LOG_WARN("ServerSession::readHeader ", ec.message());
            return;
        }
        hdr_to_host(header_);
        dispatch();
    });
}

void ServerSession::dispatch()
{
    switch (header_.opcode) {

        case OP_SEND: {
            if (header_.size == 0) {
                readHeader();
                return;
            }
            auto slot = recv_matcher_();
            if (!slot.buffer || slot.length == 0) {
                SLIME_LOG_WARN("ServerSession: OP_SEND with no pending recv");
                readHeader();
                return;
            }
            if (slot.exact_size && header_.size != slot.length) {
                SLIME_LOG_WARN("ServerSession: size mismatch, send ", header_.size, " != recv ", slot.length);
                if (slot.op_state) {
                    slot.op_state->completion_status.store(TCP_FAILED, std::memory_order_release);
                    if (slot.op_state->signal)
                        slot.op_state->signal->set_comm_done(0);
                }
                readHeader();
                return;
            }

            // Always drain the full send payload from the wire.  If recv buffer
            // is smaller, read into a temp buffer then copy what fits.
            size_t n_read   = static_cast<size_t>(header_.size);
            size_t n_copy   = std::min(n_read, slot.length);
            auto*  dst      = reinterpret_cast<char*>(slot.buffer);
            bool   overflow = false;

            if (header_.size > slot.length) {
                dst      = new char[n_read];
                overflow = true;
            }

            auto self = shared_from_this();
            asio::async_read(socket_,
                             asio::buffer(dst, n_read),
                             [this, self, slot, n_copy, dst, overflow](asio::error_code ec, size_t /*rn*/) {
                                 if (ec) {
                                     if (is_fatal(ec))
                                         SLIME_LOG_WARN("ServerSession SEND read: ", ec.message());
                                     if (overflow)
                                         delete[] dst;
                                     return;
                                 }
                                 if (overflow) {
                                     std::memcpy(reinterpret_cast<void*>(slot.buffer), dst, n_copy);
                                     delete[] dst;
                                 }
                                 if (slot.post_read)
                                     slot.post_read();
                                 if (slot.op_state) {
                                     slot.op_state->bytes_copied = n_copy;
                                     slot.op_state->completion_status.store(TCP_SUCCESS, std::memory_order_release);
                                     if (slot.op_state->signal)
                                         slot.op_state->signal->set_comm_done(0);
                                 }
                                 readHeader();
                             });
            break;
        }

        case OP_WRITE:
            if (header_.size == 0) {
                readHeader();
                return;
            }
            readBody(reinterpret_cast<void*>(header_.addr), header_.size);
            break;

        case OP_READ: {
            uintptr_t addr = static_cast<uintptr_t>(header_.addr);
            size_t    sz   = static_cast<size_t>(header_.size);
            if (sz == 0) {
                readHeader();
                return;
            }
            writeBody(reinterpret_cast<const void*>(addr), sz);
            break;
        }

        default:
            SLIME_LOG_WARN("ServerSession: unknown opcode ", static_cast<int>(header_.opcode));
            readHeader();
            break;
    }
}

void ServerSession::readBody(void* dst, size_t len)
{
    auto* ptr     = static_cast<char*>(dst);
    bool  is_cuda = false;
#ifdef USE_CUDA
    if (is_cuda_memory(dst)) {
        ptr     = new char[len];
        is_cuda = true;
    }
#endif

    auto self = shared_from_this();
    asio::async_read(socket_,
                     asio::buffer(ptr, len),
                     [this, self, real_addr = reinterpret_cast<uintptr_t>(dst), len, is_cuda, ptr](asio::error_code ec,
                                                                                                   size_t /*n*/) {
                         if (ec) {
                             if (is_fatal(ec))
                                 SLIME_LOG_WARN("ServerSession::readBody ", ec.message());
                             if (is_cuda)
                                 delete[] ptr;
                             return;
                         }
#ifdef USE_CUDA
                         if (is_cuda) {
                             auto cu_err =
                                 cudaMemcpy(reinterpret_cast<void*>(real_addr), ptr, len, cudaMemcpyHostToDevice);
                             if (cu_err != cudaSuccess)
                                 SLIME_LOG_ERROR("readBody cudaMemcpy H2D: ", cudaGetErrorString(cu_err));
                             delete[] ptr;
                         }
#endif
                         readHeader();
                     });
}

void ServerSession::writeBody(const void* src, size_t len)
{
    auto* ptr     = static_cast<const char*>(src);
    bool  is_cuda = false;
#ifdef USE_CUDA
    if (is_cuda_memory(src)) {
        auto* buf    = new char[len];
        auto  cu_err = cudaMemcpy(buf, src, len, cudaMemcpyDeviceToHost);
        if (cu_err != cudaSuccess) {
            SLIME_LOG_ERROR("writeBody cudaMemcpy D2H: ", cudaGetErrorString(cu_err));
            delete[] buf;
            ptr = static_cast<const char*>(src);
        }
        else {
            ptr     = buf;
            is_cuda = true;
        }
    }
#endif

    auto self = shared_from_this();
    asio::async_write(socket_, asio::buffer(ptr, len), [this, self, is_cuda, ptr](asio::error_code ec, size_t /*n*/) {
        if (is_cuda)
            delete[] ptr;
        if (ec && is_fatal(ec))
            SLIME_LOG_WARN("ServerSession::writeBody ", ec.message());
        readHeader();
    });
}

// ── ClientSession ───────────────────────────────────────

ClientSession::ClientSession(asio::ip::tcp::socket sock, DoneCallback on_done):
    socket_(std::move(sock)), on_done_(std::move(on_done))
{
}

void ClientSession::start_write(const SessionHeader& hdr, const void* payload)
{
    auto          self = shared_from_this();
    SessionHeader net  = hdr;
    hdr_to_net(net);
    std::array<asio::const_buffer, 2> bufs = {asio::buffer(&net, sizeof(net)), asio::buffer(payload, hdr.size)};
    asio::async_write(socket_, bufs, [this, self](asio::error_code ec, size_t) {
        if (on_done_)
            on_done_(ec);
    });
}

void ClientSession::start_read(const SessionHeader& hdr, void* dst)
{
    auto self         = shared_from_this();
    hdr_              = hdr;
    SessionHeader net = hdr;
    hdr_to_net(net);
    asio::async_write(socket_, asio::buffer(&net, sizeof(net)), [this, self, dst](asio::error_code ec, size_t) {
        if (ec) {
            if (on_done_)
                on_done_(ec);
            return;
        }
        asio::async_read(socket_, asio::buffer(dst, hdr_.size), [this, self](asio::error_code ec, size_t) {
            if (on_done_)
                on_done_(ec);
        });
    });
}

}  // namespace tcp
}  // namespace dlslime
