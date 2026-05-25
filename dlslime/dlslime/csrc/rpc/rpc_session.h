// rpc_session.h - C++ replacement for the Python Channel + _ClientRuntime.
//
// Design parity with the Python implementation in dlslime/rpc/{channel,proxy,
// service}.py:
//   * Slotted mailbox: recv buffer split into ``slot_count`` slots of
//     ``slot_size`` bytes, addressed via the IB imm_data field.
//   * Single send buffer guarded by a mutex (per-slot send pipelining is
//     a deliberate non-goal of this revision; matches the Python code).
//   * Single recv pump thread per session that dispatches REPLY_BIT
//     messages to client futures and other messages to the server inbox.
//
// The session does not own the registered MR memory; the caller (Python
// proxy.py) keeps the underlying GrowableBuffer alive for the lifetime
// of the session.
#pragma once

#include <pybind11/pybind11.h>
#include <pybind11/pytypes.h>

#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <unordered_map>
#include <unordered_set>

#include "dlslime/csrc/rpc/rpc_constants.h"

namespace dlslime {
class RDMAEndpoint;
}

namespace dlslime::rpc {

// Forward decl.
class RpcSession;

// One outstanding RPC reply on the client side. Mirrors RpcFuture in
// dlslime/rpc/proxy.py but lives inside the C++ session.
class ClientFuture: public std::enable_shared_from_this<ClientFuture> {
public:
    explicit ClientFuture(uint64_t request_id, uint32_t slot_id);

    // Block until the reply arrives or the session closes. Releases the
    // GIL via pybind11. Returns the raw reply bytes. Throws on local /
    // wire-level errors (session closed, slot mismatch, ...). Remote
    // exceptions are NOT thrown here: the bytes returned in that case
    // are the pickled exception payload and ``is_remote_exception()``
    // is true. The Python wrapper decodes that payload into a proper
    // ``RemoteRpcError`` so we don't pay pickle/cross-language overhead
    // in the C++ recv pump.
    pybind11::bytes wait();

    // Bounded wait -- block at most ``timeout_ms`` ms. Returns true if
    // the future completed, false if the timer fired first. ``out`` is
    // populated only on success. The caller must observe
    // ``is_remote_exception()`` and ``has_exception()`` to interpret
    // ``out`` exactly like for ``wait()``.
    bool wait_for(int64_t timeout_ms, pybind11::bytes* out);

    bool has_exception() const
    {
        std::lock_guard<std::mutex> lock(mu_);
        return ready_ && exc_ != nullptr;
    }

    // Re-throw the captured exception (if any). Python wrapper uses
    // this after a successful wait_for() to surface local errors.
    void rethrow_if_failed();

    bool done() const;

    uint64_t request_id() const
    {
        return request_id_;
    }
    uint32_t slot_id() const
    {
        return slot_id_;
    }

    // True iff the future was completed via ``set_result(.., true)``.
    bool is_remote_exception() const
    {
        std::lock_guard<std::mutex> lock(mu_);
        return is_remote_exception_;
    }

    // ---- private to the session ----
    // ``is_remote_exception`` flags the bytes as a pickled remote
    // exception so the Python wrapper can decode and re-raise. wait()
    // still returns the bytes successfully.
    void set_result(std::string data, bool is_remote_exception = false);
    // Fast path used by the no-pump single-flight call(): the caller
    // already holds the GIL and can build py::bytes directly from the
    // RDMA recv buffer, skipping the std::string intermediate (saves
    // one alloc + one full payload copy per RPC, which dominates at
    // multi-MB payloads where the intermediate alloc page-faults). The
    // py::object MUST be a bytes-like object the Python wrapper will
    // hand back to user code as-is.
    void set_result_obj(pybind11::object data_obj, bool is_remote_exception);
    void set_exception(std::exception_ptr exc);

private:
    mutable std::mutex      mu_;
    std::condition_variable cv_;
    bool                    ready_{false};
    bool                    is_remote_exception_{false};
    std::string             data_;
    // Set ONLY by set_result_obj() (no-pump fast path). Lives on the
    // GIL: read/written only while holding the Python GIL. Future
    // destruction is also GIL-bound because the no-pump session has no
    // background thread that could drop the last shared_ptr.
    pybind11::object   data_obj_;
    std::exception_ptr exc_;
    uint64_t           request_id_;
    uint32_t           slot_id_;
};

// Server-side inbound message handed to Python by ``poll_request``.
//
// Tier A zero-copy: payload_ptr is a *borrowed* pointer into the local
// recv mailbox slot. It is valid from the moment the WRITE_WITH_IMM
// completion is dispatched (recv_loop) until the matching post_reply
// is posted: the peer cannot reuse this slot until they receive the
// reply, so no other thread can scribble on [payload_ptr,
// payload_ptr + payload_nbytes) in that window.
//
// Do NOT free the buffer; do NOT keep the pointer beyond the matching
// post_reply call. If the handler needs the bytes after replying, it
// must materialise them itself (e.g. via the ``payload`` lazy
// py::bytes accessor in the bind, which copies once into Python heap).
struct InboundRequest {
    // raw_tag is the on-wire tag (with FLAG bits intact).
    uint32_t    raw_tag{0};
    uint32_t    base_tag{0};
    uint64_t    request_id{0};
    uint32_t    slot_id{0};
    const char* payload_ptr{nullptr};
    size_t      payload_nbytes{0};
};

class RpcSession: public std::enable_shared_from_this<RpcSession> {
public:
    // local_send_handler  -- handle returned by RDMAEndpoint::registerOrAccessMemoryRegion
    //                        for the (single) local send buffer.
    // local_send_ptr      -- start of the local send buffer.
    // local_recv_handler  -- handle for the local recv mailbox (slot_count*slot_size).
    // local_recv_ptr      -- start of the local recv mailbox.
    // remote_recv_handler -- handle returned by registerOrAccessRemoteMemoryRegion
    //                        for the remote peer's recv mailbox.
    // start_recv_pump=false skips the background thread entirely. In
    // that mode call() drives recv inline on the caller thread (one
    // immRecv per call). This is the C++ analogue of Python's
    // _LazyRuntime: legal only when this session is used as a client
    // (i.e. nobody calls poll_request()), and it saves the ~30µs that
    // a cross-thread cv wakeup + GIL handoff would cost per RPC.
    RpcSession(std::shared_ptr<RDMAEndpoint> endpoint,
               int                           local_send_handler,
               uintptr_t                     local_send_ptr,
               int                           local_recv_handler,
               uintptr_t                     local_recv_ptr,
               int                           remote_recv_handler,
               size_t                        slot_size,
               size_t                        slot_count,
               bool                          start_recv_pump = true);

    ~RpcSession();

    RpcSession(const RpcSession&)            = delete;
    RpcSession& operator=(const RpcSession&) = delete;

    // ---- Client side ----

    // Allocate request_id, acquire a slot (blocks under backpressure),
    // copy [header|payload] into the send buffer, post writeWithImm,
    // wait for the WR completion, register the pending future and
    // return it. ``raw_tag`` should be the user method's base tag with
    // the optional REPLY/EXC bits already OR'd in.
    std::shared_ptr<ClientFuture> call(uint32_t tag, pybind11::buffer payload);

    // Tier B zero-copy send: like call() but the writer fills the
    // registered send buffer in place. ``writer(send_payload_ptr,
    // send_capacity)`` runs under the GIL (with send_mu_ held) and
    // returns the number of bytes written. No intermediate
    // ``py::bytes`` and no memcpy from Python heap into the send
    // buffer. The reply is still materialised as ``py::bytes`` on
    // the future as in call(); use a future revision to make replies
    // zero-copy too if the workload demands it.
    std::shared_ptr<ClientFuture> call_inplace(uint32_t tag, pybind11::function writer);

    // ---- Server side ----

    // Block until a non-reply message arrives or the session closes.
    // Returns std::nullopt when the session has been closed and the
    // inbox is empty.
    std::optional<InboundRequest> poll_request();

    // Post a reply for an inbound request. ``client_slot_id`` MUST be
    // the slot_id from the matching InboundRequest -- it is what the
    // client allocated and what we echo back in imm_data so the reply
    // lands in the correct mailbox slot on the client side.
    void post_reply(uint32_t tag, uint64_t request_id, uint32_t client_slot_id, pybind11::buffer payload);

    // Tier B zero-copy reply path. Calls ``writer(send_payload_ptr,
    // send_capacity)`` while holding ``send_mu_`` and the GIL: the
    // writer fills the registered send buffer in place and returns the
    // number of bytes it wrote. The session then posts the WR using
    // those bytes directly -- no intermediate ``py::bytes`` and no
    // memcpy from Python heap into the send buffer.
    //
    // Concurrency: the writer holds ``send_mu_`` for its full duration,
    // serialising all outgoing traffic on this session against itself
    // and against post_reply / call. A slow writer therefore stalls
    // other senders -- intentional, because the send buffer is shared.
    //
    // If the writer raises, ``send_mu_`` is released and the exception
    // is propagated to the Python caller. The send buffer's contents
    // are undefined after a failure but become valid again on the next
    // successful post_reply / call (header is rewritten unconditionally).
    void post_reply_inplace(uint32_t tag, uint64_t request_id, uint32_t client_slot_id, pybind11::function writer);

    // Cooperatively shut down. After close() returns, the recv pump
    // thread has joined, all pending futures are failed with
    // std::runtime_error("RPC session closed"), and ``poll_request``
    // returns std::nullopt forever.
    void close();

    // Tell the session that the caller is no longer interested in the
    // reply for ``request_id`` (e.g. Python-level timeout). The
    // matching future is removed from the pending table and failed; if
    // the reply does eventually land, the recv pump silently reclaims
    // the slot instead of warning about an unknown request_id.
    void detach_request(uint64_t request_id);

    // ---- Diagnostics / Python-facing accessors ----

    size_t slot_count() const
    {
        return slot_count_;
    }
    size_t slot_size() const
    {
        return slot_size_;
    }
    size_t payload_capacity() const
    {
        return payload_capacity_;
    }
    bool closed() const
    {
        return closed_.load(std::memory_order_acquire);
    }
    uint64_t requests_sent() const
    {
        return requests_sent_.load();
    }
    uint64_t replies_received() const
    {
        return replies_received_.load();
    }
    uint64_t requests_received() const
    {
        return requests_received_.load();
    }

private:
    void     recv_loop();
    uint32_t acquire_slot();
    void     release_slot(uint32_t slot_id);
    int      post_write(uint32_t tag, uint64_t request_id, uint32_t target_slot_id, const char* payload, size_t nbytes);
    // Like post_write but assumes the payload is already in place at
    // local_send_ptr_ + HEADER_SIZE; used by post_reply_inplace and any
    // future caller that has prefilled the send buffer.
    int  post_write_committed(uint32_t tag, uint64_t request_id, uint32_t target_slot_id, size_t nbytes);
    void fail_all_pending(std::exception_ptr exc);

    std::shared_ptr<RDMAEndpoint> endpoint_;
    int                           local_send_handler_;
    uintptr_t                     local_send_ptr_;
    int                           local_recv_handler_;
    uintptr_t                     local_recv_ptr_;
    int                           remote_recv_handler_;

    size_t slot_size_;
    size_t slot_count_;
    size_t payload_capacity_;

    // Single send buffer is shared across all in-flight requests; this
    // lock serialises the (memcpy + post + wait) trio so no second
    // sender can scribble over an in-flight WR.
    std::mutex send_mu_;

    // Slot semaphore: the client-side outgoing concurrency limit.
    std::mutex              slot_mu_;
    std::condition_variable slot_cv_;
    std::deque<uint32_t>    free_slots_;

    // Pending replies awaiting a recv-pump dispatch.
    std::mutex                                                  pending_mu_;
    std::unordered_map<uint64_t, std::shared_ptr<ClientFuture>> pending_;
    // request_ids of futures that the user already abandoned (e.g.
    // session::call() failed mid-way after registering pending; or
    // future was timed out by the Python wrapper). When a late reply
    // arrives we still need to release the slot.
    std::unordered_set<uint64_t> detached_;

    // Server-side inbound queue. recv_loop pushes; poll_request pops.
    std::mutex                 inbox_mu_;
    std::condition_variable    inbox_cv_;
    std::deque<InboundRequest> inbox_;

    std::atomic<uint64_t> next_request_id_{1};
    std::atomic<bool>     closed_{false};
    std::atomic<uint64_t> requests_sent_{0};
    std::atomic<uint64_t> replies_received_{0};
    std::atomic<uint64_t> requests_received_{0};

    // True iff a background recv pump thread is running. When false,
    // call() drives recv inline on the caller thread.
    bool        has_pump_{true};
    std::thread recv_thread_;
};

}  // namespace dlslime::rpc
