// rpc_session.cpp - C++ implementation of the slotted-mailbox RPC session.
//
// See rpc_session.h for the high-level design notes. The implementation
// is a direct, mostly mechanical port of:
//   * dlslime/rpc/channel.py        (slot pool + send/recv primitives)
//   * dlslime/rpc/proxy.py          (client recv pump + pending table)
//   * dlslime/rpc/service.py        (server serve loop)
//
// Why C++ and not just keep the Python reactor?
//   * The Python reactor pays ~50µs of cross-thread coordination
//     (Event.wait, GIL, dict ops, queue.Queue) per RPC. C++ side does
//     the same coordination with bare std::mutex/condition_variable
//     (~3-5µs) and never touches the GIL until it has the final reply
//     bytes ready.
//   * Recycling the recv slot, demuxing requests vs replies, and
//     posting WRs are all amenable to lockfree refinement once the
//     slow Python pieces are out of the way.

#include "dlslime/csrc/rpc/rpc_session.h"

#include <algorithm>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <utility>

#include "dlslime/csrc/engine/assignment.h"
#include "dlslime/csrc/engine/rdma/rdma_endpoint.h"
#include "dlslime/csrc/engine/rdma/rdma_future.h"
#include "dlslime/csrc/logging.h"

namespace py = pybind11;

namespace dlslime::rpc {

// ───────────────────────────── ClientFuture ────────────────────────────

ClientFuture::ClientFuture(uint64_t request_id, uint32_t slot_id): request_id_(request_id), slot_id_(slot_id) {}

void ClientFuture::set_result(std::string data, bool is_remote_exception)
{
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (ready_)
            return;
        data_                = std::move(data);
        is_remote_exception_ = is_remote_exception;
        ready_               = true;
    }
    cv_.notify_all();
}

void ClientFuture::set_result_obj(py::object data_obj, bool is_remote_exception)
{
    // Caller MUST hold the GIL (we're moving a py::object). For the
    // no-pump path that's automatic since call() reacquired the GIL
    // before invoking us. We don't bother with the cv: the no-pump
    // wait() path checks ready_ before going through the cv branch.
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (ready_)
            return;
        data_obj_            = std::move(data_obj);
        is_remote_exception_ = is_remote_exception;
        ready_               = true;
    }
    cv_.notify_all();
}

void ClientFuture::set_exception(std::exception_ptr exc)
{
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (ready_)
            return;
        exc_   = exc;
        ready_ = true;
    }
    cv_.notify_all();
}

bool ClientFuture::done() const
{
    std::lock_guard<std::mutex> lock(mu_);
    return ready_;
}

py::bytes ClientFuture::wait()
{
    // Fast path: caller holds the GIL on entry. If we're already
    // ready (e.g. populated synchronously by the no-pump call() path)
    // there's no point swapping out the GIL just to observe ``ready_``
    // and walk back in.
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (ready_) {
            if (exc_) {
                std::exception_ptr e = exc_;
                lock.unlock();
                std::rethrow_exception(e);
            }
            if (data_obj_) {
                // No-pump path: bytes were built directly from the
                // RDMA recv buffer under GIL by call(). Return the
                // borrowed reference; consumes data_obj_.
                py::object obj = std::move(data_obj_);
                lock.unlock();
                return py::reinterpret_steal<py::bytes>(obj.release());
            }
            std::string out = std::move(data_);
            data_.clear();
            lock.unlock();
            return py::bytes(out.data(), out.size());
        }
    }

    // Slow path: actually wait. Drop the GIL across the cv block so
    // any other Python thread can keep running while we sleep.
    std::string        out;
    std::exception_ptr exc;
    {
        py::gil_scoped_release       release;
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait(lock, [&] { return ready_; });
        if (exc_) {
            exc = exc_;
        }
        else {
            out = std::move(data_);
            data_.clear();
        }
    }
    if (exc) {
        std::rethrow_exception(exc);
    }
    return py::bytes(out.data(), out.size());
}

bool ClientFuture::wait_for(int64_t timeout_ms, py::bytes* out)
{
    {
        std::unique_lock<std::mutex> lock(mu_);
        if (ready_) {
            if (exc_) {
                std::exception_ptr e = exc_;
                lock.unlock();
                std::rethrow_exception(e);
            }
            if (data_obj_) {
                py::object obj = std::move(data_obj_);
                lock.unlock();
                if (out) {
                    *out = py::reinterpret_steal<py::bytes>(obj.release());
                }
                return true;
            }
            std::string payload = std::move(data_);
            data_.clear();
            lock.unlock();
            if (out) {
                *out = py::bytes(payload.data(), payload.size());
            }
            return true;
        }
    }

    bool               completed = false;
    std::string        payload;
    std::exception_ptr exc;
    {
        py::gil_scoped_release       release;
        std::unique_lock<std::mutex> lock(mu_);
        completed = cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms), [&] { return ready_; });
        if (!completed) {
            return false;
        }
        if (exc_) {
            exc = exc_;
        }
        else {
            payload = std::move(data_);
            data_.clear();
        }
    }
    if (exc) {
        std::rethrow_exception(exc);
    }
    if (out) {
        *out = py::bytes(payload.data(), payload.size());
    }
    return true;
}

void ClientFuture::rethrow_if_failed()
{
    std::exception_ptr exc;
    {
        std::lock_guard<std::mutex> lock(mu_);
        exc = exc_;
    }
    if (exc) {
        std::rethrow_exception(exc);
    }
}

// ───────────────────────────── RpcSession ──────────────────────────────

RpcSession::RpcSession(std::shared_ptr<RDMAEndpoint> endpoint,
                       int                           local_send_handler,
                       uintptr_t                     local_send_ptr,
                       int                           local_recv_handler,
                       uintptr_t                     local_recv_ptr,
                       int                           remote_recv_handler,
                       size_t                        slot_size,
                       size_t                        slot_count,
                       bool                          start_recv_pump):
    endpoint_(std::move(endpoint)),
    local_send_handler_(local_send_handler),
    local_send_ptr_(local_send_ptr),
    local_recv_handler_(local_recv_handler),
    local_recv_ptr_(local_recv_ptr),
    remote_recv_handler_(remote_recv_handler),
    slot_size_(slot_size),
    slot_count_(slot_count),
    has_pump_(start_recv_pump)
{
    if (!endpoint_) {
        throw std::invalid_argument("RpcSession: endpoint must not be null");
    }
    if (slot_count_ < 1 || slot_count_ > MAX_SLOT_COUNT) {
        throw std::invalid_argument("RpcSession: slot_count out of range [1, " + std::to_string(MAX_SLOT_COUNT) + "]");
    }
    if (slot_size_ < HEADER_SIZE + 1) {
        throw std::invalid_argument("RpcSession: slot_size too small for header");
    }
    payload_capacity_ = slot_size_ - HEADER_SIZE;

    free_slots_.resize(slot_count_);
    for (size_t i = 0; i < slot_count_; ++i) {
        free_slots_[i] = static_cast<uint32_t>(i);
    }

    if (has_pump_) {
        recv_thread_ = std::thread(&RpcSession::recv_loop, this);
    }
}

RpcSession::~RpcSession()
{
    try {
        close();
    }
    catch (...) {
        // Destructors must not throw.
    }
}

void RpcSession::close()
{
    bool was_open = !closed_.exchange(true, std::memory_order_acq_rel);
    if (!was_open) {
        // Idempotent.
        if (recv_thread_.joinable() && std::this_thread::get_id() != recv_thread_.get_id()) {
            recv_thread_.join();
        }
        return;
    }

    // Wake any thread blocked in poll_request().
    {
        std::lock_guard<std::mutex> lock(inbox_mu_);
        inbox_cv_.notify_all();
    }
    // Wake any thread blocked in acquire_slot().
    {
        std::lock_guard<std::mutex> lock(slot_mu_);
        slot_cv_.notify_all();
    }

    // Fail every in-flight client future. We synthesise a generic
    // session-closed runtime_error; the Python wrapper translates it
    // to ConnectionError to keep parity with the previous API.
    fail_all_pending(std::make_exception_ptr(std::runtime_error("RPC session closed")));

    // Try to unblock the recv pump if it is stuck inside immRecv().
    // shutdown() drains the endpoint's internal queues and causes any
    // pending future->wait() to return a non-zero code, which the
    // recv loop interprets as session close.
    if (endpoint_) {
        try {
            endpoint_->cancelAll();
        }
        catch (...) {
            // best effort
        }
    }

    if (recv_thread_.joinable() && std::this_thread::get_id() != recv_thread_.get_id()) {
        recv_thread_.join();
    }
}

void RpcSession::detach_request(uint64_t request_id)
{
    std::shared_ptr<ClientFuture> doomed;
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        auto                        it = pending_.find(request_id);
        if (it != pending_.end()) {
            doomed = it->second;
            pending_.erase(it);
        }
        // Even if pending_ already lost it (race with recv pump), record
        // the detachment so a late reply on the wire silently reclaims
        // its slot instead of being logged as unknown.
        detached_.insert(request_id);
    }
    if (doomed) {
        doomed->set_exception(std::make_exception_ptr(std::runtime_error("RPC future detached")));
    }
}

// ───────────────────────── Slot semaphore ─────────────────────────

uint32_t RpcSession::acquire_slot()
{
    std::unique_lock<std::mutex> lock(slot_mu_);
    slot_cv_.wait(lock, [&] { return !free_slots_.empty() || closed_.load(); });
    if (closed_.load()) {
        throw std::runtime_error("RPC session closed");
    }
    uint32_t slot = free_slots_.front();
    free_slots_.pop_front();
    return slot;
}

void RpcSession::release_slot(uint32_t slot_id)
{
    {
        std::lock_guard<std::mutex> lock(slot_mu_);
        if (slot_id >= slot_count_) {
            // Defensive: don't poison the deque with garbage IDs.
            return;
        }
        free_slots_.push_back(slot_id);
    }
    slot_cv_.notify_one();
}

// ───────────────────────── Send (client + server) ─────────────────────

int RpcSession::post_write(
    uint32_t tag, uint64_t request_id, uint32_t target_slot_id, const char* payload, size_t nbytes)
{
    const size_t total = HEADER_SIZE + nbytes;
    if (total > slot_size_) {
        throw std::runtime_error("RPC message exceeds slot size");
    }

    // Caller (call() / post_reply) owns send_mu_. Memcpy the payload
    // into the registered send buffer first; post_write_committed will
    // pack the header on top and fire the WR.
    if (nbytes > 0) {
        std::memcpy(reinterpret_cast<void*>(local_send_ptr_ + HEADER_SIZE), payload, nbytes);
    }
    return post_write_committed(tag, request_id, target_slot_id, nbytes);
}

int RpcSession::post_write_committed(uint32_t tag, uint64_t request_id, uint32_t target_slot_id, size_t nbytes)
{
    const size_t total = HEADER_SIZE + nbytes;
    if (total > slot_size_) {
        throw std::runtime_error("RPC message exceeds slot size");
    }

    // Pack the wire header on top of the (already-written) payload.
    auto* hdr       = reinterpret_cast<WireHeader*>(local_send_ptr_);
    hdr->tag        = tag;
    hdr->total_len  = static_cast<uint32_t>(total);
    hdr->request_id = request_id;

    // imm_data carries slot_id only; the wire header carries everything else.
    int32_t imm_data = static_cast<int32_t>(target_slot_id);

    // assign_tuple_t = (local_handler, remote_handler,
    //                   target_offset_REMOTE, source_offset_LOCAL, length)
    std::vector<assign_tuple_t> assigns;
    assigns.emplace_back(static_cast<uintptr_t>(local_send_handler_),
                         static_cast<uintptr_t>(remote_recv_handler_),
                         static_cast<uint64_t>(target_slot_id) * slot_size_,
                         static_cast<uint64_t>(0),
                         total);

    auto fut = endpoint_->writeWithImm(assigns, imm_data, /*stream=*/nullptr);
    if (!fut) {
        throw std::runtime_error("RpcSession: writeWithImm returned null future");
    }
    return fut->wait();
}

std::shared_ptr<ClientFuture> RpcSession::call(uint32_t tag, py::buffer payload)
{
    py::buffer_info info = payload.request();
    if (info.itemsize <= 0) {
        throw std::invalid_argument("RpcSession::call: payload has zero itemsize");
    }
    const size_t nbytes = static_cast<size_t>(info.size) * static_cast<size_t>(info.itemsize);
    if (nbytes > payload_capacity_) {
        throw std::runtime_error("RPC payload exceeds the per-inflight slot capacity");
    }

    if (closed_.load()) {
        throw std::runtime_error("RPC session closed");
    }

    const uint64_t request_id = next_request_id_.fetch_add(1, std::memory_order_relaxed);
    const uint32_t slot_id    = acquire_slot();
    auto           future     = std::make_shared<ClientFuture>(request_id, slot_id);

    // ----- Fast path: no recv pump -----
    //
    // When the session is constructed with start_recv_pump=false the
    // caller has promised that nobody is going to call poll_request()
    // on this session, so call() can drive recv inline on the calling
    // thread. This is the C++ analogue of Python's _LazyRuntime fast
    // path -- it removes:
    //   1. the cross-thread wakeup of the recv pump
    //   2. the std::condition_variable wait + GIL re-acquisition that
    //      ClientFuture::wait() would otherwise pay
    //   3. the pending_ map insert/lookup
    //   4. the std::string intermediate in ClientFuture::data_; we
    //      build py::bytes directly from the RDMA recv buffer once,
    //      under the GIL we already own. At multi-MB payloads the
    //      saved alloc + copy avoids ~5-10 ms of malloc/page-fault
    //      time per RPC (16MB std::string ≈ 4096 fresh page faults).
    //
    // The flow becomes a tight: post-send-WR, post-recv-WR, parse, fill
    // future. The future is returned already in the "ready" state so
    // wait() is a cheap no-op on the result path.
    if (!has_pump_) {
        // Coordinates collected while GIL is dropped; they describe a
        // [payload_p, payload_p + payload_n) slice of the recv slot
        // that we'll copy into py::bytes once we re-own the GIL.
        const char*        payload_p = nullptr;
        size_t             payload_n = 0;
        bool               is_remote = false;
        std::exception_ptr local_exc;
        try {
            py::gil_scoped_release release;
            // Single-flight: only one call() can be inline at a time
            // (a slot was already taken; for slot_count>1 single-flight
            // is degenerate but still correct, just serialised).
            std::lock_guard<std::mutex> send_lock(send_mu_);

            int rc = post_write(tag, request_id, slot_id, static_cast<const char*>(info.ptr), nbytes);
            if (rc != 0) {
                throw std::runtime_error("RPC writeWithImm failed (rc != 0)");
            }
            requests_sent_.fetch_add(1, std::memory_order_relaxed);

            // Drive recv on this thread. The reply for the request we
            // just posted is the next imm-completion to land on this
            // session (because nobody else is reading from it).
            auto recv_fut = endpoint_->immRecv(/*stream=*/nullptr);
            if (!recv_fut) {
                throw std::runtime_error("immRecv returned null future");
            }
            int rc_recv = recv_fut->wait();
            if (closed_.load(std::memory_order_acquire)) {
                throw std::runtime_error("RPC session closed");
            }
            if (rc_recv != 0) {
                throw std::runtime_error("RDMA recv completion failed");
            }

            const int32_t  imm        = recv_fut->immData();
            const uint32_t reply_slot = static_cast<uint32_t>(imm) & 0xFFFFu;
            if (reply_slot >= slot_count_) {
                throw std::runtime_error("Incoming slot_id out of range");
            }
            if (reply_slot != slot_id) {
                // Single-flight: the slot we used must be the slot the
                // peer wrote into.
                throw std::runtime_error("RPC reply slot mismatch");
            }

            const uintptr_t slot_ptr = local_recv_ptr_ + reply_slot * slot_size_;
            WireHeader      hdr{};
            std::memcpy(&hdr, reinterpret_cast<const void*>(slot_ptr), HEADER_SIZE);
            if (hdr.total_len < HEADER_SIZE || hdr.total_len > slot_size_) {
                throw std::runtime_error("RDMA channel closed or oversized message");
            }
            if ((hdr.tag & REPLY_BIT) == 0) {
                throw std::runtime_error("Single-flight session received a non-reply message");
            }
            if ((hdr.tag & CHUNK_BIT) != 0) {
                throw std::runtime_error("Chunked replies not supported by C++ session");
            }
            if (hdr.request_id != request_id) {
                throw std::runtime_error("RPC reply request_id mismatch");
            }

            payload_n = hdr.total_len - HEADER_SIZE;
            payload_p = reinterpret_cast<const char*>(slot_ptr + HEADER_SIZE);
            is_remote = (hdr.tag & EXC_BIT) != 0;
            // NOTE: do NOT release the slot here; we still need to read
            // [payload_p, payload_p+payload_n) into py::bytes once the
            // GIL is back. The peer cannot reuse the slot until we
            // post a fresh receive credit anyway, but we keep the
            // bookkeeping defensive.
        }
        catch (...) {
            local_exc = std::current_exception();
        }
        // GIL reacquired here (gil_scoped_release went out of scope).

        if (local_exc) {
            release_slot(slot_id);
            replies_received_.fetch_add(1, std::memory_order_relaxed);
            future->set_exception(local_exc);
            return future;
        }

        // Build py::bytes ONCE directly from the RDMA recv buffer.
        // PyBytes_FromStringAndSize would be marginally faster than
        // pybind11's wrapper but this is already the only payload-sized
        // copy on the no-pump path, so the difference is in the noise.
        py::bytes data(payload_p, payload_n);
        release_slot(slot_id);
        replies_received_.fetch_add(1, std::memory_order_relaxed);
        future->set_result_obj(std::move(data), is_remote);
        return future;
    }

    // ----- Slow path: shared recv pump -----

    // Register the future BEFORE posting the WR so a fast reply on the
    // recv pump can find it. There is no race against the recv pump
    // because the reply cannot legally arrive before the WR completes.
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        if (closed_.load()) {
            release_slot(slot_id);
            throw std::runtime_error("RPC session closed");
        }
        pending_.emplace(request_id, future);
    }

    int rc = 0;
    try {
        py::gil_scoped_release      release;
        std::lock_guard<std::mutex> send_lock(send_mu_);
        rc = post_write(tag, request_id, slot_id, static_cast<const char*>(info.ptr), nbytes);
    }
    catch (...) {
        std::lock_guard<std::mutex> lock(pending_mu_);
        pending_.erase(request_id);
        release_slot(slot_id);
        throw;
    }

    if (rc != 0) {
        std::lock_guard<std::mutex> lock(pending_mu_);
        pending_.erase(request_id);
        release_slot(slot_id);
        throw std::runtime_error("RPC writeWithImm failed (rc != 0)");
    }

    requests_sent_.fetch_add(1, std::memory_order_relaxed);
    return future;
}

void RpcSession::post_reply(uint32_t tag, uint64_t request_id, uint32_t client_slot_id, py::buffer payload)
{
    py::buffer_info info   = payload.request();
    const size_t    nbytes = static_cast<size_t>(info.size) * static_cast<size_t>(info.itemsize);
    if (nbytes > payload_capacity_) {
        throw std::runtime_error("RPC reply exceeds per-inflight slot capacity");
    }
    if (client_slot_id >= slot_count_) {
        throw std::invalid_argument("post_reply: client_slot_id out of range");
    }
    if (closed_.load()) {
        throw std::runtime_error("RPC session closed");
    }

    py::gil_scoped_release      release;
    std::lock_guard<std::mutex> send_lock(send_mu_);
    int rc = post_write(tag, request_id, client_slot_id, static_cast<const char*>(info.ptr), nbytes);
    if (rc != 0) {
        throw std::runtime_error("RPC writeWithImm failed for reply (rc != 0)");
    }
}

// Take send_mu_ without holding the GIL: drop GIL first, lock, reacquire
// GIL. This avoids a contended send_mu_ wait blocking other Python
// threads via the GIL. Caller is responsible for unlocking via the
// returned guard going out of scope.
namespace {
struct SendLockNoGil {
    py::gil_scoped_release      release_;
    std::lock_guard<std::mutex> lock_;
    py::gil_scoped_acquire      acquire_;
    SendLockNoGil(std::mutex& m): release_(), lock_(m), acquire_() {}
};
}  // namespace

void RpcSession::post_reply_inplace(uint32_t tag, uint64_t request_id, uint32_t client_slot_id, py::function writer)
{
    if (client_slot_id >= slot_count_) {
        throw std::invalid_argument("post_reply_inplace: client_slot_id out of range");
    }
    if (closed_.load()) {
        throw std::runtime_error("RPC session closed");
    }

    int rc = 0;
    {
        // Drop GIL while waiting on send_mu_; reacquire to call the
        // writer under GIL; drop again to post the WR. Net effect:
        // GIL is held only for the writer call itself, which is
        // unavoidable because we're calling Python.
        SendLockNoGil send_guard(send_mu_);

        const uintptr_t send_payload_ptr = local_send_ptr_ + static_cast<uintptr_t>(HEADER_SIZE);
        const size_t    send_capacity    = payload_capacity_;

        py::object ret           = writer(send_payload_ptr, send_capacity);
        ssize_t    nbytes_signed = ret.cast<ssize_t>();
        if (nbytes_signed < 0) {
            throw std::runtime_error("post_reply_inplace: writer returned negative byte count");
        }
        const size_t nbytes = static_cast<size_t>(nbytes_signed);
        if (nbytes > send_capacity) {
            throw std::runtime_error("post_reply_inplace: writer overflowed send capacity");
        }

        // Drop GIL for the WR post + wait. The data is already in
        // registered memory; post_write_committed only packs the header
        // and fires the WR, no Python touching needed.
        py::gil_scoped_release release;
        rc = post_write_committed(tag, request_id, client_slot_id, nbytes);
    }
    if (rc != 0) {
        throw std::runtime_error("RPC writeWithImm failed for inplace reply (rc != 0)");
    }
}

std::shared_ptr<ClientFuture> RpcSession::call_inplace(uint32_t tag, py::function writer)
{
    if (closed_.load()) {
        throw std::runtime_error("RPC session closed");
    }

    const uint64_t request_id = next_request_id_.fetch_add(1, std::memory_order_relaxed);
    const uint32_t slot_id    = acquire_slot();
    auto           future     = std::make_shared<ClientFuture>(request_id, slot_id);

    // ----- Fast path: no recv pump -----
    if (!has_pump_) {
        const char*        payload_p = nullptr;
        size_t             payload_n = 0;
        bool               is_remote = false;
        std::exception_ptr local_exc;
        try {
            // Take send_mu_ first (without dropping GIL because we're
            // single-flight and uncontended); then write under GIL,
            // drop GIL, post WR, drive recv inline.
            std::lock_guard<std::mutex> send_lock(send_mu_);

            const uintptr_t send_payload_ptr = local_send_ptr_ + static_cast<uintptr_t>(HEADER_SIZE);
            const size_t    send_capacity    = payload_capacity_;

            py::object ret           = writer(send_payload_ptr, send_capacity);
            ssize_t    nbytes_signed = ret.cast<ssize_t>();
            if (nbytes_signed < 0) {
                throw std::runtime_error("call_inplace: writer returned negative byte count");
            }
            const size_t nbytes = static_cast<size_t>(nbytes_signed);
            if (nbytes > send_capacity) {
                throw std::runtime_error("call_inplace: writer overflowed send capacity");
            }

            // Now drop GIL: post WR, wait for completion, drive recv.
            py::gil_scoped_release release;
            int                    rc = post_write_committed(tag, request_id, slot_id, nbytes);
            if (rc != 0) {
                throw std::runtime_error("RPC writeWithImm failed (rc != 0)");
            }
            requests_sent_.fetch_add(1, std::memory_order_relaxed);

            auto recv_fut = endpoint_->immRecv(/*stream=*/nullptr);
            if (!recv_fut) {
                throw std::runtime_error("immRecv returned null future");
            }
            int rc_recv = recv_fut->wait();
            if (closed_.load(std::memory_order_acquire)) {
                throw std::runtime_error("RPC session closed");
            }
            if (rc_recv != 0) {
                throw std::runtime_error("RDMA recv completion failed");
            }

            const int32_t  imm        = recv_fut->immData();
            const uint32_t reply_slot = static_cast<uint32_t>(imm) & 0xFFFFu;
            if (reply_slot >= slot_count_) {
                throw std::runtime_error("Incoming slot_id out of range");
            }
            if (reply_slot != slot_id) {
                throw std::runtime_error("RPC reply slot mismatch");
            }

            const uintptr_t slot_ptr = local_recv_ptr_ + reply_slot * slot_size_;
            WireHeader      hdr{};
            std::memcpy(&hdr, reinterpret_cast<const void*>(slot_ptr), HEADER_SIZE);
            if (hdr.total_len < HEADER_SIZE || hdr.total_len > slot_size_) {
                throw std::runtime_error("RDMA channel closed or oversized message");
            }
            if ((hdr.tag & REPLY_BIT) == 0) {
                throw std::runtime_error("Single-flight session received a non-reply message");
            }
            if ((hdr.tag & CHUNK_BIT) != 0) {
                throw std::runtime_error("Chunked replies not supported by C++ session");
            }
            if (hdr.request_id != request_id) {
                throw std::runtime_error("RPC reply request_id mismatch");
            }

            payload_n = hdr.total_len - HEADER_SIZE;
            payload_p = reinterpret_cast<const char*>(slot_ptr + HEADER_SIZE);
            is_remote = (hdr.tag & EXC_BIT) != 0;
        }
        catch (...) {
            local_exc = std::current_exception();
        }
        // GIL reacquired here.

        if (local_exc) {
            release_slot(slot_id);
            replies_received_.fetch_add(1, std::memory_order_relaxed);
            future->set_exception(local_exc);
            return future;
        }

        py::bytes data(payload_p, payload_n);
        release_slot(slot_id);
        replies_received_.fetch_add(1, std::memory_order_relaxed);
        future->set_result_obj(std::move(data), is_remote);
        return future;
    }

    // ----- Slow path: shared recv pump -----

    // Register the future BEFORE posting the WR so a fast reply on the
    // recv pump can find it.
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        if (closed_.load()) {
            release_slot(slot_id);
            throw std::runtime_error("RPC session closed");
        }
        pending_.emplace(request_id, future);
    }

    int rc = 0;
    try {
        SendLockNoGil send_guard(send_mu_);

        const uintptr_t send_payload_ptr = local_send_ptr_ + static_cast<uintptr_t>(HEADER_SIZE);
        const size_t    send_capacity    = payload_capacity_;

        py::object ret           = writer(send_payload_ptr, send_capacity);
        ssize_t    nbytes_signed = ret.cast<ssize_t>();
        if (nbytes_signed < 0) {
            throw std::runtime_error("call_inplace: writer returned negative byte count");
        }
        const size_t nbytes = static_cast<size_t>(nbytes_signed);
        if (nbytes > send_capacity) {
            throw std::runtime_error("call_inplace: writer overflowed send capacity");
        }

        py::gil_scoped_release release;
        rc = post_write_committed(tag, request_id, slot_id, nbytes);
    }
    catch (...) {
        std::lock_guard<std::mutex> lock(pending_mu_);
        pending_.erase(request_id);
        release_slot(slot_id);
        throw;
    }

    if (rc != 0) {
        std::lock_guard<std::mutex> lock(pending_mu_);
        pending_.erase(request_id);
        release_slot(slot_id);
        throw std::runtime_error("RPC writeWithImm failed (rc != 0)");
    }

    requests_sent_.fetch_add(1, std::memory_order_relaxed);
    return future;
}

// ───────────────────────── Recv pump ─────────────────────────

void RpcSession::recv_loop()
{
    while (!closed_.load(std::memory_order_acquire)) {
        std::shared_ptr<ImmRecvFuture> fut;
        try {
            fut = endpoint_->immRecv(/*stream=*/nullptr);
        }
        catch (...) {
            // Endpoint torn down underneath us.
            fail_all_pending(std::current_exception());
            return;
        }
        if (!fut) {
            fail_all_pending(std::make_exception_ptr(std::runtime_error("immRecv returned null future")));
            return;
        }
        int rc = fut->wait();
        if (closed_.load(std::memory_order_acquire)) {
            return;
        }
        if (rc != 0) {
            fail_all_pending(std::make_exception_ptr(std::runtime_error("RDMA recv completion failed")));
            return;
        }

        const int32_t  imm     = fut->immData();
        const uint32_t slot_id = static_cast<uint32_t>(imm) & 0xFFFFu;
        if (slot_id >= slot_count_) {
            // Wire-version mismatch or scrambled imm. Treat as a hard
            // session error -- the byte stream is no longer trustworthy.
            fail_all_pending(std::make_exception_ptr(std::runtime_error("Incoming slot_id out of range")));
            return;
        }

        const uintptr_t slot_ptr = local_recv_ptr_ + slot_id * slot_size_;
        WireHeader      hdr{};
        std::memcpy(&hdr, reinterpret_cast<const void*>(slot_ptr), HEADER_SIZE);
        if (hdr.total_len < HEADER_SIZE) {
            // Zero-length completion: peer shut down.
            fail_all_pending(std::make_exception_ptr(std::runtime_error("RDMA channel closed")));
            return;
        }
        if (hdr.total_len > slot_size_) {
            fail_all_pending(std::make_exception_ptr(std::runtime_error("Incoming message exceeds slot size")));
            return;
        }

        const size_t payload_n = hdr.total_len - HEADER_SIZE;
        const char*  payload_p = reinterpret_cast<const char*>(slot_ptr + HEADER_SIZE);

        const bool is_reply = (hdr.tag & REPLY_BIT) != 0;
        const bool is_chunk = (hdr.tag & CHUNK_BIT) != 0;

        if (is_chunk) {
            // Chunked transfers are unsupported in this revision; drop
            // and surface as an error to the matching future (or warn
            // for server-side requests).
            if (is_reply) {
                std::shared_ptr<ClientFuture> matched;
                {
                    std::lock_guard<std::mutex> lock(pending_mu_);
                    auto                        it = pending_.find(hdr.request_id);
                    if (it != pending_.end()) {
                        matched = it->second;
                        pending_.erase(it);
                    }
                    detached_.erase(hdr.request_id);
                }
                if (matched) {
                    matched->set_exception(
                        std::make_exception_ptr(std::runtime_error("Chunked replies not supported by C++ session")));
                }
                release_slot(slot_id);
            }
            continue;
        }

        if (is_reply) {
            std::shared_ptr<ClientFuture> matched;
            bool                          detached = false;
            {
                std::lock_guard<std::mutex> lock(pending_mu_);
                auto                        it = pending_.find(hdr.request_id);
                if (it != pending_.end()) {
                    matched = it->second;
                    pending_.erase(it);
                }
                auto dit = detached_.find(hdr.request_id);
                if (dit != detached_.end()) {
                    detached_.erase(dit);
                    detached = true;
                }
            }

            if (!matched) {
                // Reply for a future that has been cancelled/timed out
                // by the Python wrapper, or genuinely unknown. Either
                // way: reclaim the slot so we don't leak inflight
                // accounting and move on.
                if (detached) {
                    release_slot(slot_id);
                }
                else {
                    SLIME_LOG_DEBUG("RpcSession: reply for unknown request_id ", hdr.request_id, " tag=", hdr.tag);
                    release_slot(slot_id);
                }
                replies_received_.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            if (matched->slot_id() != slot_id) {
                matched->set_exception(std::make_exception_ptr(std::runtime_error("RPC reply slot mismatch")));
            }
            else {
                const bool is_remote = (hdr.tag & EXC_BIT) != 0;
                matched->set_result(std::string(payload_p, payload_n), is_remote);
            }
            release_slot(slot_id);
            replies_received_.fetch_add(1, std::memory_order_relaxed);
        }
        else {
            // Server-side inbound request. Tier A zero-copy: borrow a
            // pointer into the recv slot instead of copying. The slot
            // is owned by us until the matching post_reply is posted
            // (the peer cannot reuse the slot_id until it receives the
            // reply), so payload_ptr is stable for the full handler
            // lifetime regardless of how long it runs.
            InboundRequest req;
            req.raw_tag        = hdr.tag;
            req.base_tag       = hdr.tag & ~FLAG_MASK;
            req.request_id     = hdr.request_id;
            req.slot_id        = slot_id;
            req.payload_ptr    = payload_p;
            req.payload_nbytes = payload_n;
            {
                std::lock_guard<std::mutex> lock(inbox_mu_);
                inbox_.push_back(std::move(req));
            }
            inbox_cv_.notify_one();
            requests_received_.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

// ───────────────────────── Server pop ─────────────────────────

std::optional<InboundRequest> RpcSession::poll_request()
{
    if (!has_pump_) {
        throw std::runtime_error("RpcSession::poll_request: session was constructed with "
                                 "start_recv_pump=false (client-only fast path); the recv pump "
                                 "is required to receive inbound requests.");
    }
    InboundRequest req;
    {
        // Drop the GIL BEFORE blocking on the mutex/cv -- the recv pump
        // doesn't need the GIL but Python sweepers or timer threads
        // might, so holding it here can deadlock.
        py::gil_scoped_release       release;
        std::unique_lock<std::mutex> lock(inbox_mu_);
        inbox_cv_.wait(lock, [&] { return !inbox_.empty() || closed_.load(); });
        if (inbox_.empty()) {
            return std::nullopt;
        }
        req = std::move(inbox_.front());
        inbox_.pop_front();
    }
    return req;
}

void RpcSession::fail_all_pending(std::exception_ptr exc)
{
    std::vector<std::shared_ptr<ClientFuture>> doomed;
    std::vector<uint32_t>                      slots;
    {
        std::lock_guard<std::mutex> lock(pending_mu_);
        doomed.reserve(pending_.size());
        slots.reserve(pending_.size());
        for (auto& kv : pending_) {
            doomed.push_back(kv.second);
            slots.push_back(kv.second->slot_id());
        }
        pending_.clear();
        detached_.clear();
    }
    for (auto& f : doomed) {
        f->set_exception(exc);
    }
    for (uint32_t s : slots) {
        release_slot(s);
    }
}

}  // namespace dlslime::rpc
