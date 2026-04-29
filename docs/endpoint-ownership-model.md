# RDMAEndpoint Completion-Ownership Model

This document fixes the language for the RDMAEndpoint completion refactor.
Before the refactor the codebase mixed three concerns in one struct — the
"slot" was simultaneously the execution-storage, the completion owner,
and the wire work-request. That made it impossible to describe who owned
what, so subtle ABA hazards kept slipping in. The refactor separates
those concerns into three layers with explicit ownership rules.

## The Four Objects

```
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 1: Future (SendFuture / RecvFuture / ReadWriteFuture /        │
│           ImmRecvFuture)                                             │
│           — user handle; wait() and read result                      │
└──────────────────────────────────────────────────────────────────────┘
                          │ shared_ptr<EndpointOpState>
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 2: EndpointOpState                                            │
│           — per-op identity + completion signal + result fields      │
│           — NOT pooled; created per op, released per op              │
└──────────────────────────────────────────────────────────────────────┘
                          │ referenced by slot.op_state
                          │ and by transport callbacks (strong, on CQ
                          │ thread stack while invoking)
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 3: Slot (ReadWriteContext / SendContext / RecvContext)        │
│           — POOLED dispatch coordinator                              │
│           — holds wr_id-backing RDMAAssign storage                   │
│           — lease/release via free-slot ring; never modulo-reused    │
└──────────────────────────────────────────────────────────────────────┘
                          │ wr_id = &slot.assigns_[qpi]
                          ▼
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 4: RDMAAssign + RDMAChannel / RDMAContext                     │
│           — wire WR; CQ polling + callback dispatch                  │
└──────────────────────────────────────────────────────────────────────┘
```

`ImmRecvContext` is a separate Layer-3 object with different semantics
(see below) but follows the same ownership rules.

## Who Owns What

| Owner              | Owns                          | Reference kind                                                         |
| ------------------ | ----------------------------- | ---------------------------------------------------------------------- |
| User code          | Future                        | `shared_ptr<RDMAFuture>`                                               |
| Future             | EndpointOpState               | `shared_ptr<EndpointOpState>`                                          |
| Transport callback | EndpointOpState (during fire) | `shared_ptr` captured in lambda, moved to CQ stack, released on return |
| Slot               | Current op's EndpointOpState  | `shared_ptr` in `slot.op_state`, cleared on release                    |
| Slot               | Its own `RDMAAssign` storage  | by value                                                               |
| Endpoint           | Slot pool                     | owning array                                                           |
| Endpoint           | Free-slot ring                | owning ring                                                            |
| Endpoint           | In-flight op registry         | `weak_ptr<EndpointOpState>`                                            |

The registry is weak so it does not pin retired ops; it exists only so
`cancelAll()` can fail-out every outstanding op without walking slots.

## Boundary Invariants

These are the rules the refactor enforces. Violating any of them
reintroduces the ABA hazard.

### I1 — Signal exclusivity

An `EndpointOpState::signal` is created per op and never shared. Two
futures can never observe the same signal, and a signal is never
reset / rebound across ops.

### I2 — Slot-reuse safety

A slot enters its free ring at init. It leaves the free ring on
`acquireXxxSlot()` and returns only after `slot_qp_mask` reaches
`slot_qp_expected` — i.e. every per-qp callback for its current
tenant has fired. While a slot is out of the free ring, no other op
can claim it; while a WR referencing `&slot.assigns_[qpi]` is
outstanding, the slot is out of the free ring. Therefore **no CQE
whose `wr_id` points into `slot.assigns_[qpi]` can ever dispatch
into a callback installed by a different op.**

### I3 — Callback lifetime

Transport callbacks capture `shared_ptr<EndpointOpState>`. The
`RDMAAssign::callback_` storage would normally close a cycle
(`op_state → assigns → callback_ → op_state`); the CQ poller
(`rdma_context.cpp`) moves the callback to its own stack before
invoking it, so the cycle dies the moment the lambda returns. No
reference counting games are needed to reclaim completed ops.

### I4 — Op lifetime

An `EndpointOpState` lives exactly as long as at least one of these
holds a strong reference:

- its future (owned by user code),
- a slot currently leasing it (`slot.op_state`),
- a callback in flight (moved onto the CQ thread stack).

Once all three are gone, `shared_ptr` reclaims the op_state. The
endpoint's weak registry has no impact on lifetime.

### I5 — RNR avoidance

The inbound RECV windows on the meta channel, the message data
channel, and the io-data channel are pre-posted at `connect()` and
kept primed across op boundaries. The refactor never removes a
posted RECV without first reposting a replacement. Slot release
happens strictly AFTER the callback has run — and the callback, for
message paths, is the place that reposts — so the RQ is never empty
just because an op retired.

## What the Slot Is NOT

This is what changed. Historically the slot did everything; after
the refactor each of these is explicitly elsewhere:

- **Not the completion owner.** The slot does not store the
  user-visible signal, completion status, or `imm_data`. Those
  live on `EndpointOpState`. `SendFuture`/`RecvFuture`/
  `ReadWriteFuture`/`ImmRecvFuture` do not hold slot pointers.
- **Not the op identity.** The slot is a dispatch vehicle reused
  across ops. `op_id`-style identity is unnecessary once the free
  ring invariant (I2) is in place; no generation counter is kept
  on the slot.
- **Not the WR ownership root.** `RDMAAssign` lives inside the
  slot by value, but the slot cannot be re-leased while any WR is
  outstanding (I2). So the WRs are effectively owned by "the
  current lease" for their useful lifetime.

## Message-Path Specifics (Send / Recv)

The two-sided path uses the same slot lease model but has extra
slot-local transport state that is reset per lease:

- `SendContext::meta_arrived_flag_`  — reset in `acquireSendSlot()`.
  Set by the `meta_recv_assign_` callback when the peer writes a
  meta reply. Preserved across slot releases because the RECV
  itself is continually reposted for RNR safety.
- `SendContext::remote_meta_info_`   — overwritten by the peer via
  `WRITE_WITH_IMM`. Slot-local buffer.
- `RecvContext::local_meta_info_`    — filled by user's `recv()`
  call, then written to the peer.

The meta handshake callback installed at `connect()` captures the
slot (not the op_state) and only flips the slot-local flag. The
user-visible `op_state.signal` is set only by the `data_send_assigns_`
/ `data_recv_assigns_` callbacks installed when the op actually runs
(`sendProcess` / `recvProcess`), which capture the current
`op_state` by `shared_ptr` — consistent with I3.

## Imm-Recv Path Specifics

`immRecv()` is a pure matching path — it does not acquire a slot.
The transport-owned `ImmRecvContext` array stays permanently posted
on the hardware RQ (this is what keeps WRITE_WITH_IMM senders from
hitting RNR). Each completion is copied into an `ImmRecvEvent` value
object and matched to either a pending user op (if `immRecv()` was
called first) or queued for the next caller (if the WRITE_WITH_IMM
arrived first). The CQ-side slot refill uses a lock-free MPSC stack;
the endpoint progress loop drains the stack and reposts the slots.
This is entirely orthogonal to the send/recv/read/write free-slot
rings and intentionally so — the semantics are different.

## What cancelAll() Does

`cancelAll()` walks the weak in-flight registry and force-completes
every live `EndpointOpState` (sets status FAILED, wakes signal). It
also drains the imm-recv matching queues and the refill stack. It
deliberately does NOT touch slot pools — the pool invariant is that
slots re-enter the free ring only when their WRs have fired, and
cancelAll does not violate that. Anything still sitting in
`pending_{rw,send,recv}_queue_` un-posted will either post normally
if the endpoint resumes or be flushed as FLUSH_ERR when the channel
is destroyed, at which point the normal callback path releases the
slot.

## How To Extend This Safely

Adding a new endpoint operation?

1. Allocate a fresh `EndpointOpState` (via `makeOpState`) with a
   unique signal. Register with `registerInFlight()`.
2. If the op needs a dispatch slot, lease one via the appropriate
   `acquireXxxSlot()`. Set `slot.op_state` to your new op_state.
3. When building callbacks: capture `shared_ptr<EndpointOpState>`
   for per-op state updates. Capture the slot pointer (if needed)
   only to drive `slot_qp_mask` and release the slot.
4. In the final callback for the slot, `fetch_or` the qp bit into
   `slot.slot_qp_mask`. If it equals `slot_qp_expected`, call the
   matching `releaseXxxSlot()`.
5. Return an `RDMAFuture` subclass that holds the
   `shared_ptr<EndpointOpState>`. The future must never dereference
   the slot.
