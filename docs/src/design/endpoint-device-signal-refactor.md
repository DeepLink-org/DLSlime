# Endpoint DeviceSignal Refactor

## Status

This document proposes a focused refactor of `RDMAEndpoint` completion ownership.
The goal is to fix a correctness hazard in the current endpoint design before
adding more async features on top of it.

This is intentionally **not** an RPC-only design. The issue exists in the
generic endpoint layer and affects all paths built on top of it.

Current branch status:

- first slice landed: futures now bind to `EndpointOpState` instead of raw
  reusable slot contexts
- second slice landed: slot release / queue cleanup is stricter during
  shutdown and reuse
- current in-progress slice: add operation ownership guards (`op_id` /
  generation) to protect against late completions after cancel / slot reuse

Important constraint:

- message-path pre-posted receive windows must remain in place
- removing those pre-posts causes `RNR` on the two-sided message path
- so correctness work in this area must preserve the always-posted receive
  window and only tighten ownership / completion semantics around it

## Problem Summary

Today, `RDMAEndpoint` pre-allocates context arrays and device signals by slot:

- `ReadWriteContext[]`
- `ImmRecvContext[]`
- `SendContext[]`
- `RecvContext[]`

Each slot owns:

- a reusable `DeviceSignal`
- reusable context-local completion fields
- a reusable future object that points back to that slot

New operations select a slot with:

- `counter % FIFO_DEPTH`

and then overwrite the slot in place.

This creates an ABA-style hazard:

1. operation A gets slot `k`
2. future A waits on slot `k`
3. operation B later reuses slot `k`
4. slot `k`'s signal / stream / expected mask / completion fields are reset
5. future A may observe B's completion state instead of A's

This is a generic endpoint design problem, not an RPC-specific bug.

## Evidence In Current Code

Current slot-owned signal and future construction:

- `dlslime/csrc/engine/rdma/rdma_endpoint.cpp`
  - IO context/signal pool initialization
  - message context/signal pool initialization
- `dlslime/csrc/engine/rdma/rdma_future.cpp`
  - futures wait directly on `ctx_->signal`

Current modulo-based slot reuse:

- `read/write/writeWithImm`: `rw_slot_id_ % SLIME_MAX_IO_FIFO_DEPTH`
- `send`: `send_slot_id_ % SLIME_MAX_MSG_FIFO_DEPTH`
- `recv`: `msg_recv_slot_id_ % SLIME_MAX_MSG_FIFO_DEPTH`
- `immRecv`: `io_recv_slot_id_ % SLIME_MAX_IO_FIFO_DEPTH`

The current `DeviceSignal` interface is reusable mutable state:

- bind stream
- reset all flags
- mark comm done
- wait on current flags

That is fine for a pooled resource, but unsafe if the resource is also the
user-visible completion owner for a previously returned future.

## Why This Matters Beyond RPC

The same slot-owned completion model is used by all endpoint operations:

- two-sided `send/recv`
- one-sided `read/write/writeWithImm`
- immediate receive `immRecv`

So even if RPC is the first place the bug shows up, the underlying hazard is in
the generic endpoint contract.

This also blocks future work:

- async issue/wait decoupling
- streaming
- higher in-flight depth
- background progress threads
- power-user direct endpoint use

## Goals

1. Make completion ownership belong to an operation, not to a reusable slot.
2. Keep the endpoint fast and pool-based.
3. Preserve the existing public API shape as much as possible.
4. Avoid reintroducing this class of bugs in new transports or protocols.

## Non-Goals

1. Redesigning SlimeRPC itself.
2. Redesigning `PeerAgent`.
3. Adding streaming in the same patch.
4. Refactoring non-RDMA endpoints in the first step.

## Root Cause

The current design conflates two different concepts:

- execution storage: reusable slot/context memory used by the endpoint worker
- completion ownership: the thing a caller waits on after the API returns

Execution storage can be safely pooled.
Completion ownership must remain stable for the lifetime of one logical
operation.

Right now both are represented by the same slot-local state.

## Proposed Design

### 1. Introduce Per-Operation Completion State

Add a new operation-owned object, tentatively named `EndpointOpState`.

Suggested contents:

- `std::shared_ptr<DeviceSignal> signal`
- `uint64_t op_id`
- `uint32_t expected_mask`
- `std::atomic<uint64_t> completion_mask`
- `std::atomic<int32_t> completion_status`
- `std::atomic<int32_t> imm_data`

This object is the stable identity of one operation.

Returned futures should own a `std::shared_ptr<EndpointOpState>`, not a raw
pointer to a reusable slot context.

### 2. Keep Context Slots Reusable

Keep pooled endpoint contexts for transport execution:

- `ReadWriteContext`
- `ImmRecvContext`
- `SendContext`
- `RecvContext`

But they should become internal worker-side execution slots only.

Each in-flight context should point to the current `EndpointOpState` for the
logical operation it is serving.

### 3. Add Explicit Slot Ownership Instead of Blind Modulo Reuse

Replace:

- `counter % FIFO_DEPTH`

with explicit slot ownership checks per context type.

Suggested structure:

- a checked acquire path for each context class
- release only after the transport callback has finished using the slot
- a generation token on each slot so late callbacks can detect reuse

This fixes the deeper correctness issue that exists even if signals are
decoupled: reusable context memory itself should not be overwritten while an
earlier operation may still be consuming it.

### 4. Add a DeviceSignal Pool

We do not need to bind one permanent signal to one permanent slot.

Instead:

- build a `DeviceSignalPool`
- operation submission acquires a signal lease
- the lease is attached to `EndpointOpState`
- when the future and endpoint no longer need that operation state, the signal
  returns to the pool

This keeps allocation costs low without making signal identity depend on slot
identity.

### 5. Futures Wait On Operation State

All RDMA futures should be changed to own operation state:

- `SendFuture`
- `RecvFuture`
- `ReadWriteFuture`
- `ImmRecvFuture`

`wait()` should:

1. wait on `op_state->signal`
2. read `op_state->completion_status`
3. for `ImmRecvFuture`, read `op_state->imm_data`

The future should not directly dereference mutable slot-local state after the
operation has been submitted.

### 6. Callbacks Update Operation State, Not Slot-Owned User State

Transport callbacks should update:

- `op_state->completion_status`
- `op_state->imm_data`
- `op_state->signal`

For callbacks that may run after cancellation or slot reuse:

- capture the submitting operation's `op_id`
- compare that against the slot's current generation before touching slot-local
  bookkeeping
- if generations do not match, treat the callback as stale and ignore its
  slot-local effects

Once the callback knows the slot-local transport bookkeeping is done, the slot
can be returned to the slot pool.

### 7. Cancellation Should Target In-Flight Operation States

`RDMAEndpoint::cancelAll()` should stop force-completing every slot signal in
the static arrays.

Instead, the endpoint should track currently leased `EndpointOpState` objects
and complete those explicitly during shutdown.

That makes teardown semantics reflect actual in-flight operations rather than
whatever slot happened to exist in the pool.

## Operation-Specific Notes

### Read / Write / WriteWithImm

These are the clearest first target.

Current behavior:

- one logical request may consume one or more context slots
- only the final slot signals user-visible completion

Proposed behavior:

- one logical request gets one `EndpointOpState`
- each internal slot callback contributes to that same op state
- the final slot marks the operation complete

### Send / Recv

Two-sided message operations should also move to operation-owned completion.

The current message path has the same ownership problem:

- slot-local signal
- slot-local reusable future
- modulo slot reuse

However, the message transport has an additional constraint:

- the pre-posted receive window must remain live to avoid `RNR`

So the safe refactor order here is:

1. keep the pre-posted receive window
2. add operation ownership guards around completion handling
3. only revisit message-path pre-post strategy in a separate documented step

### immRecv

`immRecv` needs extra care because the current code mixes:

- user-visible receive requests
- speculative pre-posted receive windows

For the first safe refactor, prefer correctness over pre-post cleverness:

- bind each user `immRecv()` call to a stable `EndpointOpState`
- bind posted receive contexts explicitly to that operation

If needed, a larger pre-post window can be reintroduced later on top of the new
ownership model.

## Suggested Implementation Order

### Phase 1: Mechanical Safety Refactor

1. Add `EndpointOpState`
2. Convert RDMA futures to own `shared_ptr<EndpointOpState>`
3. Route callback completion into operation state
4. Stop reading completion data from reusable slot memory in futures

### Phase 2: Slot Reuse Hardening

1. Replace modulo slot reuse with explicit free-slot pools
2. Release slots only after callback-side transport ownership ends
3. Track active in-flight operation states

### Phase 3: Signal Pooling

1. Add `DeviceSignalPool`
2. Make operation state lease signals from the pool
3. Return signals to the pool when op state is retired

### Phase 4: immRecv Window Recovery

1. Re-evaluate pre-post strategy
2. Reintroduce speculative posting only if ownership remains explicit and safe

## Compatibility

Public Python APIs should remain stable:

- `send()`
- `recv()`
- `read()`
- `write()`
- `writeWithImm()`
- `immRecv()`

The refactor should be largely internal to endpoint/future implementation.

## Expected Benefits

1. Removes stale-wakeup / wrong-owner completion hazards
2. Makes async usage safe by construction
3. Makes RPC bugs easier to reason about because endpoint ownership is no
   longer ambiguous
4. Provides a clean base for future streaming and overlap work

## Acceptance Criteria

The refactor is considered complete when:

1. No returned future depends on mutable slot-local signal ownership.
2. No endpoint path reuses a context slot without explicit release.
3. `cancelAll()` targets in-flight operations rather than all pooled slots.
4. Existing endpoint tests and RPC benchmark still pass.
5. Direct endpoint users can issue multiple outstanding operations without
   ABA-style completion aliasing.

## Recommended Scope For The First Branch

The first implementation branch should focus only on:

- `RDMAEndpoint`
- RDMA futures
- `DeviceSignal` pooling/ownership as used by RDMA

It should not mix in:

- new RPC features
- streaming
- scheduler/executor changes in NanoDeploy

That keeps the patch reviewable and directly addresses the underlying design
risk first.
