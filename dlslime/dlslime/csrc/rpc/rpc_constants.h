// rpc_constants.h - SlimeRPC wire format constants.
//
// This header is the C++ source of truth for the on-wire representation
// of a single SlimeRPC message. Everything here MUST stay byte-identical
// with `dlslime/rpc/channel.py`. Bump WIRE_VERSION on any incompatible
// change so cross-version pairs fail fast at handshake time.
//
// Wire layout per message:
//
//   [ tag (u32) | total_len (u32) | request_id (u64) | payload (bytes) ]
//   imm_data = slot_id (small unsigned int packed into 32-bit IB imm)
//
// total_len includes the 16-byte header. The payload region is whatever
// the user passed to call() / postReply().
#pragma once

#include <cstddef>
#include <cstdint>

namespace dlslime::rpc {

// Bumped on any wire-incompatible change. Keep this aligned with the
// Python WIRE_VERSION in dlslime/rpc/channel.py.
constexpr uint32_t WIRE_VERSION = 3;

#pragma pack(push, 1)
struct WireHeader {
    uint32_t tag;
    uint32_t total_len;
    uint64_t request_id;
};
#pragma pack(pop)
static_assert(sizeof(WireHeader) == 16, "WireHeader must be exactly 16 bytes; do not pad.");

constexpr size_t HEADER_SIZE = sizeof(WireHeader);

// Tag-space conventions (high bits of the u32 tag):
//   REPLY_BIT = reply message
//   EXC_BIT   = reply carries a remote-side exception (pickled dict)
//   ACK_BIT   = chunk-ack control message (legacy, currently unused)
//   CHUNK_BIT = chunked transfer (legacy, currently unused)
//   LAST_BIT  = final chunk in a chunked transfer (legacy, currently unused)
//
// These flags must be independent. Reusing REPLY_BIT|CHUNK_BIT as LAST_BIT
// makes a reply chunk indistinguishable from a last chunk.
constexpr uint32_t EXC_BIT   = 1u << 27;
constexpr uint32_t ACK_BIT   = 1u << 28;
constexpr uint32_t REPLY_BIT = 1u << 29;
constexpr uint32_t CHUNK_BIT = 1u << 30;
constexpr uint32_t LAST_BIT  = 1u << 31;
constexpr uint32_t FLAG_MASK = REPLY_BIT | EXC_BIT | ACK_BIT | CHUNK_BIT | LAST_BIT;

// IB imm_data is a 32-bit field on the wire; pybind binds it as int32_t,
// so the largest slot_count we can ever encode is 2**31. In practice
// slot counts are O(16) and we cap at 1<<16 to keep accounting sane.
constexpr uint32_t MAX_SLOT_COUNT = 1u << 16;

}  // namespace dlslime::rpc
