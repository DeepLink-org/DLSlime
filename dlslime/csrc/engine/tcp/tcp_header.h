#pragma once

#include <cstdint>
#include <cstring>

namespace dlslime {
namespace tcp {

// 17-byte wire header, referenced from Mooncake SessionHeader.
//   offset  size  field
//   0       8     size       payload byte count (htole64 / le64toh)
//   8       8     addr       remote buffer virtual address
//   16      1     opcode     SEND=0x00  READ=0x01  WRITE=0x02

#pragma pack(push, 1)
struct SessionHeader {
    uint64_t size;
    uint64_t addr;
    uint8_t  opcode;
};
#pragma pack(pop)

static_assert(sizeof(SessionHeader) == 17, "SessionHeader must be 17 bytes");

enum OpCode : uint8_t {
    OP_SEND  = 0x00,   // header + payload → peer recv matches
    OP_READ  = 0x01,   // header only → peer reads local memory → sends data back
    OP_WRITE = 0x02,   // header + payload → peer writes to local memory
};

}  // namespace tcp
}  // namespace dlslime
