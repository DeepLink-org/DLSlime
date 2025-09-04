#pragma once

#include <map>
#include <string>
#include <vector>

// Mock class for testing

namespace adxl {

const int SUCCESS = 0;

enum MemType {
    MEM_HOST,
    MEM_DEVICE
};

enum TransferOp {
    WRITE,
    READ
};

struct MemDesc {
    uint64_t addr = 0;
    size_t   len  = 0;
};

struct TransferOpDesc {
    uintptr_t local_addr;
    uintptr_t remote_addr;
    size_t    len;
};

class MemHandle {};

class AscendString {
public:
    AscendString(const char* s) {}
};

class AdxlEngine {
public:
    int Initialize(const AscendString& as, const std::map<AscendString, AscendString>& m)
    {
        return SUCCESS;
    }

    int Connect(const AscendString& as, int timeout_mills)
    {
        return SUCCESS;
    }

    int Disconnect(const AscendString& as, int timeout_mills)
    {
        return SUCCESS;
    }

    int RegisterMem(const MemDesc& mem_desc, const MemType& mem_type, const MemHandle& mem_handle)
    {
        return SUCCESS;
    }

    void DeregisterMem(const MemHandle& mem_handle) {}

    int TransferSync(const AscendString&                as,
                     const TransferOp&                  operation,
                     const std::vector<TransferOpDesc>& op_descs,
                     int                                timeout_mills)
    {
        return SUCCESS;
    }
};

}  // namespace adxl