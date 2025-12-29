#pragma once
#include <cstdint>

namespace dlslime {
class NVLinkFuture {
public:
    NVLinkFuture()  = default;
    ~NVLinkFuture() = default;

    int32_t wait() const
    {
        return 0;
    }
};
}  // namespace dlslime
