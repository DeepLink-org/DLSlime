#pragma once
#include <nvtx3/nvToolsExt.h>

#include <string>

namespace dlslime {
enum NvtxColor {
    COLOR_GREEN  = 0x00FF00,
    COLOR_RED    = 0xFF0000,
    COLOR_BLUE   = 0x0000FF,
    COLOR_YELLOW = 0xFFFF00,
    COLOR_PURPLE = 0x800080
};

class NvtxScope {
public:
    NvtxScope(const std::string& name, uint32_t color)
    {
        nvtxEventAttributes_t eventAttrib = {0};
        eventAttrib.version               = NVTX_VERSION;
        eventAttrib.size                  = NVTX_EVENT_ATTRIB_STRUCT_SIZE;
        eventAttrib.colorType             = NVTX_COLOR_ARGB;
        eventAttrib.color                 = 0xFF000000 | color;  // Alpha = 0xFF
        eventAttrib.messageType           = NVTX_MESSAGE_TYPE_ASCII;
        eventAttrib.message.ascii         = name.c_str();
        nvtxRangePushEx(&eventAttrib);
    }

    ~NvtxScope()
    {
        nvtxRangePop();
    }
};

#define NVTX_RANGE(name, color) slime::NvtxScope _nvtx_scope(name, color)
#define NVTX_THREAD(name) nvtxNameOsThread(pthread_self(), name)

}  // namespace dlslime
