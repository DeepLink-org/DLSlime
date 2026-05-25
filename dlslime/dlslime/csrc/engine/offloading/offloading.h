#pragma once

namespace dlslime {

/*
Motivation:
1. GPU Direct RDMA is a little hard to configure (modify Linux Kernel)
2. TCP mode is very easy to achieve (General transport protocol)

TODO List:
1. Add async offloading and loading support
2. Support VRAM <=> DRAM first
3. A memory pool
4. A backpressure Strategy
5. Gather transport
*/

class Offloader {};
class Loader {};

class DRAMMemoryPool {};

}  // namespace dlslime
