# DLSlime TcpEndpoint v3 Primitives 架构与实现计划

**分支**: `tcp-v3` | **基准**: `main` | **日期**: 2026-05-14

---

## 1. 架构设计

### 1.1 总体架构

```
┌──────────────────────────────────────────────────────────────┐
│  Python 调用者线程                                            │
│  ep.async_send(chunk, timeout_ms=30000) → Future             │
│  ep.async_recv(chunk, timeout_ms=30000) → Future             │
│  ep.async_read(assign, timeout_ms=30000) → Future            │
│  ep.async_write(assign, timeout_ms=30000) → Future           │
│         │                                                    │
│         │ post lambda                                        │
│         ▼                                                    │
│  ┌──────────────────────┐    ┌─────────────────────────────┐ │
│  │  asio::io_context     │    │  TcpConnectionPool          │ │
│  │  (单后台线程)          │◄───│  (host, port) → deque<conn> │ │
│  │                      │    │  IDLE / ACTIVE / RESERVED   │ │
│  │  async_write ────────┼───►│  60s 空闲超时               │ │
│  │  async_read ◄────────┼───►│                             │ │
│  │  async_accept ───────┼───►│  ServerSession              │ │
│  │                      │    │  (readHeader→dispatch→      │ │
│  │                      │    │   readBody→readHeader 循环) │ │
│  └──────────────────────┘    └─────────────────────────────┘ │
└──────────────────────────────────────────────────────────────┘
```

### 1.2 线程模型

| 角色 | 线程 | 职责 |
|------|------|------|
| io_context | 1 个 daemon 线程 | `io_ctx_.run()` — 所有 asio async I/O 回调 |
| 调用者 | N 个 Python 线程 | 调 async_* → 立即返回 Future；wait() 自旋阻塞 |
| accept | io_context | `async_accept` 回调链，每连接创建 ServerSession |

### 1.3 asio 操作模型

```
调用者线程                               io_context 线程
──────────                              ──────────────
async_send(chunk, 5000):
  ├─ getConnection()  [sync, fast]      ┌─ async_write(header+payload)
  ├─ SO_SNDTIMEO=5s                     │    → 归还连接 → signal op_state
  ├─ asio::post(lambda) ──────────────► │
  └─ return Future ◄─── signal ────────┘

async_recv(chunk, 5000):
  ├─ pending_recvs_.push(op_state)      ┌─ ServerSession::dispatch(OP_SEND)
  └─ return Future                      │    → pop pending_recvs_
       │                                │    → memcpy → signal op_state
       └── wait_for(5.0) ── timeout? ──┘

async_read(assign, 5000):
  ├─ getConnection() [RESERVE]          ┌─ async_write(OP_READ header)
  ├─ asio::post(lambda) ──────────────► │    → async_read(response data)
  └─ return Future ◄─── signal ────────┘    → 归还连接 → signal op_state

async_write(assign, 5000):
  ├─ getConnection()  [sync, fast]      ┌─ async_write(header+payload)
  ├─ SO_SNDTIMEO=5s                     │    → 归还连接 → signal op_state
  ├─ asio::post(lambda) ──────────────► │
  └─ return Future ◄─── signal ────────┘
```

---

## 2. 线协议设计

### 2.1 SessionHeader (17 字节，对齐 Mooncake)

```
偏移  大小  字段
0     8     size    (payload 字节数, little-endian: htole64 / le64toh)
8     8     addr    (远端 buffer 虚拟地址)
16    1     opcode  (操作码)
─────────────────
     17 bytes total
```

### 2.2 为什么 3 个 opcode 支持 4 个原语？

OP_SEND 同时承载 `async_send`（发起方主动 push 数据）和 `async_recv`（接收方
被动等待）。recv 方不在线上发送任何操作码——它只是向本地 `pending_recvs_` 队列注册
一个 buffer，然后对端 ServerSession 在收到 OP_SEND 时通过 `RecvMatcher` 回调 pop
队列前端、memcpy 数据并 signal op_state。

这与 Mooncake 的设计一致：ServerSession::dispatch(OP_SEND) 先分块读取 payload，
然后通过 recv_matcher_ 匹配本地注册的 recv buffer。不需要独立的 recv opcode——
SEND 到达本身就隐含了"有一端在等待"的语义。

OP_READ 和 OP_WRITE 各需独立 opcode，因为服务端 dispatch 分支逻辑完全不同：
- OP_READ：读取本地内存后异步写回原始数据（无 header）
- OP_WRITE：读取 payload 后 memcpy 到 hdr.addr

如果有 4 个 opcode（比如独立的 OP_RECV），反而增加冗余——OP_RECV 在语义上等于
"我准备好接收了"，但这已在连接建立时通过 endpoint_info 交换 MR 信息隐式表达，
不需要每个操作发一次。

| opcode | 值 | 线格式 | 远端 ServerSession 动作 | DLSlime 原语 |
|--------|-----|--------|------------------------|-------------|
| `OP_SEND` | 0x00 | header{sz, 0, 0x00} + payload | 读 payload → recv_matcher pop → memcpy → signal | **async_send** (发起) / **async_recv** (被动) |
| `OP_READ` | 0x01 | 仅 header{sz, addr, 0x01} | 从本地 addr 读 sz 字节 → async_write 原始数据发回 | **async_read** (调用者 pull) |
| `OP_WRITE` | 0x02 | header{sz, addr, 0x02} + payload | 读 payload → memcpy 到本地 addr | **async_write** (调用者 push) |

### 2.3 四个原语在线上的完整流程

```
async_send(chunk):
  调用者: getConnection → post to io_ctx → return Future
  io_ctx: async_write(sock, [header{OP_SEND}|payload])
    → on_complete: returnConnection → signal op_state
  对端 ServerSession: async_read(header) → dispatch(OP_SEND)
    → chunk_buf_.resize → readBody 分块读 payload → recv_matcher_()
    → pop pending_recv → memcpy → signal recv op_state

async_recv(chunk):
  调用者: pending_recvs_.push({buffer, op_state}) → return Future → wait_for(timeout)
  (无 opcode 在线路上 — recv 是 SEND 的被动消费方)

async_read(assign):
  调用者: getConnection(RESERVED) → post to io_ctx → return Future
  io_ctx: async_write(sock, header{OP_READ, sz, remote_addr})
    → async_read(sock, user_buffer, sz)
    → on_complete: returnConnection → signal op_state
  对端 ServerSession: async_read(header) → dispatch(OP_READ)
    → async_write(sock, local[addr], sz) → readHeader 继续

async_write(assign):
  调用者: getConnection → post to io_ctx → return Future
  io_ctx: async_write(sock, [header{OP_WRITE, sz, remote_addr}|payload])
    → on_complete: returnConnection → signal op_state
  对端 ServerSession: async_read(header) → dispatch(OP_WRITE)
    → chunk_buf_.resize → readBody 分块读 payload → memcpy 到 addr
```

---

## 3. 接口设计

### 3.1 C++ TcpEndpoint 公共接口

```cpp
class TcpEndpoint : public std::enable_shared_from_this<TcpEndpoint> {
public:
    // 默认超时 30 秒
    static constexpr int64_t kDefaultTimeoutMs = 30000;

    // ── 构造 ──

    // 【主构造】每个 endpoint 内部自动创建 TcpContext, 调用者无需关心。
    // 这是最常用的场景: 一个 endpoint = 一个 peer 连接。
    explicit TcpEndpoint(uint16_t port = 0);

    // 【次构造】注入外部共享 TcpContext, 用于多 endpoint 复用单 io_context 线程
    // 的高级优化场景 (如 PeerAgent 连接 N 个 peer 时节省 N-1 个线程)。
    // 仅在明确需要跨 endpoint 共享资源时使用。
    TcpEndpoint(TcpContext& ctx, uint16_t port = 0);

    // ── 连接 ──
    json endpoint_info() const;   // {host, port, mr_info}
    void connect(const json& remote_info);
    void shutdown();

    // ── 内存 ──
    int32_t register_memory_region(const std::string& name,
                                   uintptr_t ptr, uintptr_t offset, size_t length);
    int32_t register_remote_memory_region(const std::string& name,
                                           const json& mr_info);
    json mr_info() const;

    // ── 异步通信原语 (全部返回 Future, I/O 在 io_context 线程) ──
    //
    // timeout_ms 由调用者通过 future.wait_for() 控制实际操作时限;
    // 方法签名的 timeout_ms 仅作为 op_state 的提示值传入。
    // recv 的超时完全由 future.wait_for() 控制, 不需要 timeout_ms 参数。

    // 双边发送
    std::shared_ptr<TcpSendFuture> async_send(
        const chunk_tuple_t& chunk,
        int64_t timeout_ms = kDefaultTimeoutMs);

    // 双边接收 (超时通过 future.wait_for())
    std::shared_ptr<TcpRecvFuture> async_recv(
        const chunk_tuple_t& chunk);

    // 单边读
    std::shared_ptr<TcpReadWriteFuture> async_read(
        const std::vector<assign_tuple_t>& assign,
        int64_t timeout_ms = kDefaultTimeoutMs);

    // 单边写
    std::shared_ptr<TcpReadWriteFuture> async_write(
        const std::vector<assign_tuple_t>& assign,
        int64_t timeout_ms = kDefaultTimeoutMs);

    // ── 访问器 ──
    void setId(int64_t id);
    int64_t getId() const;
    bool is_connected() const;
};
```

### 3.2 C++ TcpFuture 接口

```cpp
class TcpFuture : public DeviceFuture {
public:
    // 无限期阻塞等待
    int32_t wait() const override;

    // 限时等待: timeout_ms 毫秒, 成功返回 true 并写 *out
    // 超时返回 false (操作仍在进行, 可重试)
    bool wait_for(int64_t timeout_ms, int32_t* out) const;
};

class TcpSendFuture      : public TcpFuture { };
class TcpRecvFuture      : public TcpFuture { };
class TcpReadWriteFuture : public TcpFuture { };
```

### 3.3 Python 接口

```python
from dlslime import TcpEndpoint, TcpMemoryPool

pool = TcpMemoryPool()
buf = ctypes.create_string_buffer(4096)
h = pool.register_memory_region(ctypes.addressof(buf), 0, 4096, "buf")

ep = TcpEndpoint(port=0)         # 0 = 随机端口
info = ep.endpoint_info()        # {'host': '...', 'port': N, 'mr_info': {...}}

ep.connect(peer_info)

# ── 异步原语, 默认 30s 超时 ──
fut = ep.async_send((h, 0, 128))           # 30s 默认超时
fut = ep.async_send((h, 0, 128), 5000)     # 5s 超时
status = fut.wait()                         # 阻塞直到完成, 返回 0=成功

fut = ep.async_recv((h, 0, 128))           # 超时通过 future 控制
result = fut.wait_for(3.0)                  # 3 秒超时, 返回 int 或 None

fut = ep.async_read([(local_h, remote_h, 0, 0, 128)])
fut = ep.async_write([(local_h, remote_h, 0, 0, 128)])

ep.shutdown()
```

---

## 4. 通信原语设计详解

### 4.1 async_send(chunk, timeout_ms = 30000)

**语义**: 将本地注册内存的数据异步发送到对端。对端必须已调用 `async_recv()` 注册接收缓冲区。

**调用者线程**:
1. `local_pool_->get_mr_fast(mr_key)` — resolve 本地 MR
2. `conn_pool_.getConnection(peer_host_, peer_port_)` — 获取或创建 TCP 连接
3. `TcpOpState::create()` + `signal->reset_all()` — 创建完成信号
4. 如果 `timeout_ms > 0`: `setsockopt(fd, SO_SNDTIMEO, timeout_ms)`
5. `asio::post(io_ctx_, lambda)` — 提交到 io_context
6. 立即返回 `TcpSendFuture(op_state)`

**io_context 线程**:
1. `hdr_hton()` — 字节序转换 header
2. `asio::async_write(sock, [header_buf, payload_buf], callback)` — gather write
3. callback:
   - 如果 `timeout_ms > 0`: 恢复 `SO_SNDTIMEO = 0`
   - `op->completion_status = ec ? TCP_FAILED : TCP_SUCCESS`
   - `conn_pool_.returnConnection(conn)`
   - `op->signal->set_comm_done(0)`

**超时行为**: socket 写超时 → write 失败 → completion_status = TCP_FAILED。调用者 `future.wait()` 得到 -1。

### 4.2 async_recv(chunk, timeout_ms = 30000)

**语义**: 注册接收意图。当对端 `async_send()` 的数据到达时，io_context 线程自动匹配并 memcpy 到注册的 buffer。

**调用者线程**:
1. `local_pool_->get_mr_fast(mr_key)` — resolve 本地 MR
2. `TcpOpState::create()` + 设置 `user_buffer`, `user_length`
3. `pending_recvs_.push_back({op_state})` — FIFO 入队
4. 立即返回 `TcpRecvFuture(op_state)`

**io_context 线程** (ServerSession::dispatch, OP_SEND 分支):
1. `readBody()` — 分块读取 payload 到 `chunk_buf_`
2. `RecvSlot slot = recv_matcher_()` — pop FIFO 前端
3. `memcpy(slot.buffer, chunk_buf_.data(), min(payload_len, slot.length))`
4. `slot.op_state->completion_status = TCP_SUCCESS`
5. `slot.op_state->signal->set_comm_done(0)`

**超时行为**: 调用者使用 `future.wait_for(timeout_ms)` 限时等待。超时返回 None，但 recv 保留在队列中——后续到达的 SEND 仍会完成它（调用者可重试）。

### 4.3 async_read(assign, timeout_ms = 30000)

**语义**: 从对端的注册内存异步读取数据。两步异步操作：发 OP_READ header → 收原始响应数据。

**调用者线程**:
1. resolve local + remote MRs
2. `conn_pool_.getConnection(peer_host_, peer_port_)` — RESERVE 连接
3. `TcpOpState::create()` + 设置 `user_buffer`, `user_length`
4. `asio::post(io_ctx_, lambda)` — 提交到 io_context
5. 立即返回 `TcpReadWriteFuture(op_state)`

**io_context 线程**:
1. `hdr_hton()` → `asio::async_write(sock, header_buf, callback_1)`
2. callback_1: 如果写失败 → signal TCP_FAILED + returnConnection
3. `asio::async_read(sock, user_buffer_buf, callback_2)`
4. callback_2:
   - `op->completion_status = ec ? TCP_FAILED : TCP_SUCCESS`
   - `conn_pool_.returnConnection(conn)`
   - `op->signal->set_comm_done(0)`

**对端 ServerSession** (OP_READ 分支):
1. 从 `hdr.addr` 读取 `hdr.size` 字节本地内存
2. `asio::async_write(sock, raw_data, callback)` — 直接写回原始数据（无 header）
3. `readHeader()` — 继续监听下个请求

**超时行为**: `future.wait_for(timeout_ms)`。连接在整个读取期间被 RESERVED，超时后操作继续在后台运行。

### 4.4 async_write(assign, timeout_ms = 30000)

**语义**: 将本地注册内存的数据异步写入对端注册内存。

与 `async_send` 相同的 post+async_write 模式，区别：
- header.opcode = OP_WRITE
- header.addr = remote_addr（对端目标 buffer 地址）
- 对端 ServerSession dispatch(OP_WRITE) → readBody → memcpy 到 `hdr.addr`

**超时行为**: 同 async_send — SO_SNDTIMEO + future.wait_for()。

---

## 5. 连接池设计

### 5.1 状态机

```
                  getConnection()
  [不存在] ────────────────────────► [ACTIVE] (in_use=true)
                                         │
                                    returnConnection()
                                         │
                                         ▼
  [IDLE] (in_use=false, 在 deque 中) ──► 60s 无使用 → cleanupIdleConnections() → 关闭
         │
         │ getConnection() 命中
         ▼
  [ACTIVE] (in_use=true, 离开 deque)
```

### 5.2 接口

```cpp
class TcpConnectionPool {
    // 获取 IDLE 连接或创建新 TCP 连接
    std::shared_ptr<PooledConnection> getConnection(host, port);

    // 归还连接到 IDLE 状态 (或关闭, 如果 socket 已断开)
    void returnConnection(std::shared_ptr<PooledConnection> conn);

    // 淘汰超过 kIdleTimeout (60s) 的空闲连接
    void cleanupIdleConnections();

    // 关闭所有连接 (shutdown 时调用)
    void clear();
};
```

---

## 6. ServerSession 设计

### 6.1 生命周期

```
acceptor.async_accept(socket)
  → ServerSession(socket, local_pool, recv_matcher)
  → session->start()
    → readHeader() ──────────────────────────────────────┐
      → async_read(sock, 17B header)                     │
        → hdr_to_host()                                  │
        → dispatch()                                     │
          ├─ OP_SEND: chunk_buf_.resize → readBody()     │
          │     → memcpy → recv_matcher_() → signal      │
          ├─ OP_WRITE: chunk_buf_.resize → readBody()    │
          │     → memcpy → hdr.addr                      │
          └─ OP_READ: async_write(sock, local[addr])     │
        → readHeader() ──────────────────────────────────┘
```

### 6.2 RecvMatcher

```cpp
// ServerSession 持有的回调, 由 TcpEndpoint 注入
using RecvMatcher = std::function<RecvSlot()>;

// TcpEndpoint::make_recv_matcher():
//   返回一个 lambda, 持有 weak_ptr<TcpEndpoint>
//   在 recv_mu_ 下 pop pending_recvs_ 队列前端
//   返回 {buffer, length, op_state}
```

---

## 7. 文件结构

### 新建文件

```
dlslime/csrc/engine/tcp/
├── CMakeLists.txt              # asio 依赖 + _slime_tcp 共享库
├── tcp_header.h                # 17B SessionHeader + 3 opcodes
├── tcp_memory_pool.h/.cpp      # 纯簿记 (addr, offset, length)
├── tcp_context.h/.cpp          # 共享 io_context + connection_pool + thread
├── tcp_session.h/.cpp          # ServerSession (accept 端) + 分块 I/O
├── tcp_connection_pool.h/.cpp  # (host, port) 连接池
├── tcp_op_state.h              # 操作状态 (signal + atomic status)
├── tcp_future.h                # TcpFuture 层次 (header-only)
├── tcp_endpoint.h/.cpp         # TcpEndpoint: async_send/recv/read/write
├── build_and_test.sh           # 一键构建+测试
└── test_tcp_endpoint.py        # Python 端到端测试 (4 用例)
```

### 修改文件

| 文件 | 变更 |
|------|------|
| `CMakeLists.txt` | `slime_option(BUILD_TCP "Build TCP transport" ON)` |
| `dlslime/csrc/engine/CMakeLists.txt` | `if(BUILD_TCP) add_subdirectory(tcp) endif()` |
| `dlslime/csrc/CMakeLists.txt` | `if(BUILD_TCP) target_link_libraries(dlslime INTERFACE _slime_tcp) endif()` |
| `dlslime/csrc/python/CMakeLists.txt` | `if(BUILD_TCP) target_compile_definitions + list(APPEND ... _slime_tcp) endif()` |
| `dlslime/csrc/python/bind.cpp` | `#ifdef BUILD_TCP` — TcpEndpoint, TcpMemoryPool, TcpFuture pybind11 bindings |

---

## 8. 超时机制总结

| 原语 | 超时位置 | 默认值 | 实现方式 |
|------|---------|--------|---------|
| async_send | socket write | 30000ms | `setsockopt(SO_SNDTIMEO)` + `future.wait_for()` |
| async_recv | 等待数据到达 | 30000ms | `future.wait_for(timeout_ms)` — 定时自旋轮询 signal |
| async_read | 等待远端响应 | 30000ms | `future.wait_for(timeout_ms)` — 定时自旋轮询 signal |
| async_write | socket write | 30000ms | `setsockopt(SO_SNDTIMEO)` + `future.wait_for()` |

**wait_for 实现**:
```cpp
bool TcpFuture::wait_for(int64_t timeout_ms, int32_t* out) const {
    auto deadline = steady_clock::now() + milliseconds(timeout_ms);
    while (true) {
        if (signal->get_comm_done_mask() matches expected_mask) {
            *out = completion_status; return true;
        }
        if (steady_clock::now() >= deadline) {
            // last check before declaring timeout
            if (signal->get_comm_done_mask() matches expected_mask) {
                *out = completion_status; return true;
            }
            return false;
        }
        machnet_pause();  // CPU relax
    }
}
```

---

## 11. 实现步骤

| 阶段 | 文件 | 说明 |
|------|------|------|
| 1. 分支 | `git checkout -b tcp-v3 main` | 基于 main 创建新分支 |
| 2. 头文件 | tcp_header.h, tcp_op_state.h | 17B header + 3 opcodes + op state |
| 3. 内存池 | tcp_memory_pool.h/.cpp | 纯簿记, 无硬件注册 |
| 4. Future | tcp_future.h | header-only, wait + wait_for |
| 5. Context | tcp_context.h/.cpp | 共享 io_context + connection_pool + thread |
| 6. 连接池 | tcp_connection_pool.h/.cpp | get/return/cleanup/clear |
| 7. Session | tcp_session.h/.cpp | ServerSession async_read 回调链 |
| 8. 端点 | tcp_endpoint.h/.cpp | async_send/recv/read/write |
| 9. 构建 | CMakeLists 链 + bind.cpp | BUILD_TCP + pybind11 |
| 10. 测试 | test_tcp_endpoint.py | 5 用例 + timeout 测试 |
| 11. 脚本 | build_and_test.sh | 一键构建+测试 |
| 12. 提交 | git commit | 单 commit, 清晰消息 |

---

## 9. send/recv 设计深度分析

### 核心矛盾：RDMA vs TCP 的 send/recv 语义差异

RDMA 的 send/recv 是**硬件匹配**的：
- 发送方 post Send WR → 硬件从本地 buffer 取数据 → 发到对端 RQ
- 接收方 post Recv WR → 硬件在 RQ 上预置 WQE (buffer地址 + 长度)
- 硬件按**FIFO 顺序**匹配：第 N 个到达的 SEND 消费第 N 个预置的 RECV
- 如果 SEND 到达时没有 RECV → RNR NAK (Receiver Not Ready) → 发送方重试
- 如果 SEND 数据量 > RECV buffer → 截断或报错

TCP **没有硬件匹配**，所有匹配逻辑必须在软件中实现。这带来了三个核心问题：

| 问题 | RDMA 方案 | TCP 需要解决 |
|------|---------|------------|
| 匹配: 哪个 SEND 对哪个 RECV？ | 硬件 RQ FIFO | 软件队列或 tag 匹配 |
| 顺序: SEND 先到还是 RECV 先到？ | 硬件 RNR 重试 | 缓冲或拒绝 |
| 大小: 发送量 > 接收 buffer？ | 截断/报错 | 截断或分片 |

### 三种匹配策略

#### 策略 A: FIFO 队列匹配（v3 plan 默认）

```
recv(chunk) → pending_recvs_.push_back({buffer, op_state})
ServerSession dispatch(OP_SEND):
  payload = readBody()
  slot = recv_matcher_()       // pop front
  memcpy(slot.buffer, payload, min(len, slot.length))
  signal slot.op_state
```

**优点**: 实现简单，与 RDMA 语义一致，足够支持双端点 ping-pong 通信。
**缺点**: 严格 FIFO——调用者无法指定"这个 recv 对应后面第 N 个 send"。多 slot 场景（如 SlimeRPC 的 slotted mailbox）无法用 FIFO 区分。

#### 策略 B: Tag 匹配（Gloo 风格）

```
wire: [header{OP_SEND, sz, tag}] + payload
recv(tag, buffer) → pending_recvs_[tag].push({buffer, op_state})
ServerSession dispatch(OP_SEND):
  payload = readBody()
  slot = pending_recvs_[hdr.addr_as_tag].pop()
  memcpy(slot.buffer, payload)
```

**优点**: 灵活，支持多路复用——一个 TCP 连接可以承载多个逻辑流（如 RPC slot）。
**缺点**: header.addr 字段被复用为 tag（牺牲了 addr 的原始语义），协议复杂度增加。

#### 策略 C: Slot 预注册（Gloo Buffer 风格）

```
每个 Pair 预先创建 N 个 slot buffer:
  pair.createSendBuffer(slot=0, ptr, size)
  pair.createRecvBuffer(slot=1, ptr, size)
wire: [header{OP_SEND, sz, 0, slot}] + payload
ServerSession: 直接 lookup slot → memcpy
```

**优点**: 零队列开销，O(1) slot 查找，SlimeRPC 天然适配。
**缺点**: 需要预注册 slot（与当前 DLSlime MR 模型不兼容），灵活度低。

### 推荐策略：分层渐进

```
Phase 1 (v3) — FIFO 基础:
  pending_recvs_ = deque<{buffer, op_state}>
  wire: header{OP_SEND, sz, addr=0}
  匹配: 严格 FIFO
  足够: 双端点 ping-pong、简单 RPC

Phase 2 — 缓冲早到 SEND:
  early_sends_ = deque<{payload_data}>
  如果 dispatch(OP_SEND) 时 pending_recvs_ 为空:
    → 缓存 payload 到 early_sends_（带大小上限）
    → 下次 recv() 先检查 early_sends_ 再入队
  避免数据丢失

Phase 3 — Tag 匹配 (如需要):
  扩展 header: 用 2 字节 reserved 字段承载 tag
  pending_recvs_ = map<tag, deque<{buffer, op_state}>>
  支持多路复用
```

### send/recv 与 read/write 的本质区别

很多人混淆 send/recv 和 write/read：

| | send/recv | write/read |
|---|---|---|
| 语义 | **双边**：双方都需要显式操作 | **单边**：一方发起，另一方无感知 |
| 数据方向 | send=push, recv=pull (被动) | write=push to remote addr, read=pull from remote addr |
| 远端参与 | recv 方必须预先注册 buffer | 远端 ServerSession 自动处理，无需注册 |
| 寻址方式 | **无地址**（匹配决定目标 buffer） | **有地址**（header.addr 指定远端 buffer） |
| RDMA 类比 | ibv_post_send / ibv_post_recv | ibv_post_send with RDMA_WRITE/RDMA_READ |

核心洞察：**send/recv 的"地址"是隐式的——通过匹配关系决定；
write/read 的"地址"是显式的——header.addr 直接指向远端内存。**

这就是为什么 v3 plan 中：
- OP_SEND: header.addr = 0（不使用），通过 FIFO 匹配目标 buffer
- OP_WRITE: header.addr = remote_addr（直接指定远端目标地址）
- OP_READ: header.addr = remote_addr（直接指定远端源地址）

### v3 实现策略

v3 采用策略 A（FIFO），但为策略 C（slot）预留空间：

```cpp
// 当前: deque — 简单 FIFO
std::deque<PendingRecv> pending_recvs_;

// Phase 3 可演进为: map — tag 匹配
// std::unordered_map<uint16_t, std::deque<PendingRecv>> pending_recvs_;
// 同时扩展 header: 用 reserved 字段承载 tag

void TcpEndpoint::async_recv(const chunk_tuple_t& chunk,
                               int64_t timeout_ms, void*) {
    // resolve MR → op_state → push to FIFO
    // Phase 3: push to pending_recvs_[tag] instead
}

ServerSession::dispatch(OP_SEND):
    readBody() → chunk_buf_
    RecvSlot slot = recv_matcher_()
    if (slot.buffer == 0):
        // Phase 2: buffer early send to early_sends_
        return
    memcpy(slot.buffer, chunk_buf_, min(payload_len, slot.length))
    signal slot.op_state
```

**recv timeout 语义**（区别于 socket timeout）:
- SO_RCVTIMEO 是 socket 级超时（读数据超时）
- `future.wait_for()` 是**注册后等待匹配**的超时
- 超时后 recv 保留在队列中：后续 SEND 仍可完成它（调用者可重试 wait_for）

## 12. 验证计划

```bash
# 构建
./dlslime/csrc/engine/tcp/build_and_test.sh build

# 测试
./dlslime/csrc/engine/tcp/build_and_test.sh test

# 全流程
./dlslime/csrc/engine/tcp/build_and_test.sh
```

**测试用例**:
1. `test_async_send_recv` — A async_send → B async_recv, B async_send → A async_recv
2. `test_async_write_read` — A async_write → B buffer, A async_read → verify
3. `test_recv_timeout` — async_recv + wait_for(0.3s) → None (无对端发送)
4. `test_send_timeout` — async_send(timeout_ms=10000) 参数
5. `test_default_timeout` — async_send() 无参数 → 使用 30000ms 默认值

## 10. TcpContext 设计 — 为同步通信和资源共享做准备

### 使用优先级

TcpContext 类始终存在，ctx_ 成员始终非空。但构造方式有两种优先级：

| 优先级 | 构造 | 场景 | 占比 |
|--------|------|------|------|
| **主** | `TcpEndpoint(port)` | 单 endpoint, 内部自动 new TcpContext | ~90% |
| **次** | `TcpEndpoint(ctx, port)` | 多 endpoint 共享 io_context 线程 | ~10% |

**默认路径**：调用者无需感知 TcpContext——每个 endpoint 构造时内部 `make_unique<TcpContext>()`，
自动创建 io_context + 后台线程 + 连接池。代码最简洁。

**高级路径**：当 PeerAgent 连接 N 个 peer 时，可手动创建一个 TcpContext 并注入到 N 个
TcpEndpoint，将 N 个线程合并为 1 个。TcpContext 也用于测试中精确控制 io_context 生命周期。

两种路径不互斥——同一进程可混合使用。TcpContext 类永不删除，ctx_ 成员永不删除。

### TcpContext 接口

```cpp
class TcpContext {
public:
    TcpContext();               // 创建 io_context + 启动后台线程
    ~TcpContext();              // stop + join + clear pool

    asio::io_context& io_context() { return io_ctx_; }
    TcpConnectionPool& conn_pool() { return conn_pool_; }
    void shutdown();

private:
    asio::io_context   io_ctx_;
    std::thread        io_thread_;
    TcpConnectionPool  conn_pool_{io_ctx_};
    bool               running_{true};
};
```

### TcpEndpoint 与 TcpContext 的关系

```cpp
class TcpEndpoint {
    // 【主构造】自包含 — 内部创建 TcpContext
    explicit TcpEndpoint(uint16_t port = 0)
        : own_ctx_(std::make_unique<TcpContext>())   // 自动创建
        , acceptor_(own_ctx_->io_context())
        , ... {
        ctx_ = own_ctx_.get();  // ctx_ → 内部 context
    }

    // 【次构造】共享 — 注入外部 TcpContext
    TcpEndpoint(TcpContext& ctx, uint16_t port = 0)
        : acceptor_(ctx.io_context())
        , ... {
        ctx_ = &ctx;            // ctx_ → 外部 context, own_ctx_ = nullptr
    }

private:
    TcpContext*                 ctx_{nullptr};   // 始终非空
    std::unique_ptr<TcpContext> own_ctx_;        // 仅主构造时非空
    // ...
};
```

### 为同步通信预留

有了共享 TcpContext，同步包装器可以不依赖单个 endpoint 的事件循环：

```cpp
// 未来 sync_send: 调 async_send + 立刻 future.wait()
std::shared_ptr<TcpSendFuture> sync_send(TcpEndpoint& ep,
                                          const chunk_tuple_t& chunk,
                                          int64_t timeout_ms = 30000) {
    auto fut = ep.async_send(chunk, timeout_ms);
    fut->wait();  // 阻塞调用者线程直到 io_context 完成
    return fut;
}
```

同步版本只是 async + wait() 的语法糖，不需要独立的底层实现。