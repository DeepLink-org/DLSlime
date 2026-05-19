# TcpEndpoint v4 — Future / OpState / Session / Primitive 关系重构

**状态**: 已实现并测试通过 (2026-05-18)

## 已实现功能

- 4 个 async 原语基于 ClientSession + Future + OpState 模型
- ClientSession 与 ServerSession 对称：start_write/start_read vs readBody/writeBody
- 多 assign 支持：迭代 vector，每个 assign 创建独立 ClientSession，共享 OpState
- CUDA 两端 staging：async_send/write/read + ServerSession readBody/writeBody
- send/recv 脱离 MemoryPool（裸指针模式），read/write 继续使用 MR 寻址
- `register_memory_region(name, ptr, offset, length)` 接口对齐 RDMAEndpoint
- 编译开关：`USE_CUDA=ON ./build_and_test.sh all` 启用 CUDA 路径
- 宽松截断 + exact_size 拒绝 + overflow 保护

## 当前状态（已过时，仅供参考）

```
async_send(chunk):
  取连接 → TcpOpState → asio::post(lambda) → return Future
    lambda: async_write(header+payload) → signal op → return conn

async_read(assign):
  取连接(RESERVE) → TcpOpState → asio::post(lambda) → return Future
    lambda: async_write(header) → async_read(response) → signal op → return conn
```

问题：
1. I/O 生命周期散落在 lambda 捕获中，无显式状态机
2. async_read 的 write_header → read_response 是两个回调嵌套
3. ServerSession 有清晰的 `readHeader → dispatch → readBody/writeBody`，
   但客户端没有对应的 ClientSession
4. `assign_tuple_t` (local_mr, remote_mr, remote_off, local_off, length) 的解析
   散落在 endpoint 方法中，与 I/O 执行耦合

## 接口对齐

与 RDMAEndpoint 保持一致（已去除 void* stream, writeWithImm/immRecv）：

```cpp
// TwoSide (对应 RDMA send/recv, 异步化)
std::shared_ptr<TcpSendFuture> async_send(const chunk_tuple_t& chunk,
                                           int64_t timeout_ms = kDefaultTimeoutMs);
std::shared_ptr<TcpRecvFuture> async_recv(const chunk_tuple_t& chunk);

// OneSide (对应 RDMA read/write, 异步化)
std::shared_ptr<TcpReadWriteFuture> async_read(
    const std::vector<assign_tuple_t>& assign,
    int64_t timeout_ms = kDefaultTimeoutMs);
std::shared_ptr<TcpReadWriteFuture> async_write(
    const std::vector<assign_tuple_t>& assign,
    int64_t timeout_ms = kDefaultTimeoutMs);
```

- `async_` 前缀：TCP 全部为异步（I/O 在 io_context 线程），与 RDMA 的同步 Future 区分
- `void* stream`：已删除（TCP 无 CUDA stream）
- `std::vector<assign_tuple_t>`：接口接受 vector，但 v4 不对多个 assign 做聚合。
  每个原语调用 = 一个 ClientSession = 一个 Future。
  多个 assign 的聚合留给上层（SlimeRPC）。

## 关键数据结构

### 两种 tuple，两种寻址模型

```cpp
// send/recv — 双边，只需要本地 buffer 信息
using chunk_tuple_t = std::tuple<uintptr_t, uint64_t, size_t>;
//                       mr_handle    offset    length

// read/write — 单边，指定本地+远端两个 buffer
using assign_tuple_t = std::tuple<uintptr_t, uintptr_t, uint64_t, uint64_t, size_t>;
//                        local_mr    remote_mr   remote_off   local_off   length
```

`assign_tuple_t` 已经包含了完成一次单边操作所需的**所有**寻址信息：
- 远端地址 = remote_mr.addr + remote_off → `SessionHeader.addr`
- 本地地址 = local_mr.addr + local_off → 本地读写位置
- 长度 = length → `SessionHeader.size`

### 从 assign_tuple_t 到 SessionHeader 的映射（在 Primitive 中完成 MR 解析）

```cpp
// async_write: assign_tuple_t → SessionHeader + local_src
const auto& a = assign[0];
auto local  = local_pool_->get_mr_fast(std::get<0>(a));   // local_mr handle
auto remote = remote_pool_->get_remote_mr_fast(std::get<1>(a)); // remote_mr handle

uint64_t remote_addr = remote.addr + std::get<2>(a);  // remote_off
uint64_t local_src   = local.addr  + std::get<3>(a);  // local_off
size_t   len         = std::get<4>(a);                  // length

SessionHeader hdr{len, remote_addr, OP_WRITE};
// ClientSession 拿到的是解析后的 hdr + local_src，不接触 assign_tuple_t
```

## v4 目标：四者关系

```
┌─────────────────────────────────────────────────────────┐
│  Primitive (TcpEndpoint::async_xxx)                     │
│                                                         │
│  1. 解析 assign_tuple_t / chunk_tuple_t → MR 寻址       │
│  2. 构建 SessionHeader (wire format)                    │
│  3. 创建 OpState (completion signal)                    │
│  4. 获取连接 (from pool)                                │
│  5. 创建 ClientSession(sock, op, hdr, payload_src/dst)  │
│  6. return Future(op)                                   │
└────────────┬────────────────────────────────────────────┘
             │ 创建
    ┌────────▼──────────┐         ┌──────────────────┐
    │  ClientSession    │────────→│  TcpOpState      │←──────┐
    │  (I/O 状态机)      │ signal  │  (完成信号)       │       │
    │  shared_ptr 自管理 │         └────────┬─────────┘       │
    │                    │                  │ 被持有           │
    │  start_write()     │                  │                 │
    │  start_read()      │         ┌────────▼─────────┐       │
    │  on_done → 归还连接 │         │  TcpFuture       │       │
    └────────────────────┘         │  (用户句柄)       │───────┘
                                   │  wait()/wait_for()│
                                   └──────────────────┘
```

### 关系矩阵

| 对象 | 生命周期 | 知道什么 | 不知道什么 |
|------|---------|---------|-----------|
| **Primitive** | 单次调用 | MR 寻址, hdr 构建, assign_tuple_t 解析 | 线协议细节, async I/O 回调链 |
| **OpState** | ≥ Future 生命周期 | completion_status, signal | I/O 如何完成, 谁在驱动 |
| **Future** | 调用者持有 | wait()/wait_for() | 线协议, socket, 连接池 |
| **ClientSession** | I/O 进行中 | hdr, socket, payload 指针 | MR handle, assign_tuple_t |
| **ServerSession** | 连接存续期间 | socket, recv_matcher | 连接池, Future, OpState |

## ClientSession 设计

一个 ClientSession = 一次出站 I/O 操作的完整生命周期。

```cpp
class ClientSession : public std::enable_shared_from_this<ClientSession> {
public:
    using DoneCallback = std::function<void(asio::error_code ec)>;

    ClientSession(asio::ip::tcp::socket sock, DoneCallback on_done);

    // write: header + payload (both async_write, gather)
    void start_write(const SessionHeader& hdr, const void* payload);

    // read: write header → read response into dst
    void start_read(const SessionHeader& hdr, void* dst);

private:
    asio::ip::tcp::socket socket_;
    DoneCallback          on_done_;
    SessionHeader         hdr_{};
    // chunk_buf_ 不需要 — write 直接用 payload 指针, read 直接用 dst 指针
};
```

关键设计决策：
- **ClientSession 不持有 OpState** — 它只报告 `ec`。由 Primitive 在 on_done 中 signal OpState
- **ClientSession 不持有 PooledConnection** — 它只持有 socket。由 Primitive 在 on_done 中归还连接
- 这样 ClientSession 是纯粹的 I/O 状态机，不耦合 Future/OpState/Pool

### 原语 → ClientSession 映射

```
async_send(chunk):
  ┌─ 解析 chunk_tuple_t → mr.addr + offset → src_ptr, length
  ├─ hdr = {length, 0, OP_SEND}
  ├─ op = TcpOpState::create()
  ├─ conn = pool.getConnection()
  ├─ session = make_shared<ClientSession>(move(conn->socket),
  │      [op, conn, &pool](ec) {
  │          op->completion_status = ec ? FAILED : SUCCESS;
  │          op->signal->set_comm_done(0);
  │          pool.returnConnection(conn);
  │      });
  ├─ session->start_write(hdr, src_ptr);
  └─ return TcpSendFuture(op);

async_write(assign):     ← 同上, hdr.opcode = OP_WRITE, hdr.addr = remote_addr
async_read(assign):       ← session->start_read(hdr, dst_ptr)
                            dst_ptr = local_mr.addr + local_off
async_recv(chunk):       ← 无 ClientSession (注册到 pending_recvs_)
```

### std::vector<assign_tuple_t> 的多 assign 处理

RDMA 中多个 assign 可聚合为一个 WR chain（一次 `ibv_post_send`，一个 Future）。
TCP 没有硬件聚合——每个 assign 对应一个独立的线消息（一个 header + payload）。
但接口约定是一个 `std::vector<assign_tuple_t>` → 一个 Future。

处理方式：**迭代 vector，每个 assign 创建一个 ClientSession，共享一个 OpState**。

```
async_write([assign_0, assign_1, assign_2]):
  op = TcpOpState::create()
  op->expected_mask = (1 << 3) - 1    // 3 个 assign, 等 3 个 session 完成

  for i, a in enumerate(assign):
    解析 a → hdr + src_ptr
    conn = pool.getConnection()        // 复用同一连接
    session = ClientSession(sock, [op, conn, i, &pool](ec) {
        if (!ec) op->signal->set_comm_done(i);  // 设置第 i 位
        pool.returnConnection(conn);
    })
    session->start_write(hdr, src_ptr)

  return TcpReadWriteFuture(op)   // wait 等待 expected_mask 所有位就绪
```

每个 assign → 一个 session → 一次 `async_write`（串行在线路上，同连接）。
Future.wait() 自旋等待 `completion_mask` 达到 `expected_mask`。

**与单 assign 的统一**：单 assign 是 `expected_mask = 1` 的特例。
ClientSession 不感知是单还是多——只负责一个 I/O 操作。

### 不再需要的

- `asio::post` — ClientSession 构造后直接在调用者线程调 start_xxx，asio async_write/async_read 已经在 io_context 上
- `weak_ptr<TcpEndpoint>` — ClientSession 不持有 endpoint 引用
- `pending_reads_` map — 不再需要按 request_id 匹配响应。async_read 创建的 ClientSession 在 start_read 的 on_done 中直接拿到结果

## 入站/出站对称

```
ServerSession (入站, 持久)            ClientSession (出站, 瞬态)
──────────────────────────            ──────────────────────────
readHeader()   ← socket              start_write(hdr, payload) → socket
dispatch()                            start_read(hdr, dst)      → socket
  ├─ OP_SEND: async_read → signal     write_header → callback
  ├─ OP_WRITE: readBody → memcpy       read_response → callback
  └─ OP_READ: writeBody → done        on_done → Primitive signal → 析构
readHeader() ← 循环
```

## 文件变更

| 文件 | 变更 |
|------|------|
| `tcp_session.h` | 新增 ClientSession 类 (约 35 行) |
| `tcp_session.cpp` | 新增 ClientSession 实现 (约 50 行): start_write, start_read |
| `tcp_endpoint.cpp` | async_send/write/read 从 ad-hoc lambda → ClientSession; 删除 pending_reads_ 相关逻辑; 删除 asio::post |
| `tcp_endpoint.h` | 删除 `pending_reads_`, `read_mu_`, `next_req_id_` (不再需要 request_id 匹配); 公开 API 不变 |

## 不聚合的理由

`assign_tuple_t` 是一个单次 I/O 操作的完整描述——不是可拆分的子操作集合。
每个 async_read/async_write 调用对应一个 ClientSession。
多个 assign 的聚合留给上层（如 SlimeRPC channel 的多个 slot），
不在 TcpEndpoint 层处理。

## Timeout 设计

### 两层 timeout，不同归属

| 层 | 机制 | 归属 | 语义 |
|----|------|------|------|
| **Future 层** | `wait_for(ms)` 定时自旋轮询 signal | Future / 调用者 | "我等不了了，但操作还在后台跑" |
| **I/O 层** | `asio::steady_timer` + `socket.cancel()` | ClientSession | "真的取消这个 I/O" |

### v4 实现 Future 层，v5 实现 I/O 层

**v4**：
- `timeout_ms` 参数保留在方法签名中，但仅作为 OpState 的提示值存储
- 真正的超时由 `future.wait_for(seconds)` 控制——调用者决定等待多久
- ClientSession 不感知 timeout——它总是跑完 I/O 链

```cpp
fut = ep.async_send((h, 0, 128), timeout_ms=5000);
// timeout_ms 存入 op_state, 但 async I/O 链不受影响
status = fut.wait_for(3.0);  // 调用者侧超时 — 3 秒后返回 None
// 3 秒后 ClientSession 可能还在写, 完成后仍会 signal op_state
// 只是没有人等这个 signal 了
```

**v5**：加 `asio::steady_timer` 给 ClientSession
```cpp
void ClientSession::start_write(...) {
    if (timeout_ms_ > 0) {
        timer_.expires_after(ms(timeout_ms_));
        timer_.async_wait([this](ec) { if (!ec) socket_.cancel(); });
    }
    asio::async_write(socket_, bufs, ...);
}
// timer 触发 → socket.cancel() → async_write 回调收到 operation_aborted
// → on_done(operation_aborted) → op->completion_status = TCP_TIMEOUT
```

### 为什么不把 timeout_ms 去掉

保留它的两个理由：
1. 接口与 RDMA 的 `send(chunk, stream)` 模式一致——都有一个"额外控制参数"的位置
2. 它为 v5 的 timer 实现预留了参数位，届时只需改内部实现，不改变 API

## 为什么不做

- **recv 无 ClientSession** — 无出站 I/O
- **不拆 WriteSession/ReadSession** — 差异小，合并为一个 ClientSession
- **不在 Future 中持有 Session** — Future 只 wait，通过 OpState 间接关联
- **ClientSession 不持有 OpState** — 只报 ec，由 Primitive 的 on_done 统一 signal

## 未来规划

### CUDA 锁页内存

当前 CUDA staging 使用 `new char[]`（可分页内存），D2H/H2D `cudaMemcpy` 走的是同步 device→host 拷贝，pageable memory 路径较慢。

后续改为 `cudaHostAlloc()` 分配锁页（pinned）内存，使 `cudaMemcpy` 能走 DMA 快速路径。同时可考虑 `cudaMemcpyAsync` + `cudaStream` 与 io_context 的异步重叠。

### async_recv exact_size 自适应

当前 `exact_size` 是 opt-in boolean 参数，默认 `false`（宽松截断）。未来改为默认自适应：
- 当 `send_size <= recv_size`：自动启用严格检查（exact match）
- 当 `send_size > recv_size`：自动宽松截断
- 移除 `exact_size` 参数，行为由实际数据量驱动
