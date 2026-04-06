# NanoDeploy MLA SP `hao_basic` All2All 迁移需求文档

## 1. 文档目的

本文用于把当前的迁移诉求、边界、基线版本、关键语义、测试方式和验收标准固定下来，作为后续在 `DLSlime` 与 `NanoDeploy` 两个仓库中实施迁移的统一输入。

本文关注的是：

- 将新版 all2all 后端从独立仓库迁移回主仓库 `DLSlime`
- 让该后端服务于 NanoDeploy 的 MLA SP decode 路径
- 覆盖 `q` 与 `res/lse` 的 all2all 传输
- 其中 `q` 路径必须支持 `offsets`
- 保留 legacy 路径，便于新老后端对照验证

## 2. 背景与目标

当前已经有一版新的 all2all 后端实现，但它位于单独仓库：

- 新版实现来源仓库：`/mnt/nvme1n1/ml_research/linbinbin1/DLSlime_hao_0403`

现在需要把其中和 NanoDeploy MLA SP decode 路径相关的部分迁移到主仓库：

- 目标 DLSlime 仓库：`/mnt/nvme1n1/ml_research/linbinbin1/DLSlime`
- 目标 NanoDeploy 仓库：`/mnt/nvme1n1/ml_research/linbinbin1/NanoDeploy-April-v2`

本次迁移的最终目标是：

1. 在 `DLSlime` 中落地新的 `hao_basic` 后端能力。
2. 在 `NanoDeploy` 中接入并切换到该后端，用于 MLA SP 路径的 `q` 和 `res/lse` all2all。
3. 其中 `q` 路径必须支持 packed `offsets` 语义，`res/lse` 路径保持 transpose 且不使用 `offsets`。
4. 保留 legacy 路径，允许对照新旧后端做功能验证。

## 3. 基线仓库与版本约束

### 3.1 DLSlime

- 工作仓库：`/mnt/nvme1n1/ml_research/linbinbin1/DLSlime`
- 当前仓库 HEAD：`78ddb39`
- 之前尝试提交：`78ddb39`，主题为 `feat: add native basic all-to-all buffer for MLA SP`
- 本次正式施工基线：从 `2dcd8af` 切出新分支

约束：

- `78ddb39` 可作为已有尝试和差异参考
- 本次迁移不应直接在 `78ddb39` 上继续堆改动
- 应从 `2dcd8af` 重新切分支，选择性迁移需要的实现

### 3.2 NanoDeploy

- 工作仓库：`/mnt/nvme1n1/ml_research/linbinbin1/NanoDeploy-April-v2`
- 之前相关修改范围：`33addd0..2eeb57b`
- 本次正式施工基线：从 `1b22b94` 切出新分支

约束：

- `33addd0..2eeb57b` 作为历史设计、测试脚本和验证方式的参考范围
- 本次应从 `1b22b94` 重新拉新分支做迁移，不直接叠在旧尝试链路上

### 3.3 Legacy 对照版本

为了做新老后端功能对照，需要把旧版 DLSlime 安装成 `dlslime_legacy`。

已确认的 legacy 源码仓库：

- `/mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a`

确认依据：

- `DLSlime-a2a/pyproject.toml` 中包名定义为 `dlslime_legacy`
- `DLSlime-a2a/tests/python/test_nanodeploy_prepare_decode_all_to_all.py` 直接从 `dlslime_legacy` 导入旧版 `AllToAllIntraLLBuffer`

旧版本参考测试与文档：

- 测试：`/mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a/tests/python/test_nanodeploy_prepare_decode_all_to_all.py`
- 文档：`/mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a/docs/nanodeploy_prepare_decode_all2all.md`

## 4. 参考材料

### 4.1 旧版本语义与行为基准

以如下文档作为本次 `q/res/lse` all2all 语义基准：

- `/mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a/docs/nanodeploy_prepare_decode_all2all.md`

该文档定义了：

- `prepare_decode_cpp` 产出的元数据如何约束 q/res/lse all2all
- `q_offsets`、`q_slice_*`、`res_slice_*` 的含义
- `q_mask`、`res_lse_mask` 的方向和索引语义
- `q` 的 packed non-transpose 路径与 `res/lse` 的 transpose 路径

### 4.2 旧版本目标测试

本次功能验证以如下测试为主：

- `/mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a/tests/python/test_nanodeploy_prepare_decode_all_to_all.py`

该测试覆盖两部分：

1. CPU 元数据验证
   - 验证 RPC 过滤前后 `prepare_decode_cpp` 产出的 q/res/lse all2all 元数据保持一致
2. GPU 功能验证
   - 验证 `q`、`res`、`lse` 的 all2all 行为符合预期

### 4.3 可复用实现来源

新版实现的主要参考来源是：

- `/mnt/nvme1n1/ml_research/linbinbin1/DLSlime_hao_0403`

优先参考的文件包括：

- `dlslime/buffer/intra/all_to_all_intra_ll_buffer.py`
- `csrc/python/bind.cpp`
- `csrc/ops/intra_ll/all_to_all/all_to_all_intra_ll_buffer.h`
- `csrc/ops/intra_ll/all_to_all/all_to_all_intra_ll_buffer.cpp`
- `csrc/ops/intra_ll/all_to_all/alltoall_buffer.h`
- `csrc/ops/intra_ll/all_to_all/alltoall_buffer.cpp`

## 5. 本次需求范围

### 5.1 在范围内

- 将 `DLSlime_hao_0403` 中与 `hao_basic` 后端有关的实现迁移到 `DLSlime`
- 让 NanoDeploy MLA SP decode 路径支持切换到新后端
- 支持 MLA SP 路径中的以下三段通信：
  - `q` all2all
  - `res` all2all
  - `lse` all2all
- 让 `q` 路径支持 `offsets=q_offsets`
- 保留 legacy 路径，便于和新后端做功能一致性对比
- 以 `DLSlime-a2a` 中现有测试为主做验收

### 5.2 不在主范围内

- prefill 路径
- 与 MLA SP decode 无关的 all2all 使用场景
- 泛化到所有可能的 backend / kernel 组合
- 新版后端的发版、wheel 发布、自动安装流程
- 任何与本次 `q/res/lse` 迁移无关的业务逻辑调整

## 6. 关键语义要求

以下语义以 `DLSlime-a2a/docs/nanodeploy_prepare_decode_all2all.md` 为准。

### 6.1 `q` all2all

要求：

- `q` 走 non-transpose all2all
- `q` 必须支持 `offsets`
- `offsets` 使用 `prepare_decode_cpp` 生成的 `q_offsets`
- 输入是当前 rank 的本地 master q，形状按 `q.view(bs, -1)` 展平
- 输出容量视图仍保持兼容布局，但有效数据按 `q_offsets` 指定的 packed 前缀排布
- 上层只消费 packed 前缀 `[0:attention_compute_bs)`

可执行定义：

- `offsets.shape == [sp_size + 1]`
- `offsets[0] == 0`
- `counts[i] = offsets[i + 1] - offsets[i]`
- source rank `i` 的有效 q 输出区间为 `[offsets[i], offsets[i + 1])`

### 6.2 `res / lse` all2all

要求：

- `res` 与 `lse` 走 transpose all2all
- 这两条路径不传 `offsets`
- 输入布局是 `target_rank * max_num_seqs + seq_id` 的严格展开布局
- 输出布局是 `[source_rank, seq_id, ...]`

### 6.3 `mask` 语义

必须保持与 NanoDeploy `prepare_decode` 产出一致：

- `q_mask[target_rank, seq_id] = 1`
  - 表示当前 rank 需要把该 master request 的 q 发给 `target_rank`
- `res_lse_mask[target_rank, seq_id] = 1`
  - 表示当前 rank 需要把对应 partial `res/lse` 发回 `target_rank`

额外约束：

- self 行会被清零
- self 路径依赖 `local_buffer` 的预写入，不通过 IPC 通信补齐

### 6.4 与 RPC 过滤的兼容性

当前测试覆盖的约束是：

- 开启 `use_dlslime_rpc=True` 与 `optimize_decode_block_table=True` 后
- 若被过滤掉的序列只发生在各 `master_sp_idx` 分组尾部
- 且保留序列的相对顺序不变
- 则 `prepare_decode_cpp` 产出的 q/res/lse all2all 元数据应保持不变

因此本次迁移后，应继续满足该测试假设，不得破坏其元数据等价性。

## 7. 两个仓库分别要完成的事情

### 7.1 DLSlime 侧需求

`DLSlime` 需要完成以下工作：

1. 将 `hao_basic` 相关实现从 `DLSlime_hao_0403` 迁移到主仓库。
2. 保留 legacy 实现，不覆盖现有 `AllToAllIntraLLBuffer` 路径。
3. 提供新后端所需的 Python 绑定、C++ 绑定和编译入口。
4. 支持 MLA SP 所需的三类行为：
   - `q` 的 non-transpose + offsets
   - `res` 的 transpose
   - `lse` 的 transpose
5. 保证上层仍能访问 `local_buffer`，以维持 self-path 预写入语义。
6. 保证新后端可以与旧后端同时存在，便于 A/B 测试。

DLSlime 侧的结果要求：

- 既能保留 legacy 行为
- 又能为 NanoDeploy 暴露可切换的新后端能力
- 且 `hao_basic` 在 `q` 路径上原生支持 `offsets`

### 7.2 NanoDeploy 侧需求

`NanoDeploy-April-v2` 需要完成以下工作：

1. 从 `1b22b94` 切新分支接入新的 backend。
2. 在 MLA SP decode 路径中切换 `q/res/lse` 的 all2all 后端。
3. 对 `q` 路径显式传入 `offsets=context.q_offsets`。
4. 对 `res/lse` 路径继续走 transpose 且不传 `offsets`。
5. 保留 legacy backend，便于新旧对照。
6. 将 backend 选择做成明确的启动配置或上下文配置，而不是硬编码单一路径。

NanoDeploy 侧的结果要求：

- 可以在不破坏 legacy 路径的前提下切到 `hao_basic`
- `MLA SP` 主路径能稳定跑通
- `q/res/lse` 的上层张量格式与后续 combine 逻辑保持兼容

## 8. 建议的实施边界

为了降低迁移风险，本文采用如下施工边界：

1. 旧后端和新后端共存。
2. 默认保留 legacy 作为对照基线。
3. 新后端通过明确 backend 名称启用，例如 `hao_basic`。
4. backend 切换按 worker 生命周期生效，不要求运行中热切换。
5. 本次优先完成功能正确性，再做性能验证。

这组边界的目的不是限制后续扩展，而是保证本轮迁移聚焦在“把 MLA SP 的 q/res/lse 通起来并验证正确”。

## 9. 安装与测试方式

### 9.1 安装

#### 当前 DLSlime

```bash
cd /mnt/nvme1n1/ml_research/linbinbin1/DLSlime
BUILD_INTRA_OPS=ON pip install -v -e . --no-build-isolation
```

#### Legacy DLSlime

```bash
cd /mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a
BUILD_INTRA_OPS=ON pip install -v -e . --no-build-isolation
```

说明：

- `DLSlime-a2a` 安装后包名为 `dlslime_legacy`
- 当前 `DLSlime` 继续作为新版待迁移实现的安装入口

#### NanoDeploy

```bash
cd /mnt/nvme1n1/ml_research/linbinbin1/NanoDeploy-April-v2
pip install -v -e . --no-build-isolation
```

### 9.2 主要测试入口

主要测试文件：

- `/mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a/tests/python/test_nanodeploy_prepare_decode_all_to_all.py`

推荐分两层执行：

1. 先跑 CPU 元数据测试，验证 `prepare_decode_cpp` 元数据和 RPC 过滤假设
2. 再跑 GPU all2all 测试，验证 `q/res/lse` 行为

GPU 测试示例命令：

```bash
torchrun --nproc_per_node=2 -m pytest -q \
  /mnt/nvme1n1/ml_research/linbinbin1/DLSlime-a2a/tests/python/test_nanodeploy_prepare_decode_all_to_all.py \
  -s
```

### 9.3 测试环境注意事项

需要明确以下环境细节：

1. GPU 测试至少需要 2 个 rank。
2. 测试涉及 CUDA / NCCL / IPC / GPU 通信时，需要先申请提权。
3. 该测试默认会尝试从相对路径下的 `NanoDeploy-legacy/build/lib/_nanodeploy_cpp*.so` 加载 C++ 扩展。
4. 如果实际使用的是 `NanoDeploy-April-v2`，则需要：
   - 要么显式设置 `NANODEPLOY_CPP_SO`
   - 要么调整测试环境，使其能找到正确的 `_nanodeploy_cpp` 动态库

## 10. 验收标准

满足以下条件，视为本轮迁移完成：

1. `DLSlime` 中已具备可用的 `hao_basic` 后端实现，且不破坏 legacy 路径。
2. `NanoDeploy-April-v2` 可以从 `1b22b94` 基线切出新分支并接入该后端。
3. MLA SP decode 路径中：
   - `q` 走 non-transpose + `offsets=q_offsets`
   - `res` / `lse` 走 transpose 且不带 `offsets`
4. `DLSlime-a2a/tests/python/test_nanodeploy_prepare_decode_all_to_all.py` 能针对 `hao_basic` 后端通过功能测试。
5. 能同时保留 `dlslime_legacy` 作为旧后端对照基线。
6. 新旧后端在测试覆盖场景下行为一致，至少包括：
   - `prepare_decode_cpp` 元数据一致性
   - `q/res/lse` all2all 功能一致性

## 11. 风险与待确认项

### 11.1 已知风险

- `q` 路径要求 `offsets` 真正减少通信量，不能只是接口透传
- `res/lse` 路径必须维持 transpose 语义，不能因为新后端接入而退化
- 测试脚本默认寻找 `NanoDeploy-legacy`，与当前目标仓库 `NanoDeploy-April-v2` 存在环境命名差异

### 11.2 待确认项

- NanoDeploy 中最终使用的 backend 配置项命名
- `_nanodeploy_cpp` 动态库在 `NanoDeploy-April-v2` 下的最终构建与发现方式
- 迁移时是选择性手工搬运代码，还是按文件级别部分 cherry-pick；当前建议是“从 `2dcd8af` / `1b22b94` 新分支上选择性迁移”，不要直接复用旧尝试分支历史

## 12. 一句话结论

本次任务不是简单“把一个新 all2all 文件拷回来”，而是要在 `DLSlime` 和 `NanoDeploy` 两个仓库中同时完成一轮带语义约束的双仓迁移：保留 legacy，对接 `hao_basic`，让 MLA SP decode 的 `q` 支持 offsets，让 `res/lse` 保持 transpose，并用 `DLSlime-a2a` 里的现成测试把功能闭环跑通。
