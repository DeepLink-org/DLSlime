# DLSlime

<div class="hero">
  <div>
    <h1>面向 AI 服务的可组合通信运行时</h1>
    <p>
      DLSlime 以 PeerAgent 为中心，提供面向分布式 AI 系统的通信与微服务工具。
      应用可以按需采用 Endpoint、PeerAgent、SlimeRPC 和 DLSlimeCache，而不必一次性迁移整套系统。
    </p>
  </div>
  <div>
    <img src="../assets/images/dlslime_arch.png" alt="DLSlime 架构">
  </div>
</div>

## 快速入口

<div class="feature-grid">
  <a href="installation/"><strong>安装指南</strong><br>从 PyPI 安装，或从源码启用可选传输后端。</a>
  <a href="quickstart/"><strong>快速开始</strong><br>运行 Endpoint、PeerAgent、Cache 和 RPC 示例。</a>
  <a href="guide/"><strong>用户指南</strong><br>把 DLSlime 组件接入服务化应用。</a>
  <a href="design/architecture/"><strong>架构设计</strong><br>理解 PeerAgent、NanoCtrl 和 Endpoint 的协作方式。</a>
</div>

## 核心层次

| 层次         | 适用场景                                                            |
| ------------ | ------------------------------------------------------------------- |
| Endpoint API | 应用已经自己管理 peer 放置和元数据交换。                            |
| PeerAgent    | 希望 DLSlime 管理连接建立、内存区域发现和过期状态清理。             |
| DLSlimeCache | 多个 PeerAgent 客户端需要共享 RDMA-backed cache service。           |
| SlimeRPC     | 希望通过 Python 服务调用表达业务逻辑，同时复用 DLSlime 的传输协调。 |
