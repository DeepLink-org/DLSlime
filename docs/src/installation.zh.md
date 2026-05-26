# 安装指南

## 从 PyPI 安装

```bash
pip install dlslime dlslime-ctrl
```

PyPI 包使用默认 CMake 选项构建。如果需要可选传输后端或本地 C++ 修改，请从源码构建。

## 从源码安装

```bash
git clone https://github.com/DeepLink-org/DLSlime.git
cd DLSlime
pip install -v --no-build-isolation -e dlslime
pip install -e dlslime-ctrl                 # 可选：Rust 控制面
```

启用可选组件时，可以通过环境变量传递 CMake 选项：

```bash
BUILD_NVLINK=ON BUILD_TORCH_PLUGIN=ON \
  pip install -v --no-build-isolation -e dlslime
```

纯 C++ 构建：

```bash
cmake -S dlslime -B build -GNinja -DBUILD_PYTHON=OFF -DBUILD_RDMA=ON
cmake --build build
```
