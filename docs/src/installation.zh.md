# 安装指南

## 从 PyPI 安装

```bash
pip install dlslime==0.0.3
```

PyPI 包使用默认 CMake 选项构建。如果需要可选传输后端或本地 C++ 修改，请从源码构建。

## 从源码安装

```bash
git clone https://github.com/DeepLink-org/DLSlime.git
cd DLSlime
pip install -v --no-build-isolation -e .
```

启用可选组件时，可以通过环境变量传递 CMake 选项：

```bash
BUILD_NVLINK=ON BUILD_TORCH_PLUGIN=ON \
  pip install -v --no-build-isolation -e .
```

纯 C++ 构建：

```bash
cmake -S . -B build -GNinja -DBUILD_PYTHON=OFF -DBUILD_RDMA=ON
cmake --build build
```
