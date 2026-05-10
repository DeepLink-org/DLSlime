# Installation

## From PyPI

```bash
pip install dlslime==0.0.3
```

The PyPI package is built with the default CMake flags. Build from source when
you need optional transports or local C++ changes.

## From Source

```bash
git clone https://github.com/DeepLink-org/DLSlime.git
cd DLSlime
pip install -v --no-build-isolation -e .
```

Pass CMake flags through the environment when enabling optional components:

```bash
BUILD_NVLINK=ON BUILD_TORCH_PLUGIN=ON \
  pip install -v --no-build-isolation -e .
```

For a pure C++ build:

```bash
cmake -S . -B build -GNinja -DBUILD_PYTHON=OFF -DBUILD_RDMA=ON
cmake --build build
```

## Build Flags

| Flag                  |                                  Default | Description                                            |
| --------------------- | ---------------------------------------: | ------------------------------------------------------ |
| `BUILD_RDMA`          |                                     `ON` | Build the RDMA transfer engine                         |
| `BUILD_PYTHON`        | `OFF` in CMake, `ON` in `pyproject.toml` | Build Python bindings                                  |
| `BUILD_NVLINK`        |                                    `OFF` | Build the NVLink transfer engine                       |
| `BUILD_ASCEND_DIRECT` |                                    `OFF` | Build Ascend Direct transport                          |
| `BUILD_TORCH_PLUGIN`  |                                    `OFF` | Build DLSlime as a torch backend                       |
| `BUILD_BENCH`         |                                    `OFF` | Build C++ transfer-engine benchmarks                   |
| `BUILD_TEST`          |                                    `OFF` | Build C++ tests                                        |
| `USE_MACA`            |                                    `OFF` | Enable Metax platform support for torch backend builds |
