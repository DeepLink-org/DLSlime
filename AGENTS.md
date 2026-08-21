# Repository Guidelines

## Build Environment

Before building or reinstalling DLSlime, expose the CUDA headers and libraries
shipped by the NVIDIA Python packages:

```bash
export NVIDIA_PYTHON_ROOT=/usr/local/lib/python3.12/dist-packages/nvidia

export CPATH="$(find "$NVIDIA_PYTHON_ROOT" -maxdepth 2 -type d -name include | paste -sd: -)${CPATH:+:$CPATH}"

export LIBRARY_PATH="$(find "$NVIDIA_PYTHON_ROOT" -maxdepth 2 -type d -name lib | paste -sd: -)${LIBRARY_PATH:+:$LIBRARY_PATH}"

export LD_LIBRARY_PATH="$(find "$NVIDIA_PYTHON_ROOT" -maxdepth 2 -type d -name lib | paste -sd: -)${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
```
