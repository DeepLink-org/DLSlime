import os


def setup_nvshmem_env(qp_num: int = 8):
    # NVSHMEM ENVS
    # Adapted from https://github.com/Deepseek-ai/DeepEP.git
    os.environ["NVSHMEM_DISABLE_P2P"] = "0"
    os.environ["NVSHMEM_IB_ENABLE_IBGDA"] = "1"
    os.environ["NVSHMEM_IBGDA_NUM_RC_PER_PE"] = str(qp_num)
    # Make sure QP depth is always larger than the number of on-flight WRs, so that we can skip WQ slot check
    os.environ["NVSHMEM_QP_DEPTH"] = os.environ.get("NVSHMEM_QP_DEPTH", "1024")

    # Reduce gpu memory usage
    # 6 default teams + 1 extra team
    os.environ["NVSHMEM_MAX_TEAMS"] = "7"
    # Disable NVLink SHArP
    os.environ["NVSHMEM_DISABLE_NVLS"] = "1"
    # NOTES: NVSHMEM initialization requires at least 256 MiB
    os.environ["NVSHMEM_CUMEM_GRANULARITY"] = f"{2 **29}"
