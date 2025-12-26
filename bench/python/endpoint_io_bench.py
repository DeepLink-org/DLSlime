import argparse
import time

import torch
from dlslime import _slime_c
from dlslime._slime_c import available_nic


def get_readable_size(size_in_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.1f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.1f} TB"


def run_benchmark(device_type="cuda", num_qp=1, iterations=200):
    # 检查设备
    nic_devices = available_nic()
    if not nic_devices:
        raise RuntimeError("No RDMA NIC available.")
    dev = nic_devices[0]

    print(f"Initializing IO Endpoints: Initiator[{dev}] -> Target[{dev}]")
    print(f"Tensor Device: {device_type.upper()}")

    # 2. 创建 IO Endpoint
    # ep1 作为 Initiator (Client), ep2 作为 Target (Server)
    ep1 = _slime_c.RDMAEndpoint(num_qp=1)
    ep2 = _slime_c.RDMAEndpoint(num_qp=1)

    ep1.connect(ep2.endpoint_info())
    ep2.connect(ep1.endpoint_info())

    # 定义测试范围：2KB 到 1GB
    start_size = 128
    end_size = 1024 * 1024 * 1024

    current_size = start_size
    test_sizes = []
    while current_size <= end_size:
        test_sizes.append(current_size)
        current_size *= 2

    print(
        f"{'Size':<15} | {'Latency (us)':<15} | {'Bandwidth (GB/s)':<20} | {'Check':<10}"
    )
    print("-" * 70)

    for size in test_sizes:
        # ---------------------------------------------------------
        # A. 准备数据 (Tensor Allocation)
        # ---------------------------------------------------------
        if device_type == "cuda":
            send_tensor = torch.randint(
                0, 255, (size,), dtype=torch.uint8, device="cuda:0"
            )
            recv_tensor = torch.zeros((size,), dtype=torch.uint8, device="cuda:1")
            torch.cuda.synchronize()
        else:
            send_tensor = torch.randint(
                0, 255, (size,), dtype=torch.uint8, device="cpu"
            )
            recv_tensor = torch.zeros((size,), dtype=torch.uint8, device="cpu")

        # ---------------------------------------------------------
        # B. 内存注册 (Memory Registration)
        # ---------------------------------------------------------
        # One-Sided 操作必须显式注册目标内存，并获取 RKey
        local_ptr = send_tensor.data_ptr()
        remote_ptr = recv_tensor.data_ptr()

        # 注册 Initiator 内存 (虽然 Write 主要需要注册 Remote，但本地也需要注册在 PD 中)
        ep1.register_memory_region(local_ptr, local_ptr, size)
        # 注册 Target 内存
        ep2.register_memory_region(remote_ptr, remote_ptr, size)

        ep1.register_remote_memory_region(
            remote_ptr, ep2.endpoint_info()["mr_info"][str(remote_ptr)]
        )

        # ---------------------------------------------------------
        # C. 预热 (Warmup)
        # ---------------------------------------------------------
        warmup_iters = 5
        for _ in range(warmup_iters):
            # Target: 发布 Recv 请求以捕获 WriteWithImm 的立即数
            recv_slot = ep2.imm_recv()

            # Initiator: 执行 WriteWithImm
            send_slot = ep1.write_with_imm(
                [(local_ptr, remote_ptr, 0, 0, size)], 888, None
            )

            # 等待完成
            send_slot.wait()
            recv_slot.wait()

        if device_type == "cuda":
            torch.cuda.synchronize()

        # ---------------------------------------------------------
        # D. 正式评测 (Benchmark Loop)
        # ---------------------------------------------------------
        t_start = time.perf_counter()

        for _ in range(iterations):
            # 1. Target Post Recv (必须在数据到达前 Ready，或者在硬件队列中有空位)
            # IO Endpoint 内部有队列，这里调用只是入队
            recv_slot = ep2.imm_recv()

            # 2. Initiator RDMA Write with Immediate
            send_slot = ep1.write_with_imm(
                [(local_ptr, remote_ptr, 0, 0, size)], 888, None
            )

            # 3. Synchronization
            # 等待 Write 完成 (ACK 返回)
            send_slot.wait()
            # 等待 Recv 完成 (立即数到达，意味着 Write 数据已落地)
            recv_slot.wait()

        if device_type == "cuda":
            torch.cuda.synchronize()

        t_end = time.perf_counter()

        # ---------------------------------------------------------
        # E. 计算指标
        # ---------------------------------------------------------
        total_time = t_end - t_start
        avg_latency_s = total_time / iterations
        avg_latency_us = avg_latency_s * 1e6
        throughput_gbs = (size * iterations) / total_time / 1e9

        # ---------------------------------------------------------
        # F. 数据校验
        # ---------------------------------------------------------
        check_status = "OK"
        if device_type == "cuda":
            if not torch.equal(
                send_tensor[:128].cpu(), recv_tensor[:128].cpu()
            ) or not torch.equal(send_tensor[-128:].cpu(), recv_tensor[-128:].cpu()):
                check_status = "FAIL"
        else:
            if not torch.equal(send_tensor, recv_tensor):
                check_status = "FAIL"

        print(
            f"{get_readable_size(size):<15} | {avg_latency_us:<15.2f} | "
            f"{throughput_gbs:<20.4f} | {check_status:<10}"
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--device",
        type=str,
        default="cuda",
        choices=["cpu", "cuda"],
        help="Device type for tensors",
    )
    parser.add_argument("--qp", type=int, default=1, help="Number of Queue Pairs")
    parser.add_argument(
        "--iters", type=int, default=200, help="Number of iterations for averaging"
    )

    args = parser.parse_args()

    if args.device == "cuda" and not torch.cuda.is_available():
        print("CUDA not available, switching to CPU.")
        args.device = "cpu"

    run_benchmark(device_type=args.device, num_qp=args.qp, iterations=args.iters)
