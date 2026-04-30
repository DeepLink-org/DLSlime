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
    ep1 = _slime_c.RDMAEndpoint(num_qp=num_qp)
    ep2 = _slime_c.RDMAEndpoint(num_qp=num_qp)

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

    device = torch.device(device_type)
    max_size = max(test_sizes)
    send_buffer = torch.empty((max_size,), dtype=torch.uint8, device=device)
    recv_buffer = torch.empty((max_size,), dtype=torch.uint8, device=device)

    if device_type == "cuda":
        torch.cuda.synchronize()

    # One-Sided 操作必须显式注册目标内存，并获取 RKey。这里注册最大 buffer 一次，
    # 每个 size 的测试只使用 buffer 前缀，避免循环中累积注册资源。
    local_ptr = send_buffer.data_ptr()
    remote_ptr = recv_buffer.data_ptr()

    local_name = str(local_ptr)
    mr_name = str(remote_ptr)

    local_handle = ep1.register_memory_region(
        local_name,
        local_ptr,
        int(send_buffer.storage_offset()),
        max_size,
    )
    ep2.register_memory_region(
        mr_name,
        remote_ptr,
        int(recv_buffer.storage_offset()),
        max_size,
    )

    remote_handle = ep1.register_remote_memory_region(mr_name, ep2.mr_info()[mr_name])

    print(
        f"{'Size':<15} | {'Latency (us)':<15} | {'Bandwidth (GB/s)':<20} | {'Check':<10}"
    )
    print("-" * 70)

    for size in test_sizes:
        # ---------------------------------------------------------
        # A. 准备数据 (Tensor View)
        # ---------------------------------------------------------
        send_tensor = send_buffer[:size]
        recv_tensor = recv_buffer[:size]
        send_tensor.random_(0, 255)
        recv_tensor.zero_()
        if device_type == "cuda":
            torch.cuda.synchronize()

        # ---------------------------------------------------------
        # B. 预热 (Warmup)
        # ---------------------------------------------------------
        warmup_iters = 5
        for _ in range(warmup_iters):
            # Target: 发布 Recv 请求以捕获 WriteWithImm 的立即数
            recv_slot = ep2.imm_recv()

            # Initiator: 执行 WriteWithImm
            send_slot = ep1.write_with_imm(
                [(local_handle, remote_handle, 0, 0, size)], 888, None
            )

            # 等待完成
            send_slot.wait()
            recv_slot.wait()

        if device_type == "cuda":
            torch.cuda.synchronize()

        # ---------------------------------------------------------
        # C. 正式评测 (Benchmark Loop)
        # ---------------------------------------------------------
        t_start = time.perf_counter()

        for _ in range(iterations):
            # 1. Target Post Recv (必须在数据到达前 Ready，或者在硬件队列中有空位)
            # IO Endpoint 内部有队列，这里调用只是入队
            recv_slot = ep2.imm_recv()

            # 2. Initiator RDMA Write with Immediate
            send_slot = ep1.write_with_imm(
                [(local_handle, remote_handle, 0, 0, size)], 888, None
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
        # D. 计算指标
        # ---------------------------------------------------------
        total_time = t_end - t_start
        avg_latency_s = total_time / iterations
        avg_latency_us = avg_latency_s * 1e6
        throughput_gbs = (size * iterations) / total_time / 1e9

        # ---------------------------------------------------------
        # E. 数据校验
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
