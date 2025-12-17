import argparse
import time

import torch
from dlslime import _slime_c, available_nic


def get_readable_size(size_in_bytes):
    for unit in ["B", "KB", "MB", "GB"]:
        if size_in_bytes < 1024.0:
            return f"{size_in_bytes:.1f} {unit}"
        size_in_bytes /= 1024.0
    return f"{size_in_bytes:.1f} TB"


def run_benchmark(device_type="cuda", num_qp=1, iterations=200):
    # 检查设备
    nic_devices = available_nic()
    if len(nic_devices) < 2:
        print(
            f"Warning: Only {len(nic_devices)} RDMA device found. Trying loopback on device 0 if possible, or script might fail."
        )
        dev0 = nic_devices[0]
        dev1 = nic_devices[0]  # Loopback
    else:
        dev0 = nic_devices[0]
        dev1 = nic_devices[1]

    print(f"Initializing Endpoints: Send[{dev0}] <-> Recv[{dev1}]")
    print(f"Tensor Device: {device_type.upper()}")

    # 初始化 Endpoint
    ctx = _slime_c.rdma_context()
    ctx.init_rdma_context(dev0, 1, "RoCE")
    send_endpoint = _slime_c.rdma_endpoint(ctx, num_qp)
    ctx = _slime_c.rdma_context()
    ctx.init_rdma_context(dev0, 1, "RoCE")
    recv_endpoint = _slime_c.rdma_endpoint(ctx, num_qp)

    # 建立连接
    send_endpoint.connect(recv_endpoint.endpoint_info())
    recv_endpoint.connect(send_endpoint.endpoint_info())

    # 定义测试大小：2KB 到 128MB
    # 2KB = 2 * 1024
    # 128MB = 128 * 1024 * 1024
    start_size = 512
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
        if 1:
            # 1. 准备数据
            # 使用 uint8，这样 numel 就等于 bytes
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

            # 2. 注册 RDMA Buffer (MR 注册通常发生在这里)
            send_buffer = _slime_c.rdma_buffer(
                send_endpoint,
                send_tensor.data_ptr(),
                send_tensor.storage_offset(),
                send_tensor.numel(),
            )
            recv_buffer = _slime_c.rdma_buffer(
                recv_endpoint,
                recv_tensor.data_ptr(),
                recv_tensor.storage_offset(),
                recv_tensor.numel(),
            )

            # 3. 预热 (Warmup)
            # 让 MR 建立，TLB 预热，消除第一次慢启动的影响
            warmup_iters = 10
            for _ in range(warmup_iters):
                send_buffer = _slime_c.rdma_buffer(
                    send_endpoint,
                    send_tensor.data_ptr(),
                    send_tensor.storage_offset(),
                    send_tensor.numel(),
                )
                recv_buffer = _slime_c.rdma_buffer(
                    recv_endpoint,
                    recv_tensor.data_ptr(),
                    recv_tensor.storage_offset(),
                    recv_tensor.numel(),
                )
                recv_buffer.recv(None)
                send_buffer.send(None)
                send_buffer.wait_send()
                recv_buffer.wait_recv()

            if device_type == "cuda":
                torch.cuda.synchronize()

            # 4. 正式评测
            t_start = time.perf_counter()

            for _ in range(iterations):
                send_buffer = _slime_c.rdma_buffer(
                    send_endpoint,
                    send_tensor.data_ptr(),
                    send_tensor.storage_offset(),
                    send_tensor.numel(),
                )
                recv_buffer = _slime_c.rdma_buffer(
                    recv_endpoint,
                    recv_tensor.data_ptr(),
                    recv_tensor.storage_offset(),
                    recv_tensor.numel(),
                )
                # 标准 RDMA 流程：先 Post Recv，再 Post Send
                recv_buffer.recv(None)
                send_buffer.send(None)
                # 等待完成
                # 注意：Stop-and-Wait 模式。如果是流水线模式，吞吐量会更高，
                # 但这里我们测的是单次操作的 Latency
                send_buffer.wait_send()
                recv_buffer.wait_recv()

            if device_type == "cuda":
                torch.cuda.synchronize()

            t_end = time.perf_counter()

            # 5. 计算指标
            total_time = t_end - t_start
            avg_latency_s = total_time / iterations
            avg_latency_us = avg_latency_s * 1e6

            # Bandwidth in GB/s (1 GB = 10^9 Bytes for network, or 1024^3 for storage.
            # Usually network throughput uses GB/s = Bytes / 1e9 / s or GiB/s)
            # 这里我们用 GB/s (10^9)
            throughput_gbs = (size * iterations) / total_time / 1e9

            # 6. 数据校验 (抽样检查，避免大内存 copy 耗时)
            check_status = "OK"
            if device_type == "cuda":
                # CUDA 校验需要把数据拷回 CPU，为了不影响后续测试，简单检查头尾
                if not torch.equal(
                    send_tensor[:100].cpu(), recv_tensor[:100].cpu()
                ) or not torch.equal(
                    send_tensor[-100:].cpu(), recv_tensor[-100:].cpu()
                ):
                    check_status = "FAIL"
            else:
                if not torch.equal(send_tensor, recv_tensor):
                    check_status = "FAIL"

            print(
                f"{get_readable_size(size):<15} | {avg_latency_us:<15.2f} |"
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
        "--iters", type=int, default=100, help="Number of iterations for averaging"
    )

    args = parser.parse_args()

    # 如果没有 CUDA，强制回退到 CPU
    if args.device == "cuda" and not torch.cuda.is_available():
        print("CUDA not available, switching to CPU.")
        args.device = "cpu"

    run_benchmark(device_type=args.device, num_qp=args.qp, iterations=args.iters)
    torch.cuda.synchronize()
