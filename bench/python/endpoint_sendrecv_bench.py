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

    # 1. 创建 Endpoint
    send_endpoint = _slime_c.rdma_endpoint(dev0, 1, "RoCE", num_qp)
    recv_endpoint = _slime_c.rdma_endpoint(dev1, 1, "RoCE", num_qp)

    # 2. [NEW] 创建并配置 RDMA Worker
    # 如果两个 Endpoint 在同一个物理设备上（或 Loopback），我们可以用一个 Worker。
    # 如果是不同的设备，为了最佳性能，建议创建两个 Worker 分别绑定 NUMA。
    
    workers = []
    
    if dev0 == dev1:
        # Loopback 模式：只需要一个 Worker
        print(f"Loopback mode: Creating 1 Worker on {dev0}")
        worker = _slime_c.rdma_worker(dev0, 0)
        worker.add_endpoint(send_endpoint)
        worker.add_endpoint(recv_endpoint)
        worker.start()
        workers.append(worker)
    else:
        # 双卡模式：两个 Worker
        print(f"Dual-NIC mode: Creating Worker 0 on {dev0} and Worker 1 on {dev1}")
        worker0 = _slime_c.rdma_worker(dev0, 0)
        worker0.add_endpoint(send_endpoint)
        worker0.start()
        workers.append(worker0)

        worker1 = _slime_c.rdma_worker(dev1, 1)
        worker1.add_endpoint(recv_endpoint)
        worker1.start()
        workers.append(worker1)

    # 3. 建立连接 (QPs Exchange)
    send_endpoint.context_connect(
        recv_endpoint.get_data_context_info(), recv_endpoint.get_meta_context_info()
    )
    recv_endpoint.context_connect(
        send_endpoint.get_data_context_info(), send_endpoint.get_meta_context_info()
    )

    # 定义测试大小：2KB 到 128MB
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

    try:
        for size in test_sizes:
            if 1:
                # 准备数据
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

                # 创建 Buffer
                # 注意：buf 构造时会自动将请求入队到 Ring，Worker 会在后台处理
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

                # Warmup
                warmup_iters = 10
                for _ in range(warmup_iters):
                    # 重新创建 buffer 模拟每次新的传输
                    sb = _slime_c.rdma_buffer(
                        send_endpoint, send_tensor.data_ptr(), 0, size
                    )
                    rb = _slime_c.rdma_buffer(
                        recv_endpoint, recv_tensor.data_ptr(), 0, size
                    )
                    
                    rb.recv(None) # 入队 Recv Task
                    sb.send(None) # 入队 Send Task
                    
                    sb.wait_send() # 阻塞等待信号
                    rb.wait_recv() # 阻塞等待信号

                if device_type == "cuda":
                    torch.cuda.synchronize()

                # 正式评测
                t_start = time.perf_counter()

                for _ in range(iterations):
                    sb = _slime_c.rdma_buffer(
                        send_endpoint, send_tensor.data_ptr(), 0, size
                    )
                    rb = _slime_c.rdma_buffer(
                        recv_endpoint, recv_tensor.data_ptr(), 0, size
                    )
                    
                    rb.recv(None)
                    sb.send(None)
                    
                    sb.wait_send()
                    rb.wait_recv()

                if device_type == "cuda":
                    torch.cuda.synchronize()

                t_end = time.perf_counter()

                total_time = t_end - t_start
                avg_latency_s = total_time / iterations
                avg_latency_us = avg_latency_s * 1e6
                throughput_gbs = (size * iterations) / total_time / 1e9

                check_status = "OK"
                if device_type == "cuda":
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
    
    finally:
        # [NEW] 清理：停止 Worker
        print("Stopping workers...")
        for w in workers:
            w.stop()


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

    if args.device == "cuda" and not torch.cuda.is_available():
        print("CUDA not available, switching to CPU.")
        args.device = "cpu"

    run_benchmark(device_type=args.device, num_qp=args.qp, iterations=args.iters)
    if torch.cuda.is_available():
        torch.cuda.synchronize()
