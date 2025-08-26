import statistics
import argparse
import torch
import json
import time
import zmq
import os
import zmq


from tabulate import tabulate
from dlslime import _slime_c


def parse_args():
    parser = argparse.ArgumentParser(description='RDMA Send/Recv Benchmark')
    parser.add_argument('--mode', type=str, required=True, choices=['send', 'recv'], help='Operation mode: send or recv')
    parser.add_argument('--device', type=str, default='rxe_0', help='RDMA device name')
    parser.add_argument('--rdmaport', type=int, default=1, help='RDMA port number')
    parser.add_argument('--type', type=str, default='RoCE', help='IB or RoCE')
    parser.add_argument('--port', type=int, default=6007, help='ZMQ control plane port')
    parser.add_argument('--master-addr', type=str, default='localhost', help='Master address')
    parser.add_argument('--master-port', type=int, default=6008, help='Master port')
    parser.add_argument('--sizes', nargs='+', default=None, help='List of message sizes in bytes to test')
    parser.add_argument('--iterations', type=int, default=50, help='Number of iterations')
    parser.add_argument('--warmup', type=int, default=5, help='Warmup iterations')
    parser.add_argument('--use-gpu', action='store_true', help='Use GPU memory')
    return parser.parse_args()



def setup_rdma_connection(args):
    """Establish RDMA connection between sender and receiver"""
    print(f'Initializing RDMA endpoint for {args.mode}...')
    
    num_qp = 4
    end_point = _slime_c.rdma_endpoint(args.device, args.rdmaport, args.type, num_qp)
    
    zmq_ctx = zmq.Context()
    
    if args.mode == 'send':
        sock = zmq_ctx.socket(zmq.REP)
        sock.bind(f'tcp://*:{args.port}')
        print(f'Sender waiting for receiver connection on port {args.port}...')
        
        peer_info = sock.recv_json()
        data_channel_info = peer_info['data_channel']
        meta_channel_info = peer_info['meta_channel']

        local_info = {
            'data_channel': json.dumps(end_point.get_data_context_info()),
            'meta_channel': json.dumps(end_point.get_meta_context_info()),
        }
        sock.send_json(local_info)
    
    else:  # recv mode
        # Receiver acts as client
        sock = zmq_ctx.socket(zmq.REQ)
        sock.connect(f'tcp://{args.master_addr}:{args.port}')
        print(f'Receiver connecting to sender at {args.master_addr}:{args.port}...')
        
        local_info = {
            'data_channel': json.dumps(end_point.get_data_context_info()),
            'meta_channel': json.dumps(end_point.get_meta_context_info()),
        }
        sock.send_json(local_info)
        
        peer_info = sock.recv_json()
        data_channel_info = peer_info['data_channel']
        meta_channel_info = peer_info['meta_channel']
    
    
    end_point.context_connect(json.loads(data_channel_info), json.loads(meta_channel_info))
    print('RDMA connection established successfully')
    
    return end_point, sock


def benchmark_rdma_send_recv(args, end_point):
    """Run RDMA send/recv benchmark"""
    # Prepare data sizes
    if args.sizes:
        sizes = [int(s) for s in args.sizes]
    else:
        sizes = [2**n for n in range(12, 13)]  # 1KB to 8MB
        
    
    benchmark_data = []
    for size in sizes:
        print(f'Testing size: {size} bytes')
        num_elements = max(1, size // 4)  # Assuming float32 (4 bytes per element)
        if args.use_gpu:
            data_tensor = torch.ones(num_elements, device='cuda', dtype=torch.float32)
        else:
            data_tensor = torch.ones(num_elements, dtype=torch.float32)
        
        
        ptrs = [data_tensor.data_ptr()]
        data_sizes = [data_tensor.numel() * data_tensor.element_size()]
        offsets = [0]
        
        # Create RDMA buffer
        # rdma_buf = _slime_c.rdma_buffer(end_point, ptrs, offsets, data_sizes)
        print(f'  Warming up with {args.warmup} iterations...')
        for _ in range(args.warmup):
            rdma_buf = _slime_c.rdma_buffer(end_point, ptrs, offsets, data_sizes)
            if args.mode == 'send':
                rdma_buf.send()
                rdma_buf.wait_send()
            else:
                rdma_buf.recv()
                rdma_buf.wait_recv()
        
        latencies = []
        start_time = time.time()
        
        for i in range(args.iterations):
            rdma_buf = _slime_c.rdma_buffer(end_point, ptrs, offsets, data_sizes)
            iter_start = time.time()
            
            if args.mode == 'send':
                rdma_buf.send()
                rdma_buf.wait_send()
            else:
                rdma_buf.recv()
                rdma_buf.wait_recv()

            iter_end = time.time()
            latency_ns = iter_end - iter_start
            latencies.append(latency_ns * 1000)  # Convert to ms
            if (i + 1) % 100 == 0:
                print(f'  Completed {i + 1}/{args.iterations} iterations')
        
        total_time_ns = time.time() - start_time
        print(total_time_ns)
        
        # Calculate statistics
        if latencies:
            avg_latency = statistics.mean(latencies)
            min_latency = min(latencies)
            max_latency = max(latencies)
            p99_latency = statistics.quantiles(latencies, n=100)[-1] if len(latencies) > 1 else avg_latency
            
            total_data = size * args.iterations
            bandwidth_mbps = (total_data) / (total_time_ns) / 1_000_000
            
            benchmark_data.append([
                f"{size:,}",
                f"{avg_latency:.3f} ms",
                f"{min_latency:.3f} ms",
                f"{max_latency:.3f} ms",
                f"{p99_latency:.3f} ms",
                f"{total_time_ns * 1000:.3f} ms",
                f"{bandwidth_mbps:.2f} MB/s",
                "GPU" if args.use_gpu else "CPU"
            ])
    
    return benchmark_data


def main():
    args = parse_args()
    
    # Set environment variables
    os.environ['MASTER_ADDR'] = args.master_addr
    os.environ['MASTER_PORT'] = str(args.master_port)
    
    print(f"Running in {args.mode} mode")
    print(f"Using {'GPU' if args.use_gpu else 'CPU'} memory")
    
    if args.mode == "send":
        torch.cuda.set_device(0) 
    else:
        torch.cuda.set_device(4) 
    
    try:
        # Setup RDMA connection
        end_point, zmq_sock = setup_rdma_connection(args)
        
        # Run benchmark
        benchmark_data = benchmark_rdma_send_recv(args, end_point)
        
        # Print results
        if benchmark_data:
            headers = ["Message Size", "Avg Latency", "Min Latency", "Max Latency", 
                      "P99 Latency", "total time", "Bandwidth", "Device"]
            print("\n" + "="*80)
            print("RDMA Benchmark Results:")
            print("="*80)
            print(tabulate(benchmark_data, headers=headers, tablefmt="grid"))
            
            # Save results to file
            results = {
                'mode': args.mode,
                'device': 'GPU' if args.use_gpu else 'CPU',
                'iterations': args.iterations,
                'warmup': args.warmup,
                'data': benchmark_data
            }
            
            with open(f'rdma_benchmark_{args.mode}.json', 'w') as f:
                json.dump(results, f, indent=2)
            print(f"\nResults saved to rdma_benchmark_{args.mode}.json")
        
        # Cleanup
        zmq_sock.close()
        print("Benchmark completed successfully")
        
    except Exception as e:
        print(f"Error during benchmark: {e}")
        import traceback
        traceback.print_exc()
        return 1
    
    return 0


if __name__ == '__main__':
    main()

