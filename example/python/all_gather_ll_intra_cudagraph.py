import os

import torch
import torch.distributed as dist

from dlslime import _slime_c


os.environ['NVSHMEM_DISABLE_P2P'] = '1'
os.environ['NVSHMEM_IB_ENABLE_IBGDA'] = '1'
os.environ['NVSHMEM_IBGDA_NUM_RC_PER_PE'] = '1'
# Make sure QP depth is always larger than the number of on-flight WRs, so that we can skip WQ slot check
os.environ['NVSHMEM_QP_DEPTH'] = os.environ.get('NVSHMEM_QP_DEPTH', '1024')

# Reduce gpu memory usage
# 6 default teams + 1 extra team
os.environ['NVSHMEM_MAX_TEAMS'] = '7'
# Disable NVLink SHArP
os.environ['NVSHMEM_DISABLE_NVLS'] = '1'
# NOTES: NVSHMEM initialization requires at least 256 MiB
os.environ['NVSHMEM_CUMEM_GRANULARITY'] = f'{2 ** 29}'


class DLSlimeQGather:

    def __init__(self, rank: int):
        self.rank = rank
        self.buffer = _slime_c.AllGatherIntraLLBuffer(64, 576, torch.bfloat16, 8, self.rank)
        buffer_info = self.buffer.buffer_info()
        all_buffer_info = [None for _ in range(8)]
        dist.all_gather_object(all_buffer_info, buffer_info)
        self.buffer.connect_full_mesh(all_buffer_info)

    def forward(self, input_tensor: torch.Tensor) -> torch.Tensor:
        return self.buffer.all_gather_ll(input_tensor)


def main():
    dist.init_process_group(backend='nccl')
    rank = dist.get_rank()
    torch.cuda.set_device(rank)

    output_dir = './'
    os.makedirs(output_dir, exist_ok=True)

    gather = DLSlimeQGather(rank)

    input_tensor = torch.ones(2, 8, 1152 * 2, dtype=torch.bfloat16, device=f'cuda:{rank}') * rank * 0

    for _ in range(10):
        output = gather.forward(input_tensor)
    torch.cuda.synchronize()
    # 设置profiler
    profiler_output = os.path.join(output_dir, f'rank_{rank}_profile')

    # 捕获CUDA Graph
    graph = torch.cuda.CUDAGraph()
    # 显式指定设备和流
    device = torch.device(f'cuda:{rank}')
    stream = torch.cuda.Stream(device=device)

    with torch.cuda.stream(stream):
        with torch.cuda.graph(graph, stream=stream):
            # static_input = input_tensor.clone()
            output = gather.forward(input_tensor)

    # static_input.copy_(torch.ones(2, 8, 1152 * 2, dtype=torch.bfloat16, device=f'cuda:{rank}') * rank)
    input_tensor.copy_(torch.ones(2, 8, 1152 * 2, dtype=torch.bfloat16, device=f'cuda:{rank}') * rank)
    with torch.profiler.profile(
            activities=[torch.profiler.ProfilerActivity.CPU, torch.profiler.ProfilerActivity.CUDA],
            schedule=torch.profiler.schedule(wait=1, warmup=3, active=5, repeat=1),
            on_trace_ready=torch.profiler.tensorboard_trace_handler(profiler_output),
            record_shapes=True,
            profile_memory=True,
            with_stack=True,
            with_flops=True,
            with_modules=True,
    ) as prof:
        for i in range(10):
            torch.profiler.record_function(f'replay_start_{i}')
            dist.barrier()
            graph.replay()
            torch.profiler.record_function(f'replay_end_{i}')
            torch.cuda.synchronize()
            prof.step()
    print(output)
    dist.destroy_process_group()


if __name__ == '__main__':
    main()
