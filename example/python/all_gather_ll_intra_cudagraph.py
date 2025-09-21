import os

import torch
import torch.distributed as dist

from dlslime import _slime_c


class DLSlimeQGather:

    def __init__(self, rank: int):
        self.rank = rank
        self.buffer = _slime_c.AllGatherLLBuffer(2, 16, 1152, 2, 8, self.rank)
        ipc_info = self.buffer.ipc_info()
        all_ipc_info = [None for _ in range(8)]
        dist.all_gather_object(all_ipc_info, ipc_info)
        self.buffer.connect_full_mesh(all_ipc_info)

    def forward(self, input_tensor: torch.Tensor) -> torch.Tensor:
        return self.buffer.all_gather_ll(input_tensor)


def main():
    dist.init_process_group(backend='nccl')
    rank = dist.get_rank()
    torch.cuda.set_device(rank)

    output_dir = '/workspace/Triton-distributed/profiler/128_sp_batch_8gpu_dlslime'
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
    # 开始profiling
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
            graph.replay()
            torch.profiler.record_function(f'replay_end_{i}')
            torch.cuda.synchronize()
            prof.step()
    print(output)
    dist.destroy_process_group()


if __name__ == '__main__':
    main()
