import torch

from dlslime import Assignment, RDMAEndpoint, available_nic

if __name__ == '__main__':
    devices = available_nic()
    initiator_endpoint = RDMAEndpoint(devices[0])
    target_endpoint = RDMAEndpoint(devices[1])

    initiator_tensor = torch.arange(0, 16, device='cuda', dtype=torch.int32)
    target_tensor = torch.zeros([16], device='cpu', dtype=torch.int32)

    initiator_info = initiator_endpoint.endpoint_info
    target_info = target_endpoint.endpoint_info

    initiator_endpoint.connect(target_info)
    target_endpoint.connect(initiator_info)

    initiator_endpoint.register_memory_region(
        'buffer',
        initiator_tensor.data_ptr(),
        initiator_tensor.storage_offset(),
        initiator_tensor.numel() * initiator_tensor.itemsize,
    )
    target_endpoint.register_memory_region(
        'buffer',
        target_tensor.data_ptr(),
        target_tensor.storage_offset(),
        target_tensor.numel() * target_tensor.itemsize,
    )

    print(f'before recv: {target_tensor}')
    initiator_assignment = initiator_endpoint.send_batch(
        [
            Assignment(mr_key='buffer', target_offset=0, source_offset=0, length=16),
            Assignment(mr_key='buffer', target_offset=0, source_offset=32, length=16),
            Assignment(mr_key='buffer', target_offset=0, source_offset=16, length=16),
            Assignment(mr_key='buffer', target_offset=0, source_offset=48, length=16),
        ],
        async_op=True,
    )

    target_assignment = target_endpoint.recv_batch(
        [
            Assignment(mr_key='buffer', target_offset=0, source_offset=0, length=64),
            Assignment(mr_key='buffer', target_offset=0, source_offset=16, length=16),
            Assignment(mr_key='buffer', target_offset=0, source_offset=32, length=16),
            Assignment(mr_key='buffer', target_offset=0, source_offset=48, length=16),
        ],
        async_op=True,
    )

    initiator_assignment.wait()
    target_assignment.wait()
    print(f'after recv: {target_tensor}')
