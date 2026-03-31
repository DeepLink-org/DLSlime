#!/usr/bin/env python3
"""
Ascend Direct P2P Read Test

Tests peer-to-peer read operations between NPU devices using AscendDirectEndpoint.
Similar to p2p_nvlink.py but for Ascend NPUs with torch_npu backend.
Uses ZMQ for simple synchronization (similar to C++ test).

Usage:
    # Terminal 1 (Target - Device 1):
    python3 p2p_ascend_read.py --is_target --local_port 17001 --remote_port 17000 --device 1

    # Terminal 2 (Initiator - Device 0):
    python3 p2p_ascend_read.py --local_port 17000 --remote_port 17001 --device 0
"""

import argparse

import torch
import xxhash
import zmq
from dlslime import _slime_c

# Import torch_npu for Ascend NPU support
try:
    import torch_npu
except ImportError:
    print("Error: torch_npu not found. Please install torch_npu:")
    print("  pip install torch-npu")
    exit(1)


def parse_args():
    parser = argparse.ArgumentParser(description="Ascend Direct P2P Read Test")
    parser.add_argument(
        "--is_target", action="store_true", help="Run as target (receiver)"
    )
    parser.add_argument(
        "--local_port", type=int, default=17000, help="Local port for ZMQ"
    )
    parser.add_argument(
        "--remote_port", type=int, default=17001, help="Remote port for ZMQ"
    )
    parser.add_argument(
        "--localhost", type=str, default="127.0.0.1", help="Localhost IP"
    )
    parser.add_argument("--device", type=int, default=0, help="NPU device ID")
    return parser.parse_args()


def run_p2p_test():
    """
    Run P2P read test between two NPU devices in different processes.
    """
    args = parse_args()

    # Set NPU device
    torch.npu.set_device(args.device)
    device = torch.device(f"npu:{args.device}")

    role = "Target" if args.is_target else "Initiator"
    print(f"[{role}] Running on NPU device {args.device}")

    # Create AscendDirectEndpoint
    ep = _slime_c.AscendDirectEndpoint()

    # Initialize endpoint with local host and port
    # AdxlEngine uses port+1 internally, so we pass local_port+1
    ep.init(args.localhost, args.local_port + 1)

    print(
        f"[{role}] AscendDirectEndpoint initialized on {args.localhost}:{args.local_port+1}"
    )

    # Create test tensor on NPU
    mr_key = xxhash.xxh64_intdigest("buffer")

    if args.is_target:
        # Target: has ones that initiator will read
        tensor = torch.ones([16], device=device, dtype=torch.uint8)
    else:
        # Initiator: starts with zeros, will read ones from target
        tensor = torch.zeros([16], device=device, dtype=torch.uint8)

    print(f"[{role}] Initial tensor: {tensor}")

    # Register local memory region
    ep.register_memory_region(
        mr_key,
        tensor.data_ptr(),
        int(tensor.storage_offset()),
        tensor.numel() * tensor.element_size(),
    )
    print(f"[{role}] Registered local memory region")

    # Exchange endpoint info via ZMQ
    local_info = ep.endpoint_info()
    local_info["data_ptr"] = tensor.data_ptr()
    local_info["mr_key"] = mr_key

    # ZMQ setup for info exchange
    context = zmq.Context()

    if args.is_target:
        # Target: bind and wait for connection
        socket = context.socket(zmq.REP)
        socket.bind(f"tcp://*:{args.local_port}")
        print(f"[{role}] Waiting for initiator connection on port {args.local_port}...")

        # Receive initiator's info
        message = socket.recv_json()
        remote_info = message
        print(f"[{role}] Received initiator info")

        # Send our info back
        socket.send_json(local_info)
        print(f"[{role}] Sent target info")
    else:
        # Initiator: connect to target
        socket = context.socket(zmq.REQ)
        socket.connect(f"tcp://{args.localhost}:{args.remote_port}")
        print(f"[{role}] Connecting to target on port {args.remote_port}...")

        # Send our info
        socket.send_json(local_info)
        print(f"[{role}] Sent initiator info")

        # Receive target's info
        remote_info = socket.recv_json()
        print(f"[{role}] Received target info")

    # Connect to remote peer
    print(f"[{role}] Connecting to remote endpoint...")
    ep.connect(remote_info)
    print(f"[{role}] Connected!")

    # Register remote memory region
    remote_mr_key = xxhash.xxh64_intdigest("remote_buffer")
    remote_mr_info = {
        "addr": remote_info["data_ptr"],
        "offset": 0,
        "length": 16,
    }
    ep.register_remote_memory_region(remote_mr_key, "remote_tensor", remote_mr_info)
    print(f"[{role}] Registered remote memory region")

    # Perform P2P read operation (only initiator reads)
    if not args.is_target:
        print(f"\n[{role}] === Starting P2P Read Test ===")
        print(f"[{role}] Before read: {tensor}")

        # Read 8 bytes from remote offset 0 to local offset 8
        # Format: (local_mr_key, remote_mr_key, remote_offset, local_offset, length)
        assignments = [(mr_key, remote_mr_key, 0, 8, 8)]

        future = ep.read(assignments, None)

        if future:
            print(f"[{role}] Waiting for transfer to complete...")
            future.wait()
            print(f"[{role}] Transfer complete!")

        # Synchronize NPU
        torch.npu.synchronize()

        print(f"[{role}] After read:  {tensor}")

        # Verify results
        expected_first_half = torch.zeros(8, dtype=torch.uint8, device=device)
        expected_second_half = torch.ones(8, dtype=torch.uint8, device=device)

        assert torch.all(
            tensor[:8] == expected_first_half
        ), f"First half check failed: {tensor[:8]}"
        assert torch.all(
            tensor[8:] == expected_second_half
        ), f"Second half check failed: {tensor[8:]}"

        print(f"\n[{role}] ✅ Test PASSED! Successfully read remote data via P2P")
        print("\n" + "=" * 60)
        print("Ascend Direct P2P Read Test Completed Successfully!")
        print("=" * 60)
    else:
        print(f"[{role}] Waiting as target...")
        # Keep target alive for a bit
        import time

        time.sleep(2)

    # Cleanup
    socket.close()
    context.term()
    del ep


if __name__ == "__main__":
    try:
        run_p2p_test()
    except Exception as e:
        print(f"Error: {e}")
        import traceback

        traceback.print_exc()
        exit(1)
