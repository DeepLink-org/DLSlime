#!/usr/bin/env python3
"""
Test case to verify RDMA read offset parameter order.
This test will verify whether the format is:
  (local_handler, remote_handler, local_offset, remote_offset, length)
or
  (local_handler, remote_handler, remote_offset, local_offset, length)
"""

import os
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

import time

import torch
from dlslime import start_peer_agent


def test_rdma_read_offset_order():
    """Test RDMA read offset order by writing known values and reading them back."""

    print("Starting RDMA read offset order test...")

    # Start two peer agents
    print("\n1. Starting peer agents...")
    agent1 = start_peer_agent(
        nanoctrl_url="http://127.0.0.1:3000",
        alias="test_agent_1",
    )

    agent2 = start_peer_agent(
        nanoctrl_url="http://127.0.0.1:3000",
        alias="test_agent_2",
    )

    print("   Agents started")

    # PeerAgent connection
    print("\n2. Connecting peers...")
    conn1 = agent1.connect_to("test_agent_2", ib_port=1, qp_num=1)
    conn2 = agent2.connect_to("test_agent_1", ib_port=1, qp_num=1)
    conn1.wait()
    conn2.wait()
    print("   Connection established")

    # Register local buffers
    print("\n3. Registering local buffers...")
    local_buffer_1 = torch.zeros([1024], dtype=torch.int32, device="cuda")
    local_buffer_2 = (
        torch.ones([1024], dtype=torch.int32, device="cuda") * 42
    )  # Fill with 42

    local_mr_1 = agent1.register_memory_region(
        "test_buffer",
        local_buffer_1.data_ptr(),
        int(local_buffer_1.storage_offset()),
        local_buffer_1.numel() * 4,
    )
    local_mr_2 = agent2.register_memory_region(
        "test_buffer",
        local_buffer_2.data_ptr(),
        int(local_buffer_2.storage_offset()),
        local_buffer_2.numel() * 4,
    )

    print(f"   Agent1 local MR handler: {local_mr_1}")
    print(f"   Agent2 local MR handler: {local_mr_2}")

    # Get remote MR info and register
    print("\n4. Registering remote memory regions...")
    remote_mr_1 = agent1.get_handle("test_buffer", "test_agent_2")
    remote_mr_2 = agent2.get_handle("test_buffer", "test_agent_1")

    print(f"   Agent1 remote MR handler: {remote_mr_1}")
    print(f"   Agent2 remote MR handler: {remote_mr_2}")

    # Get endpoint
    print("\n5. Getting endpoint...")
    endpoint_1 = agent1.query_connection("test_agent_2").endpoint
    endpoint_2 = agent2.query_connection("test_agent_1").endpoint
    print("   Endpoints obtained")

    # Test Case 1: Read from remote offset 0 to local offset 0
    # If format is (local_handler, remote_handler, local_offset, remote_offset, length)
    # Then: (local_mr_1, remote_mr_1, 0, 0, 16) means read from remote[0:16] to local[0:16]
    # If format is (local_handler, remote_handler, remote_offset, local_offset, length)
    # Then: (local_mr_1, remote_mr_1, 0, 0, 16) means read from remote[0:16] to local[0:16]
    # Both are the same for offset 0, so we need different offsets

    print("\n6. Testing RDMA read with different offsets...")
    print("   Agent2 buffer[0:4] =", local_buffer_2[0:4].cpu().numpy())
    print("   Agent1 buffer[0:4] =", local_buffer_1[0:4].cpu().numpy())

    # Clear agent1's buffer
    local_buffer_1.zero_()

    # Test: Read from remote offset 16 to local offset 0
    # Remote buffer (agent2) has value 42 at all positions
    # We want to read remote[16:20] (4 int32s = 16 bytes) to local[0:4]

    print("\n7. Performing RDMA read: remote[16:20] -> local[0:4]")
    print(
        "   Testing format: (local_handler, remote_handler, local_offset, remote_offset, length)"
    )

    # Format 1: (local_offset, remote_offset) - what we think is correct
    try:
        slot1 = endpoint_1.read(
            [(local_mr_1, remote_mr_1, 0, 16, 16)], None
        )  # local_off=0, remote_off=16
        slot1.wait()
        result1 = local_buffer_1[0:4].cpu().numpy()
        print(f"   Format 1 (local_off, remote_off): local[0:4] = {result1}")
        if result1[0] == 42:
            print(
                "   ✓ Format 1 is CORRECT: (local_handler, remote_handler, local_offset, remote_offset, length)"
            )
        else:
            print(f"   ✗ Format 1 is WRONG: got {result1[0]}, expected 42")
    except Exception as e:
        print(f"   ✗ Format 1 failed: {e}")
        result1 = None

    # Clear and test format 2
    local_buffer_1.zero_()
    time.sleep(0.1)

    print("\n8. Performing RDMA read: remote[16:20] -> local[0:4]")
    print(
        "   Testing format: (local_handler, remote_handler, remote_offset, local_offset, length)"
    )

    # Format 2: (remote_offset, local_offset) - what user thinks is correct
    try:
        slot2 = endpoint_1.read(
            [(local_mr_1, remote_mr_1, 16, 0, 16)], None
        )  # remote_off=16, local_off=0
        slot2.wait()
        result2 = local_buffer_1[0:4].cpu().numpy()
        print(f"   Format 2 (remote_off, local_off): local[0:4] = {result2}")
        if result2[0] == 42:
            print(
                "   ✓ Format 2 is CORRECT: (local_handler, remote_handler, remote_offset, local_offset, length)"
            )
        else:
            print(f"   ✗ Format 2 is WRONG: got {result2[0]}, expected 42")
    except Exception as e:
        print(f"   ✗ Format 2 failed: {e}")
        result2 = None

    # Summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY:")
    print("=" * 60)
    if result1 is not None and result1[0] == 42:
        print("✓ Format 1 (local_offset, remote_offset) is CORRECT")
    elif result2 is not None and result2[0] == 42:
        print("✓ Format 2 (remote_offset, local_offset) is CORRECT")
    else:
        print("✗ Both formats failed or gave incorrect results")
    print("=" * 60)

    # Cleanup
    print("\n9. Cleaning up...")
    agent1.shutdown()
    agent2.shutdown()
    print("   Done")


if __name__ == "__main__":
    test_rdma_read_offset_order()
