"""
Example: Multiple Peer Agents using Control Plane (Declarative Topology).

8 agents, mesh network (each agent connects to all others). Measures timing.
Uses set_desired_topology() and TopologyReconciler for declarative connection setup.
"""

import contextlib
import threading
import time

import torch
from dlslime import start_peer_agent


# Helper: Time measurement context manager
@contextlib.contextmanager
def time_measure(operation_name):
    """Context manager to measure and print execution time."""
    t0 = time.perf_counter()
    yield
    elapsed = time.perf_counter() - t0
    print(f"[TIME] {operation_name}: {elapsed:.3f}s")


# Helper: Run function in parallel for all agents
def run_parallel(agents, func):
    """Run a function in parallel for all agents."""
    threads = []
    for alias, agent in agents.items():
        t = threading.Thread(target=func, args=(alias, agent))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()


# Start multiple peer agents
num_agents = 8
verbose = False  # Set True to print each init/connect/read

print("=" * 60)
print(f"Starting {num_agents} peer agents (mesh)...")
print("=" * 60)

# Use ExitStack to manage multiple context managers (auto-cleanup)
with contextlib.ExitStack() as stack:
    agents = {}
    with time_measure("start"):
        for i in range(num_agents):
            # NanoCtrl auto-generates unique name (no alias parameter)
            agent = start_peer_agent(
                # alias=None (default) - NanoCtrl will auto-generate unique name
                name_prefix="agent",  # Prefix for generated names (e.g., "agent-1a", "agent-2b")
                server_url="http://127.0.0.1:3000",
                device=None,  # Auto-select
                ib_port=1,
                link_type="RoCE",
                qp_num=1,
            )
            stack.enter_context(agent)  # Auto-cleanup on exit
            # Use allocated name as key
            allocated_name = agent.alias
            agents[allocated_name] = agent
            if verbose:
                print(f"Started {allocated_name}")

    # Query available peers
    print("\n" + "=" * 60)
    print("Available peers:")
    print("=" * 60)
    for alias, agent in agents.items():
        peers = agent.query()
        if verbose:
            print(f"{alias} sees: {list(peers.keys())}")

    # Declarative: set desired topology (mesh - each agent connects to all others)
    print("\n" + "=" * 60)
    print("Setting desired topology (mesh)...")
    print("=" * 60)

    def set_topology(agent_alias, agent):
        target_peers = [p for p in agents.keys() if p != agent_alias]
        agent.set_desired_topology(target_peers=target_peers)
        if verbose:
            print(f"  {agent_alias}: target_peers={target_peers}")

    with time_measure("set_desired_topology"):
        run_parallel(agents, set_topology)

    # Wait for TopologyReconciler to establish all connections
    print("\n" + "=" * 60)
    print("Waiting for connections (reconciliation)...")
    print("=" * 60)

    def wait_for_peers(agent_alias, agent):
        target_peers = [p for p in agents.keys() if p != agent_alias]
        agent.wait_for_peers(target_peers, timeout_sec=30)
        if verbose:
            print(f"  {agent_alias}: all peers connected")

    with time_measure("wait_for_peers"):
        run_parallel(agents, wait_for_peers)

    # Register memory regions for each agent
    print("\n" + "=" * 60)
    print("Registering memory regions...")
    print("=" * 60)

    # Source tensors: each agent's data (never overwritten by reads)
    source_tensors = {}
    source_handlers = {}
    # Receive buffers: per (reader, peer) to avoid overwrite when one agent reads from multiple peers
    recv_buffers = {}  # (reader_alias, peer_alias) -> tensor
    recv_handlers = {}  # (reader_alias, peer_alias) -> handler

    with time_measure("register"):
        for idx, (alias, agent) in enumerate(agents.items()):
            # Use enumeration index instead of parsing alias
            agent_id = idx
            tensor = torch.full([32], agent_id, device="cpu", dtype=torch.uint8)
            source_tensors[alias] = tensor

            try:
                handler = agent.register_memory_region(
                    "data",
                    tensor.data_ptr() + int(tensor.storage_offset()),
                    tensor.numel() * tensor.itemsize,
                )
                source_handlers[alias] = handler
                if verbose:
                    print(f"  {alias} registered MR 'data' (self)")
            except Exception as e:
                print(f"  {alias}: register MR FAILED - {e}")

        # Each agent needs a recv buffer per peer (to avoid overwriting when reading from multiple peers)
        for reader_alias, agent in agents.items():
            for peer_alias in agents.keys():
                if peer_alias != reader_alias:
                    recv_tensor = torch.zeros([32], device="cpu", dtype=torch.uint8)
                    recv_buffers[(reader_alias, peer_alias)] = recv_tensor
                    recv_name = f"recv_{reader_alias}_from_{peer_alias}"
                    handler = agent.register_memory_region(
                        recv_name,
                        recv_tensor.data_ptr() + int(recv_tensor.storage_offset()),
                        recv_tensor.numel() * recv_tensor.itemsize,
                    )
                    recv_handlers[(reader_alias, peer_alias)] = handler

    # Perform RDMA operations: each agent reads from all others
    print("\n" + "=" * 60)
    print("Performing RDMA reads...")
    print("=" * 60)

    def perform_reads(agent_alias, agent):
        """Agent reads from all other agents."""
        # Get our index to know what value we expect
        agent_list = list(agents.keys())
        for peer_alias in agent_list:
            if peer_alias != agent_alias:
                try:
                    remote_mr_info = agent.get_mr_info(peer_alias, "data")
                    if remote_mr_info is None:
                        print(f"  {agent_alias} -> {peer_alias}: MR info not found")
                        continue

                    remote_handler = agent.register_remote_memory_region(
                        peer_alias,
                        "data",
                        remote_mr_info,
                    )

                    local_handler = recv_handlers.get((agent_alias, peer_alias))
                    if local_handler is None:
                        print(
                            f"  {agent_alias} -> {peer_alias}: local handler not found"
                        )
                        continue

                    endpoint = agent.get_endpoint(peer_alias)
                    slot = endpoint.read(
                        [(local_handler, remote_handler, 0, 0, 8)], None
                    )
                    slot.wait()

                    # Use index from agent_list instead of parsing alias
                    expected_value = agent_list.index(peer_alias)
                    read_value = recv_buffers[(agent_alias, peer_alias)][0].item()

                    if verbose:
                        if read_value == expected_value:
                            print(
                                f"  {agent_alias} <- {peer_alias}: read OK (value={read_value})"
                            )
                        else:
                            print(
                                f"  {agent_alias} <- {peer_alias}: read MISMATCH (got={read_value}, expected={expected_value})"
                            )
                    elif read_value != expected_value:
                        print(
                            f"  {agent_alias} <- {peer_alias}: MISMATCH (got={read_value}, expected={expected_value})"
                        )

                except Exception as e:
                    print(f"  {agent_alias} -> {peer_alias}: read FAILED - {e}")

    with time_measure("read (56 ops)"):
        run_parallel(agents, perform_reads)

    # Cleanup is automatic via ExitStack context manager
    print("\n" + "=" * 60)
    print("Multi-agent control plane example completed!")
    print("=" * 60)
    print("(Cleanup will happen automatically when exiting context)")
