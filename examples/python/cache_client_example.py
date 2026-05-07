#!/usr/bin/env python3
"""Minimal DLSlimeCache assignment-directory client.

Start NanoCtrl and the cache service first:

    nanoctrl start
    dlslime-cache start --ctrl http://127.0.0.1:3000 \
        --host 127.0.0.1 --port 8765 --memory-size 1G

Then run this client:

    python examples/python/cache_client_example.py --url http://127.0.0.1:8765

The client asks the cache service for its PeerAgent, creates a local
PeerAgent, connects over an available RDMA NIC, stores a generated read
manifest to allocate cache slabs, writes bytes into those cache MR offsets,
then queries the same (peer_agent_id, version) back and RDMA-reads the bytes.
Stop the service when finished:

    dlslime-cache stop
"""

from __future__ import annotations

import argparse
import ctypes

from dlslime._slime_c import Assignment
from dlslime.cache import CacheClient
from dlslime.cache.types import assignment_from_json, assignment_to_tuple


def first_available_nic(resource: dict | None) -> str:
    if resource is None:
        return "<unavailable>"
    for nic in resource.get("nics") or []:
        if nic.get("health", "AVAILABLE") == "UNAVAILABLE":
            continue
        for port in nic.get("ports") or []:
            state = str(port.get("state", "ACTIVE")).upper()
            if state not in {"ACTIVE", "UNKNOWN"}:
                continue
            return "{name}:port{port}:{link}".format(
                name=nic.get("name", "<unknown>"),
                port=port.get("port", 1),
                link=port.get("link_type", "UNKNOWN"),
            )
    return "<none>"


def make_payload(nbytes: int) -> bytes:
    return bytes((i % 251 for i in range(nbytes)))


def check_manifest(stored: dict, queried: dict) -> None:
    if queried != stored:
        raise RuntimeError(
            "manifest mismatch: queried assignment manifest differs from stored manifest"
        )


def check_payload(expected: bytes, actual: bytes) -> None:
    if actual == expected:
        return

    if len(actual) != len(expected):
        raise RuntimeError(
            "payload length mismatch: " f"expected={len(expected)} actual={len(actual)}"
        )

    mismatch = next(
        idx for idx, (left, right) in enumerate(zip(expected, actual)) if left != right
    )
    raise RuntimeError(
        "payload mismatch: "
        f"offset={mismatch} expected={expected[mismatch]} actual={actual[mismatch]}"
    )


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--url", default="http://127.0.0.1:8765", help="Cache service URL")
    p.add_argument("--length", type=int, default=640 * 1024)
    p.add_argument("--timeout", type=float, default=60.0)
    return p.parse_args()


def main() -> None:
    args = parse_args()
    payload = make_payload(args.length)
    source = ctypes.create_string_buffer(payload, args.length)
    readback = ctypes.create_string_buffer(args.length)

    client = CacheClient(url=args.url)
    server = client.connect_to_server(timeout_sec=args.timeout)
    agent = client.peer_agent
    stored = None
    server_peer = server["peer_agent_id"]
    cache_mr_name = server["cache_mr_name"]
    print(
        "connected:",
        f"client_peer_agent_id={agent.alias}",
        f"server_peer_agent_id={server_peer}",
        f"server_nic={first_available_nic(server.get('resource'))}",
        f"cache_mr={cache_mr_name}",
    )

    source_name = "cache-example-source"
    readback_name = "cache-example-readback"
    agent.register_memory_region(
        source_name,
        ctypes.addressof(source),
        0,
        args.length,
    )
    readback_handle = agent.register_memory_region(
        readback_name,
        ctypes.addressof(readback),
        0,
        args.length,
    )

    cache_remote_handle = agent.get_handle(cache_mr_name, server_peer)

    try:
        stored = client.store(
            [Assignment(readback_handle, cache_remote_handle, 0, 0, args.length)],
        )
        print(
            "stored:",
            f"peer_agent_id={stored['peer_agent_id']}",
            f"version={stored['version']}",
            f"assignments={len(stored['assignments'])}",
            f"total_bytes={stored['total_bytes']}",
            f"slabs={stored['slab_ids']}",
        )

        print("writing bytes into allocated cache slabs...")
        write_future = agent.write(
            server_peer,
            [
                (
                    source_name,
                    cache_mr_name,
                    item["target_offset"],
                    item["source_offset"],
                    item["length"],
                )
                for item in stored["assignments"]
            ],
        )
        write_future.wait()

        queried = client.load(stored["peer_agent_id"], stored["version"])
        print(
            "queried:",
            f"peer_agent_id={queried['peer_agent_id']}",
            f"version={queried['version']}",
            f"assignments={len(queried['assignments'])}",
            f"total_bytes={queried['total_bytes']}",
        )

        check_manifest(stored, queried)
        read_assignments = [
            assignment_to_tuple(assignment_from_json(item))
            for item in queried["assignments"]
        ]
        print("reading bytes back from cache service...")
        read_future = agent.read(server_peer, read_assignments)
        read_future.wait()

        check_payload(payload, bytes(readback.raw[: args.length]))
        print("correctness: ok")
        print("slab lengths:", [a["length"] for a in queried["assignments"]])
    finally:
        cleanup_error = None
        if stored is not None:
            try:
                deleted = client.delete(stored["peer_agent_id"], stored["version"])
                print(
                    "deleted:",
                    f"peer_agent_id={stored['peer_agent_id']}",
                    f"version={stored['version']}",
                    f"deleted={deleted}",
                )
            except Exception as exc:
                cleanup_error = exc
        agent.shutdown()
        if cleanup_error is not None:
            raise cleanup_error


if __name__ == "__main__":
    main()
