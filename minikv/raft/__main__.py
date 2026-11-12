"""Run one cluster node:

    python -m minikv.raft --id 1 \\
        --cluster 1=127.0.0.1:7001:6381,2=127.0.0.1:7002:6382,3=127.0.0.1:7003:6383 \\
        --data data/node1

Each cluster entry is id=host:raft_port:client_port. Start one process per node.
"""

import argparse
import asyncio
import logging

from minikv.raft.node import ClusterNode, parse_cluster


async def main() -> None:
    parser = argparse.ArgumentParser(description="mini-redis cluster node (Raft)")
    parser.add_argument("--id", type=int, required=True)
    parser.add_argument("--cluster", required=True, help="id=host:raft_port:client_port,...")
    parser.add_argument("--data", help="directory for the Raft log; omit to keep everything in memory")
    parser.add_argument("--tick-ms", type=int, default=10)
    parser.add_argument("--election-ticks", type=int, default=15)
    parser.add_argument("--snapshot-every", type=int, default=10_000, help="log entries between snapshots")
    parser.add_argument("--fault-injection", action="store_true", help="allow RAFT.PARTITION / RAFT.HEAL")
    parser.add_argument("--log-level", default="INFO")
    args = parser.parse_args()
    logging.basicConfig(level=args.log_level, format=f"%(asctime)s node{args.id} %(name)s %(message)s")
    node = ClusterNode(args.id, parse_cluster(args.cluster), args.data, tick_ms=args.tick_ms,
                       election_ticks=args.election_ticks, snapshot_every=args.snapshot_every,
                       fault_injection=args.fault_injection)
    await node.start()
    try:
        await asyncio.Event().wait()
    finally:
        await node.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
