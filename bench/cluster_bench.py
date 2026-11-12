"""Throughput and latency of a 3-node cluster on one machine.

    python bench/cluster_bench.py [--nodes 3] [--clients 1,8] [--seconds 5]

Every write is fsynced to the leader's disk and replicated to a majority before the
reply; every read is confirmed with a ReadIndex heartbeat round.
"""

import argparse
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from minikv.raft.chaos import Cluster  # noqa: E402
from minikv.raft.client import ClusterClient  # noqa: E402


def measure(cluster, clients, seconds, command):
    latencies, lock = [], threading.Lock()
    stop = time.monotonic() + seconds

    def worker(n):
        c = ClusterClient([("127.0.0.1", p) for p in cluster.client_ports.values()])
        c.execute("SET", "bench", "0")
        local = []
        while time.monotonic() < stop:
            t = time.perf_counter()
            c.execute(*command)
            local.append(time.perf_counter() - t)
        with lock:
            latencies.extend(local)
        c.close()

    threads = [threading.Thread(target=worker, args=(n,)) for n in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    latencies.sort()
    p50 = latencies[len(latencies) // 2] * 1000
    p99 = latencies[int(len(latencies) * 0.99)] * 1000
    return len(latencies) / seconds, p50, p99


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--clients", default="1,8")
    parser.add_argument("--seconds", type=float, default=5)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory() as tmp:
        cluster = Cluster(args.nodes, Path(tmp), snapshot_every=10_000)
        for i in cluster.ids:
            cluster.start(i)
        while cluster.leader() is None:
            time.sleep(0.05)
        print(f"| Operation | Clients | ops/s | p50 latency | p99 latency |\n|---|---|---|---|---|")
        try:
            for clients in [int(c) for c in args.clients.split(",")]:
                for name, command in [("INCR (write)", ("INCR", "bench")), ("GET (read)", ("GET", "bench"))]:
                    ops, p50, p99 = measure(cluster, clients, args.seconds, command)
                    print(f"| {name} | {clients} | {ops:,.0f} | {p50:.2f} ms | {p99:.2f} ms |")
        finally:
            cluster.stop()


if __name__ == "__main__":
    main()
