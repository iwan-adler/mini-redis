"""Throughput of mini-redis with the official redis-py client.

Usage: python bench/bench.py [--port PORT]   (default: start an in-process server)

Measures SET and GET one command per round trip, then pipelined in batches of
100. Point --port at a real Redis to compare on the same machine.
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import redis

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from minikv.server import ServerThread  # noqa: E402

N = 20_000


def timed(fn) -> float:
    started = time.perf_counter()
    fn()
    return N / (time.perf_counter() - started)


def run(r: redis.Redis) -> dict[str, float]:
    r.flushdb()
    results = {}
    results["SET (1 per round trip)"] = timed(lambda: [r.set(f"k{i}", "v") for i in range(N)])
    results["GET (1 per round trip)"] = timed(lambda: [r.get(f"k{i}") for i in range(N)])

    def pipelined(make):
        for start in range(0, N, 100):
            pipe = r.pipeline(transaction=False)
            for i in range(start, start + 100):
                make(pipe, i)
            pipe.execute()

    results["SET (pipelined x100)"] = timed(lambda: pipelined(lambda p, i: p.set(f"p{i}", "v")))
    results["GET (pipelined x100)"] = timed(lambda: pipelined(lambda p, i: p.get(f"p{i}")))
    return results


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--port", type=int, help="benchmark an already-running server instead")
    args = parser.parse_args()
    thread = None
    if args.port is None:
        thread = ServerThread()
        port = thread.start()
    else:
        port = args.port
    try:
        r = redis.Redis(host="127.0.0.1", port=port)
        for name, ops in run(r).items():
            print(f"{name:26} {ops:>10,.0f} ops/s")
    finally:
        if thread:
            thread.stop()


if __name__ == "__main__":
    main()
