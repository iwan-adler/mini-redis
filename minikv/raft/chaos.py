"""Chaos test against real processes: a small Jepsen.

Starts a cluster of real `python -m minikv.raft` processes with on-disk logs, runs
concurrent clients against it, and meanwhile:

- kills nodes with SIGKILL (no clean shutdown) and restarts them from disk;
- freezes nodes with SIGSTOP and thaws them with SIGCONT (a long GC pause or a VM stall);
- partitions the network (via RAFT.PARTITION; nodes must run with --fault-injection);

then heals everything and checks that the replicas converged and that the clients'
history is linearizable.

    python -m minikv.raft.chaos --nodes 5 --duration 30
"""

from __future__ import annotations

import argparse
import math
import os
import random
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

from minikv.raft import linearizability
from minikv.raft.client import ClusterClient, ClusterError, OutcomeUnknown, Unavailable, _encode, _Reader

FAULTS = ("kill", "pause", "partition", "isolate-leader")


def _free_ports(n: int) -> list[int]:
    socks = [socket.socket() for _ in range(n)]
    for s in socks:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in socks]
    for s in socks:
        s.close()
    return ports


def admin(port: int, *args: str, timeout: float = 1.0):
    """Send one command to one node, without redirects."""
    with socket.create_connection(("127.0.0.1", port), timeout=timeout) as sock:
        sock.sendall(_encode([a.encode() for a in args]))
        return _Reader(sock).reply()


def status(port: int) -> dict | None:
    try:
        reply = admin(port, "RAFT.STATUS")
    except OSError:
        return None
    if not isinstance(reply, list):
        return None
    return {reply[i].decode(): reply[i + 1].decode() for i in range(0, len(reply), 2)}


class Cluster:
    def __init__(self, n: int, data_root: Path, snapshot_every: int = 200) -> None:
        ports = _free_ports(2 * n)
        self.ids = list(range(1, n + 1))
        self.client_ports = {i: ports[i - 1] for i in self.ids}
        self.raft_ports = {i: ports[n + i - 1] for i in self.ids}
        self.spec = ",".join(f"{i}=127.0.0.1:{self.raft_ports[i]}:{self.client_ports[i]}" for i in self.ids)
        self.data_root = data_root
        self.snapshot_every = snapshot_every
        self.procs: dict[int, subprocess.Popen | None] = {i: None for i in self.ids}
        self.paused: set[int] = set()

    def start(self, i: int) -> None:
        self.procs[i] = subprocess.Popen(
            [sys.executable, "-m", "minikv.raft", "--id", str(i), "--cluster", self.spec,
             "--data", str(self.data_root / f"node{i}"), "--fault-injection",
             "--snapshot-every", str(self.snapshot_every), "--log-level", "WARNING"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )

    def kill(self, i: int) -> None:
        proc = self.procs[i]
        if proc is not None:
            proc.send_signal(signal.SIGCONT)
            proc.kill()  # SIGKILL: no flushing, no goodbye
            proc.wait()
            self.procs[i] = None
            self.paused.discard(i)

    def pause(self, i: int) -> None:
        if self.procs[i] is not None:
            self.procs[i].send_signal(signal.SIGSTOP)
            self.paused.add(i)

    def resume_all(self) -> None:
        for i in list(self.paused):
            if self.procs[i] is not None:
                self.procs[i].send_signal(signal.SIGCONT)
        self.paused.clear()

    def partition(self, groups: list[list[int]]) -> None:
        for group in groups:
            others = [str(j) for j in self.ids if j not in group]
            for i in group:
                self._admin_quiet(i, "RAFT.PARTITION", *others)

    def heal(self) -> None:
        for i in self.ids:
            self._admin_quiet(i, "RAFT.HEAL")

    def _admin_quiet(self, i: int, *args: str) -> None:
        if self.procs[i] is None or i in self.paused:
            return
        try:
            admin(self.client_ports[i], *args)
        except OSError:
            pass

    def leader(self) -> int | None:
        for i in self.ids:
            if self.procs[i] is not None and i not in self.paused:
                st = status(self.client_ports[i])
                if st and st["state"] == "leader":
                    return i
        return None

    def stop(self) -> None:
        self.resume_all()
        for i in self.ids:
            self.kill(i)


def _worker(cid: int, cluster: Cluster, stop: threading.Event, history: list, lock: threading.Lock,
            rng: random.Random, keys: int, errors: list) -> None:
    client = ClusterClient([("127.0.0.1", p) for p in cluster.client_ports.values()], timeout=1.0, deadline=3.0)
    while not stop.is_set():
        key = f"k{rng.randrange(keys)}"
        roll = rng.random()
        value = None
        if roll < 0.4:
            kind, args = "get", ("GET", key)
        elif roll < 0.7:
            value = rng.randrange(1000)
            kind, args = "set", ("SET", key, value)
        elif roll < 0.9:
            kind, args = "incr", ("INCR", key)
        else:
            kind, args = "del", ("DEL", key)
        call = time.monotonic()
        try:
            reply = client.execute(*args)
        except OutcomeUnknown:
            op = linearizability.Op(cid, kind, key, value, call, math.inf, linearizability.UNKNOWN)
        except Unavailable:
            continue  # nothing was applied
        except ClusterError as exc:
            errors.append(f"client {cid}: {args} -> {exc}")
            continue
        else:
            if kind == "get":
                reply = None if reply is None else int(reply)
            op = linearizability.Op(cid, kind, key, value, call, time.monotonic(), reply)
        with lock:
            history.append(op)
    client.close()


def run(nodes: int = 3, clients: int = 5, keys: int = 3, duration: float = 20.0, seed: int | None = None,
        faults: tuple[str, ...] = FAULTS, verbose: bool = True) -> dict:
    seed = seed if seed is not None else random.randrange(1 << 30)
    rng = random.Random(seed)
    log = print if verbose else (lambda *a, **k: None)
    with tempfile.TemporaryDirectory(prefix="minikv-chaos-") as tmp:
        cluster = Cluster(nodes, Path(tmp))
        for i in cluster.ids:
            cluster.start(i)
        deadline = time.monotonic() + 10
        while cluster.leader() is None:
            if time.monotonic() > deadline:
                cluster.stop()
                raise RuntimeError("the cluster did not elect a leader")
            time.sleep(0.05)

        history: list[linearizability.Op] = []
        errors: list[str] = []
        lock = threading.Lock()
        stop = threading.Event()
        threads = [threading.Thread(target=_worker, args=(c, cluster, stop, history, lock,
                                                          random.Random(rng.random()), keys, errors))
                   for c in range(clients)]
        for t in threads:
            t.start()

        fault_log = []
        start = time.monotonic()
        try:
            while time.monotonic() - start < duration:
                time.sleep(rng.uniform(0.3, 1.5))
                kind = rng.choice(faults)
                victim = rng.choice(cluster.ids)
                if kind == "kill":
                    cluster.kill(victim)
                elif kind == "pause":
                    cluster.pause(victim)
                elif kind == "partition":
                    shuffled = cluster.ids[:]
                    rng.shuffle(shuffled)
                    cut = rng.randint(1, nodes - 1)
                    cluster.partition([shuffled[:cut], shuffled[cut:]])
                    victim = shuffled[:cut]
                elif kind == "isolate-leader":
                    victim = cluster.leader()
                    if victim is not None:
                        cluster.partition([[victim], [j for j in cluster.ids if j != victim]])
                fault_log.append((round(time.monotonic() - start, 1), kind, victim))
                log(f"  t={fault_log[-1][0]:5.1f}s {kind} {victim}")
                time.sleep(rng.uniform(0.5, 2.5))
                cluster.resume_all()
                cluster.heal()
                for i in cluster.ids:
                    if cluster.procs[i] is None:
                        cluster.start(i)
        finally:
            stop.set()
            for t in threads:
                t.join()

        # Let every replica catch up, then compare them.
        cluster.resume_all()
        cluster.heal()
        converged, statuses = False, {}
        give_up = time.monotonic() + 15
        while time.monotonic() < give_up:
            statuses = {i: status(cluster.client_ports[i]) for i in cluster.ids}
            values = [(s["applied"], s["digest"]) for s in statuses.values() if s]
            if len(values) == nodes and len(set(values)) == 1:
                converged = True
                break
            time.sleep(0.2)
        cluster.stop()

    result = linearizability.check(history)
    ok_ops = sum(1 for op in history if not op.indeterminate)
    summary = {
        "seed": seed, "nodes": nodes, "duration_s": duration, "faults": len(fault_log),
        "ops_ok": ok_ops, "ops_indeterminate": len(history) - ok_ops,
        "linearizable": result.ok, "converged": converged, "errors": errors,
        "final": {i: (s or {}).get("applied") for i, s in statuses.items()},
    }
    if not result.ok:
        summary["bad_key"] = result.key
    summary["ok"] = result.ok and converged and not errors
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Chaos-test a real mini-redis cluster.")
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--clients", type=int, default=5)
    parser.add_argument("--duration", type=float, default=20.0, help="seconds of faults")
    parser.add_argument("--seed", type=int)
    parser.add_argument("--faults", default=",".join(FAULTS), help=f"comma-separated subset of {FAULTS}")
    args = parser.parse_args()
    if os.name != "posix":
        sys.exit("the chaos test needs POSIX signals (Linux or macOS)")
    print(f"chaos: {args.nodes} nodes, {args.clients} clients, {args.duration}s of faults")
    summary = run(args.nodes, args.clients, duration=args.duration, seed=args.seed,
                  faults=tuple(args.faults.split(",")))
    print(f"seed {summary['seed']}: {summary['faults']} faults, {summary['ops_ok']} ops ok, "
          f"{summary['ops_indeterminate']} indeterminate")
    print(f"linearizable: {summary['linearizable']}   replicas converged: {summary['converged']}   "
          f"final applied index per node: {summary['final']}")
    for e in summary["errors"][:10]:
        print("  error:", e)
    sys.exit(0 if summary["ok"] else 1)


if __name__ == "__main__":
    main()
