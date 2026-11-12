"""A deterministic simulator for testing the cluster under failures.

Everything runs in one thread against a virtual clock: Raft nodes, a network that
delays, drops, duplicates and reorders messages, clients, and a "nemesis" that
partitions the network and crashes and restarts nodes. All randomness comes from one
seed, so any failure can be replayed exactly with `python -m minikv.raft.sim --seed N`.

While it runs, the simulator checks Raft's safety properties directly:

- election safety: at most one leader per term;
- state machine safety: no two nodes ever apply different entries at the same index;
- leader completeness: a new leader's log holds every entry already applied anywhere;

and afterwards:

- convergence: once the network heals, every replica ends in the same state;
- linearizability: every reply the clients saw is consistent with one sequential order.

This is the approach of FoundationDB's simulation testing and Jepsen, scaled down.
"""

from __future__ import annotations

import argparse
import heapq
import math
import random
from dataclasses import dataclass, field

from minikv.raft import linearizability
from minikv.raft.core import Message
from minikv.raft.replica import NotLeader, Replica
from minikv.raft.storage import MemoryStorage
from minikv.resp import Error


@dataclass
class SimConfig:
    nodes: int = 3
    clients: int = 4
    keys: int = 2
    duration_ms: int = 4000
    settle_ms: int = 2500         # fault-free time at the end for everything to catch up
    tick_ms: int = 10
    election_ticks: int = 10
    heartbeat_ticks: int = 2
    min_delay_ms: int = 1
    max_delay_ms: int = 15
    drop_rate: float = 0.03
    duplicate_rate: float = 0.02
    nemesis: tuple[str, ...] = ("partition", "isolate-leader", "crash")
    fault_ms: tuple[int, int] = (100, 700)  # how long each fault lasts
    calm_ms: tuple[int, int] = (50, 400)    # gap between faults
    client_timeout_ms: int = 400
    snapshot_every: int = 40      # small, so snapshots and InstallSnapshot happen often
    pre_vote: bool = True
    check_quorum: bool = True
    # Deliberate bugs, used to show the checks catch them.
    stale_reads: bool = False     # leaders answer reads without confirming leadership
    forget_votes: bool = False    # a restarted node forgets who it voted for


@dataclass
class SimResult:
    seed: int
    ok: bool
    violations: list[str]
    stats: dict = field(default_factory=dict)
    history: list[linearizability.Op] = field(default_factory=list)

    def summary(self) -> str:
        s = self.stats
        head = "ok" if self.ok else "FAILED"
        line = (f"seed {self.seed}: {head} | {s.get('ops_ok', 0)} ops ok, {s.get('ops_unknown', 0)} indeterminate, "
                f"{s.get('leaders', 0)} leaders elected, {s.get('faults', 0)} faults, "
                f"{s.get('snapshots_installed', 0)} snapshots installed, {s.get('dropped', 0)} messages dropped")
        return line + "".join(f"\n  - {v}" for v in self.violations)


class _Client:
    def __init__(self, cid: int) -> None:
        self.id = cid
        self.target = 1
        self.op: linearizability.Op | None = None
        self.args: list[bytes] = []
        self.attempt = 0


class Simulation:
    def __init__(self, seed: int, config: SimConfig | None = None) -> None:
        self.seed = seed
        self.cfg = config or SimConfig()
        self.rng = random.Random(seed)
        self.now = 0
        self._queue: list = []
        self._seq = 0
        self.node_ids = list(range(1, self.cfg.nodes + 1))
        self.storages = {i: MemoryStorage() for i in self.node_ids}
        self.replicas: dict[int, Replica | None] = {}
        self.blocked: set[tuple[int, int]] = set()
        self.history: list[linearizability.Op] = []
        self.violations: list[str] = []
        self.committed: dict[int, tuple[int, tuple | None]] = {}
        self.leaders: dict[int, int] = {}
        self.stats = {"leaders": 0, "faults": 0, "snapshots_installed": 0, "dropped": 0, "delivered": 0}
        self._leaders_checked: set[tuple[int, int]] = set()
        for i in self.node_ids:
            self._start_node(i)
            self._at(self.rng.randrange(self.cfg.tick_ms), lambda i=i: self._tick(i))

    # --- event loop -------------------------------------------------------------------

    def _at(self, delay: int, fn) -> None:
        self._seq += 1
        heapq.heappush(self._queue, (self.now + delay, self._seq, fn))

    def run(self) -> SimResult:
        clients = [_Client(c) for c in range(self.cfg.clients)]
        for client in clients:
            client.target = self.rng.choice(self.node_ids)
            self._at(self.rng.randrange(20), lambda c=client: self._next_op(c))
        if self.cfg.nemesis:
            self._at(self.rng.randint(*self.cfg.calm_ms), self._nemesis)
        end = self.cfg.duration_ms + self.cfg.settle_ms
        self._at(self.cfg.duration_ms, self._heal_everything)
        while self._queue and self._queue[0][0] <= end:
            self.now, _, fn = heapq.heappop(self._queue)
            fn()
        return self._verify(clients)

    # --- nodes --------------------------------------------------------------------------

    def _start_node(self, i: int) -> None:
        self.replicas[i] = Replica(
            i, self.node_ids, self.storages[i], rng=random.Random(self.rng.random()),
            election_ticks=self.cfg.election_ticks, heartbeat_ticks=self.cfg.heartbeat_ticks,
            pre_vote=self.cfg.pre_vote, check_quorum=self.cfg.check_quorum,
            snapshot_every=self.cfg.snapshot_every, stale_reads=self.cfg.stale_reads,
            on_apply=self._on_apply,
        )
        self._flush(i)

    def _tick(self, i: int) -> None:
        replica = self.replicas[i]
        if replica is not None:
            replica.tick()
            self._flush(i)
        self._at(self.cfg.tick_ms, lambda: self._tick(i))

    def _flush(self, i: int) -> None:
        replica = self.replicas[i]
        for m in replica.process(self.now):
            self._send(m)
        raft = replica.raft
        if raft.is_leader:
            first = self.leaders.setdefault(raft.term, i)
            if first != i:
                self._violation(f"election safety: nodes {first} and {i} are both leader in term {raft.term}")
            elif (i, raft.term) not in self._leaders_checked:
                self._leaders_checked.add((i, raft.term))
                self.stats["leaders"] += 1
                self._check_leader_completeness(i)

    def _on_apply(self, node: int, entry) -> None:
        seen = self.committed.setdefault(entry.index, (entry.term, entry.args))
        if seen != (entry.term, entry.args):
            self._violation(f"state machine safety: node {node} applied a different entry at index {entry.index}")

    def _check_leader_completeness(self, i: int) -> None:
        raft = self.replicas[i].raft
        for index, (term, _) in self.committed.items():
            if index > raft.snap_index and raft.term_at(index) != term:
                self._violation(f"leader completeness: leader {i} of term {raft.term} lacks committed entry {index}")
                return

    def _violation(self, text: str) -> None:
        if text not in self.violations:
            self.violations.append(f"t={self.now}ms {text}")

    # --- network ------------------------------------------------------------------------

    def _send(self, m: Message) -> None:
        if (m.frm, m.to) in self.blocked or self.rng.random() < self.cfg.drop_rate:
            self.stats["dropped"] += 1
            return
        copies = 2 if self.rng.random() < self.cfg.duplicate_rate else 1
        for _ in range(copies):
            self._at(self.rng.randint(self.cfg.min_delay_ms, self.cfg.max_delay_ms), lambda: self._receive(m))

    def _receive(self, m: Message) -> None:
        replica = self.replicas.get(m.to)
        if replica is None or (m.frm, m.to) in self.blocked:
            self.stats["dropped"] += 1
            return
        self.stats["delivered"] += 1
        if m.type == "snapshot":
            self.stats["snapshots_installed"] += 1
        replica.step(m)
        self._flush(m.to)

    def _client_delay(self) -> int:
        return self.rng.randint(self.cfg.min_delay_ms, self.cfg.max_delay_ms)

    # --- nemesis ------------------------------------------------------------------------

    def _nemesis(self) -> None:
        if self.now >= self.cfg.duration_ms:
            return
        kind = self.rng.choice(self.cfg.nemesis)
        self.stats["faults"] += 1
        heal_after = self.rng.randint(*self.cfg.fault_ms)
        if kind == "partition":
            nodes = self.node_ids[:]
            self.rng.shuffle(nodes)
            cut = self.rng.randint(1, len(nodes) - 1)
            self._partition(nodes[:cut], nodes[cut:])
            self._at(heal_after, self._heal_network)
        elif kind == "isolate-leader":
            leader = next((i for i, r in self.replicas.items() if r and r.raft.is_leader), None)
            if leader is not None:
                self._partition([leader], [i for i in self.node_ids if i != leader])
            self._at(heal_after, self._heal_network)
        elif kind == "crash":
            victim = self.rng.choice(self.node_ids)
            if self.replicas[victim] is not None:
                self.replicas[victim] = None  # in-flight client requests to it are lost
                self._at(heal_after, lambda: self._restart(victim))
        self._at(heal_after + self.rng.randint(*self.cfg.calm_ms), self._nemesis)

    def _partition(self, a: list[int], b: list[int]) -> None:
        for x in a:
            for y in b:
                self.blocked.add((x, y))
                self.blocked.add((y, x))

    def _heal_network(self) -> None:
        self.blocked.clear()

    def _restart(self, i: int) -> None:
        if self.replicas[i] is not None:
            return
        if self.cfg.forget_votes:
            self.storages[i].vote = None
        self._start_node(i)

    def _heal_everything(self) -> None:
        self._heal_network()
        for i in self.node_ids:
            self._restart(i)

    # --- clients ------------------------------------------------------------------------

    def _next_op(self, c: _Client) -> None:
        if self.now >= self.cfg.duration_ms:
            return
        key = f"k{self.rng.randrange(self.cfg.keys)}"
        roll = self.rng.random()
        if roll < 0.4:
            kind, value, args = "get", None, [b"GET", key.encode()]
        elif roll < 0.7:
            value = self.rng.randrange(100)
            kind, args = "set", [b"SET", key.encode(), str(value).encode()]
        elif roll < 0.9:
            kind, value, args = "incr", None, [b"INCR", key.encode()]
        else:
            kind, value, args = "del", None, [b"DEL", key.encode()]
        c.op = linearizability.Op(c.id, kind, key, value, self.now, math.inf, linearizability.UNKNOWN)
        c.args = args
        self.history.append(c.op)
        op = c.op
        self._at(self.cfg.client_timeout_ms, lambda: self._timeout(c, op))
        self._attempt(c)

    def _attempt(self, c: _Client) -> None:
        c.attempt += 1
        attempt, node, args = c.attempt, c.target, list(c.args)

        def arrive() -> None:
            replica = self.replicas.get(node)
            if replica is None:
                return  # crashed: the client will time out

            def reply(value) -> None:
                if self.rng.random() < self.cfg.drop_rate:
                    return
                self._at(self._client_delay(), lambda: self._on_reply(c, attempt, value))

            replica.submit(args, self.now, reply)
            self._flush(node)

        self._at(self._client_delay(), arrive)

    def _on_reply(self, c: _Client, attempt: int, value) -> None:
        if c.op is None or attempt != c.attempt:
            return  # a reply to an attempt we already gave up on
        if isinstance(value, NotLeader):
            # Definitely not applied: retry at the node it named (or anywhere).
            c.target = value.leader_id or self.rng.choice(self.node_ids)
            self._retry(c, self.rng.randint(1, 20))
            return
        if isinstance(value, Error):
            if value.startswith("TRYAGAIN"):
                c.target = self.rng.choice(self.node_ids)
                self._retry(c, self.rng.randint(5, 30))
                return
            if value.startswith("UNKNOWN"):
                self._finish(c, linearizability.UNKNOWN)
                return
            self._violation(f"unexpected error reply {value!r} to {c.args}")
            self._finish(c, linearizability.UNKNOWN)
            return
        result = value
        if c.op.kind == "get" and value is not None:
            result = int(value)
        elif c.op.kind == "set":
            result = "OK"
        self._finish(c, result, ret=self.now)

    def _retry(self, c: _Client, delay: int) -> None:
        op = c.op

        def again() -> None:
            if c.op is op:  # not if the operation timed out in the meantime
                self._attempt(c)

        self._at(delay, again)

    def _timeout(self, c: _Client, op) -> None:
        if c.op is op:
            c.target = self.rng.choice(self.node_ids)
            self._finish(c, linearizability.UNKNOWN)

    def _finish(self, c: _Client, result, ret: float = math.inf) -> None:
        c.op.result, c.op.ret = result, ret
        c.op = None
        c.attempt += 1  # ignore late replies
        self._at(self.rng.randint(0, 10), lambda: self._next_op(c))

    # --- final checks -------------------------------------------------------------------

    def _verify(self, clients: list[_Client]) -> SimResult:
        alive = [r for r in self.replicas.values() if r is not None]
        applied = {r.id: r.sm.applied for r in alive}
        digests = {r.id: r.sm.digest() for r in alive}
        if len(set(applied.values())) > 1 or len(set(digests.values())) > 1:
            self._violation(f"replicas did not converge after healing: applied {applied}, digests {digests}")
        result = linearizability.check(self.history)
        if not result.ok:
            self._violation(f"not linearizable on key {result.key}: " + _describe(result.ops))
        ok_ops = sum(1 for op in self.history if not op.indeterminate)
        self.stats.update(ops_ok=ok_ops, ops_unknown=len(self.history) - ok_ops,
                          final_index=max(applied.values(), default=0))
        return SimResult(self.seed, not self.violations, self.violations, self.stats, self.history)


def _describe(ops: list[linearizability.Op], limit: int = 12) -> str:
    def fmt(op):
        arg = f" {op.value}" if op.value is not None else ""
        ret = "?" if op.ret == math.inf else op.ret
        res = "?" if op.indeterminate else op.result
        return f"[c{op.client} {op.kind}{arg} -> {res} @{op.call}..{ret}]"
    return " ".join(fmt(op) for op in sorted(ops, key=lambda o: o.call)[:limit]) + (" ..." if len(ops) > limit else "")


def run(seed: int, **overrides) -> SimResult:
    return Simulation(seed, SimConfig(**overrides)).run()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the cluster simulator over many seeds.")
    parser.add_argument("--seed", type=int, help="run one seed (to replay a failure)")
    parser.add_argument("--seeds", type=int, default=100, help="how many seeds to run, starting at --start")
    parser.add_argument("--start", type=int, default=0)
    parser.add_argument("--nodes", type=int, default=3)
    parser.add_argument("--duration", type=int, default=4000, help="simulated milliseconds of faults")
    parser.add_argument("--bug", choices=["stale-reads", "forget-votes"], help="turn on a deliberate bug")
    args = parser.parse_args()
    overrides = {"nodes": args.nodes, "duration_ms": args.duration}
    if args.bug == "stale-reads":
        overrides.update(stale_reads=True, check_quorum=False)
    elif args.bug == "forget-votes":
        overrides.update(forget_votes=True)
    seeds = [args.seed] if args.seed is not None else range(args.start, args.start + args.seeds)
    failures = 0
    totals = {"ops_ok": 0, "ops_unknown": 0, "faults": 0, "leaders": 0}
    for seed in seeds:
        result = run(seed, **overrides)
        for k in totals:
            totals[k] += result.stats.get(k, 0)
        if not result.ok or args.seed is not None:
            print(result.summary())
        failures += not result.ok
    print(f"{len(seeds)} seeds, {failures} failed | {totals['ops_ok']} ops checked, "
          f"{totals['ops_unknown']} indeterminate, {totals['faults']} faults injected, "
          f"{totals['leaders']} leaders elected")
    raise SystemExit(1 if failures else 0)


if __name__ == "__main__":
    main()
