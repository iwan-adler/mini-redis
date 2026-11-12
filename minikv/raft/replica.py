"""One cluster member: a Raft node plus the key-value state machine it drives.

Replica turns client commands into Raft operations and Raft's output back into
replies. It is still free of I/O and clocks (callers pass `now_ms`), so the real
server and the simulator share it unchanged.

Replies a client can get besides the command's normal reply:

    NotLeader(id)   this node isn't the leader; nothing happened. Retry at `id`.
    TRYAGAIN ...    definitely not applied (e.g. a leader change discarded the write).
    UNKNOWN ...     might or might not have been applied (the node lost track of it).
"""

from __future__ import annotations

import random
from collections.abc import Callable
from dataclasses import dataclass

from minikv.raft.core import Message, RaftNode
from minikv.raft.statemachine import KVStateMachine, classify
from minikv.resp import Error

Callback = Callable[[object], None]

WRITE_LOST = Error("TRYAGAIN the write was discarded by a leader change and not applied")
READ_LOST = Error("TRYAGAIN leadership changed before the read was confirmed")
OUTCOME_UNKNOWN = Error("UNKNOWN the write may or may not have been applied")


@dataclass(frozen=True)
class NotLeader:
    leader_id: int | None


class Replica:
    def __init__(
        self,
        node_id: int,
        peer_ids: list[int],
        storage,
        *,
        rng: random.Random | None = None,
        election_ticks: int = 10,
        heartbeat_ticks: int = 2,
        pre_vote: bool = True,
        check_quorum: bool = True,
        snapshot_every: int = 1000,
        stale_reads: bool = False,
        on_apply: Callable[[int, object], None] | None = None,
    ) -> None:
        self.raft = RaftNode(node_id, peer_ids, storage, election_ticks=election_ticks,
                             heartbeat_ticks=heartbeat_ticks, rng=rng, pre_vote=pre_vote,
                             check_quorum=check_quorum)
        self.storage = storage
        self.sm = KVStateMachine()
        self.snapshot_every = snapshot_every
        # Deliberately unsafe: serve reads from local state with no leadership check.
        # Only for showing that the test harness catches stale reads.
        self.stale_reads = stale_reads
        self.on_apply = on_apply
        self._writes: dict[int, tuple[int, Callback]] = {}  # log index -> (term, callback)
        self._reads: dict[int, tuple[list[bytes], Callback]] = {}
        self._confirmed_reads: list[tuple[int, list[bytes], Callback]] = []
        self._next_ctx = 0
        self._now_ms = 0

    @property
    def id(self) -> int:
        return self.raft.id

    def submit(self, args: list[bytes], now_ms: int, callback: Callback) -> None:
        """Run a client command. `callback` gets the reply, now or after consensus."""
        self._now_ms = now_ms
        kind = classify(args)
        if kind == "local":
            callback(self.sm.execute_local(args))
            return
        if not self.raft.is_leader:
            callback(NotLeader(self.raft.leader_id))
            return
        if kind == "write":
            index = self.raft.propose(args, now_ms)
            self._writes[index] = (self.raft.term, callback)
        elif self.stale_reads:
            callback(self.sm.read(args, now_ms))
        else:
            self._next_ctx += 1
            self._reads[self._next_ctx] = (args, callback)
            self.raft.read_index(self._next_ctx)

    def tick(self) -> None:
        self.raft.tick()

    def step(self, message: Message) -> None:
        self.raft.step(message)

    def process(self, now_ms: int) -> list[Message]:
        """Apply what Raft has decided and return the messages to send. The storage is
        synced first, so no message promises anything that isn't on disk."""
        self._now_ms = now_ms
        rd = self.raft.ready()
        self.storage.sync()
        if rd.snapshot is not None:
            self.sm.restore(rd.snapshot.data)
            for index in [i for i in self._writes if i <= rd.snapshot.index]:
                self._writes.pop(index)[1](OUTCOME_UNKNOWN)
        for entry in rd.committed:
            reply = self.sm.apply(entry)
            if self.on_apply:
                self.on_apply(self.id, entry)
            pending = self._writes.pop(entry.index, None)
            if pending is not None:
                term, callback = pending
                # Another leader's entry at our index means ours was overwritten.
                callback(reply if term == entry.term else WRITE_LOST)
        for rs in rd.reads:
            args, callback = self._reads.pop(rs.ctx)
            if rs.index < 0:
                callback(READ_LOST)
            else:
                self._confirmed_reads.append((rs.index, args, callback))
        if self._confirmed_reads:
            waiting = []
            for index, args, callback in self._confirmed_reads:
                if index <= self.sm.applied:
                    callback(self.sm.read(args, now_ms))
                else:
                    waiting.append((index, args, callback))
            self._confirmed_reads = waiting
        if self.raft.applied - self.raft.snap_index >= self.snapshot_every:
            self.raft.compact(self.raft.applied, self.sm.snapshot())
        return rd.messages

    def fail_pending(self) -> None:
        """Answer every waiting client with UNKNOWN (used when shutting down)."""
        for _, callback in self._writes.values():
            callback(OUTCOME_UNKNOWN)
        for _, callback in self._reads.values():
            callback(READ_LOST)
        for _, _, callback in self._confirmed_reads:
            callback(READ_LOST)
        self._writes, self._reads, self._confirmed_reads = {}, {}, []

    def status(self) -> dict:
        return {**self.raft.status(), "digest": self.sm.digest(), "keys": self.sm.store.size()}
