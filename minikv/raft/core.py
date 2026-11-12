"""The Raft consensus algorithm as a deterministic state machine with no I/O.

RaftNode never touches a socket, a clock or a thread. It is driven from outside:

    node.tick()            one unit of logical time has passed
    node.step(message)     a message arrived from a peer
    node.propose(args)     the leader should replicate a command
    node.read_index(ctx)   the leader should confirm it may serve a linearizable read
    node.ready()           collect what to do next: messages to send, committed
                           entries to apply, a snapshot to load, confirmed reads

Persistent state (term, vote, log, snapshot) is written to a Storage object as it
changes. The driver must call storage.sync() before it sends the messages from a
ready(), so nothing is promised to a peer before it is durable.

Because the node is deterministic, the same code runs inside a real asyncio
server (minikv/raft/node.py) and inside a seeded network simulator
(minikv/raft/sim.py) that can replay any failure exactly.

Implemented: leader election with randomised timeouts, log replication with
fast conflict back-off, commitment only of current-term entries (Raft §5.4.2),
PreVote and CheckQuorum (Raft thesis §9.6, §6.2), ReadIndex reads (§6.4), and
log compaction with InstallSnapshot (§7). Not implemented: membership changes.
"""

from __future__ import annotations

import random
from dataclasses import dataclass

FOLLOWER = "follower"
PRE_CANDIDATE = "pre-candidate"
CANDIDATE = "candidate"
LEADER = "leader"

VOTE = "vote"
VOTE_RESP = "vote-resp"
PRE_VOTE = "pre-vote"
PRE_VOTE_RESP = "pre-vote-resp"
APPEND = "append"
APPEND_RESP = "append-resp"
SNAPSHOT = "snapshot"


@dataclass(frozen=True)
class Entry:
    term: int
    index: int
    args: tuple[bytes, ...] | None  # None is a no-op, appended by every new leader
    time_ms: int = 0                # the leader's clock when proposed; drives expiry


@dataclass(frozen=True)
class Snapshot:
    index: int  # last log index the snapshot covers
    term: int   # term of that entry
    data: bytes


@dataclass
class Message:
    type: str
    frm: int
    to: int
    term: int
    # votes and pre-votes
    last_index: int = 0
    last_term: int = 0
    granted: bool = False
    # append entries; in a rejected append-resp, prev_index is the index that was rejected
    prev_index: int = 0
    prev_term: int = 0
    entries: tuple[Entry, ...] = ()
    commit: int = 0
    seq: int = 0  # heartbeat round number, echoed back; ReadIndex counts these
    # append-resp
    success: bool = False
    match_index: int = 0
    conflict_index: int = 0
    conflict_term: int = 0
    # install snapshot
    snapshot: Snapshot | None = None


@dataclass(frozen=True)
class ReadState:
    """The answer to read_index(ctx): once the state machine has applied `index`, a
    read reflects every write acknowledged before the request. index -1 means
    leadership was lost first and the read must be retried."""
    ctx: int
    index: int


@dataclass
class Ready:
    messages: list[Message]
    committed: list[Entry]
    snapshot: Snapshot | None
    reads: list[ReadState]


@dataclass
class _Progress:
    next: int
    match: int = 0


class RaftNode:
    def __init__(
        self,
        node_id: int,
        peers: list[int],
        storage,
        *,
        election_ticks: int = 10,
        heartbeat_ticks: int = 2,
        rng: random.Random | None = None,
        pre_vote: bool = True,
        check_quorum: bool = True,
        max_batch: int = 64,
    ) -> None:
        if heartbeat_ticks >= election_ticks:
            raise ValueError("heartbeats must be more frequent than elections")
        self.id = node_id
        self.peers = sorted(p for p in peers if p != node_id)
        self.quorum = (len(self.peers) + 1) // 2 + 1
        self.storage = storage
        self.election_ticks = election_ticks
        self.heartbeat_ticks = heartbeat_ticks
        self.rng = rng or random.Random()
        self.pre_vote = pre_vote
        self.check_quorum = check_quorum
        self.max_batch = max_batch

        term, vote, snapshot, entries = storage.load()
        self.term: int = term
        self.voted_for: int | None = vote
        self.snap_index = snapshot.index if snapshot else 0
        self.snap_term = snapshot.term if snapshot else 0
        self.log: list[Entry] = [e for e in entries if e.index > self.snap_index]
        for offset, entry in enumerate(self.log):
            if entry.index != self.snap_index + 1 + offset:
                raise ValueError(f"storage returned a non-contiguous log at index {entry.index}")
        self.commit_index = self.snap_index
        self.applied = self.snap_index  # everything up to here has been handed out by ready()
        self._restore: Snapshot | None = snapshot

        self.state = FOLLOWER
        self.leader_id: int | None = None
        self._msgs: list[Message] = []
        self._read_states: list[ReadState] = []
        self._votes: dict[int, bool] = {}
        self._progress: dict[int, _Progress] = {}
        self._active: set[int] = set()
        self._seq = 0
        self._acked_seq: dict[int, int] = {}
        self._reads: list[tuple[int, int, int]] = []  # (ctx, read index, heartbeat seq)
        self._reads_waiting: list[int] = []           # ctxs waiting for a current-term commit
        self._broadcast_pending = False
        self._read_round_pending = False
        self.elapsed = 0
        self.timeout = 0
        self._hb_elapsed = 0
        self._reset_timer()

    # --- log helpers -------------------------------------------------------------

    def last_index(self) -> int:
        return self.log[-1].index if self.log else self.snap_index

    def last_term(self) -> int:
        return self.log[-1].term if self.log else self.snap_term

    def term_at(self, index: int) -> int | None:
        """Term of the entry at `index`, or None if it is compacted away or absent."""
        if index == self.snap_index:
            return self.snap_term
        if index < self.snap_index or index > self.last_index():
            return None
        return self.log[index - self.snap_index - 1].term

    def entries(self, start: int, stop: int) -> list[Entry]:
        """Entries with start <= index < stop (start must be past the snapshot)."""
        lo = start - self.snap_index - 1
        return self.log[lo:max(lo, stop - self.snap_index - 1)]

    def _append(self, entries: list[Entry]) -> None:
        self.log.extend(entries)
        self.storage.append(entries)

    def _truncate_from(self, index: int) -> None:
        assert index > self.commit_index, "a committed entry must never be removed"
        del self.log[index - self.snap_index - 1:]
        self.storage.truncate_from(index)

    # --- state transitions -------------------------------------------------------

    @property
    def is_leader(self) -> bool:
        return self.state == LEADER

    def _set_hard_state(self, term: int, vote: int | None) -> None:
        if (term, vote) != (self.term, self.voted_for):
            self.term, self.voted_for = term, vote
            self.storage.save_hard_state(term, vote)

    def _reset_timer(self) -> None:
        self.elapsed = 0
        self.timeout = self.election_ticks + self.rng.randrange(self.election_ticks)

    def _become_follower(self, term: int, leader: int | None) -> None:
        self._fail_reads()
        self._set_hard_state(term, self.voted_for if term == self.term else None)
        self.state = FOLLOWER
        self.leader_id = leader
        self._reset_timer()

    def _campaign(self, pre: bool) -> None:
        self._fail_reads()
        if pre:
            self.state = PRE_CANDIDATE
            term, kind = self.term + 1, PRE_VOTE  # our term only changes if the pre-vote wins
        else:
            self._set_hard_state(self.term + 1, self.id)
            self.state = CANDIDATE
            term, kind = self.term, VOTE
        self.leader_id = None
        self._reset_timer()
        self._votes = {self.id: True}
        if self.quorum == 1:
            self._won_vote(pre)
            return
        for peer in self.peers:
            self._send(Message(kind, self.id, peer, term,
                               last_index=self.last_index(), last_term=self.last_term()))

    def _won_vote(self, pre: bool) -> None:
        if pre:
            self._campaign(pre=False)
        else:
            self._become_leader()

    def _become_leader(self) -> None:
        self.state = LEADER
        self.leader_id = self.id
        self.elapsed = 0
        self._hb_elapsed = 0
        self._progress = {p: _Progress(next=self.last_index() + 1) for p in self.peers}
        self._active = set()
        self._seq = 0
        self._acked_seq = {}
        self._reads, self._reads_waiting = [], []
        # A no-op from the new term: once it commits, so has everything before it
        # (a leader may only count replicas for entries of its own term, §5.4.2).
        last_time = self.log[-1].time_ms if self.log else 0
        self._append([Entry(self.term, self.last_index() + 1, None, last_time)])
        self._maybe_commit()
        self._broadcast_append()

    # --- driving the node --------------------------------------------------------

    def tick(self) -> None:
        self.elapsed += 1
        if self.state == LEADER:
            self._hb_elapsed += 1
            if self.check_quorum and self.elapsed >= self.election_ticks:
                # Step down if a majority hasn't been heard from for a whole election
                # timeout, so a leader cut off by a partition stops claiming leadership.
                self.elapsed = 0
                if 1 + len(self._active) < self.quorum:
                    self._become_follower(self.term, None)
                    return
                self._active = set()
            if self._hb_elapsed >= self.heartbeat_ticks:
                self._hb_elapsed = 0
                self._broadcast_append()
        elif self.elapsed >= self.timeout:
            self._campaign(pre=self.pre_vote)

    def propose(self, args: list[bytes] | tuple[bytes, ...], now_ms: int = 0) -> int | None:
        """Append a command to the leader's log. Returns its index, or None if not leader."""
        if self.state != LEADER:
            return None
        last_time = self.log[-1].time_ms if self.log else 0
        entry = Entry(self.term, self.last_index() + 1, tuple(args), max(now_ms, last_time))
        self._append([entry])
        self._maybe_commit()
        self._broadcast_pending = True  # sent from ready(): proposals made together share messages
        return entry.index

    def read_index(self, ctx: int) -> bool:
        """Ask to serve a linearizable read. Returns False if not leader; otherwise a
        ReadState for `ctx` will come out of ready() later."""
        if self.state != LEADER:
            return False
        if self.term_at(self.commit_index) != self.term:
            # Until the no-op commits, this leader's commit index may be behind writes
            # that an earlier leader acknowledged.
            self._reads_waiting.append(ctx)
        else:
            self._start_read(ctx)
        return True

    def _start_read(self, ctx: int) -> None:
        if self.quorum == 1:
            self._read_states.append(ReadState(ctx, self.commit_index))
            return
        # Confirm we are still the leader with a fresh heartbeat round: if a majority
        # answers it, no other leader can have committed anything newer. Reads that
        # arrive together share one round, sent from ready().
        self._reads.append((ctx, self.commit_index, self._seq + 1))
        self._read_round_pending = True

    def _check_reads(self) -> None:
        while self._reads:
            ctx, index, seq = self._reads[0]
            acks = 1 + sum(1 for p in self.peers if self._acked_seq.get(p, 0) >= seq)
            if acks < self.quorum:
                return
            self._reads.pop(0)
            self._read_states.append(ReadState(ctx, index))

    def _fail_reads(self) -> None:
        for ctx, _, _ in self._reads:
            self._read_states.append(ReadState(ctx, -1))
        for ctx in self._reads_waiting:
            self._read_states.append(ReadState(ctx, -1))
        self._reads, self._reads_waiting = [], []

    def ready(self) -> Ready:
        if self.state == LEADER and (self._broadcast_pending or self._read_round_pending):
            if self._read_round_pending:
                self._seq += 1
            self._broadcast_append()
        self._broadcast_pending = self._read_round_pending = False
        snapshot, self._restore = self._restore, None
        committed: list[Entry] = []
        if self.applied < self.commit_index:
            committed = self.entries(self.applied + 1, self.commit_index + 1)
            self.applied = self.commit_index
        messages, self._msgs = self._msgs, []
        reads, self._read_states = self._read_states, []
        return Ready(messages, committed, snapshot, reads)

    def compact(self, index: int, data: bytes) -> None:
        """Replace the log up to `index` (already applied) with a state machine snapshot."""
        if index <= self.snap_index or index > self.applied:
            return
        snapshot = Snapshot(index, self.term_at(index), data)
        self.log = self.log[index - self.snap_index:]
        self.snap_index, self.snap_term = index, snapshot.term
        self.storage.save_snapshot(snapshot)

    # --- messages ----------------------------------------------------------------

    def _send(self, message: Message) -> None:
        self._msgs.append(message)

    def step(self, m: Message) -> None:
        if m.term > self.term:
            if m.type in (VOTE, PRE_VOTE) and self._in_lease():
                return  # we heard from a live leader recently: ignore a disruptive candidate
            if m.type == PRE_VOTE or (m.type == PRE_VOTE_RESP and m.granted):
                pass  # pre-votes never change anybody's term
            else:
                leader = m.frm if m.type in (APPEND, SNAPSHOT) else None
                self._become_follower(m.term, leader)
        elif m.term < self.term:
            if m.type in (APPEND, SNAPSHOT):
                # A deposed leader: tell it the current term so it steps down.
                self._send(Message(APPEND_RESP, self.id, m.frm, self.term, prev_index=-1))
            elif m.type in (VOTE, PRE_VOTE):
                kind = VOTE_RESP if m.type == VOTE else PRE_VOTE_RESP
                self._send(Message(kind, self.id, m.frm, self.term, granted=False))
            return

        handler = {
            VOTE: self._on_vote, PRE_VOTE: self._on_vote,
            VOTE_RESP: self._on_vote_resp, PRE_VOTE_RESP: self._on_vote_resp,
            APPEND: self._on_append, APPEND_RESP: self._on_append_resp,
            SNAPSHOT: self._on_snapshot,
        }[m.type]
        handler(m)

    def _in_lease(self) -> bool:
        return self.check_quorum and self.leader_id is not None and self.elapsed < self.election_ticks

    def _on_vote(self, m: Message) -> None:
        pre = m.type == PRE_VOTE
        can_vote = (
            self.voted_for == m.frm
            or (self.voted_for is None and self.leader_id is None)
            or (pre and m.term > self.term)
        )
        up_to_date = (m.last_term, m.last_index) >= (self.last_term(), self.last_index())
        kind = PRE_VOTE_RESP if pre else VOTE_RESP
        if can_vote and up_to_date:
            if not pre:
                self._set_hard_state(self.term, m.frm)  # durable before the reply is sent
                self._reset_timer()
            self._send(Message(kind, self.id, m.frm, m.term, granted=True))
        else:
            self._send(Message(kind, self.id, m.frm, self.term, granted=False))

    def _on_vote_resp(self, m: Message) -> None:
        pre = m.type == PRE_VOTE_RESP
        if self.state != (PRE_CANDIDATE if pre else CANDIDATE):
            return
        if pre and m.granted and m.term != self.term + 1:
            return  # an answer to an older pre-vote round
        self._votes[m.frm] = m.granted
        granted = sum(1 for v in self._votes.values() if v)
        rejected = len(self._votes) - granted
        if granted >= self.quorum:
            self._won_vote(pre)
        elif rejected >= self.quorum:
            self._become_follower(self.term, None)

    def _on_append(self, m: Message) -> None:
        if self.state != FOLLOWER:
            self._become_follower(m.term, m.frm)  # a leader exists for our term
        self.leader_id = m.frm
        self.elapsed = 0

        if m.prev_index < self.commit_index:
            # Everything up to our commit index already matches the leader's log.
            self._send(Message(APPEND_RESP, self.id, m.frm, self.term, success=True,
                               match_index=self.commit_index, seq=m.seq))
            return

        local_term = self.term_at(m.prev_index)
        if local_term != m.prev_term:
            # Reject, with a hint so the leader can skip a whole term at a time.
            if local_term is None:
                conflict_index, conflict_term = self.last_index() + 1, 0
            else:
                conflict_term = local_term
                conflict_index = m.prev_index
                while conflict_index - 1 > self.commit_index and self.term_at(conflict_index - 1) == conflict_term:
                    conflict_index -= 1
            self._send(Message(APPEND_RESP, self.id, m.frm, self.term, success=False,
                               prev_index=m.prev_index, conflict_index=conflict_index,
                               conflict_term=conflict_term, seq=m.seq))
            return

        for i, entry in enumerate(m.entries):
            existing = self.term_at(entry.index)
            if existing == entry.term:
                continue  # already have it (a duplicate or re-sent message)
            if existing is not None:
                self._truncate_from(entry.index)  # conflicting suffix from an old leader
            self._append(list(m.entries[i:]))
            break
        last_new = m.prev_index + len(m.entries)
        if m.commit > self.commit_index:
            # Only up to what this message proved matches the leader's log.
            self.commit_index = max(self.commit_index, min(m.commit, last_new))
        self._send(Message(APPEND_RESP, self.id, m.frm, self.term, success=True,
                           match_index=last_new, seq=m.seq))

    def _on_snapshot(self, m: Message) -> None:
        if self.state != FOLLOWER:
            self._become_follower(m.term, m.frm)
        self.leader_id = m.frm
        self.elapsed = 0
        snap = m.snapshot
        if snap.index <= self.commit_index:
            match = self.commit_index
        elif self.term_at(snap.index) == snap.term:
            self.commit_index = match = snap.index  # our log already has it; just commit
        else:
            # Our log conflicts with the snapshot or is too short: throw all of it away.
            # Truncate first, so a crash in between leaves a shorter log, never one
            # where an old suffix follows the new snapshot.
            self.storage.truncate_from(self.snap_index + 1)
            self.log = []
            self.snap_index, self.snap_term = snap.index, snap.term
            self.commit_index = self.applied = snap.index
            self.storage.save_snapshot(snap)
            self._restore = snap
            match = snap.index
        self._send(Message(APPEND_RESP, self.id, m.frm, self.term, success=True, match_index=match, seq=m.seq))

    def _on_append_resp(self, m: Message) -> None:
        if self.state != LEADER:
            return
        self._active.add(m.frm)
        if m.seq > self._acked_seq.get(m.frm, 0):
            self._acked_seq[m.frm] = m.seq
            self._check_reads()
        pr = self._progress[m.frm]
        if m.success:
            if m.match_index > pr.match:
                pr.match = m.match_index
                self._maybe_commit()
            pr.next = max(pr.next, pr.match + 1)
            if pr.next <= self.last_index():
                self._send_append(m.frm)
            return
        if m.prev_index != pr.next - 1:
            return  # a stale rejection of an earlier attempt
        next_index = m.conflict_index
        if m.conflict_term:
            last_of_term = self._last_index_of_term(m.conflict_term)
            if last_of_term is not None:
                next_index = last_of_term + 1
        pr.next = max(pr.match + 1, min(next_index, m.prev_index))
        self._send_append(m.frm)

    def _last_index_of_term(self, term: int) -> int | None:
        for entry in reversed(self.log):
            if entry.term == term:
                return entry.index
            if entry.term < term:
                return None
        return None

    def _maybe_commit(self) -> None:
        matches = sorted([self.last_index()] + [pr.match for pr in self._progress.values()], reverse=True)
        candidate = matches[self.quorum - 1]
        # Only entries of the current term are committed by counting replicas (§5.4.2).
        if candidate > self.commit_index and self.term_at(candidate) == self.term:
            first_in_term = self.term_at(self.commit_index) != self.term
            self.commit_index = candidate
            if first_in_term:
                waiting, self._reads_waiting = self._reads_waiting, []
                for ctx in waiting:
                    self._start_read(ctx)

    def _broadcast_append(self) -> None:
        for peer in self.peers:
            self._send_append(peer)

    def _send_append(self, peer: int) -> None:
        pr = self._progress[peer]
        if pr.next <= self.snap_index:
            self._send(Message(SNAPSHOT, self.id, peer, self.term, snapshot=self.storage.snapshot(),
                               seq=self._seq))
            return
        prev = pr.next - 1
        entries = tuple(self.entries(pr.next, pr.next + self.max_batch))
        self._send(Message(APPEND, self.id, peer, self.term, prev_index=prev, prev_term=self.term_at(prev),
                           entries=entries, commit=self.commit_index, seq=self._seq))

    def status(self) -> dict:
        return {"id": self.id, "state": self.state, "term": self.term, "leader": self.leader_id,
                "commit": self.commit_index, "applied": self.applied, "last_index": self.last_index(),
                "snapshot_index": self.snap_index}

