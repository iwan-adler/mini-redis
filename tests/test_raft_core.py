"""Unit tests of the Raft state machine, driven message by message."""

import random

import pytest

from minikv.raft.core import APPEND, CANDIDATE, FOLLOWER, LEADER, Entry, Message, RaftNode
from minikv.raft.storage import MemoryStorage


class Net:
    """Nodes plus a manual network: messages are only delivered when asked."""

    def __init__(self, n=3, **kwargs):
        self.ids = list(range(1, n + 1))
        self.storage = {i: MemoryStorage() for i in self.ids}
        self.kwargs = kwargs
        self.nodes = {i: self._make(i) for i in self.ids}
        self.cut: set[int] = set()
        self.applied = {i: [] for i in self.ids}
        self.reads = {i: [] for i in self.ids}

    def _make(self, i):
        return RaftNode(i, self.ids, self.storage[i], rng=random.Random(i), **self.kwargs)

    def restart(self, i):
        self.nodes[i] = self._make(i)

    def pump(self, rounds=50):
        """Deliver messages until the network is quiet."""
        for _ in range(rounds):
            msgs = []
            for i, node in self.nodes.items():
                rd = node.ready()
                self.applied[i].extend(rd.committed)
                self.reads[i].extend(rd.reads)
                msgs.extend(rd.messages)
            msgs = [m for m in msgs if m.frm not in self.cut and m.to not in self.cut]
            if not msgs:
                return
            for m in msgs:
                self.nodes[m.to].step(m)

    def tick(self, i, n=1):
        for _ in range(n):
            self.nodes[i].tick()

    def elect(self, i):
        """Make node i time out and win an election."""
        node = self.nodes[i]
        for j, other in self.nodes.items():
            if j != i:  # let the others' leader lease run out, as if time had passed
                other.elapsed = max(other.elapsed, other.election_ticks)
        self.tick(i, node.timeout)
        self.pump()
        assert node.state == LEADER
        return node

    def leaders(self):
        return [i for i, n in self.nodes.items() if n.state == LEADER]


def test_single_node_elects_itself_and_commits():
    net = Net(1)
    leader = net.elect(1)
    index = leader.propose([b"SET", b"a", b"1"])
    net.pump()
    assert leader.commit_index == index
    assert [e.args for e in net.applied[1]] == [None, (b"SET", b"a", b"1")]  # no-op first


def test_election_and_replication():
    net = Net(3)
    leader = net.elect(1)
    assert net.leaders() == [1]
    assert all(n.leader_id == 1 for n in net.nodes.values())
    leader.propose([b"SET", b"a", b"1"])
    net.pump()
    for i in (1, 2, 3):
        assert net.nodes[i].last_index() == 2
    # Followers learn the commit index on the next heartbeat.
    net.tick(1, leader.heartbeat_ticks)
    net.pump()
    assert all(n.commit_index == 2 for n in net.nodes.values())
    assert all(len(a) == 2 for a in net.applied.values())


def test_vote_is_persisted_before_reply():
    net = Net(3, pre_vote=False)
    net.tick(1, net.nodes[1].timeout)
    # Node 2 receives the vote request; its vote must be on "disk" right away.
    request = [m for m in net.nodes[1].ready().messages if m.to == 2][0]
    net.nodes[2].step(request)
    assert net.storage[2].vote == 1
    assert net.storage[2].term == request.term


def test_restarted_node_does_not_vote_twice():
    net = Net(3, pre_vote=False, check_quorum=False)
    net.tick(1, net.nodes[1].timeout)
    req1 = [m for m in net.nodes[1].ready().messages if m.to == 3][0]
    net.nodes[3].step(req1)
    assert net.nodes[3].ready().messages[0].granted
    net.restart(3)  # crash and come back: the vote is still remembered
    rival = Message("vote", 2, 3, req1.term, last_index=5, last_term=5)
    net.nodes[3].step(rival)
    assert not net.nodes[3].ready().messages[0].granted


def test_candidate_with_stale_log_loses():
    net = Net(3)
    leader = net.elect(1)
    net.cut = {3}  # node 3 misses some writes
    leader.propose([b"SET", b"a", b"1"])
    net.pump()
    net.cut = set()
    voter = net.nodes[2]
    # Node 3 asks for votes with a shorter log: node 2 must refuse.
    stale = Message("vote", 3, 2, leader.term + 1, last_index=1, last_term=leader.term)
    voter.elapsed = voter.election_ticks  # not in a leader lease
    voter.step(stale)
    granted = [m.granted for m in voter.ready().messages if m.to == 3]
    assert granted == [False]


def test_follower_drops_conflicting_suffix():
    net = Net(3)
    leader = net.elect(1)
    net.cut = {2, 3}
    leader.propose([b"SET", b"lost", b"1"])  # only on the old leader: never committed
    net.pump()
    net.cut = {1}
    new = net.elect(2)
    new.propose([b"SET", b"kept", b"2"])
    net.pump()
    net.cut = set()
    net.tick(2, new.heartbeat_ticks)
    net.pump()
    old = net.nodes[1]
    assert old.state == FOLLOWER
    assert [e.args for e in old.log] == [e.args for e in new.log]
    assert all(e.args != (b"SET", b"lost", b"1") for e in net.applied[1])


def test_leader_only_commits_entries_from_its_own_term():
    """Raft §5.4.2 (Figure 8): an entry from an older term stored on a majority is not
    committed by counting replicas; only a current-term entry commits it."""
    storage = MemoryStorage()
    storage.save_hard_state(3, 1)
    storage.append([Entry(1, 1, (b"SET", b"a", b"1")), Entry(2, 2, (b"SET", b"b", b"2"))])
    node = RaftNode(1, [1, 2, 3], storage, rng=random.Random(0))
    node._become_leader()  # term 3; appends a no-op at index 3
    assert node.term_at(3) == 3
    node.step(Message("append-resp", 2, 1, 3, success=True, match_index=2))
    assert node.commit_index == 0  # index 2 is on a majority, but it is from term 2
    node.step(Message("append-resp", 2, 1, 3, success=True, match_index=3))
    assert node.commit_index == 3  # the term-3 no-op commits, and everything before it


def test_pre_vote_keeps_a_partitioned_node_from_inflating_terms():
    net = Net(3)
    net.elect(1)
    term = net.nodes[1].term
    net.cut = {3}
    net.tick(3, 200)  # many election timeouts while cut off
    net.pump()
    assert net.nodes[3].term == term  # pre-votes never succeeded, so the term never moved
    net.cut = set()
    net.tick(1, net.nodes[1].heartbeat_ticks)
    net.pump()
    assert net.leaders() == [1]  # rejoining doesn't depose the healthy leader


def test_check_quorum_makes_an_isolated_leader_step_down():
    net = Net(3)
    leader = net.elect(1)
    net.cut = {1}
    net.tick(1, 2 * leader.election_ticks)
    assert leader.state == FOLLOWER


def test_read_index_needs_a_quorum_heartbeat():
    net = Net(3)
    leader = net.elect(1)
    net.pump()
    net.cut = {2, 3}
    assert leader.read_index(7)
    net.pump()
    assert net.reads[1] == []  # no majority has confirmed the leadership yet
    net.cut = set()
    net.tick(1, leader.heartbeat_ticks)
    net.pump()
    assert [(r.ctx, r.index) for r in net.reads[1]] == [(7, leader.commit_index)]


def test_read_index_fails_when_leadership_is_lost():
    net = Net(3)
    leader = net.elect(1)
    net.pump()
    net.cut = {1}
    leader.read_index(9)
    net.tick(1, 2 * leader.election_ticks)  # check-quorum deposes it
    net.pump()
    assert [(r.ctx, r.index) for r in net.reads[1]] == [(9, -1)]


def test_lagging_follower_is_caught_up_with_a_snapshot():
    net = Net(3)
    leader = net.elect(1)
    net.cut = {3}
    for i in range(20):
        leader.propose([b"SET", b"k", str(i).encode()])
    net.pump()
    net.tick(1, leader.heartbeat_ticks)
    net.pump()
    leader.ready()
    leader.compact(leader.commit_index, b"state-at-commit")
    assert leader.snap_index == leader.commit_index
    net.cut = set()
    net.tick(1, leader.heartbeat_ticks)
    msgs = leader.ready().messages
    assert any(m.type == "snapshot" and m.to == 3 for m in msgs)
    for m in msgs:
        net.nodes[m.to].step(m)
    rd = net.nodes[3].ready()
    assert rd.snapshot is not None and rd.snapshot.data == b"state-at-commit"
    assert net.nodes[3].snap_index == leader.snap_index


def test_install_snapshot_discards_a_stale_log_on_disk():
    """Regression: the simulator found that installing a snapshot kept the old log's
    tail in storage, so a restarted node replayed entries from a dead term."""
    storage = MemoryStorage()
    storage.append([Entry(1, i, (b"SET", b"old", b"x")) for i in range(1, 6)])
    node = RaftNode(3, [1, 2, 3], storage, rng=random.Random(0))
    from minikv.raft.core import Snapshot
    node.step(Message("snapshot", 1, 3, 2, snapshot=Snapshot(3, 2, b"s")))
    assert storage.entries == []
    restarted = RaftNode(3, [1, 2, 3], storage, rng=random.Random(0))
    assert restarted.last_index() == 3 and restarted.term_at(4) is None


def test_stale_leader_steps_down_on_hearing_a_higher_term():
    net = Net(3)
    old = net.elect(1)
    net.cut = {1}
    net.elect(2)
    net.cut = set()
    old.step(Message(APPEND, 2, 1, net.nodes[2].term))
    assert old.state == FOLLOWER and old.term == net.nodes[2].term


def test_heartbeat_must_be_faster_than_election():
    with pytest.raises(ValueError):
        RaftNode(1, [1], MemoryStorage(), election_ticks=2, heartbeat_ticks=2)


def test_candidate_state_after_timeout_without_pre_vote():
    net = Net(3, pre_vote=False)
    net.tick(2, net.nodes[2].timeout)
    assert net.nodes[2].state == CANDIDATE
