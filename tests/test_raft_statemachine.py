"""The replicated state machine must be deterministic, including key expiry."""

from minikv.raft.core import Entry
from minikv.raft.statemachine import KVStateMachine, classify


def log(*commands, start_ms=1_000_000, step_ms=1000):
    return [Entry(1, i + 1, tuple(c.encode() if isinstance(c, str) else c for c in cmd.split()),
                  start_ms + i * step_ms) for i, cmd in enumerate(commands)]


def test_replicas_agree_on_expiry_regardless_of_their_own_clocks():
    entries = log("SET a 1", "EXPIRE a 2", "INCR a", "INCR a", "INCR a", "SET b x PX 500", "GET b")
    a, b = KVStateMachine(), KVStateMachine()
    replies_a = [a.apply(e) for e in entries]
    replies_b = [b.apply(e) for e in entries]  # applied "later", but time comes from the log
    assert replies_a == replies_b
    # EXPIRE at t=1001s set a deadline of 1003s; the INCR at 1004s sees a fresh key.
    assert replies_a[2:5] == [2, 1, 2]
    assert a.digest() == b.digest()


def test_incr_on_an_expired_key_starts_without_a_ttl():
    sm = KVStateMachine()
    for e in log("SET a 5 PX 100", "INCR a"):
        sm.apply(e)
    assert sm.read([b"TTL", b"a"], 0) == -1
    assert sm.read([b"GET", b"a"], 0) == b"1"


def test_reads_use_the_wall_clock_but_never_change_state():
    sm = KVStateMachine()
    for e in log("SET a 1 PX 500"):
        sm.apply(e)
    before = sm.digest()
    assert sm.read([b"GET", b"a"], now_ms=1_000_100) == b"1"
    assert sm.read([b"GET", b"a"], now_ms=1_000_600) is None  # expired by wall-clock time
    assert sm.digest() == before  # ...but the read deleted nothing
    # A wall clock behind the log's clock can't bring the key back.
    for e in log("SET b 1", start_ms=1_001_000):
        sm.apply(Entry(1, 2, e.args, e.time_ms))
    assert sm.read([b"GET", b"a"], now_ms=0) is None


def test_clock_never_goes_backwards():
    sm = KVStateMachine()
    sm.apply(Entry(1, 1, (b"SET", b"a", b"1", b"PX", b"100"), 5000))
    sm.apply(Entry(2, 2, (b"INCR", b"a"), 4000))  # a new leader with a slower clock
    assert sm.clock_ms == 5000
    assert sm.read([b"PTTL", b"a"], 0) == 100


def test_snapshot_restore():
    sm = KVStateMachine()
    for e in log("SET a 1", "SET b 2 EX 100", "SET c 3 PX 1", "INCR a"):
        sm.apply(e)
    copy = KVStateMachine()
    copy.restore(sm.snapshot())
    assert copy.digest() == sm.digest()
    assert copy.clock_ms == sm.clock_ms and copy.applied == sm.applied
    assert copy.read([b"GET", b"c"], 0) is None


def test_classify():
    assert classify([b"GET", b"a"]) == "read"
    assert classify([b"set", b"a", b"1"]) == "write"
    assert classify([b"PING"]) == "local"
    assert classify([b"GET"]) == "local"        # arity error: answered locally
    assert classify([b"NOPE", b"x"]) == "local"  # unknown command
    assert classify([b"FLUSHDB"]) == "write"
