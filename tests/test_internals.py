"""Unit tests for the parser, the store (with a fake clock) and AOF persistence."""

import random

import pytest

from minikv import aof, commands
from minikv.resp import OK, Error, ProtocolError, SimpleString, encode, encode_command, parse_command
from minikv.store import Store


class Clock:
    def __init__(self, now: float = 1_000_000.0) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now


# --- RESP -------------------------------------------------------------------------------------


def test_parse_array_command():
    assert parse_command(b"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n") == ([b"GET", b"k"], 20)


def test_parse_waits_for_more_data_at_every_cut_point():
    raw = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nva\r\nl\r\n"
    for cut in range(len(raw)):
        assert parse_command(raw[:cut]) == (None, 0)  # binary-safe: \r\n inside a value is fine
    assert parse_command(raw)[0] == [b"SET", b"key", b"va\r\nl"]


def test_parse_pipelined_commands_one_at_a_time():
    raw = encode_command([b"INCR", b"a"]) + encode_command([b"GET", b"a"])
    first, used = parse_command(raw)
    second, _ = parse_command(raw[used:])
    assert first == [b"INCR", b"a"] and second == [b"GET", b"a"]


def test_inline_commands():
    assert parse_command(b"SET k  v\r\n") == ([b"SET", b"k", b"v"], 10)
    assert parse_command(b"PING\n") == ([b"PING"], 5)
    assert parse_command(b"PIN") == (None, 0)


@pytest.mark.parametrize("raw", [
    b"*x\r\n", b"*1\r\n:5\r\n", b"*1\r\n$-5\r\n", b"*1\r\n$abc\r\n",
    b"*1\r\n$3\r\nabcXY", b"*1\r\n$" + str(2 * 1024 * 1024).encode() + b"\r\n",
])
def test_malformed_input_raises(raw):
    with pytest.raises(ProtocolError):
        parse_command(raw)


def test_encode_resp2_and_resp3():
    assert encode(OK) == b"+OK\r\n"
    assert encode(Error("ERR boom")) == b"-ERR boom\r\n"
    assert encode(42) == b":42\r\n"
    assert encode(b"hi") == b"$2\r\nhi\r\n"
    assert encode(None) == b"$-1\r\n" and encode(None, protocol=3) == b"_\r\n"
    assert encode([b"a", None]) == b"*2\r\n$1\r\na\r\n$-1\r\n"
    assert encode({"proto": 3}, protocol=3) == b"%1\r\n$5\r\nproto\r\n:3\r\n"
    assert encode({"proto": 2}) == b"*2\r\n$5\r\nproto\r\n:2\r\n"
    with pytest.raises(TypeError):
        encode(True)


# --- store and expiry ---------------------------------------------------------------------------


def test_lazy_expiry_with_fake_clock():
    clock = Clock()
    store = Store(clock)
    commands.execute(store, [b"SET", b"k", b"v", b"EX", b"10"])
    clock.now += 9.9
    assert store.get(b"k") == b"v"
    clock.now += 0.2
    assert store.get(b"k") is None and store.ttl_ms(b"k") == -2


def test_ttl_rounds_like_redis():
    clock = Clock()
    store = Store(clock)
    commands.execute(store, [b"SET", b"k", b"v", b"PX", b"1600"])
    assert commands.execute(store, [b"TTL", b"k"])[0] == 2  # 1.6 s rounds to 2
    clock.now += 1.2
    assert commands.execute(store, [b"TTL", b"k"])[0] == 0  # 0.4 s rounds to 0


def test_active_expiry_reclaims_keys_nobody_reads():
    clock = Clock()
    store = Store(clock)
    for i in range(1000):
        store.set(f"k{i}".encode(), b"v", expire_at_ms=store.now_ms() + 1000)
    store.set(b"keep", b"v")
    clock.now += 2
    removed = store.active_expire_cycle(rng=random.Random(0))
    assert removed == 1000 and store.size() == 1  # keeps sampling while >25% were expired


def test_set_option_validation():
    store = Store(Clock())
    for args in ([b"NX", b"XX"], [b"EX", b"1", b"PX", b"1"], [b"EX"], [b"KEEPTTL", b"EX", b"5"]):
        assert commands.execute(store, [b"SET", b"k", b"v", *args])[0] == commands.SYNTAX


def test_write_commands_are_logged_with_absolute_expiry():
    clock = Clock(1000.0)
    store = Store(clock)
    _, logged = commands.execute(store, [b"SET", b"k", b"v", b"EX", b"10"])
    assert logged == [b"SET", b"k", b"v", b"PXAT", b"1010000"]
    _, logged = commands.execute(store, [b"EXPIRE", b"k", b"5"])
    assert logged == [b"PEXPIREAT", b"k", b"1005000"]
    assert commands.execute(store, [b"GET", b"k"])[1] is None  # reads aren't logged
    assert commands.execute(store, [b"DEL", b"nothing"])[1] is None  # no-ops aren't logged


# --- append-only file -------------------------------------------------------------------------


def write_log(path, *cmds):
    path.write_bytes(b"".join(encode_command(c) for c in cmds))


def test_aof_replay(tmp_path):
    log = tmp_path / "a.aof"
    write_log(log, [b"SET", b"a", b"1"], [b"SET", b"n", b"5", b"KEEPTTL"], [b"DEL", b"a"])
    store = Store(Clock())
    assert aof.load(log, store) == (3, 0)
    assert store.get(b"a") is None and store.get(b"n") == b"5"


def test_aof_truncated_tail_is_recovered(tmp_path):
    log = tmp_path / "a.aof"
    good = encode_command([b"SET", b"a", b"1"])
    log.write_bytes(good + b"*3\r\n$3\r\nSET\r\n$1\r\nb")  # crash in the middle of the 2nd write
    store = Store(Clock())
    assert aof.load(log, store) == (1, len(b"*3\r\n$3\r\nSET\r\n$1\r\nb"))
    assert store.get(b"a") == b"1"
    assert log.read_bytes() == good  # the broken tail was cut off the file


def test_aof_corruption_in_the_middle_refuses_to_start(tmp_path):
    log = tmp_path / "a.aof"
    log.write_bytes(encode_command([b"SET", b"a", b"1"]) + b"!!garbage!!\r\n" + encode_command([b"SET", b"b", b"2"]))
    with pytest.raises(aof.CorruptAOF):
        aof.load(log, Store(Clock()))


def test_keys_expired_while_the_server_was_down_stay_expired(tmp_path):
    log = tmp_path / "a.aof"
    write_log(log, [b"SET", b"k", b"v", b"PXAT", b"1000500"])  # expires at t=1000.5 s
    store = Store(Clock(2000.0))
    aof.load(log, store)
    assert store.get(b"k") is None


def test_rewrite_compacts_and_preserves_data(tmp_path):
    log = tmp_path / "a.aof"
    clock = Clock()
    store = Store(clock)
    file = aof.AppendOnlyFile(log, fsync="always")
    for i in range(100):
        _, logged = commands.execute(store, [b"INCR", b"counter"])
        file.append(logged)
    _, logged = commands.execute(store, [b"SET", b"t", b"x", b"EX", b"60"])
    file.append(logged)
    file.close()
    size_before = log.stat().st_size
    assert aof.rewrite(log, store) == 2
    assert log.stat().st_size < size_before / 10
    restored = Store(clock)
    aof.load(log, restored)
    assert restored.get(b"counter") == b"100" and restored.ttl_ms(b"t") == 60_000


def test_fsync_policy_validation(tmp_path):
    with pytest.raises(ValueError):
        aof.AppendOnlyFile(tmp_path / "a.aof", fsync="sometimes")
    assert isinstance(SimpleString("OK"), str)
