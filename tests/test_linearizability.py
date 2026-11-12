"""The checker must accept every linearizable history and reject the others."""

import math

from minikv.raft.linearizability import UNKNOWN, Op, check


def op(client, kind, call, ret, result, value=None, key="k"):
    return Op(client, kind, key, value, call, ret, result)


def test_sequential_history():
    assert check([op(1, "set", 0, 1, "OK", 5), op(1, "get", 2, 3, 5), op(1, "incr", 4, 5, 6)]).ok


def test_stale_read_is_caught():
    h = [op(1, "set", 0, 1, "OK", 5), op(2, "get", 2, 3, None)]
    result = check(h)
    assert not result.ok and result.key == "k"


def test_concurrent_operations_may_go_either_way():
    h = [op(1, "set", 0, 10, "OK", 5), op(2, "get", 1, 2, None), op(3, "get", 3, 4, 5)]
    assert check(h).ok


def test_reads_cannot_travel_back_in_time():
    h = [op(1, "set", 0, 10, "OK", 5), op(2, "get", 1, 2, 5), op(3, "get", 3, 4, None)]
    assert not check(h).ok


def test_lost_update_is_caught():
    h = [op(1, "incr", 0, 2, 1), op(2, "incr", 1, 3, 1)]
    assert not check(h).ok


def test_indeterminate_write_may_or_may_not_happen():
    took_effect = [op(1, "set", 0, math.inf, UNKNOWN, 7), op(2, "get", 5, 6, 7)]
    never_happened = [op(1, "set", 0, math.inf, UNKNOWN, 7), op(2, "get", 5, 6, None)]
    assert check(took_effect).ok and check(never_happened).ok


def test_indeterminate_write_cannot_take_effect_before_it_was_sent():
    h = [op(2, "get", 0, 1, 7), op(1, "set", 5, math.inf, UNKNOWN, 7)]
    assert not check(h).ok


def test_keys_are_checked_independently():
    h = [op(1, "set", 0, 1, "OK", 1, key="a"), op(2, "get", 2, 3, 1, key="a"),
         op(3, "set", 0, 1, "OK", 2, key="b"), op(4, "get", 2, 3, None, key="b")]
    result = check(h)
    assert not result.ok and result.key == "b"


def test_delete_reports_whether_the_key_existed():
    assert check([op(1, "del", 0, 1, 0), op(1, "set", 2, 3, "OK", 1), op(1, "del", 4, 5, 1)]).ok
    assert not check([op(1, "set", 0, 1, "OK", 1), op(1, "del", 2, 3, 0)]).ok


def test_large_concurrent_history_is_fast():
    # 4 clients, 400 interleaved increments: the memoised search handles it quickly.
    h, value = [], 0
    for i in range(400):
        value += 1
        h.append(op(i % 4, "incr", i, i + 3.5, value))
    assert check(h).ok
