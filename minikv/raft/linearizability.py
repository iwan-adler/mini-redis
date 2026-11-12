"""A linearizability checker for key-value histories.

A history is a list of operations, each with the time the client sent it, the time
the reply came back and the reply. It is linearizable if every operation can be
given a single instant between its call and its return such that, replayed in that
order against a plain dictionary, every reply matches. That is the guarantee a
single Redis server gives, and the one a replicated cluster must keep.

The search is the Wing & Gong algorithm with Lowe's memoisation (the approach used
by Knossos and Porcupine). Keys are independent, so each key's sub-history is
checked on its own, which keeps the search small.

Operations whose reply never arrived (a timeout, a crashed node) are "indeterminate":
they may have taken effect at any point after they were sent, or never. Their
return time is treated as infinity and the checker may leave them out.
"""

from __future__ import annotations

import math
from collections import defaultdict
from dataclasses import dataclass

# A reply that never arrived.
UNKNOWN = object()


@dataclass
class Op:
    client: int
    kind: str              # "get", "set", "incr" or "del"
    key: str
    value: int | None      # argument of set
    call: float
    ret: float             # math.inf if the reply never arrived
    result: object         # reply value, or UNKNOWN

    @property
    def indeterminate(self) -> bool:
        return self.result is UNKNOWN


def step(state: int | None, op: Op) -> tuple[bool, int | None]:
    """Apply `op` to the model. Returns (reply matches?, new state)."""
    if op.kind == "get":
        return op.indeterminate or op.result == state, state
    if op.kind == "set":
        return True, op.value
    if op.kind == "incr":
        new = (state or 0) + 1
        return op.indeterminate or op.result == new, new
    if op.kind == "del":
        return op.indeterminate or op.result == (0 if state is None else 1), None
    raise ValueError(f"unknown operation {op.kind}")


@dataclass
class Result:
    ok: bool
    key: str | None = None         # the first key that is not linearizable
    ops: list[Op] | None = None    # that key's operations, for a report
    longest_prefix: list[Op] | None = None  # the deepest partial order the search found


def check(history: list[Op]) -> Result:
    by_key: dict[str, list[Op]] = defaultdict(list)
    for op in history:
        if op.indeterminate and op.kind == "get":
            continue  # a read with no reply can't constrain anything
        by_key[op.key].append(op)
    for key in sorted(by_key):
        ok, longest = _check_key(by_key[key])
        if not ok:
            return Result(False, key, by_key[key], longest)
    return Result(True)


def _check_key(ops: list[Op]) -> tuple[bool, list[Op]]:
    # Doubly linked list of call and return events in time order. At equal times,
    # calls come first: two operations touching at an instant are concurrent.
    events = []
    for i, op in enumerate(ops):
        events.append((op.call, 0, i))
        events.append((op.ret if not op.indeterminate else math.inf, 1, i))
    events.sort()
    n = len(events)
    # Node k (0..n-1) is events[k]; node n is the head sentinel.
    head = n
    nxt: list[int | None] = [k + 1 if k + 1 < n else None for k in range(n)] + [0 if n else None]
    prv: list[int | None] = [k - 1 if k > 0 else head for k in range(n)] + [None]
    ret_node = {}
    for k, (_, kind, i) in enumerate(events):
        if kind == 1:
            ret_node[i] = k

    must_linearize = {i for i, op in enumerate(ops) if not op.indeterminate}
    remaining = len(must_linearize)

    def lift(call_k: int) -> None:
        i = events[call_k][2]
        for k in (call_k, ret_node[i]):
            p, q = prv[k], nxt[k]
            nxt[p] = q
            if q is not None:
                prv[q] = p

    def unlift(call_k: int) -> None:
        i = events[call_k][2]
        for k in (ret_node[i], call_k):
            p, q = prv[k], nxt[k]
            nxt[p] = k
            if q is not None:
                prv[q] = k

    state: int | None = None
    linearized = 0  # bitset of operation numbers
    cache: set[tuple[int, int | None]] = set()
    stack: list[tuple[int, int | None]] = []  # (call node, state before it)
    longest: list[int] = []
    entry = nxt[head]
    while remaining:
        if entry is None:
            # Only indeterminate operations are left in front of us; they can be dropped.
            return True, []
        _, kind, i = events[entry]
        if kind == 0:
            ok, new_state = step(state, ops[i])
            bit = linearized | (1 << i)
            if ok and (bit, new_state) not in cache:
                cache.add((bit, new_state))
                stack.append((entry, state))
                state, linearized = new_state, bit
                if i in must_linearize:
                    remaining -= 1
                lift(entry)
                if len(stack) > len(longest):
                    longest = [events[k][2] for k, _ in stack]
                entry = nxt[head]
            else:
                entry = nxt[entry]
        else:
            # A return event: its operation should already have been linearized. Undo.
            if not stack:
                return False, [ops[j] for j in longest]
            call_k, state = stack.pop()
            j = events[call_k][2]
            linearized &= ~(1 << j)
            if j in must_linearize:
                remaining += 1
            unlift(call_k)
            entry = nxt[call_k]
    return True, []
