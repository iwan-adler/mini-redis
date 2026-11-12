"""The replicated key-value state machine.

Every replica applies the same log entries in the same order and must end up in
exactly the same state. The one source of non-determinism in mini-redis is time:
key expiry. So time is part of the log. The leader stamps each entry with its clock,
and replicas apply it with that stamp as "now", never their own clock:

- `EXPIRE k 10` applied on three machines sets the same absolute deadline on all three;
- `INCR k` on a key whose TTL ends during replication sees the same answer everywhere.

The state machine's clock only moves forward (it is the largest stamp applied so far),
so a new leader whose wall clock is slightly behind can't make keys come back to life.

Reads are evaluated at max(state machine clock, the leader's wall clock), so a key whose
TTL has passed reads as gone even if no write has advanced the log clock. Reads never
delete anything (the store runs with lazy_expire=False): a deletion triggered by one
node's wall clock would make its state differ from the others'. Expired keys are
purged at deterministic points instead.
"""

from __future__ import annotations

import base64
import hashlib
import json

from minikv import commands
from minikv.raft.core import Entry
from minikv.store import Store

PURGE_EVERY = 128  # entries between sweeps of expired keys

READ_COMMANDS = {"get", "exists", "ttl", "pttl", "keys", "dbsize"}
WRITE_COMMANDS = {"set", "incr", "decr", "incrby", "decrby", "del", "expire", "pexpire",
                  "pexpireat", "persist", "flushdb"}


def classify(args: list[bytes]) -> str:
    """'read', 'write' or 'local' (answered by any node without consensus: PING, ECHO,
    unknown commands and wrong-arity errors)."""
    if not args:
        return "local"
    name = args[0].decode("latin-1").lower()
    spec = commands.COMMANDS.get(name)
    if spec is None:
        return "local"
    min_args, max_args, _ = spec
    if len(args) - 1 < min_args or (max_args is not None and len(args) - 1 > max_args):
        return "local"  # an arity error: no need to replicate it
    if name in READ_COMMANDS:
        return "read"
    if name in WRITE_COMMANDS:
        return "write"
    return "local"


class KVStateMachine:
    def __init__(self) -> None:
        self.clock_ms = 0
        self.applied = 0
        self._read_clock: int | None = None
        self.store = Store(ms_clock=self._now, lazy_expire=False)

    def _now(self) -> int:
        return self._read_clock if self._read_clock is not None else self.clock_ms

    def apply(self, entry: Entry) -> object:
        self.clock_ms = max(self.clock_ms, entry.time_ms)
        self.applied = entry.index
        if entry.args is None:
            return None
        reply, _ = commands.execute(self.store, list(entry.args))
        if entry.index % PURGE_EVERY == 0:
            self.store.purge_expired()
        return reply

    def read(self, args: list[bytes], now_ms: int) -> object:
        self._read_clock = max(self.clock_ms, now_ms)
        try:
            return commands.execute(self.store, args)[0]
        finally:
            self._read_clock = None

    def execute_local(self, args: list[bytes]) -> object:
        return commands.execute(self.store, args)[0]

    def snapshot(self) -> bytes:
        self.store.purge_expired()
        items = [[base64.b64encode(k).decode(), base64.b64encode(v).decode(), exp]
                 for k, v, exp in sorted(self.store.items())]
        return json.dumps({"clock_ms": self.clock_ms, "applied": self.applied, "items": items}).encode()

    def restore(self, data: bytes) -> None:
        d = json.loads(data)
        self.clock_ms, self.applied = d["clock_ms"], d["applied"]
        self.store.flush()
        for k, v, exp in d["items"]:
            self.store.set(base64.b64decode(k), base64.b64decode(v), expire_at_ms=exp)

    def digest(self) -> str:
        """A hash of the live data, to check that replicas agree."""
        h = hashlib.sha256()
        for k, v, exp in sorted(self.store.items()):
            h.update(json.dumps([k.hex(), v.hex(), exp]).encode())
        return h.hexdigest()[:16]
