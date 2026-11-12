"""In-memory key/value data with Redis-style expiry.

Expiry works two ways, as in Redis:
- lazily: a key is checked (and deleted if expired) whenever it's accessed;
- actively: a background task samples keys that have a TTL and deletes expired
  ones, so memory is reclaimed even for keys nobody reads again.
"""

from __future__ import annotations

import fnmatch
import random
import time
from collections.abc import Callable


class Store:
    """`clock` returns seconds; `ms_clock`, if given, returns milliseconds and wins.

    `lazy_expire=False` makes reads side-effect free: an expired key looks absent but
    stays in memory until purge_expired(). The replicated state machine needs this,
    because a read evaluated at one node's wall-clock time must never change the state
    that later log entries are applied to (see minikv/raft/statemachine.py).
    """

    def __init__(
        self,
        clock: Callable[[], float] = time.time,
        ms_clock: Callable[[], int] | None = None,
        lazy_expire: bool = True,
    ) -> None:
        self._clock = clock
        self._ms_clock = ms_clock
        self._lazy_expire = lazy_expire
        self._data: dict[bytes, bytes] = {}
        self._expires: dict[bytes, int] = {}  # key -> absolute expiry in Unix milliseconds

    def now_ms(self) -> int:
        if self._ms_clock is not None:
            return self._ms_clock()
        return int(self._clock() * 1000)

    def _alive(self, key: bytes) -> bool:
        deadline = self._expires.get(key)
        if deadline is not None and deadline <= self.now_ms():
            if self._lazy_expire:
                self._remove(key)
            return False
        return key in self._data

    def _remove(self, key: bytes) -> None:
        self._data.pop(key, None)
        self._expires.pop(key, None)

    # --- strings --------------------------------------------------------------------

    def get(self, key: bytes) -> bytes | None:
        return self._data[key] if self._alive(key) else None

    def set(self, key: bytes, value: bytes, expire_at_ms: int | None = None, keep_ttl: bool = False) -> None:
        if keep_ttl and not self._alive(key):
            self._expires.pop(key, None)  # an expired key's old deadline must not carry over
        self._data[key] = value
        if expire_at_ms is not None:
            self._expires[key] = expire_at_ms
        elif not keep_ttl:
            self._expires.pop(key, None)  # a plain SET clears any previous TTL, as in Redis

    def exists(self, key: bytes) -> bool:
        return self._alive(key)

    def delete(self, key: bytes) -> bool:
        if not self._alive(key):
            return False
        self._remove(key)
        return True

    # --- expiry ---------------------------------------------------------------------

    def expire_at(self, key: bytes, deadline_ms: int) -> bool:
        if not self._alive(key):
            return False
        if deadline_ms <= self.now_ms():
            self._remove(key)  # an expiry in the past deletes the key immediately
        else:
            self._expires[key] = deadline_ms
        return True

    def ttl_ms(self, key: bytes) -> int:
        """-2 if the key doesn't exist, -1 if it has no TTL, else milliseconds left."""
        if not self._alive(key):
            return -2
        deadline = self._expires.get(key)
        return -1 if deadline is None else max(0, deadline - self.now_ms())

    def expiry_of(self, key: bytes) -> int | None:
        return self._expires.get(key) if self._alive(key) else None

    def persist(self, key: bytes) -> bool:
        return self._alive(key) and self._expires.pop(key, None) is not None

    def purge_expired(self) -> int:
        """Delete every key whose deadline has passed. Returns how many were deleted."""
        now = self.now_ms()
        expired = [k for k, deadline in self._expires.items() if deadline <= now]
        for key in expired:
            self._remove(key)
        return len(expired)

    def active_expire_cycle(self, sample_size: int = 20, rng: random.Random | None = None) -> int:
        """Redis's algorithm: sample keys with a TTL, delete the expired ones, and repeat
        while more than 25% of the sample was expired. Returns how many keys were deleted."""
        rng = rng or random
        removed = 0
        while self._expires:
            sample = rng.sample(list(self._expires), min(sample_size, len(self._expires)))
            now = self.now_ms()
            expired = [k for k in sample if self._expires[k] <= now]
            for key in expired:
                self._remove(key)
            removed += len(expired)
            if len(expired) <= len(sample) // 4:
                break
        return removed

    # --- whole keyspace -------------------------------------------------------------------

    def keys(self, pattern: bytes) -> list[bytes]:
        glob = pattern.decode("latin-1")
        return [k for k in list(self._data) if self._alive(k) and fnmatch.fnmatchcase(k.decode("latin-1"), glob)]

    def size(self) -> int:
        return sum(1 for k in list(self._data) if self._alive(k))

    def flush(self) -> None:
        self._data.clear()
        self._expires.clear()

    def items(self) -> list[tuple[bytes, bytes, int | None]]:
        """Live keys with their values and absolute expiry (used to rewrite the AOF)."""
        return [(k, self._data[k], self._expires.get(k)) for k in list(self._data) if self._alive(k)]
