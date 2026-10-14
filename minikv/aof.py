"""Append-only file (AOF) persistence.

Every write command is appended to the file in RESP format. On start-up the
file is replayed to rebuild the data. If the server crashed half-way through
writing the last command, that incomplete tail is cut off (like Redis's
aof-load-truncated) and everything before it is kept.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

from minikv import commands
from minikv.resp import ProtocolError, encode_command, parse_command
from minikv.store import Store

logger = logging.getLogger("minikv.aof")

FSYNC_POLICIES = ("always", "everysec", "no")


class CorruptAOF(Exception):
    """The file contains bytes that aren't valid commands (not just a truncated tail)."""


class AppendOnlyFile:
    def __init__(self, path: str | Path, fsync: str = "everysec") -> None:
        if fsync not in FSYNC_POLICIES:
            raise ValueError(f"fsync must be one of {FSYNC_POLICIES}")
        self.path = Path(path)
        self.fsync = fsync
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._file = open(self.path, "ab")
        self._dirty = False

    def append(self, args: list[bytes]) -> None:
        self._file.write(encode_command(args))
        if self.fsync == "always":
            self.sync()  # strongest durability: every acknowledged write is on disk
        else:
            self._dirty = True

    def sync(self) -> None:
        """Flush Python's buffer and ask the OS to write to disk."""
        self._file.flush()
        if self.fsync != "no":
            os.fsync(self._file.fileno())
        self._dirty = False

    def sync_if_dirty(self) -> None:
        if self._dirty:
            self.sync()

    def close(self) -> None:
        if not self._file.closed:
            self._file.flush()
            os.fsync(self._file.fileno())
            self._file.close()


def load(path: str | Path, store: Store) -> tuple[int, int]:
    """Replay the AOF into `store`. Returns (commands replayed, bytes truncated)."""
    path = Path(path)
    if not path.exists():
        return 0, 0
    data = path.read_bytes()
    pos = replayed = 0
    while pos < len(data):
        try:
            args, used = parse_command(data[pos:])
        except ProtocolError as exc:
            raise CorruptAOF(f"invalid data at byte {pos}: {exc}") from exc
        if args is None:
            break  # incomplete final command: a crash mid-write
        reply, _ = commands.execute(store, args)
        if isinstance(reply, commands.Error):
            raise CorruptAOF(f"command at byte {pos} failed on replay: {reply}")
        pos += used
        replayed += 1
    truncated = len(data) - pos
    if truncated:
        logger.warning("AOF ends with an incomplete command; truncating %d bytes", truncated)
        with open(path, "r+b") as f:
            f.truncate(pos)
    return replayed, truncated


def rewrite(path: str | Path, store: Store) -> int:
    """Compact the AOF to one SET per live key. Written to a temp file and then
    atomically renamed, so a crash during rewrite never loses the old file."""
    path = Path(path)
    tmp = path.with_suffix(path.suffix + ".rewrite")
    entries = store.items()
    with open(tmp, "wb") as f:
        for key, value, expire_at in entries:
            args = [b"SET", key, value]
            if expire_at is not None:
                args += [b"PXAT", str(expire_at).encode()]
            f.write(encode_command(args))
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    return len(entries)
