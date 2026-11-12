"""Durable Raft state: current term and vote, the log, and the latest snapshot.

Raft's safety depends on these surviving a crash. A node that forgets its vote can
vote twice in one term and let two leaders be elected (the simulator demonstrates
this with a deliberately broken storage, see tests/test_raft_sim.py).

Two implementations share one interface: MemoryStorage for the simulator, where a
"crash" throws away the node but keeps its storage, and FileStorage for real nodes.
"""

from __future__ import annotations

import json
import logging
import os
import zlib
from pathlib import Path

from minikv.raft.codec import (
    HEADER, entry_from_dict, entry_to_dict, frame, read_frames,
    snapshot_from_dict, snapshot_to_dict,
)
from minikv.raft.core import Entry, Snapshot

logger = logging.getLogger("minikv.raft.storage")


class CorruptLog(Exception):
    """The log is damaged somewhere other than a torn final record."""


class MemoryStorage:
    def __init__(self) -> None:
        self.term = 0
        self.vote: int | None = None
        self.snap: Snapshot | None = None
        self.entries: list[Entry] = []

    def load(self) -> tuple[int, int | None, Snapshot | None, list[Entry]]:
        return self.term, self.vote, self.snap, list(self.entries)

    def save_hard_state(self, term: int, vote: int | None) -> None:
        self.term, self.vote = term, vote

    def append(self, entries: list[Entry]) -> None:
        self.entries.extend(entries)

    def truncate_from(self, index: int) -> None:
        self.entries = [e for e in self.entries if e.index < index]

    def save_snapshot(self, snap: Snapshot) -> None:
        self.snap = snap
        self.entries = [e for e in self.entries if e.index > snap.index]

    def snapshot(self) -> Snapshot | None:
        return self.snap

    def sync(self) -> None:
        pass


def _write_atomically(path: Path, data: bytes) -> None:
    """Write to a temporary file, fsync it, then rename over the target: a crash
    leaves either the old file or the new one, never half of each."""
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)
    _fsync_dir(path.parent)


def _fsync_dir(directory: Path) -> None:
    if os.name != "posix":
        return  # Windows can't open a directory for fsync; the rename is still atomic
    fd = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


class FileStorage:
    """A directory holding three files:

    state     {"term", "vote"}, replaced atomically on every change
    snapshot  one checksummed frame, replaced atomically
    log       checksummed frames, one per entry, appended; rewritten atomically on
              the rare truncation (a follower dropping an old leader's suffix) or
              compaction after a snapshot
    """

    def __init__(self, directory: str | Path) -> None:
        self.dir = Path(directory)
        self.dir.mkdir(parents=True, exist_ok=True)
        self._state_path = self.dir / "state"
        self._snap_path = self.dir / "snapshot"
        self._log_path = self.dir / "log"
        self.term, self.vote = 0, None
        self.snap: Snapshot | None = None
        self.entries: list[Entry] = []
        self._load_files()
        self._log = open(self._log_path, "ab")
        self._dirty = False

    def _load_files(self) -> None:
        if self._state_path.exists():
            state = json.loads(self._state_path.read_bytes())
            self.term, self.vote = int(state["term"]), state["vote"]
        if self._snap_path.exists():
            payloads, _ = read_frames(self._snap_path.read_bytes())
            if len(payloads) != 1:
                raise CorruptLog("the snapshot file is damaged")
            self.snap = snapshot_from_dict(json.loads(payloads[0]))
        if self._log_path.exists():
            self.entries = self._read_log()
        if self.snap:
            # A crash between writing a snapshot and rewriting the log leaves entries
            # the snapshot already covers; they are simply dropped.
            self.entries = [e for e in self.entries if e.index > self.snap.index]
        first = self.snap.index + 1 if self.snap else 1
        for offset, entry in enumerate(self.entries):
            if entry.index != first + offset:
                raise CorruptLog(f"log has a gap: expected index {first + offset}, found {entry.index}")

    def _read_log(self) -> list[Entry]:
        data = self._log_path.read_bytes()
        entries, pos = [], 0
        while len(data) - pos >= HEADER.size:
            length, crc = HEADER.unpack_from(data, pos)
            end = pos + HEADER.size + length
            if end > len(data):
                break  # torn final record
            payload = data[pos + HEADER.size:end]
            if zlib.crc32(payload) != crc:
                if end == len(data):
                    break  # the final record was only partly written
                raise CorruptLog(f"checksum mismatch in the middle of the log, at byte {pos}")
            entries.append(entry_from_dict(json.loads(payload)))
            pos = end
        if pos < len(data):
            # The final record was torn by a crash mid-write. It was never fsynced, so
            # it was never acknowledged to anyone: dropping it is safe.
            logger.warning("raft log ends with an incomplete record; truncating %d bytes", len(data) - pos)
            with open(self._log_path, "r+b") as f:
                f.truncate(pos)
        return entries

    def load(self) -> tuple[int, int | None, Snapshot | None, list[Entry]]:
        return self.term, self.vote, self.snap, list(self.entries)

    def save_hard_state(self, term: int, vote: int | None) -> None:
        self.term, self.vote = term, vote
        _write_atomically(self._state_path, json.dumps({"term": term, "vote": vote}).encode())

    def append(self, entries: list[Entry]) -> None:
        for entry in entries:
            self._log.write(frame(json.dumps(entry_to_dict(entry), separators=(",", ":")).encode()))
        self.entries.extend(entries)
        self._dirty = True

    def truncate_from(self, index: int) -> None:
        self.entries = [e for e in self.entries if e.index < index]
        self._rewrite_log()

    def save_snapshot(self, snap: Snapshot) -> None:
        _write_atomically(self._snap_path, frame(json.dumps(snapshot_to_dict(snap)).encode()))
        self.snap = snap
        self.entries = [e for e in self.entries if e.index > snap.index]
        self._rewrite_log()

    def snapshot(self) -> Snapshot | None:
        return self.snap

    def _rewrite_log(self) -> None:
        self._log.close()
        data = b"".join(frame(json.dumps(entry_to_dict(e), separators=(",", ":")).encode()) for e in self.entries)
        _write_atomically(self._log_path, data)
        self._log = open(self._log_path, "ab")
        self._dirty = False

    def sync(self) -> None:
        """Make every append durable. Called before any message is sent."""
        if self._dirty:
            self._log.flush()
            os.fsync(self._log.fileno())
            self._dirty = False

    def close(self) -> None:
        if not self._log.closed:
            self.sync()
            self._log.close()

