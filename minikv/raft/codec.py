"""Wire and disk encoding for Raft entries, snapshots and messages.

Everything is JSON with binary values in base64: easy to inspect, and nothing is
ever unpickled from the network. Frames are length-prefixed with a CRC32 so a torn
write at the end of a file (a crash mid-append) is detected and cut off.
"""

from __future__ import annotations

import base64
import json
import struct
import zlib
from dataclasses import fields

from minikv.raft.core import Entry, Message, Snapshot

HEADER = struct.Struct(">II")  # payload length, CRC32 of the payload
MAX_FRAME = 64 * 1024 * 1024


class CorruptFrame(Exception):
    """A frame whose checksum doesn't match its payload."""


def _b64(data: bytes) -> str:
    return base64.b64encode(data).decode("ascii")


def _unb64(text: str) -> bytes:
    return base64.b64decode(text.encode("ascii"), validate=True)


def entry_to_dict(entry: Entry) -> dict:
    args = None if entry.args is None else [_b64(a) for a in entry.args]
    return {"t": entry.term, "i": entry.index, "a": args, "ms": entry.time_ms}


def entry_from_dict(d: dict) -> Entry:
    args = None if d["a"] is None else tuple(_unb64(a) for a in d["a"])
    return Entry(int(d["t"]), int(d["i"]), args, int(d["ms"]))


def snapshot_to_dict(snap: Snapshot) -> dict:
    return {"index": snap.index, "term": snap.term, "data": _b64(snap.data)}


def snapshot_from_dict(d: dict) -> Snapshot:
    return Snapshot(int(d["index"]), int(d["term"]), _unb64(d["data"]))


_MESSAGE_FIELDS = [f.name for f in fields(Message)]


def message_to_bytes(m: Message) -> bytes:
    d = {}
    for name in _MESSAGE_FIELDS:
        value = getattr(m, name)
        if name == "entries":
            if value:
                d[name] = [entry_to_dict(e) for e in value]
        elif name == "snapshot":
            if value is not None:
                d[name] = snapshot_to_dict(value)
        elif value or name in ("type", "frm", "to", "term"):
            d[name] = value  # zero/false fields are left out to keep heartbeats small
    return json.dumps(d, separators=(",", ":")).encode()


def message_from_bytes(data: bytes) -> Message:
    d = json.loads(data)
    if not isinstance(d, dict) or set(d) - set(_MESSAGE_FIELDS):
        raise ValueError("not a raft message")
    if "entries" in d:
        d["entries"] = tuple(entry_from_dict(e) for e in d["entries"])
    if "snapshot" in d:
        d["snapshot"] = snapshot_from_dict(d["snapshot"])
    return Message(**d)


def frame(payload: bytes) -> bytes:
    return HEADER.pack(len(payload), zlib.crc32(payload)) + payload


def read_frames(data: bytes) -> tuple[list[bytes], int]:
    """Split a buffer into complete frames. Returns (payloads, bytes used); a partial
    frame at the end is left unread. Raises CorruptFrame on a bad checksum."""
    payloads, pos = [], 0
    while len(data) - pos >= HEADER.size:
        length, crc = HEADER.unpack_from(data, pos)
        if length > MAX_FRAME:
            raise CorruptFrame(f"frame of {length} bytes at {pos}")
        end = pos + HEADER.size + length
        if end > len(data):
            break
        payload = bytes(data[pos + HEADER.size:end])
        if zlib.crc32(payload) != crc:
            raise CorruptFrame(f"checksum mismatch in the frame at byte {pos}")
        payloads.append(payload)
        pos = end
    return payloads, pos
