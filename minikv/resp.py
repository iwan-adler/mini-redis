"""RESP2, the Redis serialisation protocol: incremental parsing and encoding.

Clients send commands as arrays of bulk strings:
    *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
or, from a terminal (telnet, nc), as a plain "inline" line: SET key value\r\n

TCP delivers a byte stream, not messages, so the parser works on a growing
buffer and reports how many bytes a complete command used; partial commands
wait for more data, and several pipelined commands are returned one by one.
"""

from __future__ import annotations

MAX_BULK = 1024 * 1024      # 1 MB per argument (real Redis allows 512 MB)
MAX_ARGS = 1024 * 1024
MAX_INLINE = 64 * 1024


class ProtocolError(Exception):
    """The client sent bytes that can't be RESP; the connection must be closed."""


class Error(str):
    """A RESP error reply such as 'ERR unknown command'."""


class SimpleString(str):
    """A RESP simple string such as 'OK' or 'PONG' (as opposed to a bulk string)."""


OK = SimpleString("OK")


def parse_command(buffer: bytes | bytearray) -> tuple[list[bytes] | None, int]:
    """Parse one command from the start of `buffer`.

    Returns (arguments, bytes_consumed), or (None, 0) if more data is needed.
    Raises ProtocolError for malformed input.
    """
    if not buffer:
        return None, 0
    if buffer[0:1] != b"*":
        return _parse_inline(buffer)

    line_end = buffer.find(b"\r\n")
    if line_end == -1:
        if len(buffer) > MAX_INLINE:
            raise ProtocolError("invalid multibulk length")
        return None, 0
    count = _int(buffer[1:line_end], "multibulk length")
    if count > MAX_ARGS:
        raise ProtocolError("invalid multibulk length")
    pos = line_end + 2
    args: list[bytes] = []
    for _ in range(max(count, 0)):
        if pos >= len(buffer):
            return None, 0
        if buffer[pos:pos + 1] != b"$":
            raise ProtocolError(f"expected '$', got {bytes(buffer[pos:pos + 1])!r}")
        line_end = buffer.find(b"\r\n", pos)
        if line_end == -1:
            return None, 0
        size = _int(buffer[pos + 1:line_end], "bulk length")
        if size < 0 or size > MAX_BULK:
            raise ProtocolError("invalid bulk length")
        start = line_end + 2
        end = start + size
        if len(buffer) < end + 2:
            return None, 0
        if buffer[end:end + 2] != b"\r\n":
            raise ProtocolError("bulk string not terminated by CRLF")
        args.append(bytes(buffer[start:end]))
        pos = end + 2
    return args, pos


def _parse_inline(buffer: bytes | bytearray) -> tuple[list[bytes] | None, int]:
    line_end = buffer.find(b"\n")
    if line_end == -1:
        if len(buffer) > MAX_INLINE:
            raise ProtocolError("too big inline request")
        return None, 0
    line = bytes(buffer[:line_end]).rstrip(b"\r")
    return line.split(), line_end + 1


def _int(raw: bytes | bytearray, what: str) -> int:
    try:
        return int(raw)
    except ValueError:
        raise ProtocolError(f"invalid {what}") from None


def encode(value, protocol: int = 2) -> bytes:
    """Python value -> RESP reply. `protocol` is 2 or 3, negotiated per connection with HELLO.

    For the types used here RESP3 differs in two places: null is `_` instead of `$-1`,
    and maps (dicts) get their own `%` type instead of a flat key/value array.
    """
    if value is None:
        return b"_\r\n" if protocol == 3 else b"$-1\r\n"  # e.g. GET on a missing key
    if isinstance(value, Error):
        return b"-" + value.encode() + b"\r\n"
    if isinstance(value, SimpleString):
        return b"+" + value.encode() + b"\r\n"
    if isinstance(value, bool):
        raise TypeError("booleans have no RESP2 encoding")
    if isinstance(value, int):
        return b":" + str(value).encode() + b"\r\n"
    if isinstance(value, str):
        value = value.encode()
    if isinstance(value, (bytes, bytearray)):
        return b"$" + str(len(value)).encode() + b"\r\n" + bytes(value) + b"\r\n"
    if isinstance(value, list):
        return b"*" + str(len(value)).encode() + b"\r\n" + b"".join(encode(v, protocol) for v in value)
    if isinstance(value, dict):
        pairs = b"".join(encode(k, protocol) + encode(v, protocol) for k, v in value.items())
        if protocol == 3:
            return b"%" + str(len(value)).encode() + b"\r\n" + pairs
        return b"*" + str(2 * len(value)).encode() + b"\r\n" + pairs
    raise TypeError(f"cannot encode {type(value).__name__}")


def encode_command(args: list[bytes]) -> bytes:
    """Encode a command the way clients send it (used for the append-only file)."""
    return encode(list(args))
