"""A small cluster-aware client that follows leader redirects.

It is deliberately strict about what it knows. A write that was sent but got no
reply (a timeout, a dropped connection) raises OutcomeUnknown instead of being
retried: blindly re-sending `INCR` could apply it twice. Errors that prove nothing
happened (NOTLEADER, TRYAGAIN, a refused connection) are retried automatically.
"""

from __future__ import annotations

import random
import socket
import time

from minikv.raft.statemachine import classify

Address = tuple[str, int]


class ClusterError(Exception):
    """An error reply from the server (e.g. a wrong-type argument)."""


class OutcomeUnknown(Exception):
    """The request may or may not have been applied."""


class Unavailable(Exception):
    """No leader could be reached before the deadline; nothing was applied."""


def _encode(args: list[bytes]) -> bytes:
    out = [b"*%d\r\n" % len(args)]
    for a in args:
        out.append(b"$%d\r\n%s\r\n" % (len(a), a))
    return b"".join(out)


class _Reader:
    def __init__(self, sock: socket.socket) -> None:
        self.sock, self.buf = sock, b""

    def _line(self) -> bytes:
        while b"\r\n" not in self.buf:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            self.buf += chunk
        line, self.buf = self.buf.split(b"\r\n", 1)
        return line

    def _exact(self, n: int) -> bytes:
        while len(self.buf) < n + 2:
            chunk = self.sock.recv(65536)
            if not chunk:
                raise ConnectionError("connection closed")
            self.buf += chunk
        data, self.buf = self.buf[:n], self.buf[n + 2:]
        return data

    def reply(self):
        line = self._line()
        kind, rest = line[:1], line[1:]
        if kind == b"+":
            return rest.decode()
        if kind == b"-":
            return ClusterError(rest.decode())
        if kind == b":":
            return int(rest)
        if kind == b"$":
            n = int(rest)
            return None if n < 0 else self._exact(n)
        if kind == b"*":
            n = int(rest)
            return None if n < 0 else [self.reply() for _ in range(n)]
        if kind == b"_":
            return None
        raise ConnectionError(f"unexpected reply type {kind!r}")


class ClusterClient:
    def __init__(self, nodes: list[Address], timeout: float = 2.0, deadline: float = 10.0) -> None:
        self.nodes = list(nodes)
        self.timeout = timeout          # per request
        self.deadline = deadline        # total time spent retrying redirects and elections
        self.leader: Address | None = None
        self._conns: dict[Address, tuple[socket.socket, _Reader]] = {}
        self.redirects = 0

    def close(self) -> None:
        for sock, _ in self._conns.values():
            sock.close()
        self._conns.clear()

    def _conn(self, addr: Address):
        if addr not in self._conns:
            sock = socket.create_connection(addr, timeout=self.timeout)
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self._conns[addr] = (sock, _Reader(sock))
        return self._conns[addr]

    def _drop(self, addr: Address) -> None:
        conn = self._conns.pop(addr, None)
        if conn:
            conn[0].close()

    def execute(self, *args):
        argv = [a if isinstance(a, bytes) else str(a).encode() for a in args]
        is_write = classify(argv) == "write"
        give_up = time.monotonic() + self.deadline
        target = self.leader or random.choice(self.nodes)
        backoff = 0.02
        while time.monotonic() < give_up:
            try:
                sock, reader = self._conn(target)
            except OSError:
                target = self._other(target)
                time.sleep(backoff)
                backoff = min(backoff * 2, 0.5)
                continue
            try:
                sock.sendall(_encode(argv))
                reply = reader.reply()
            except (OSError, ConnectionError) as exc:
                self._drop(target)
                if self.leader == target:
                    self.leader = None
                if is_write:
                    raise OutcomeUnknown(f"no reply from {target[0]}:{target[1]}: {exc}") from exc
                target = self._other(target)  # a read changes nothing: just retry elsewhere
                continue
            if isinstance(reply, ClusterError):
                text = str(reply)
                if text.startswith("NOTLEADER "):
                    host, _, port = text.split()[1].rpartition(":")
                    target = self.leader = (host, int(port))
                    self.redirects += 1
                    continue
                if text.startswith("TRYAGAIN"):
                    self.leader = None
                    target = self._other(target)
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 0.5)
                    continue
                if text.startswith("UNKNOWN"):
                    raise OutcomeUnknown(text)
                raise reply
            self.leader = target
            return reply
        raise Unavailable("no leader reachable before the deadline")

    def _other(self, addr: Address) -> Address:
        options = [n for n in self.nodes if n != addr] or self.nodes
        return random.choice(options)
