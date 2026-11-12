"""A real cluster node: the Replica on an asyncio event loop with TCP networking.

Each node listens on two ports:

- a client port speaking RESP, so redis-cli and redis-py work against it;
- a raft port for length-prefixed, checksummed JSON messages between nodes.

Clients talk to any node. Followers answer reads and writes with a redirect,

    -NOTLEADER 127.0.0.1:6381     (the leader's client address)
    -TRYAGAIN no leader elected   (during an election)

and minikv.raft.client.ClusterClient follows them. Admin commands:

    RAFT.STATUS                   role, term, leader, commit and applied index, data digest
    RAFT.PARTITION id [id ...]    drop all traffic to and from these nodes (needs --fault-injection)
    RAFT.HEAL                     undo RAFT.PARTITION
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from dataclasses import dataclass
from pathlib import Path

from minikv.raft.codec import CorruptFrame, frame, message_from_bytes, message_to_bytes, read_frames
from minikv.raft.core import Message
from minikv.raft.replica import OUTCOME_UNKNOWN, NotLeader, Replica
from minikv.raft.storage import FileStorage, MemoryStorage
from minikv.resp import OK, Error, ProtocolError, encode, parse_command

logger = logging.getLogger("minikv.raft")

MAX_PENDING = 2 * 1024 * 1024
PEER_QUEUE = 4096  # messages buffered per peer; beyond that they are dropped (Raft retries)


@dataclass(frozen=True)
class NodeAddress:
    host: str
    raft_port: int
    client_port: int


def parse_cluster(spec: str) -> dict[int, NodeAddress]:
    """'1=127.0.0.1:7001:6381,2=...' -> {1: NodeAddress(...), ...}"""
    cluster = {}
    for part in spec.split(","):
        node_id, _, addr = part.strip().partition("=")
        host, raft_port, client_port = addr.rsplit(":", 2)
        cluster[int(node_id)] = NodeAddress(host, int(raft_port), int(client_port))
    return cluster


class _PeerLink:
    """An outgoing connection to one peer, reconnecting with back-off. Messages are
    dropped while it is down: Raft re-sends whatever matters."""

    def __init__(self, node: ClusterNode, peer_id: int, addr: NodeAddress) -> None:
        self.node, self.peer_id, self.addr = node, peer_id, addr
        self.queue: asyncio.Queue[bytes] = asyncio.Queue(PEER_QUEUE)
        self.task = asyncio.create_task(self._run())

    def send(self, payload: bytes) -> None:
        try:
            self.queue.put_nowait(payload)
        except asyncio.QueueFull:
            pass

    async def _run(self) -> None:
        backoff = 0.05
        while True:
            try:
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(self.addr.host, self.addr.raft_port), timeout=1)
            except (OSError, asyncio.TimeoutError):
                self._clear()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 1.0)
                continue
            backoff = 0.05
            try:
                writer.write(frame(str(self.node.id).encode()))  # handshake: who we are
                while True:
                    batch = [await self.queue.get()]
                    while not self.queue.empty():
                        batch.append(self.queue.get_nowait())
                    writer.write(b"".join(frame(p) for p in batch))
                    await writer.drain()
            except (OSError, ConnectionError):
                pass
            finally:
                writer.close()
            self._clear()

    def _clear(self) -> None:
        while not self.queue.empty():
            self.queue.get_nowait()


class ClusterNode:
    def __init__(
        self,
        node_id: int,
        cluster: dict[int, NodeAddress],
        data_dir: str | Path | None = None,
        *,
        tick_ms: int = 10,
        election_ticks: int = 15,
        heartbeat_ticks: int = 3,
        snapshot_every: int = 10_000,
        request_timeout: float = 5.0,
        fault_injection: bool = False,
        bind_host: str | None = None,
    ) -> None:
        self.id = node_id
        self.cluster = cluster
        self.addr = cluster[node_id]
        self.bind_host = bind_host or self.addr.host
        self.tick_ms = tick_ms
        self.request_timeout = request_timeout
        self.fault_injection = fault_injection
        self.storage = FileStorage(data_dir) if data_dir else MemoryStorage()
        self.replica = Replica(node_id, list(cluster), self.storage, rng=random.Random(),
                               election_ticks=election_ticks, heartbeat_ticks=heartbeat_ticks,
                               snapshot_every=snapshot_every)
        self.blocked: set[int] = set()
        self._links: dict[int, _PeerLink] = {}
        self._servers: list[asyncio.base_events.Server] = []
        self._tasks: list[asyncio.Task] = []
        self._writers: set[asyncio.StreamWriter] = set()
        self._flush_scheduled = False
        self._next_session = 0

    @staticmethod
    def now_ms() -> int:
        return int(time.time() * 1000)

    async def start(self) -> None:
        self._servers.append(await asyncio.start_server(self._handle_peer, self.bind_host, self.addr.raft_port))
        self._servers.append(await asyncio.start_server(self._handle_client, self.bind_host, self.addr.client_port))
        self._links = {p: _PeerLink(self, p, a) for p, a in self.cluster.items() if p != self.id}
        self._tasks.append(asyncio.create_task(self._tick_loop()))
        self._flush()
        logger.info("node %d: clients on %s:%d, raft on %s:%d", self.id, self.bind_host,
                    self.addr.client_port, self.bind_host, self.addr.raft_port)

    async def stop(self) -> None:
        for task in self._tasks + [link.task for link in self._links.values()]:
            task.cancel()
        for server in self._servers:
            server.close()
        for writer in list(self._writers):
            writer.close()
        for server in self._servers:
            await server.wait_closed()
        self.replica.fail_pending()
        if isinstance(self.storage, FileStorage):
            self.storage.close()

    # --- driving Raft -------------------------------------------------------------

    async def _tick_loop(self) -> None:
        interval = self.tick_ms / 1000
        next_tick = time.monotonic()
        while True:
            next_tick += interval
            await asyncio.sleep(max(0.0, next_tick - time.monotonic()))
            self.replica.tick()
            self._flush()

    def _kick(self) -> None:
        """Process Raft output soon; several events in one loop iteration share one
        fsync and one batch of messages."""
        if not self._flush_scheduled:
            self._flush_scheduled = True
            asyncio.get_running_loop().call_soon(self._flush)

    def _flush(self) -> None:
        self._flush_scheduled = False
        for message in self.replica.process(self.now_ms()):
            self._send(message)

    def _send(self, message: Message) -> None:
        if message.to in self.blocked:
            return
        link = self._links.get(message.to)
        if link:
            link.send(message_to_bytes(message))

    async def _handle_peer(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        buffer = bytearray()
        peer: int | None = None
        try:
            while True:
                data = await reader.read(256 * 1024)
                if not data:
                    break
                buffer += data
                payloads, used = read_frames(buffer)
                del buffer[:used]
                for payload in payloads:
                    if peer is None:
                        peer = int(payload)
                        continue
                    if peer in self.blocked:
                        continue
                    message = message_from_bytes(payload)
                    if message.frm != peer or message.to != self.id:
                        raise ValueError("message with a forged sender or recipient")
                    self.replica.step(message)
                if payloads:
                    self._kick()
        except (ConnectionError, CorruptFrame, ValueError, KeyError) as exc:
            logger.warning("node %d: dropping peer connection: %s", self.id, exc)
        finally:
            self._writers.discard(writer)
            writer.close()

    # --- clients ------------------------------------------------------------------

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._writers.add(writer)
        self._next_session += 1
        session = {"protocol": 2, "id": self._next_session}
        buffer = bytearray()
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                buffer += data
                replies, close = [], False
                while buffer and not close:
                    try:
                        args, used = parse_command(buffer)
                    except ProtocolError as exc:
                        replies.append(encode(Error(f"ERR Protocol error: {exc}"), session["protocol"]))
                        close = True
                        break
                    if args is None:
                        break
                    del buffer[:used]
                    if not args:
                        continue
                    reply, close = await self._execute(args, session)
                    replies.append(encode(reply, session["protocol"]))
                if replies:
                    writer.write(b"".join(replies))
                    await writer.drain()
                if close or len(buffer) > MAX_PENDING:
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self._writers.discard(writer)
            writer.close()

    async def _execute(self, args: list[bytes], session: dict) -> tuple[object, bool]:
        name = args[0].upper()
        if name == b"QUIT":
            return OK, True
        if name == b"HELLO":
            return self._hello(args, session), False
        if name.startswith(b"RAFT."):
            return self._admin(name, args), False
        future: asyncio.Future = asyncio.get_running_loop().create_future()

        def done(value) -> None:
            if not future.done():
                future.set_result(value)

        self.replica.submit(args, self.now_ms(), done)
        self._kick()
        try:
            value = await asyncio.wait_for(asyncio.shield(future), self.request_timeout)
        except asyncio.TimeoutError:
            return OUTCOME_UNKNOWN, False
        if isinstance(value, NotLeader):
            if value.leader_id is None or value.leader_id not in self.cluster:
                return Error("TRYAGAIN no leader elected yet"), False
            leader = self.cluster[value.leader_id]
            return Error(f"NOTLEADER {leader.host}:{leader.client_port}"), False
        return value, False

    def _hello(self, args: list[bytes], session: dict) -> object:
        if len(args) > 2:
            return Error("ERR HELLO options (AUTH, SETNAME) are not supported")
        if len(args) == 2:
            if args[1] not in (b"2", b"3"):
                return Error("NOPROTO unsupported protocol version")
            session["protocol"] = int(args[1])
        role = "master" if self.replica.raft.is_leader else "replica"
        return {"server": "mini-redis", "version": "0.2.0", "proto": session["protocol"],
                "id": session["id"], "mode": "cluster", "role": role, "modules": []}

    def _admin(self, name: bytes, args: list[bytes]) -> object:
        if name == b"RAFT.STATUS":
            status = self.replica.status()
            return [x for k, v in status.items() for x in (k, "" if v is None else str(v))]
        if name in (b"RAFT.PARTITION", b"RAFT.HEAL"):
            if not self.fault_injection:
                return Error("ERR fault injection is disabled (start the node with --fault-injection)")
            if name == b"RAFT.HEAL":
                self.blocked.clear()
                return OK
            try:
                self.blocked = {int(a) for a in args[1:]} - {self.id}
            except ValueError:
                return Error("ERR node ids must be integers")
            return OK
        return Error(f"ERR unknown command '{name.decode('latin-1')}'")
