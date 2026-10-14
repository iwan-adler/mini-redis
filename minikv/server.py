"""asyncio TCP server.

All commands run on one event loop thread, one at a time, exactly like Redis.
That makes every command atomic without any locks: two clients running INCR on
the same key can never lose an update. The cost is that one slow command blocks
everyone, so every command here is O(1) or a single pass (KEYS is the exception,
as in Redis, which warns against it in production).
"""

from __future__ import annotations

import asyncio
import logging
import threading
from pathlib import Path

from minikv import aof as aof_module
from minikv import commands
from minikv.resp import Error, ProtocolError, encode, parse_command
from minikv.store import Store

logger = logging.getLogger("minikv")

MAX_PENDING = 2 * 1024 * 1024  # max unparsed bytes per client before we hang up


class Server:
    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6380,
        aof_path: str | Path | None = None,
        fsync: str = "everysec",
        store: Store | None = None,
        expire_interval: float = 0.1,
    ) -> None:
        self.host, self.port = host, port
        self.store = store or Store()
        self.expire_interval = expire_interval
        self.aof: aof_module.AppendOnlyFile | None = None
        if aof_path is not None:
            replayed, truncated = aof_module.load(aof_path, self.store)
            logger.info("AOF: replayed %d commands (%d bytes truncated), %d keys",
                        replayed, truncated, self.store.size())
            self.aof = aof_module.AppendOnlyFile(aof_path, fsync=fsync)
        self._server: asyncio.base_events.Server | None = None
        self._tasks: list[asyncio.Task] = []
        self._writers: set[asyncio.StreamWriter] = set()
        self.clients = 0

    async def start(self) -> int:
        # A hostname like "localhost" may bind several sockets (127.0.0.1 and ::1).
        self._server = await asyncio.start_server(self._handle_client, self.host, self.port)
        self.port = self._server.sockets[0].getsockname()[1]
        self._tasks = [asyncio.create_task(self._expire_loop()), asyncio.create_task(self._fsync_loop())]
        logger.info("listening on %s:%d", self.host, self.port)
        return self.port

    async def stop(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._server:
            self._server.close()
            # Since Python 3.12, wait_closed() waits for every client connection to end,
            # so disconnect clients ourselves rather than waiting on them forever.
            for writer in list(self._writers):
                writer.close()
            await self._server.wait_closed()
        if self.aof:
            self.aof.close()

    async def _expire_loop(self) -> None:
        while True:
            await asyncio.sleep(self.expire_interval)
            self.store.active_expire_cycle()

    async def _fsync_loop(self) -> None:
        while True:
            await asyncio.sleep(1)
            if self.aof and self.aof.fsync == "everysec":
                self.aof.sync_if_dirty()  # at most ~1 s of writes can be lost in a crash

    async def _handle_client(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.clients += 1
        self._next_id = getattr(self, "_next_id", 0) + 1
        session = {"protocol": 2, "id": self._next_id}  # per-connection state
        self._writers.add(writer)
        buffer = bytearray()
        try:
            while True:
                data = await reader.read(64 * 1024)
                if not data:
                    break
                buffer += data
                replies, close = self._process(buffer, session)
                if replies:
                    writer.write(b"".join(replies))  # one write for a whole pipeline
                    await writer.drain()
                if close:
                    break
                if len(buffer) > MAX_PENDING:
                    writer.write(encode(Error("ERR Protocol error: request too large")))
                    break
        except (ConnectionError, asyncio.IncompleteReadError):
            pass
        finally:
            self.clients -= 1
            self._writers.discard(writer)
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass

    def _process(self, buffer: bytearray, session: dict) -> tuple[list[bytes], bool]:
        """Run every complete command in the buffer. Returns (replies, close connection?)."""
        replies = []
        while buffer:
            try:
                args, used = parse_command(buffer)
            except ProtocolError as exc:
                replies.append(encode(Error(f"ERR Protocol error: {exc}"), session["protocol"]))
                return replies, True
            if args is None:
                break
            del buffer[:used]
            if not args:
                continue
            if args[0].upper() == b"HELLO":
                replies.append(encode(self._hello(args, session), session["protocol"]))
                continue
            try:
                reply, logged = commands.execute(self.store, args)
            except commands.Quit:
                replies.append(encode(commands.OK, session["protocol"]))
                return replies, True
            if logged and self.aof:
                self.aof.append(logged)  # log before replying: an acknowledged write is in the log
            replies.append(encode(reply, session["protocol"]))
        return replies, False

    @staticmethod
    def _hello(args: list[bytes], session: dict) -> object:
        """HELLO [2|3]: switch this connection's protocol and describe the server."""
        if len(args) > 2:
            return Error("ERR HELLO options (AUTH, SETNAME) are not supported")
        if len(args) == 2:
            if args[1] not in (b"2", b"3"):
                return Error("NOPROTO unsupported protocol version")
            session["protocol"] = int(args[1])
        return {"server": "mini-redis", "version": "0.1.0", "proto": session["protocol"],
                "id": session["id"], "mode": "standalone", "role": "master", "modules": []}


class ServerThread:
    """Run a Server on its own event loop in a background thread (tests, benchmarks)."""

    def __init__(self, **kwargs) -> None:
        self._kwargs = kwargs
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self.server: Server | None = None

    def start(self) -> int:
        self._thread.start()
        self.server = asyncio.run_coroutine_threadsafe(self._create(), self._loop).result(10)
        return self.server.port

    async def _create(self) -> Server:
        server = Server(port=0, **self._kwargs)
        await server.start()
        return server

    def stop(self) -> None:
        asyncio.run_coroutine_threadsafe(self.server.stop(), self._loop).result(10)
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join(5)
        self._loop.close()
