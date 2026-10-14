"""Run: python -m minikv [--port 6380] [--aof data/appendonly.aof] [--fsync everysec]"""

import argparse
import asyncio
import logging

from minikv.aof import FSYNC_POLICIES
from minikv.server import Server


async def main() -> None:
    parser = argparse.ArgumentParser(description="mini-redis: a Redis-compatible key-value server")
    # "localhost" binds both 127.0.0.1 and ::1. Binding only IPv4 made clients that
    # connect to "localhost" wait ~2 s on Windows, which tries IPv6 (::1) first.
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=6380, help="default 6380, so it doesn't clash with a real Redis")
    parser.add_argument("--aof", help="append-only file path; omit for an in-memory server")
    parser.add_argument("--fsync", choices=FSYNC_POLICIES, default="everysec")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    server = Server(args.host, args.port, aof_path=args.aof, fsync=args.fsync)
    await server.start()
    try:
        await asyncio.Event().wait()
    finally:
        await server.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
