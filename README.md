# mini-redis

[![tests](https://github.com/iwan-adler/mini-redis/actions/workflows/tests.yml/badge.svg)](https://github.com/iwan-adler/mini-redis/actions/workflows/tests.yml)

A Redis-compatible key-value server written in Python with **asyncio and the standard library only**. It speaks the real Redis protocol (RESP2 *and* RESP3), so the **official `redis-py` client and `redis-cli` work with it unchanged**.

It also supports key expiry, pipelining and crash-safe **append-only-file persistence**. Its behaviour is checked against a **real Redis server in CI**.

```python
import redis
r = redis.Redis(port=6380)                # the official client, no changes
r.set("session:42", "ada", ex=300)
r.incr("visits")
r.ttl("session:42")                       # 300
```

## Features

These are all implemented and tested.

| Area | Details |
|---|---|
| **Commands** | `GET`, `SET` (`NX`, `XX`, `EX`, `PX`, `EXAT`, `PXAT`, `KEEPTTL`), `DEL`, `EXISTS`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `EXPIRE`, `PEXPIRE`, `PEXPIREAT`, `TTL`, `PTTL`, `PERSIST`, `KEYS`, `DBSIZE`, `FLUSHDB`, `PING`, `ECHO`, `SELECT 0`, `HELLO`, `QUIT` |
| **Protocol** | RESP2 arrays plus *inline* commands for telnet and nc. `HELLO 3` switches a connection to RESP3, which redis-py 8 uses by default. Binary-safe values. Pipelining, with one reply write per batch |
| **Expiry** | **Lazy:** keys are checked when accessed. **Active:** a background sweep samples keys with a TTL and repeats while over 25% of the sample was expired, which is Redis's own algorithm |
| **Persistence** | An append-only file of write commands. Expiry is logged as an **absolute** time, so restarts don't extend key lifetimes. A crash mid-write leaves a truncated tail, which is cut off on load; corruption in the middle refuses to start. Compaction by atomic rewrite. Three `fsync` policies: `always`, `everysec` and `no` |
| **Concurrency** | One event loop runs every command, so commands are **atomic without locks**, like Redis. A test runs 20 concurrent clients doing 5,000 `INCR`s and loses none |
| **Robustness** | Precise error replies matching Redis's wording. Malformed protocol gets an error and the connection is closed. A cap on unparsed bytes per client. Graceful shutdown that closes connected clients |

## Checked against real Redis

[`tests/test_conformance.py`](tests/test_conformance.py) describes the expected behaviour with the official client:
- `SET` options
- TTL rounding
- a `SET` without options clearing the TTL
- `INCR` rejecting `"+1"`, `"01"` and `" 1"`
- 64-bit overflow
- `EXISTS` counting duplicate keys
- error messages
- both protocol versions

The same tests run against mini-redis and, in CI, against a **real Redis 7 service container**. If my reading of Redis's behaviour were wrong, the real-Redis run would fail.

## Design notes

**Why one thread is enough, and is a feature.** Every command runs to completion on the event loop before the next starts, so `INCR` needs no locks and can't lose updates. The trade-off is that one slow command (`KEYS` on a huge database) blocks everyone, which is why real Redis warns against `KEYS` in production.

**TCP is a stream, not messages.** The parser works on a growing buffer:
- It returns "need more data" for a partial command; a test cuts a command at every possible byte.
- It returns each complete command from a pipelined batch, one at a time.
- It never trusts lengths from the client: bulk strings are capped at 1 MB and the pending buffer at 2 MB.

**Persistence trade-offs:**
- **What a crash can lose** depends on the `fsync` policy: `always` loses no acknowledged write, but is the slowest. `everysec` (the default, as in Redis) can lose up to about 1 second of writes.
- **Write order:** each write is appended to the log *before* the client gets its reply.
- **Replay never extends lifetimes:** `EXPIRE k 10` is stored as `PEXPIREAT k <unix-ms>`, and `SET … EX` as `SET … PXAT`. Keys that expired while the server was down stay expired.

**A Windows bug found by profiling.** The test suite took 55 seconds because every connection took exactly 2 seconds. On Windows, `localhost` resolves to IPv6 `::1` first, and the server only listened on IPv4, so each client waited for the IPv6 attempt to time out. The server now binds `localhost`, which means both `127.0.0.1` and `::1`. The suite takes 1.6 s, and a `localhost` client gets its first reply in 37 ms.

## Benchmark

`python bench/bench.py` measures redis-py against an in-process mini-redis, with 20,000 operations per row, on the development laptop: Windows, 8-core ARM64, x64 Python 3.14 under emulation.

| Operation | Run 1 | Run 2 |
|---|---|---|
| SET, one command per round trip | 4,493 ops/s | 4,432 ops/s |
| GET, one command per round trip | 4,737 ops/s | 5,033 ops/s |
| SET, pipelined in batches of 100 | 43,836 ops/s | 48,037 ops/s |
| GET, pipelined in batches of 100 | 61,369 ops/s | 64,781 ops/s |

**What this shows:** pipelining is 10–13× faster, because one command at a time is bound by the round trip (about 0.2 ms including the Python client), not by the server's work. Real Redis is written in C and far faster; no real Redis was installed on this machine to compare against, so none is claimed. To compare yourself, run `python bench/bench.py --port 6379` against a real Redis.

## Running it

Requires Python 3.11 or newer. The server has no dependencies; `redis-py` and `pytest` are only needed for tests and the benchmark.

```bash
git clone https://github.com/iwan-adler/mini-redis.git
cd mini-redis
python -m minikv --port 6380 --aof data/appendonly.aof     # persistent; omit --aof for memory only
```

From another terminal you can use any Redis client:
- **Python:** `redis.Redis(port=6380)`
- **Real CLI:** `redis-cli -p 6380`
- **Bare terminal:** `nc localhost 6380`, then type `SET a 1` (inline commands work)

```bash
pip install -r requirements-dev.txt
python -m pytest                          # 49 tests, about 2 s
REAL_REDIS_PORT=6379 python -m pytest     # also run the conformance tests against a real Redis
python bench/bench.py
```

## Tests

| File | Covers |
|---|---|
| `test_conformance.py` | Redis behaviour through redis-py, run against mini-redis and, in CI, real Redis |
| `test_internals.py` | The parser cut at every byte, pipelining, inline commands, malformed input, RESP2 and RESP3 encoding, expiry with a fake clock, TTL rounding, the active-expiry algorithm, absolute-expiry logging, AOF replay, **truncated-tail recovery**, refusing corruption, keys expired during downtime, compaction |
| `test_server.py` | Inline commands over a raw socket, protocol errors, `HELLO` negotiation, **20 concurrent clients without lost updates**, restart with data and TTLs preserved, shutdown with connected clients |

## Limitations

- **Strings only.** No lists, hashes, sets or sorted sets, so no `WRONGTYPE` errors.
- **No transactions** (`MULTI`/`EXEC`), pub/sub, Lua scripting, authentication or replication.
- **One database.** `SELECT 0` only.
- **`KEYS` glob matching** uses Python's `fnmatch`, which differs from Redis in edge cases such as `[^a]`.
- **AOF compaction** is a function (`aof.rewrite`), not a background `BGREWRITEAOF` command.
