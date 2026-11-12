# mini-redis

[![tests](https://github.com/iwan-adler/mini-redis/actions/workflows/tests.yml/badge.svg)](https://github.com/iwan-adler/mini-redis/actions/workflows/tests.yml)

A Redis-compatible key-value server written in Python with **asyncio and the standard library only**. It speaks the real Redis protocol (RESP2 *and* RESP3), so the **official `redis-py` client and `redis-cli` work with it unchanged**. CI checks both on every push.

It also supports key expiry, pipelining and crash-safe **append-only-file persistence**. Its behaviour is checked against a **real Redis server in CI**.

It can also run as a **replicated cluster**. Three or five nodes keep the same data in sync using **Raft consensus**, written from scratch. The cluster keeps working when a minority of nodes crash, freeze or get cut off by a network partition, and it never loses an acknowledged write or returns stale data. That claim is tested, not assumed: a deterministic simulator runs thousands of seeded fault schedules, and a chaos test kills, freezes and partitions real processes. Both check every client-visible result for **linearizability**. [Jump to cluster mode](#cluster-mode-raft-replication).

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

## Cluster mode: Raft replication

```bash
C=1=127.0.0.1:7001:6381,2=127.0.0.1:7002:6382,3=127.0.0.1:7003:6383   # id=host:raft_port:client_port
python -m minikv.raft --id 1 --cluster $C --data data/n1 &
python -m minikv.raft --id 2 --cluster $C --data data/n2 &
python -m minikv.raft --id 3 --cluster $C --data data/n3 &
redis-cli -p 6381 SET greeting hello       # on the leader: OK
redis-cli -p 6382 GET greeting             # on a follower: (error) NOTLEADER 127.0.0.1:6381
```

```python
from minikv.raft.client import ClusterClient
c = ClusterClient([("127.0.0.1", 6381), ("127.0.0.1", 6382), ("127.0.0.1", 6383)])
c.execute("INCR", "visits")   # finds the leader and follows redirects; survives failovers
```

### What it guarantees

- **Every acknowledged write survives** the loss of any minority of nodes. A write is replied to only after it is fsynced on a majority.
- **Reads are linearizable**: a read never returns data older than a write that finished before it started. This holds even on a leader that has just been cut off and doesn't know it yet.
- **Replicas never diverge.** All nodes apply the same commands in the same order and end in byte-identical states. A hash of each node's data is checked after every test.

### How it works

| Piece | File | Notes |
|---|---|---|
| **Raft core** | [`raft/core.py`](minikv/raft/core.py) | A pure state machine with no sockets, clocks or threads: `tick()`, `step(msg)`, `propose()`, `ready()`. The same code runs in the real server and in the simulator. |
| Leader election | | Randomised timeouts, durable votes, **PreVote** so a node returning from a partition can't depose a healthy leader by inflating its term, and **CheckQuorum** so a cut-off leader steps down. |
| Log replication | | Fast back-off using the conflicting term. A leader only commits entries from its own term by counting replicas ([Raft §5.4.2](https://raft.github.io/raft.pdf), Figure 8); each new leader appends a no-op to commit earlier entries. |
| **Linearizable reads** | | **ReadIndex**: the leader records its commit index, confirms it is still leader with one heartbeat round answered by a majority, then waits until that index is applied. Reads that arrive together share a single round. |
| Log compaction | | Snapshots of the key-value state; followers that fall too far behind get **InstallSnapshot**. |
| Durability | [`raft/storage.py`](minikv/raft/storage.py) | Term and vote replaced atomically (write, fsync, rename, fsync the directory). The log is checksummed frames: a torn final record is cut off on start, while corruption in the middle refuses to start. Storage is synced before any message leaves the node. |
| **Deterministic expiry** | [`raft/statemachine.py`](minikv/raft/statemachine.py) | TTLs depend on time, and every replica has a different clock. So the leader stamps each log entry with its clock and replicas apply it with that stamp as "now". The state machine's clock only moves forward. Reads never delete expired keys (that would make state depend on one node's clock); expired keys are purged at deterministic points. |
| Networking | [`raft/node.py`](minikv/raft/node.py) | asyncio. RESP for clients, so redis-py and redis-cli work. Length-prefixed JSON between nodes (nothing is ever unpickled from the network). Followers redirect with `-NOTLEADER host:port`. |
| Client | [`raft/client.py`](minikv/raft/client.py) | Follows redirects and retries only what is provably safe to retry. A write whose reply was lost raises `OutcomeUnknown` instead of being re-sent: re-sending `INCR` could apply it twice. |

### How it's tested

**1. Deterministic simulation** ([`raft/sim.py`](minikv/raft/sim.py)), in the style of FoundationDB and TigerBeetle. The whole cluster runs in one thread on a virtual clock, and the network delays, drops, duplicates and reorders messages. A nemesis partitions the network, isolates the leader, and crashes and restarts nodes. Every run is reproducible from its seed. While it runs, it checks Raft's invariants directly:
- at most one leader per term;
- no two nodes ever apply different commands at the same log index;
- a new leader already has every committed entry.

After healing, it checks that all replicas converged and that the clients' history is linearizable.

```text
$ python -m minikv.raft.sim --seeds 1000 --nodes 5
1000 seeds, 0 failed | 247584 ops checked, 14941 indeterminate, 6571 faults injected, 4058 leaders elected
```

**2. A linearizability checker** ([`raft/linearizability.py`](minikv/raft/linearizability.py)). It implements the Wing & Gong search with Lowe's memoisation (the algorithm behind Knossos and Porcupine) and checks each key separately. Operations whose reply never arrived are handled correctly: they may have taken effect at any point after they were sent, or never.

**3. Chaos against real processes** ([`raft/chaos.py`](minikv/raft/chaos.py)), a small Jepsen. It starts real node processes with on-disk logs and runs concurrent clients against them. Meanwhile it `SIGKILL`s nodes and restarts them from disk, freezes them with `SIGSTOP`, and partitions the network. Then it checks linearizability and convergence:

```text
$ python -m minikv.raft.chaos --nodes 3 --duration 20 --seed 1
seed 1: 9 faults, 23293 ops ok, 26 indeterminate
linearizable: True   replicas converged: True   final applied index per node: {1: '14021', 2: '14021', 3: '14021'}
```

**4. Testing the tests.** A checker that never fails proves nothing. So the simulator can switch on deliberate bugs, and the test suite asserts that each one is caught:

| Injected bug | What breaks | Caught by |
|---|---|---|
| Leaders serve reads from local state without ReadIndex | A leader cut off by a partition serves stale data | the linearizability checker (29 of 200 seeds) |
| A restarted node forgets who it voted for | Two leaders get elected in the same term | the election-safety invariant |

### Bugs the simulator found in my implementation

- **Installing a snapshot left the old log on disk.** A follower that received a snapshot cleared its in-memory log, but storage only dropped entries *covered* by the snapshot. Entries after it, from a dead term, stayed on disk. After a restart, the node replayed them and applied different commands than everyone else. The fix truncates storage before saving the snapshot, and node start-up now refuses a log with gaps or repeats. Found by the state-machine-safety check: 4 failing seeds out of 500 with 5 nodes. A regression test is in `test_raft_core.py`.
- **A retry from an abandoned request leaked into the next one.** This was in the simulated client, not in Raft. A redirect retry scheduled just before a timeout fired later carrying the *next* operation's arguments, so one `DEL` was sent twice. The linearizability checker flagged an impossible history. Shrinking it to the first failing moment pointed straight at the duplicate.

### Benchmark

Run with `python bench/cluster_bench.py`. It uses 3 nodes on one 4-vCPU Linux VM with logs on disk, so every write is fsynced on a majority before the reply. Clients are Python threads, which share the CPU with the nodes.

| Operation | Clients | ops/s | p50 latency | p99 latency |
|---|---|---|---|---|
| INCR (write) | 1 | 657 | 1.09 ms | 6.12 ms |
| GET (read) | 1 | 2,717 | 0.35 ms | 0.64 ms |
| INCR (write) | 8 | 915 | 7.63 ms | 44.89 ms |
| GET (read) | 8 | 1,964 | 1.29 ms | 37.25 ms |

**What this shows:**
- A write costs a round trip to a follower plus two fsyncs; a read costs one heartbeat round and no disk.
- Concurrent proposals share messages and fsyncs, because the node handles Raft output once per event-loop iteration.
- Throughput is bounded by CPython on four shared cores, not by the protocol.

### Design decisions

- **Why the core has no I/O.** Every interesting Raft bug depends on message timing. With the protocol as a pure function of its inputs, a test can produce any interleaving on purpose, and any failure replays from a seed. The asyncio server is a thin layer around it.
- **Why reads don't use leader leases.** A lease lets the leader skip the heartbeat round, but it is only safe if clocks don't drift more than assumed. ReadIndex costs one round trip and assumes nothing about clocks.
- **Why the client won't retry a write.** Exactly-once retries need per-client sessions in the state machine ([Raft thesis §6.3](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)). Until those exist, the honest answer to a lost reply is "outcome unknown", and the checker treats such operations exactly that way.
- **Expiry and clock skew.** Replicas always agree with each other, because time comes from the log. Whether a TTL ends exactly on time still depends on the leader's clock, as it does in Redis.

### Cluster limitations

- **Fixed membership**: no adding or removing nodes while running (Raft joint consensus is not implemented).
- **No exactly-once retries** (see above).
- **Snapshots are taken synchronously** and sent in one message, which is fine for a small data set but blocks the event loop on a large one.
- **Followers don't serve reads.** Everything goes through the leader.

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
python -m pytest                          # 106 tests, about 20 s
REAL_REDIS_PORT=6379 python -m pytest     # also run the conformance tests against a real Redis
python bench/bench.py
```

## Tests

| File | Covers |
|---|---|
| `test_conformance.py` | Redis behaviour through redis-py, run against mini-redis and, in CI, real Redis |
| `test_internals.py` | The parser cut at every byte, pipelining, inline commands, malformed input, RESP2 and RESP3 encoding, expiry with a fake clock, TTL rounding, the active-expiry algorithm, absolute-expiry logging, AOF replay, **truncated-tail recovery**, refusing corruption, keys expired during downtime, compaction |
| `test_server.py` | Inline commands over a raw socket, protocol errors, `HELLO` negotiation, **20 concurrent clients without lost updates**, restart with data and TTLs preserved, shutdown with connected clients |
| `test_raft_core.py` | Raft message by message: elections, durable votes, stale candidates losing, conflicting suffixes, the Figure 8 commit rule, PreVote, CheckQuorum, ReadIndex, InstallSnapshot, the snapshot regression |
| `test_raft_storage.py` | Log round trips, **torn-tail recovery**, refusing mid-log corruption, a crash between snapshot and log rewrite, codec |
| `test_raft_statemachine.py` | **Replicas agree on expiry** regardless of their clocks, reads that never mutate, a clock that never goes backwards, snapshot/restore |
| `test_linearizability.py` | The checker accepts linearizable histories and rejects stale reads, lost updates, time travel and impossible indeterminate writes |
| `test_raft_sim.py` | 90 seeded fault schedules on 3 and 5 nodes, lossy and snapshot-heavy variants, determinism, and **the two injected bugs being caught** |
| `test_cluster.py` | Real processes: redis-py and redirects, leader crash and catch-up, **killing every node and recovering from disk**, a minority that can't write, a short chaos run |

## Limitations

- **Strings only.** No lists, hashes, sets or sorted sets, so no `WRONGTYPE` errors.
- **No transactions** (`MULTI`/`EXEC`), pub/sub, Lua scripting or authentication. Replication is available in [cluster mode](#cluster-mode-raft-replication).
- **One database.** `SELECT 0` only.
- **`KEYS` glob matching** uses Python's `fnmatch`, which differs from Redis in edge cases such as `[^a]`.
- **AOF compaction** is a function (`aof.rewrite`), not a background `BGREWRITEAOF` command.
