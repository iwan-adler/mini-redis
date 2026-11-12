"""End-to-end tests against real cluster processes with on-disk logs."""

import os
import tempfile
import time
from pathlib import Path

import pytest
import redis

from minikv.raft.chaos import Cluster, run, status
from minikv.raft.client import ClusterClient

pytestmark = pytest.mark.skipif(os.name != "posix", reason="uses POSIX signals to kill and pause nodes")


def wait_for_leader(cluster, timeout=10.0, exclude=()):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        leader = cluster.leader()
        if leader is not None and leader not in exclude:
            return leader
        time.sleep(0.05)
    raise AssertionError("no leader elected")


@pytest.fixture
def cluster():
    with tempfile.TemporaryDirectory() as tmp:
        c = Cluster(3, Path(tmp), snapshot_every=50)
        for i in c.ids:
            c.start(i)
        wait_for_leader(c)
        yield c
        c.stop()


def client_for(cluster):
    return ClusterClient([("127.0.0.1", p) for p in cluster.client_ports.values()], timeout=2, deadline=10)


def test_redis_py_and_redirects(cluster):
    leader = wait_for_leader(cluster)
    follower = next(i for i in cluster.ids if i != leader)
    r = redis.Redis(port=cluster.client_ports[leader], retry=None)
    assert r.set("greeting", "hello")
    assert r.incr("visits") == 1
    f = redis.Redis(port=cluster.client_ports[follower], retry=None)
    with pytest.raises(redis.ResponseError, match=f"NOTLEADER 127.0.0.1:{cluster.client_ports[leader]}"):
        f.get("greeting")
    assert f.ping()  # local commands work anywhere
    c = client_for(cluster)
    assert c.execute("GET", "greeting") == b"hello"
    c.close()


def test_survives_leader_crash(cluster):
    c = client_for(cluster)
    for i in range(20):
        c.execute("INCR", "counter")
    old = wait_for_leader(cluster)
    cluster.kill(old)
    new = wait_for_leader(cluster, exclude=(old,))
    assert new != old
    assert c.execute("GET", "counter") == b"20"
    c.execute("INCR", "counter")
    cluster.start(old)  # the old leader rejoins as a follower and catches up
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        statuses = [status(cluster.client_ports[i]) for i in cluster.ids]
        if all(statuses) and len({(s["applied"], s["digest"]) for s in statuses}) == 1:
            break
        time.sleep(0.1)
    else:
        raise AssertionError(f"replicas did not converge: {statuses}")
    c.close()


def test_data_survives_killing_every_node(cluster):
    c = client_for(cluster)
    for i in range(120):  # past snapshot_every, so recovery uses a snapshot plus the log
        c.execute("SET", f"key{i}", str(i))
    c.execute("EXPIRE", "key0", "1000")
    c.close()
    for i in cluster.ids:
        cluster.kill(i)
    for i in cluster.ids:
        cluster.start(i)
    wait_for_leader(cluster)
    c = client_for(cluster)
    assert c.execute("GET", "key119") == b"119"
    assert c.execute("DBSIZE") == 120
    assert 990 <= c.execute("TTL", "key0") <= 1000
    c.close()


def test_minority_cannot_write(cluster):
    leader = wait_for_leader(cluster)
    others = [i for i in cluster.ids if i != leader]
    cluster.partition([[leader], others])
    r = redis.Redis(port=cluster.client_ports[leader], retry=None, socket_timeout=10)
    # The isolated leader can't commit; it steps down (check-quorum) or the request times out.
    with pytest.raises(redis.ResponseError, match="NOTLEADER|TRYAGAIN|UNKNOWN"):
        r.set("x", "1")
    cluster.heal()
    c = client_for(cluster)
    c.execute("SET", "x", "2")
    assert c.execute("GET", "x") == b"2"
    c.close()


def test_short_chaos_run_is_linearizable():
    summary = run(nodes=3, clients=4, duration=6, seed=7, verbose=False)
    assert summary["ok"], summary
    assert summary["ops_ok"] > 200
