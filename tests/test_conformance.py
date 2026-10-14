"""Behaviour that must match real Redis exactly.

Every test runs against mini-redis, and also against a real Redis server when
REAL_REDIS_PORT is set (CI starts one), so the expectations are checked against
the real thing rather than against my reading of the docs.
"""

import time

import pytest
import redis


def test_ping_echo(client):
    assert client.ping() is True
    assert client.echo("hi") == b"hi"


def test_set_get_and_overwrite(client):
    assert client.get("k") is None
    assert client.set("k", "v1") is True
    assert client.set("k", "v2") is True
    assert client.get("k") == b"v2"
    assert client.set("bin", b"\x00\xff\r\n") and client.get("bin") == b"\x00\xff\r\n"


def test_set_nx_xx(client):
    assert client.set("k", "a", xx=True) is None
    assert client.set("k", "a", nx=True) is True
    assert client.set("k", "b", nx=True) is None
    assert client.set("k", "c", xx=True) is True
    assert client.get("k") == b"c"


def test_set_with_expiry_and_ttl(client):
    client.set("k", "v", ex=100)
    assert 99 <= client.ttl("k") <= 100
    assert 99_000 <= client.pttl("k") <= 100_000
    client.set("k", "v2")  # a plain SET clears the TTL
    assert client.ttl("k") == -1
    assert client.ttl("missing") == -2


def test_keepttl(client):
    client.set("k", "v", ex=100)
    client.set("k", "v2", keepttl=True)
    assert client.ttl("k") > 90


def test_key_actually_expires(client):
    client.set("k", "v", px=100)
    assert client.get("k") == b"v"
    time.sleep(0.25)
    assert client.get("k") is None
    assert client.exists("k") == 0


def test_expire_persist_and_past_expiry(client):
    client.set("k", "v")
    assert client.expire("k", 50) is True
    assert client.persist("k") is True
    assert client.persist("k") is False
    assert client.ttl("k") == -1
    assert client.expire("missing", 10) is False
    assert client.expire("k", -1) is True  # an expiry in the past deletes the key
    assert client.exists("k") == 0


def test_incr_family(client):
    assert client.incr("n") == 1
    assert client.incrby("n", 41) == 42
    assert client.decr("n") == 41
    assert client.decrby("n", 50) == -9
    client.set("n", "10", ex=100)
    client.incr("n")
    assert client.ttl("n") > 90  # INCR keeps the TTL


@pytest.mark.parametrize("value", ["abc", "1.5", " 1", "+1", "01", "99999999999999999999"])
def test_incr_rejects_non_integers(client, value):
    client.set("n", value)
    with pytest.raises(redis.ResponseError, match="not an integer"):
        client.incr("n")


def test_incr_overflow(client):
    client.set("n", str(2**63 - 1))
    with pytest.raises(redis.ResponseError, match="overflow"):
        client.incr("n")


def test_del_exists_counts(client):
    client.set("a", 1)
    client.set("b", 2)
    assert client.exists("a", "b", "a", "zzz") == 3  # duplicates are counted
    assert client.delete("a", "b", "zzz") == 2


def test_keys_dbsize_flushdb(client):
    for key in ("user:1", "user:2", "post:1"):
        client.set(key, "x")
    assert sorted(client.keys("user:*")) == [b"user:1", b"user:2"]
    assert sorted(client.keys("user:?")) == [b"user:1", b"user:2"]
    assert client.dbsize() == 3
    assert client.flushdb() is True
    assert client.dbsize() == 0


def test_errors(client):
    with pytest.raises(redis.ResponseError, match="wrong number of arguments"):
        client.execute_command("GET")
    with pytest.raises(redis.ResponseError, match="unknown command"):
        client.execute_command("NOPE", "x")
    with pytest.raises(redis.ResponseError, match="syntax error"):
        client.execute_command("SET", "k", "v", "BOGUS")
    with pytest.raises(redis.ResponseError, match="invalid expire time"):
        client.execute_command("SET", "k", "v", "EX", "0")
    with pytest.raises(redis.ResponseError, match="not an integer"):
        client.execute_command("EXPIRE", "k", "soon")


def test_pipeline(client):
    pipe = client.pipeline(transaction=False)
    for i in range(500):
        pipe.set(f"k{i}", i)
    pipe.get("k499")
    assert pipe.execute()[-1] == b"499"


@pytest.mark.parametrize("protocol", [2, 3])
def test_both_protocol_versions(client, protocol):
    port = client.connection_pool.connection_kwargs["port"]
    r = redis.Redis(host="127.0.0.1", port=port, protocol=protocol)
    r.set("k", "v")
    assert r.get("k") == b"v" and r.get("missing") is None
    assert r.ttl("k") == -1
    r.close()
