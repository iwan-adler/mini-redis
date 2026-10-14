import os

import pytest
import redis

from minikv.server import ServerThread


@pytest.fixture
def mini():
    thread = ServerThread()
    port = thread.start()
    yield port
    thread.stop()


TARGETS = ["mini-redis"] + (["real redis"] if os.getenv("REAL_REDIS_PORT") else [])


@pytest.fixture(params=TARGETS)
def client(request):
    """A redis-py client for each target. Conformance tests run against mini-redis and,
    when REAL_REDIS_PORT is set (as in CI), against a real Redis server as well."""
    if request.param == "real redis":
        r = redis.Redis(host="127.0.0.1", port=int(os.environ["REAL_REDIS_PORT"]))
        r.flushdb()
        yield r
        r.flushdb()
        r.close()
        return
    thread = ServerThread()
    port = thread.start()
    r = redis.Redis(host="127.0.0.1", port=port)
    yield r
    r.close()
    thread.stop()
