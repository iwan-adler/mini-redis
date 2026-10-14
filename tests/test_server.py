"""mini-redis-specific server behaviour: raw sockets, atomicity, persistence across restarts."""

import socket
import threading

import redis

from minikv.server import ServerThread


def raw(port: int, payload: bytes) -> bytes:
    with socket.create_connection(("127.0.0.1", port), timeout=2) as sock:
        sock.sendall(payload)
        chunks = []
        try:
            while chunk := sock.recv(65536):
                chunks.append(chunk)
        except TimeoutError:
            pass
    return b"".join(chunks)


def test_inline_commands_from_a_terminal(mini):
    reply = raw(mini, b"SET greeting hello\r\nGET greeting\r\nQUIT\r\n")
    assert reply == b"+OK\r\n$5\r\nhello\r\n+OK\r\n"


def test_protocol_error_replies_and_closes(mini):
    reply = raw(mini, b"*1\r\n$abc\r\n")
    assert reply.startswith(b"-ERR Protocol error")


def test_hello_negotiates_protocol(mini):
    reply = raw(mini, b"HELLO 3\r\nGET missing\r\nHELLO 4\r\nQUIT\r\n")
    assert reply.startswith(b"%7\r\n")      # RESP3 map
    assert b"_\r\n" in reply                # RESP3 null
    assert b"-NOPROTO" in reply


def test_concurrent_increments_are_atomic(mini):
    """20 clients x 250 INCRs on one key: the single event loop must not lose any update."""
    def worker():
        r = redis.Redis(host="127.0.0.1", port=mini)
        pipe = r.pipeline(transaction=False)
        for _ in range(250):
            pipe.incr("counter")
        pipe.execute()
        r.close()

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert redis.Redis(host="127.0.0.1", port=mini).get("counter") == b"5000"


def test_data_survives_restart_and_expiry_continues(tmp_path):
    log = tmp_path / "data" / "appendonly.aof"
    first = ServerThread(aof_path=log, fsync="always")
    r = redis.Redis(host="127.0.0.1", port=first.start())
    r.set("name", "ada")
    r.incrby("visits", 7)
    r.set("session", "x", ex=300)
    r.delete("nothing")
    r.close()
    first.stop()

    second = ServerThread(aof_path=log)
    r = redis.Redis(host="127.0.0.1", port=second.start())
    assert r.get("name") == b"ada" and r.get("visits") == b"7"
    assert 290 <= r.ttl("session") <= 300  # absolute expiry: not reset to a fresh 300 s
    r.close()
    second.stop()


def test_shutdown_with_connected_clients(tmp_path):
    thread = ServerThread()
    r = redis.Redis(host="127.0.0.1", port=thread.start())
    r.ping()
    thread.stop()  # must not hang waiting for the client to disconnect
