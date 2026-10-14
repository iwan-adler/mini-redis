"""Command implementations.

execute() returns (reply, aof_args). aof_args is what to append to the
append-only file, or None for read-only commands. Relative expiries are
rewritten as absolute ones (EXPIRE 10 -> PEXPIREAT <unix ms>) so replaying the
log after a restart doesn't give keys a fresh lifetime.
"""

from __future__ import annotations

from minikv.resp import OK, Error, SimpleString
from minikv.store import Store

INT64_MIN, INT64_MAX = -(2**63), 2**63 - 1

NOT_INTEGER = Error("ERR value is not an integer or out of range")
SYNTAX = Error("ERR syntax error")


class Quit(Exception):
    """Raised after QUIT's reply is decided, so the server closes the connection."""


def _int(raw: bytes) -> int | None:
    try:
        value = int(raw)
    except ValueError:
        return None
    # Redis only accepts canonical integers: no '+', spaces or leading zeros.
    if str(value).encode() != raw or not INT64_MIN <= value <= INT64_MAX:
        return None
    return value


def _arity(name: str) -> Error:
    return Error(f"ERR wrong number of arguments for '{name}' command")


def execute(store: Store, args: list[bytes]) -> tuple[object, list[bytes] | None]:
    if not args:
        return None, None
    name = args[0].decode("latin-1").lower()
    handler = COMMANDS.get(name)
    if handler is None:
        start = " ".join(f"'{a.decode('latin-1')}'" for a in args[1:4])
        return Error(f"ERR unknown command '{args[0].decode('latin-1')}', with args beginning with: {start}"), None
    min_args, max_args, fn = handler
    if len(args) - 1 < min_args or (max_args is not None and len(args) - 1 > max_args):
        return _arity(name), None
    return fn(store, args)


# --- connection ---------------------------------------------------------------------------


def _ping(store, args):
    return (SimpleString("PONG") if len(args) == 1 else args[1]), None


def _echo(store, args):
    return args[1], None


def _select(store, args):
    return (OK if args[1] == b"0" else Error("ERR DB index is out of range")), None


def _command(store, args):
    return [], None  # redis-cli asks for command docs on connect; an empty list is enough


def _client(store, args):
    # redis-py sends CLIENT SETINFO on connect; accept it, refuse everything else.
    if args[1].upper() == b"SETINFO" and len(args) == 4:
        return OK, None
    return Error(f"ERR unknown subcommand '{args[1].decode('latin-1')}'"), None


def _quit(store, args):
    raise Quit


# --- strings ----------------------------------------------------------------------------------


def _get(store, args):
    return store.get(args[1]), None


def _set(store: Store, args):
    key, value = args[1], args[2]
    condition, expire_at, keep_ttl = None, None, False
    options = [a.upper() for a in args[3:]]
    i = 0
    while i < len(options):
        option = options[i]
        if option in (b"NX", b"XX") and condition is None:
            condition = option
        elif option == b"KEEPTTL" and expire_at is None and not keep_ttl:
            keep_ttl = True
        elif option in (b"EX", b"PX", b"EXAT", b"PXAT") and expire_at is None and not keep_ttl:
            if i + 1 >= len(options):
                return SYNTAX, None
            amount = _int(args[3 + i + 1])
            if amount is None:
                return NOT_INTEGER, None
            if amount <= 0:
                return Error("ERR invalid expire time in 'set' command"), None
            now = store.now_ms()
            expire_at = {b"EX": now + amount * 1000, b"PX": now + amount,
                         b"EXAT": amount * 1000, b"PXAT": amount}[option]
            i += 1
        else:
            return SYNTAX, None
        i += 1

    exists = store.exists(key)
    if (condition == b"NX" and exists) or (condition == b"XX" and not exists):
        return None, None
    store.set(key, value, expire_at_ms=expire_at, keep_ttl=keep_ttl)
    logged = [b"SET", key, value]
    if expire_at is not None:
        logged += [b"PXAT", str(expire_at).encode()]
    elif keep_ttl:
        logged.append(b"KEEPTTL")
    return OK, logged


def _incr_by(store: Store, key: bytes, delta: int):
    current = store.get(key)
    value = 0 if current is None else _int(current)
    if value is None:
        return NOT_INTEGER, None
    result = value + delta
    if not INT64_MIN <= result <= INT64_MAX:
        return Error("ERR increment or decrement would overflow"), None
    store.set(key, str(result).encode(), keep_ttl=True)  # INCR keeps the existing TTL
    return result, [b"SET", key, str(result).encode(), b"KEEPTTL"]


def _incr(store, args):
    return _incr_by(store, args[1], 1)


def _decr(store, args):
    return _incr_by(store, args[1], -1)


def _incrby(store, args, sign=1):
    delta = _int(args[2])
    if delta is None:
        return NOT_INTEGER, None
    return _incr_by(store, args[1], sign * delta)


def _decrby(store, args):
    return _incrby(store, args, sign=-1)


# --- keys ---------------------------------------------------------------------------------------


def _del(store, args):
    deleted = [k for k in args[1:] if store.delete(k)]
    return len(deleted), ([b"DEL", *deleted] if deleted else None)


def _exists(store, args):
    return sum(1 for k in args[1:] if store.exists(k)), None


def _expire_generic(store: Store, args, unit_ms: int, absolute: bool):
    amount = _int(args[2])
    if amount is None:
        return NOT_INTEGER, None
    deadline = amount * unit_ms if absolute else store.now_ms() + amount * unit_ms
    if not store.expire_at(args[1], deadline):
        return 0, None
    if store.exists(args[1]):
        return 1, [b"PEXPIREAT", args[1], str(deadline).encode()]
    return 1, [b"DEL", args[1]]  # expiry already in the past: the key was deleted


def _expire(store, args):
    return _expire_generic(store, args, 1000, absolute=False)


def _pexpire(store, args):
    return _expire_generic(store, args, 1, absolute=False)


def _pexpireat(store, args):
    return _expire_generic(store, args, 1, absolute=True)


def _ttl(store, args):
    ms = store.ttl_ms(args[1])
    return (ms if ms < 0 else (ms + 500) // 1000), None  # Redis rounds to the nearest second


def _pttl(store, args):
    return store.ttl_ms(args[1]), None


def _persist(store, args):
    return (1, [b"PERSIST", args[1]]) if store.persist(args[1]) else (0, None)


def _keys(store, args):
    return sorted(store.keys(args[1])), None


def _dbsize(store, args):
    return store.size(), None


def _flushdb(store, args):
    store.flush()
    return OK, [b"FLUSHDB"]


# name -> (min args, max args or None, function)
COMMANDS = {
    "ping": (0, 1, _ping), "echo": (1, 1, _echo), "select": (1, 1, _select),
    "command": (0, None, _command), "client": (1, None, _client), "quit": (0, 0, _quit),
    "get": (1, 1, _get), "set": (2, None, _set),
    "incr": (1, 1, _incr), "decr": (1, 1, _decr), "incrby": (2, 2, _incrby), "decrby": (2, 2, _decrby),
    "del": (1, None, _del), "exists": (1, None, _exists),
    "expire": (2, 2, _expire), "pexpire": (2, 2, _pexpire), "pexpireat": (2, 2, _pexpireat),
    "ttl": (1, 1, _ttl), "pttl": (1, 1, _pttl), "persist": (1, 1, _persist),
    "keys": (1, 1, _keys), "dbsize": (0, 0, _dbsize), "flushdb": (0, 0, _flushdb),
}
