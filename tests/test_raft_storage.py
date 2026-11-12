"""Durable Raft storage and the wire/disk codec."""

import pytest

from minikv.raft.codec import CorruptFrame, frame, message_from_bytes, message_to_bytes, read_frames
from minikv.raft.core import Entry, Message, Snapshot
from minikv.raft.storage import CorruptLog, FileStorage


def entries(start, stop, term=1):
    return [Entry(term, i, (b"SET", b"k", str(i).encode()), 1000 + i) for i in range(start, stop)]


def test_round_trip(tmp_path):
    s = FileStorage(tmp_path)
    s.save_hard_state(4, 2)
    s.append(entries(1, 6))
    s.sync()
    s.close()
    term, vote, snap, log = FileStorage(tmp_path).load()
    assert (term, vote, snap) == (4, 2, None)
    assert log == entries(1, 6)


def test_binary_values_and_noops_survive(tmp_path):
    s = FileStorage(tmp_path)
    weird = [Entry(1, 1, None, 5), Entry(1, 2, (b"SET", b"\x00\xff\r\n", b""), 6)]
    s.append(weird)
    s.close()
    assert FileStorage(tmp_path).load()[3] == weird


def test_torn_final_record_is_cut_off(tmp_path):
    s = FileStorage(tmp_path)
    s.append(entries(1, 4))
    s.close()
    log = tmp_path / "log"
    data = log.read_bytes()
    log.write_bytes(data[:-5])  # a crash in the middle of writing entry 3
    assert FileStorage(tmp_path).load()[3] == entries(1, 3)
    assert FileStorage(tmp_path).load()[3] == entries(1, 3)  # and the file was repaired


def test_corruption_in_the_middle_refuses_to_start(tmp_path):
    s = FileStorage(tmp_path)
    s.append(entries(1, 4))
    s.close()
    log = tmp_path / "log"
    data = bytearray(log.read_bytes())
    data[12] ^= 0xFF  # flip a byte inside entry 1
    log.write_bytes(bytes(data))
    with pytest.raises(CorruptLog):
        FileStorage(tmp_path)


def test_truncate_and_snapshot(tmp_path):
    s = FileStorage(tmp_path)
    s.append(entries(1, 10))
    s.truncate_from(8)
    s.append(entries(8, 9, term=2))
    s.save_snapshot(Snapshot(5, 1, b"state"))
    s.close()
    term, vote, snap, log = FileStorage(tmp_path).load()
    assert snap == Snapshot(5, 1, b"state")
    assert [e.index for e in log] == [6, 7, 8]
    assert log[-1].term == 2


def test_crash_between_snapshot_and_log_rewrite(tmp_path):
    """The snapshot is written first; if the log rewrite never happens, the covered
    entries are simply ignored on load."""
    s = FileStorage(tmp_path)
    s.append(entries(1, 6))
    s.close()
    old_log = (tmp_path / "log").read_bytes()
    s = FileStorage(tmp_path)
    s.save_snapshot(Snapshot(3, 1, b"x"))
    s.close()
    (tmp_path / "log").write_bytes(old_log)  # as if the rewrite was lost
    assert [e.index for e in FileStorage(tmp_path).load()[3]] == [4, 5]


def test_gap_in_log_is_detected(tmp_path):
    s = FileStorage(tmp_path)
    s.append(entries(1, 3) + entries(4, 5))
    s.close()
    with pytest.raises(CorruptLog):
        FileStorage(tmp_path)


def test_message_round_trip():
    m = Message("append", 1, 2, 7, prev_index=3, prev_term=2, entries=tuple(entries(4, 6)), commit=3, seq=9)
    assert message_from_bytes(message_to_bytes(m)) == m
    snap = Message("snapshot", 1, 2, 7, snapshot=Snapshot(10, 3, b"\x00data"))
    assert message_from_bytes(message_to_bytes(snap)) == snap


def test_message_decoder_rejects_unknown_fields():
    with pytest.raises((ValueError, TypeError)):
        message_from_bytes(b'{"type":"append","frm":1,"to":2,"term":1,"__class__":"x"}')


def test_frames_split_and_checksum():
    data = frame(b"one") + frame(b"two") + frame(b"three")[:4]
    payloads, used = read_frames(data)
    assert payloads == [b"one", b"two"] and used == len(frame(b"one") + frame(b"two"))
    bad = bytearray(frame(b"payload"))
    bad[-1] ^= 1
    with pytest.raises(CorruptFrame):
        read_frames(bytes(bad))
