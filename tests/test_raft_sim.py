"""Simulation tests: many seeded runs with crashes, partitions and a lossy network.

Each run checks Raft's safety invariants as it goes, and replica convergence and
linearizability at the end. `python -m minikv.raft.sim --seeds 1000` runs more.
"""

import pytest

from minikv.raft.sim import SimConfig, Simulation, run


@pytest.mark.parametrize("nodes,seeds", [(3, range(0, 60)), (5, range(0, 30))])
def test_cluster_is_safe_under_faults(nodes, seeds):
    failures = []
    for seed in seeds:
        result = run(seed, nodes=nodes)
        if not result.ok:
            failures.append(result.summary())
        assert result.stats["ops_ok"] > 50, "the cluster should make progress between faults"
    assert not failures, "\n".join(failures)


@pytest.mark.parametrize("overrides", [
    {"pre_vote": False, "check_quorum": False},
    {"drop_rate": 0.2, "duplicate_rate": 0.1},
    {"snapshot_every": 5},
    {"nodes": 1, "nemesis": ("crash",)},
], ids=["no-prevote", "lossy", "frequent-snapshots", "single-node"])
def test_variants(overrides):
    for seed in range(15):
        result = run(seed, **overrides)
        assert result.ok, result.summary()


def test_same_seed_same_run():
    a, b = run(42), run(42)
    assert [(o.kind, o.call, o.ret, o.result) for o in a.history] == \
           [(o.kind, o.call, o.ret, o.result) for o in b.history]


def test_harness_catches_stale_reads():
    """Mutation test: let leaders answer reads from local state without confirming
    leadership. An isolated old leader then serves stale data, and the checker must
    notice."""
    config = {"stale_reads": True, "check_quorum": False, "nemesis": ("isolate-leader",)}
    caught = next((r for s in range(40) if not (r := run(s, **config)).ok), None)
    assert caught is not None, "stale reads went unnoticed"
    assert any("not linearizable" in v for v in caught.violations)


def test_harness_catches_forgotten_votes():
    """Mutation test: a node that forgets its vote on restart can vote twice in one
    term, electing two leaders."""
    config = {"forget_votes": True, "pre_vote": False, "nemesis": ("crash",), "fault_ms": (1, 30),
              "calm_ms": (1, 30), "max_delay_ms": 40, "duration_ms": 2000, "settle_ms": 1000}
    caught = next((r for s in range(400) if not (r := run(s, **config)).ok), None)
    assert caught is not None, "double voting went unnoticed"
    assert any("election safety" in v for v in caught.violations)
    # The same schedule with votes persisted is fine.
    assert run(caught.seed, **{**config, "forget_votes": False}).ok


def test_simulation_exposes_the_config():
    sim = Simulation(1, SimConfig(nodes=5))
    assert len(sim.replicas) == 5
