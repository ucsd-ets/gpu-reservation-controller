"""Tests for the once-a-minute ``ondemand.gated`` operator WARNING.

Two layers:

- ``ControllerState.plan_ondemand_gates`` — which class-wide gates are in force,
  how many candidates each is holding, and the ``since`` bookkeeping.
- ``main._warn_ondemand_gates`` — the rendered WARNING line, parsed with
  ``kv_fields`` so assertions name the field they mean.

Plus the one population site the warning newly depends on: the capacity audit
keeping its physical snapshot.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from app.controller import ControllerState, OnDemandCandidate

from tests.conftest import GPU_CLASS_LABEL, OTHER_CLASS_LABEL, kv_fields, make_config

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-gate")
    import app.main as main

    return main


def _candidate(uid: str, label: str = GPU_CLASS_LABEL, *, best_effort: bool = False):
    return OnDemandCandidate(
        pod_uid=uid,
        pod_name=f"pod-{uid}",
        pod_namespace="alice",
        gpu_class_label=label,
        gpu_requested=1,
        min_runtime_seconds=0 if best_effort else 3600,
        pod_created_at=NOW,
        next_attempt_at=NOW,
        best_effort=best_effort,
    )


def _gated_lines(caplog):
    return [
        kv_fields(r.getMessage())
        for r in caplog.records
        if "event=ondemand.gated" in r.getMessage()
    ]


# ---------------------------------------------------------------------------
# plan_ondemand_gates
# ---------------------------------------------------------------------------


class TestPlanOndemandGates:
    def test_nothing_gated_plans_nothing(self):
        state = ControllerState()
        state.class_node_counts = {GPU_CLASS_LABEL: 3}
        assert state.plan_ondemand_gates(NOW) == []

    def test_overcommit_carries_both_counts_and_skips_best_effort(self):
        state = ControllerState()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}
        state.physical_gpu_capacity = {GPU_CLASS_LABEL: 8}
        state.ondemand_candidates = {
            "a": _candidate("a"),
            "b": _candidate("b", best_effort=True),  # guard 4 does not hold it
            "c": _candidate("c", OTHER_CLASS_LABEL),
        }

        [gate] = state.plan_ondemand_gates(NOW)

        assert (gate.label, gate.guard, gate.reason) == (
            GPU_CLASS_LABEL, 4, "class_overcommitted",
        )
        assert (gate.app_gpus, gate.phys_gpus) == (16, 8)
        assert gate.waiting == 1

    def test_stuck_holder_names_the_pods(self):
        state = ControllerState()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        state.stuck_holder_pods = {GPU_CLASS_LABEL: ["bob.nb-1", "alice.nb-0"]}
        state.ondemand_candidates = {"b": _candidate("b", best_effort=True)}

        [gate] = state.plan_ondemand_gates(NOW)

        assert (gate.guard, gate.reason) == (3, "stuck_holder_interlock")
        assert gate.stuck_pods == ("alice.nb-0", "bob.nb-1")
        assert gate.waiting == 1  # guard 3 does hold best-effort candidates

    def test_no_nodes_only_on_a_known_zero(self):
        # Guard 1b is fail-open: an absent class is unknown and never holds, so
        # warning about it would describe a gate the preflight is not applying.
        state = ControllerState()
        state.class_node_counts = {GPU_CLASS_LABEL: 0}

        [gate] = state.plan_ondemand_gates(NOW)

        assert (gate.label, gate.guard, gate.reason) == (
            GPU_CLASS_LABEL, 1, "no_class_nodes",
        )

    def test_gates_are_per_class_and_guard_and_sorted(self):
        state = ControllerState()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL, OTHER_CLASS_LABEL}
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}

        keys = [(g.label, g.guard) for g in state.plan_ondemand_gates(NOW)]

        assert keys == [
            (OTHER_CLASS_LABEL, 4), (GPU_CLASS_LABEL, 3), (GPU_CLASS_LABEL, 4),
        ]

    def test_since_is_sticky_then_reset_after_clearing(self):
        state = ControllerState()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}

        assert state.plan_ondemand_gates(NOW)[0].since == NOW
        later = NOW + timedelta(minutes=5)
        assert state.plan_ondemand_gates(later)[0].since == NOW

        state.overcommitted_gpu_classes = set()
        assert state.plan_ondemand_gates(later) == []
        assert state.ondemand_gate_since == {}

        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        again = later + timedelta(minutes=1)
        assert state.plan_ondemand_gates(again)[0].since == again


# ---------------------------------------------------------------------------
# _warn_ondemand_gates — the rendered line
# ---------------------------------------------------------------------------


class TestWarnOndemandGates:
    def test_silent_when_nothing_is_gated(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        with caplog.at_level(logging.WARNING, logger="app.main"):
            m._warn_ondemand_gates(ControllerState(), make_config())
        assert _gated_lines(caplog) == []

    def test_overcommit_warning_is_actionable(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}
        state.physical_gpu_capacity = {GPU_CLASS_LABEL: 8}
        state.ondemand_candidates = {"a": _candidate("a")}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            m._warn_ondemand_gates(state, make_config())

        [record] = [r for r in caplog.records if "event=ondemand.gated" in r.getMessage()]
        assert record.levelno == logging.WARNING
        f = kv_fields(record.getMessage())
        assert f["clabel"] == GPU_CLASS_LABEL
        assert f["guard"] == "4"
        assert f["reason"] == "class_overcommitted"
        assert (f["app_gpus"], f["phys_gpus"]) == ("16", "8")
        assert f["candidates"] == "1"
        assert f["dur_s"] == "0"
        assert "pods" not in f
        detail = f["detail"]
        # Says what is paused, both sides of the mismatch, and where to look.
        assert f"gpu-class={GPU_CLASS_LABEL}" in detail
        assert "16 GPUs" in detail and "only 8" in detail
        assert "kubectl" in detail
        assert "QUEUE_PROCESSOR_INTERVAL" in detail

    def test_stuck_holder_warning_lists_pods(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        state.stuck_holder_pods = {GPU_CLASS_LABEL: ["alice.nb-0"]}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            m._warn_ondemand_gates(state, make_config())

        [f] = _gated_lines(caplog)
        assert f["guard"] == "3"
        assert f["pods"] == "alice.nb-0"
        assert f["candidates"] == "0"  # warns even with nobody waiting
        assert "kubectl describe pod" in f["detail"]
        assert "app_gpus" not in f

    def test_best_effort_note_only_when_enabled(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            m._warn_ondemand_gates(state, make_config())
            m._warn_ondemand_gates(state, make_config(best_effort_enabled=True))

        off, on = _gated_lines(caplog)
        assert "Best-effort" not in off["detail"]
        assert "Best-effort" in on["detail"]
        # Unknown counts degrade to prose rather than printing None.
        assert "None" not in off["detail"]


# ---------------------------------------------------------------------------
# Capacity audit keeps the physical side for the warning
# ---------------------------------------------------------------------------


def test_capacity_audit_records_physical_capacity(monkeypatch):
    m = _main_module(monkeypatch)

    async def _snapshot(_key):
        return {GPU_CLASS_LABEL: 8}

    monkeypatch.setattr(m, "snapshot_node_gpu_capacity", _snapshot)
    state = ControllerState()
    state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}

    asyncio.run(m._run_capacity_audit(state, make_config()))

    assert state.physical_gpu_capacity == {GPU_CLASS_LABEL: 8}
    assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}
