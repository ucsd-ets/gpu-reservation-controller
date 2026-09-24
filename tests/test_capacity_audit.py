"""Tests for the hourly app-side vs physical GPU capacity audit.

Three layers:

- ``controller.reconcile_capacity`` — the pure comparison helper.
- ``main._run_capacity_audit`` — the async loop tick, driven via
  ``asyncio.run`` with ``snapshot_node_gpu_capacity`` monkeypatched at the
  ``app.main`` module level (the convention ``test_preemption_sweep.py`` uses).
- The per-class on-demand pause gate (guard 4) in
  ``main._preflight_ondemand_candidate``, exercised through
  ``main._try_request_lease``.

No real Kubernetes or HTTP calls are made.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from types import SimpleNamespace

from app.controller import ControllerState, OnDemandCandidate, reconcile_capacity
from app.schemas import GpuClassDetail

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    OTHER_CLASS_LABEL,
    USERNAME,
    run_locked,
)

NOW = datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# reconcile_capacity — pure helper
# ---------------------------------------------------------------------------


class TestReconcileCapacity:
    def test_equal_maps_no_diff_no_overcommit(self):
        diffs, over = reconcile_capacity({"h100": 8}, {"h100": 8})
        assert diffs == []
        assert over == set()

    def test_app_over_physical_is_diff_and_overcommit(self):
        diffs, over = reconcile_capacity({"h100": 16}, {"h100": 8})
        assert len(diffs) == 1
        d = diffs[0]
        assert (d.label, d.app_side, d.physical) == ("h100", 16, 8)
        assert d.overcommitted is True
        assert over == {"h100"}

    def test_app_under_physical_is_diff_but_not_overcommit(self):
        diffs, over = reconcile_capacity({"h100": 4}, {"h100": 8})
        assert len(diffs) == 1
        assert diffs[0].overcommitted is False
        assert over == set()

    def test_class_missing_from_physical_treated_as_zero_overcommit(self):
        # App knows the class but no node carries its taint → physical 0.
        diffs, over = reconcile_capacity({"h100": 8}, {})
        assert diffs == [("h100", 8, 0)]
        assert over == {"h100"}

    def test_class_unknown_app_side_never_overcommit(self):
        # Class present only physically (app-side total_gpus unknown): it can
        # surface as a diff but must never be flagged over-committed.
        diffs, over = reconcile_capacity({}, {"h100": 8})
        assert diffs == [("h100", 0, 8)]
        assert over == set()

    def test_mixed_classes(self):
        app_side = {"h100": 16, "a100": 4, "v100": 2}
        physical = {"h100": 8, "a100": 4, "t4": 5}
        diffs, over = reconcile_capacity(app_side, physical)
        by_label = {d.label: d for d in diffs}
        # a100 matches → no diff; h100 over; v100 over (physical 0); t4 unknown app-side
        assert set(by_label) == {"h100", "v100", "t4"}
        assert over == {"h100", "v100"}
        assert by_label["t4"].overcommitted is False


# ---------------------------------------------------------------------------
# GpuClassDetail.audit_gpus — which of the app's two counts the audit reads
# ---------------------------------------------------------------------------


def _detail(**kw) -> GpuClassDetail:
    return GpuClassDetail(id=10, name="H100", label_value=GPU_CLASS_LABEL, **kw)


class TestAuditGpus:
    def test_prefers_the_override_resolved_count(self):
        # A maintenance window halves the class today: total_gpus is still the
        # configured default, but 40 is what the app admits against.
        assert _detail(total_gpus=80, effective_gpus_today=40).audit_gpus == 40

    def test_falls_back_to_total_gpus_when_absent(self):
        # An app predating the field audits exactly as it did before.
        assert _detail(total_gpus=80).audit_gpus == 80

    def test_unknown_when_neither_is_published(self):
        assert _detail().audit_gpus is None

    def test_zero_effective_count_is_not_treated_as_absent(self):
        # A class fully withdrawn for today resolves to 0, which is a real
        # count — not a missing one to fall back from.
        assert _detail(total_gpus=80, effective_gpus_today=0).audit_gpus == 0


# ---------------------------------------------------------------------------
# The population site: what the reconcile records into gpu_class_capacity
# ---------------------------------------------------------------------------


class _ClassListClient:
    """Stub client returning a fixed GPU-class list from the bulk fetch."""

    def __init__(self, classes):
        self._classes = classes

    async def fetch_gpu_classes(self):
        return self._classes

    async def fetch_gpu_class(self, cid):  # pragma: no cover - bulk covers all
        return None


class TestCapacityMapPopulation:
    def _resolve(self, monkeypatch, classes):
        """Resolve the class maps the way a reconcile's caller now does.

        Deliberately *not* under the lock: ``_resolve_gpu_class_maps`` runs
        before it, and that is the property the push endpoint's latency depends
        on.  Calling it with the lock held would still pass, so this also
        documents the intended call shape.
        """
        m = _main_module(monkeypatch)
        empty = m.GpuClassMaps({}, {}, {})
        return asyncio.run(
            m._resolve_gpu_class_maps(_ClassListClient(classes), set(), empty)
        )

    def test_records_the_override_resolved_count(self, monkeypatch):
        # The regression: recording total_gpus here made the audit compare a
        # number the app is not enforcing.  With physical capacity drained to
        # match the override, auditing 80 vs 40 invented a mismatch and paused
        # JIT admission via guard 4 for a class that was not over-committed.
        maps = self._resolve(
            monkeypatch, [_detail(total_gpus=80, effective_gpus_today=40)]
        )
        assert maps.capacity == {GPU_CLASS_LABEL: 40}

        diffs, over = reconcile_capacity(maps.capacity, {GPU_CLASS_LABEL: 40})
        assert diffs == []
        assert over == set()

    def test_falls_back_to_total_gpus(self, monkeypatch):
        assert self._resolve(monkeypatch, [_detail(total_gpus=80)]).capacity == {
            GPU_CLASS_LABEL: 80
        }

    def test_class_with_neither_count_is_left_out(self, monkeypatch):
        maps = self._resolve(monkeypatch, [_detail()])
        assert maps.capacity == {}
        # Still resolvable for matching — only the audit map skips it.
        assert maps.labels == {10: GPU_CLASS_LABEL}

    def test_a_failed_bulk_fetch_keeps_the_prior_maps(self, monkeypatch):
        # The fallback path is the only place the resolver reads anything the
        # caller owns, so it is the one that had to be passed in by value.
        m = _main_module(monkeypatch)

        class _FailingClient:
            async def fetch_gpu_classes(self):
                return None

            async def fetch_gpu_class(self, cid):  # pragma: no cover
                return None

        prior = m.GpuClassMaps(
            {10: GPU_CLASS_LABEL}, {GPU_CLASS_LABEL: 10}, {GPU_CLASS_LABEL: 8}
        )
        maps = asyncio.run(m._resolve_gpu_class_maps(_FailingClient(), set(), prior))
        assert maps == prior
        # Copies, not aliases: mutating the result must not touch the caller's.
        maps.labels[99] = "other"
        assert 99 not in prior.labels


# ---------------------------------------------------------------------------
# main._run_capacity_audit — async tick
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-capacity")
    import app.main as main

    return main


def _patch_capacity(monkeypatch, m, capacity, *, fail=False):
    async def _snapshot(_key):
        if fail:
            raise RuntimeError("apiserver down")
        return capacity

    monkeypatch.setattr(m, "snapshot_node_gpu_capacity", _snapshot)


class TestRunCapacityAudit:
    def test_overcommit_sets_pause_and_warns(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}
        assert any(
            "event=capacity_audit.mismatch" in r.getMessage()
            and "overcommitted=true" in r.getMessage()
            and r.levelno == logging.WARNING
            for r in caplog.records
        )

    def test_matching_capacity_no_pause_no_warn(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 8}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == set()
        assert not any(
            "event=capacity_audit.mismatch" in r.getMessage() for r in caplog.records
        )

    def test_under_provisioned_warns_but_does_not_pause(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 4}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == set()
        assert any(
            "event=capacity_audit.mismatch" in r.getMessage()
            and "overcommitted=false" in r.getMessage()
            for r in caplog.records
        )

    def test_recovery_clears_pause(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}

        # First audit: physical short → class paused.
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 8})
        asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))
        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}

        # Nodes added: physical catches up → class clears automatically.
        _patch_capacity(monkeypatch, m, {GPU_CLASS_LABEL: 16})
        asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))
        assert state.overcommitted_gpu_classes == set()

    def test_snapshot_failure_leaves_pause_set_untouched(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}
        # Pre-existing pause that a transient snapshot failure must not lift.
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        _patch_capacity(monkeypatch, m, {}, fail=True)

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))

        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}
        assert any(
            "event=capacity_audit.snapshot_failed" in r.getMessage()
            and "target=node_capacity" in r.getMessage()
            for r in caplog.records
        )

    def test_only_affected_class_paused(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16, OTHER_CLASS_LABEL: 4}
        _patch_capacity(
            monkeypatch, m, {GPU_CLASS_LABEL: 8, OTHER_CLASS_LABEL: 4}
        )
        asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))
        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}

    def test_a_crashed_node_pauses_the_class(self, monkeypatch):
        """Through the real node snapshot.  A node that stops heartbeating is
        not cordoned and keeps its last allocatable count; only its Ready
        condition moves.  Before the snapshot read it, guard 4 could not see an
        outage at all — only a cordon, a drain or the device plugin lowering
        the count."""
        import app.k8s_client as k8s_client
        from tests.test_k8s_capacity import TAINT_KEY, _FakeCoreV1, _node, _taint

        m = _main_module(monkeypatch)
        nodes = [
            _node(name, taints=[_taint(TAINT_KEY, GPU_CLASS_LABEL)],
                  allocatable={"nvidia.com/gpu": "8"}, ready=ready)
            for name, ready in (("gpu-1", "True"), ("gpu-2", "Unknown"))
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))
        monkeypatch.setattr(
            m, "snapshot_node_gpu_capacity", k8s_client.snapshot_node_gpu_capacity
        )
        state = ControllerState()
        state.gpu_class_capacity = {GPU_CLASS_LABEL: 16}

        asyncio.run(m._run_capacity_audit(state, SimpleNamespace()))
        assert state.physical_gpu_capacity == {GPU_CLASS_LABEL: 8}
        assert state.overcommitted_gpu_classes == {GPU_CLASS_LABEL}


# ---------------------------------------------------------------------------
# Guard 4 — per-class on-demand pause in _preflight_ondemand_candidate
# ---------------------------------------------------------------------------


def _pod(*, uid="uid-1"):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-1", namespace=USERNAME,
            annotations=None, labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(
            phase="Pending",
            conditions=[
                SimpleNamespace(
                    type="PodScheduled", status="False", reason="Unschedulable",
                    message="0/10 nodes are available: 5 Insufficient nvidia.com/gpu.",
                )
            ],
        ),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


def _candidate(uid="uid-1"):
    return OnDemandCandidate(
        pod_uid=uid, pod_name="pod-1", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=1,
        min_runtime_seconds=600,
        pod_created_at=datetime.now(timezone.utc),
        next_attempt_at=datetime.now(timezone.utc),
        group_label=None,
        usage_group="course-a",
    )


class _NoGrantClient:
    """Fails the test if the grant path is ever reached."""

    def __init__(self):
        self.create_requests: list = []

    async def create_ondemand_reservation(self, req):  # pragma: no cover
        self.create_requests.append(req)
        raise AssertionError("grant must not be attempted while class is paused")


def test_overcommitted_class_pauses_ondemand_admission(monkeypatch):
    # ONDEMAND_OVERCOMMIT_FIT=false: the original blanket pause.  The fitting
    # default is covered in test_overcommit_fit.py.
    m = _main_module(monkeypatch)
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}

    candidate = _candidate("uid-1")
    before = candidate.next_attempt_at

    async def fake_read_pod(name, namespace):
        return _pod()

    monkeypatch.setattr(m, "read_pod", fake_read_pod)

    client = _NoGrantClient()
    config = SimpleNamespace(
        ondemand_horizon_minutes=30,
        ondemand_lease_buffer_minutes=10,
        scheduling_gate_name=None,
        ondemand_pause_event_enabled=False,
        ondemand_overcommit_fit=False,
    )
    result = asyncio.run(
        m._try_request_lease(state, client, config, "uid-1", candidate)
    )

    # Paused: not granted, cooled down for a short retry, no lease requested.
    assert result is False
    assert candidate.next_attempt_at > before
    assert client.create_requests == []
    assert state.reservations == []
