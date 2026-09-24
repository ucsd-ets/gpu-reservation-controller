"""A crashed GPU node stops counting as capacity, without anyone cordoning it.

A node that stops heartbeating keeps ``spec.unschedulable`` false and its last
``status.allocatable``, so the node snapshot used to keep counting every GPU on
it.  Once the taint-based eviction deleted its pods (after the default 300 s
``unreachable`` toleration) they sat ``Terminating`` — the kubelet that would
confirm the delete is the thing that is down — and a terminating pod's GPUs
count as free, so the dead node read as *wholly free*.  The preemption sweep
then saw no shortfall at a booking's boundary and left overstayers running
while the booking's pods sat Pending.

The snapshot now drops a NotReady node (``tests/test_k8s_capacity.py``).  That
alone would have opened the mirror-image hole this file also pins, and which
cordoning already had: a pod still bound to an excluded node was subtracted
from capacity that no longer included its node — a phantom shortfall — and
offered as a victim whose death frees nothing placeable.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import app.k8s_client as k8s_client
from app.controller import (
    PodRuntimeView,
    free_capacity_by_class,
    free_gpus_by_node_class,
)

from tests.conftest import GPU_CLASS_LABEL, USERNAME, reservation
from tests.conftest import make_state as _state
from tests.test_k8s_capacity import TAINT_KEY, _FakeCoreV1, _node, _taint
from tests.test_preemption_sweep import (
    S,
    _FakeSelectClient,
    _boundary_reservation,
    _config,
    _main_module,
    _patch_snapshots,
    _pod,
)

NOW = S - timedelta(minutes=10)  # inside the default 15-minute phase-A lead


def _view(
    uid: str,
    *,
    reservation_id: int = 2,
    gpu_count: int = 1,
    node_name: str | None = None,
    node_excluded: bool = False,
    terminating: bool = False,
) -> PodRuntimeView:
    return PodRuntimeView(
        uid=uid,
        namespace=USERNAME,
        name=f"pod-{uid}",
        gpu_class=GPU_CLASS_LABEL,
        gpu_count=gpu_count,
        reservation_id=reservation_id,
        node_resident=True,
        terminating=terminating,
        node_name=node_name,
        node_excluded=node_excluded,
    )


def _ended_reservation(res_id: int = 2):
    """The overstayers' reservation: over two hours before the boundary."""
    return reservation(
        res_id,
        start_utc=S - timedelta(hours=4),
        end_utc=S - timedelta(hours=2),
        gpu_count=1,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
        user_id=1,
    )


# ---------------------------------------------------------------------------
# The pure planners: an excluded node's pods neither occupy nor free capacity
# ---------------------------------------------------------------------------


class TestFreeCapacityIgnoresExcludedNodes:
    def test_pod_on_an_excluded_node_is_not_subtracted(self):
        pods = [
            _view("ok", node_name="gpu-ok"),
            _view("down", node_name="gpu-down", node_excluded=True),
        ]
        assert free_capacity_by_class({GPU_CLASS_LABEL: 2}, pods) == {GPU_CLASS_LABEL: 1}

    def test_unscheduled_pod_still_counts(self):
        """Only a pod bound to an excluded node is skipped — an unscheduled one
        keeps counting as before, since it is about to land somewhere."""
        pods = [_view("pending", node_name=None)]
        assert free_capacity_by_class({GPU_CLASS_LABEL: 2}, pods) == {GPU_CLASS_LABEL: 1}

    def test_class_total_agrees_with_the_per_node_breakdown(self):
        """The invariant the fix restores.  Guard 5's per-node free already
        ignored a pod on a node missing from the inventory; the per-class free
        the sweep uses subtracted it, so the two disagreed by exactly that
        pod's GPUs whenever a node hosting pods was cordoned or down."""
        inventory = {GPU_CLASS_LABEL: {"gpu-ok": 4}}  # gpu-down excluded
        pods = [
            _view("a", gpu_count=2, node_name="gpu-ok"),
            _view("b", gpu_count=4, node_name="gpu-down", node_excluded=True),
        ]
        per_node = free_gpus_by_node_class(inventory, pods)
        per_class = free_capacity_by_class({GPU_CLASS_LABEL: 4}, pods)
        assert per_class[GPU_CLASS_LABEL] == sum(per_node[GPU_CLASS_LABEL].values()) == 2


class TestVictimPoolsSkipExcludedNodes:
    def test_boundary_candidates(self):
        state = _state(_boundary_reservation(1, gpu_count=1), _ended_reservation())
        on_ok = _view("ok", node_name="gpu-ok")
        on_down = _view("down", node_name="gpu-down", node_excluded=True)
        need = state.plan_boundary_candidates(
            S, {GPU_CLASS_LABEL: 1}, [on_ok, on_down], NOW
        )
        assert need.kills_needed_by_class == {GPU_CLASS_LABEL: 1}
        assert need.candidates_by_class[GPU_CLASS_LABEL] == [on_ok]

    def test_headroom_candidates(self):
        state = _state(_ended_reservation())
        on_ok = _view("ok", node_name="gpu-ok")
        on_down = _view("down", node_name="gpu-down", node_excluded=True)
        need = state.plan_headroom_candidates(
            {GPU_CLASS_LABEL: 1}, [on_ok, on_down], NOW, 100,
            require_elapsed_notice=False,
        )
        assert need.kills_needed_by_class == {GPU_CLASS_LABEL: 1}
        assert need.candidates_by_class[GPU_CLASS_LABEL] == [on_ok]

    def test_forecast_and_termination_warning_pool(self):
        """``forecast_boundary_need`` also feeds the termination warnings, so a
        pod the sweep would never kill is neither forecast at risk nor warned."""
        state = _state(_boundary_reservation(1, gpu_count=1), _ended_reservation())
        on_ok = _view("ok", node_name="gpu-ok")
        on_down = _view("down", node_name="gpu-down", node_excluded=True)
        need = state.forecast_boundary_need(
            S, {GPU_CLASS_LABEL: 0}, [on_ok, on_down], NOW,
            {"ok": None, "down": None},
        )
        assert need.eligible_by_class == {GPU_CLASS_LABEL: [on_ok]}


# ---------------------------------------------------------------------------
# main._pod_view marks a pod against the inventory its capacity came from
# ---------------------------------------------------------------------------


class TestPodViewMarksExcludedNodes:
    INVENTORY = {GPU_CLASS_LABEL: {"gpu-ok": 8}, "a100": {"gpu-other": 8}}

    def _mark(self, monkeypatch, node_name, *, inventory=INVENTORY):
        m = _main_module(monkeypatch)
        pod = _pod("p", booking_reference="res-2", reservation_id=2, node_name=node_name)
        return m._pod_view(pod, inventory).node_excluded

    def test_pod_on_a_counted_node(self, monkeypatch):
        assert self._mark(monkeypatch, "gpu-ok") is False

    def test_pod_on_a_node_the_snapshot_dropped(self, monkeypatch):
        assert self._mark(monkeypatch, "gpu-down") is True

    def test_pod_on_a_node_counted_only_for_another_class(self, monkeypatch):
        # Its GPUs are not in its own class's capacity either.
        assert self._mark(monkeypatch, "gpu-other") is True

    def test_unscheduled_pod(self, monkeypatch):
        assert self._mark(monkeypatch, None) is False

    def test_no_inventory_means_not_checked(self, monkeypatch):
        assert self._mark(monkeypatch, "gpu-down", inventory=None) is False


# ---------------------------------------------------------------------------
# The sweep, through the real node snapshot
# ---------------------------------------------------------------------------


def _sweep(monkeypatch, *, nodes, pods, state, client=None):
    """One sweep at NOW with *nodes* behind the real ``snapshot_node_gpu_inventory``."""
    m = _main_module(monkeypatch)
    deleted, _events = _patch_snapshots(monkeypatch, m, pods=pods, inventory={})
    monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))
    monkeypatch.setattr(m, "snapshot_node_gpu_inventory", k8s_client.snapshot_node_gpu_inventory)
    asyncio.run(m._run_preemption_sweep(state, _config(), client, now=NOW))
    return deleted


def _gpu_node(name: str, gpus: int, *, ready: str = "True"):
    return _node(
        name, taints=[_taint(TAINT_KEY, GPU_CLASS_LABEL)],
        allocatable={"nvidia.com/gpu": str(gpus)}, ready=ready,
    )


class TestSweepWithADeadNode:
    def test_a_dead_nodes_evicted_pods_no_longer_read_as_free_capacity(self, monkeypatch):
        """The outage itself.  gpu-down's pod was evicted and sits Terminating;
        before the fix its GPU counted as free, the booking's demand read as
        covered, and the overstayer on gpu-ok was left running."""
        state = _state(_boundary_reservation(1, gpu_count=1), _ended_reservation())
        pods = [
            _pod("v1", booking_reference="res-2", reservation_id=2, node_name="gpu-ok"),
            _pod("evicted", booking_reference="res-2", reservation_id=2,
                 node_name="gpu-down", deletion_timestamp=S - timedelta(minutes=20)),
        ]
        deleted = _sweep(
            monkeypatch, state=state, pods=pods,
            nodes=[_gpu_node("gpu-ok", 1), _gpu_node("gpu-down", 1, ready="Unknown")],
        )
        assert deleted == [(USERNAME, "pod-v1")]

    def test_pods_still_bound_to_a_dead_node_are_no_phantom_shortfall(self, monkeypatch):
        """Before eviction the dead node's pods are still Running as far as the
        API knows.  gpu-ok has a free GPU for the booking; subtracting d1 from
        capacity that no longer includes gpu-down would read that as a
        shortfall and kill someone for nothing."""
        state = _state(_boundary_reservation(1, gpu_count=1), _ended_reservation())
        pods = [
            _pod("v1", booking_reference="res-2", reservation_id=2, node_name="gpu-ok"),
            _pod("d1", booking_reference="res-2", reservation_id=2, node_name="gpu-down"),
        ]
        deleted = _sweep(
            monkeypatch, state=state, pods=pods,
            nodes=[_gpu_node("gpu-ok", 2), _gpu_node("gpu-down", 1, ready="Unknown")],
        )
        assert deleted == []

    def test_a_pod_on_a_dead_node_is_never_the_victim(self, monkeypatch):
        """Killing d1 would free nothing the booking could use (and the delete
        cannot even complete while the kubelet is gone), so the app is never
        offered it.  The fake app names both; only offered pods are killed."""
        state = _state(_boundary_reservation(1, gpu_count=1), _ended_reservation())
        pods = [
            _pod("v1", booking_reference="res-2", reservation_id=2, node_name="gpu-ok"),
            _pod("d1", booking_reference="res-2", reservation_id=2, node_name="gpu-down"),
        ]
        client = _FakeSelectClient(["d1", "v1"])
        deleted = _sweep(
            monkeypatch, state=state, pods=pods, client=client,
            nodes=[_gpu_node("gpu-ok", 1), _gpu_node("gpu-down", 1, ready="Unknown")],
        )
        assert [c.pod_uid for c in client.requests[0].candidates] == ["v1"]
        assert deleted == [(USERNAME, "pod-v1")]

    def test_a_cordoned_node_hosting_pods_is_no_phantom_shortfall_either(self, monkeypatch):
        """The same hole existed for a cordoned node before readiness was
        checked: draining a node for maintenance could preempt overstayers
        elsewhere to cover GPUs that were never short."""
        state = _state(_boundary_reservation(1, gpu_count=1), _ended_reservation())
        pods = [
            _pod("v1", booking_reference="res-2", reservation_id=2, node_name="gpu-ok"),
            _pod("c1", booking_reference="res-2", reservation_id=2, node_name="gpu-drain"),
        ]
        cordoned = _gpu_node("gpu-drain", 1)
        cordoned.spec.unschedulable = True
        deleted = _sweep(
            monkeypatch, state=state, pods=pods,
            nodes=[_gpu_node("gpu-ok", 2), cordoned],
        )
        assert deleted == []
