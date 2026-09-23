"""Unit tests for merging a JIT on-demand lease's pod into an open booking.

Covers the pure planning surface ``ControllerState.plan_ondemand_merges`` — the
proactive counterpart to ``plan_pod_adoptions``.  Scenario: a user starts a pod
*before* their booked window opens, so the controller admits it under a
just-in-time ``kind="on_demand"`` lease; once the user's matching **booking**
window opens, the pod should be re-linked onto the booking (and the lease
retired) **without** waiting for the lease guarantee to lapse.

The two intended differences from ``plan_pod_adoptions`` are exercised here:

  - the pod's current reservation must be a ``kind="on_demand"`` lease, and
  - it merges even while the pod is still **within** its lease guarantee.

Budget checks reuse ``find_open_booking_for`` (checks budget, not equal
``gpu_count``), so a pod using **fewer** GPUs than the booking reserved still
merges.  All pure / no I/O — the async orchestrator (``main._merge_ondemand_into
_bookings``) is not exercised here.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.config import Config
from app.controller import PodRuntimeView

from tests.conftest import make_config, run_locked, GPU_CLASS_LABEL, USERNAME
from tests.conftest import make_state as _state
from tests.conftest import reservation


def _config(**overrides) -> Config:
    """Thin alias for the shared builder in tests/conftest."""
    return make_config(**overrides)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-ondemand-merge")
    import app.main as main_module

    return main_module


class _FakeClient:
    """Records cancel_reservation calls; ``ok`` controls the return value."""

    def __init__(self, ok: bool = True):
        self.ok = ok
        self.cancels: list[tuple[int, str]] = []

    async def cancel_reservation(self, reservation_id: int, reason: str) -> bool:
        self.cancels.append((reservation_id, reason))
        return self.ok


def _install_merge_stubs(monkeypatch, m):
    """Stub the k8s seams the orchestrator touches; record the re-link patches."""
    patched: list[tuple[str, str]] = []

    async def _read(name, ns):
        return SimpleNamespace(status=SimpleNamespace(phase="Running", conditions=None))

    async def _apply_toleration(name, ns, pod, key, value, booking_reference):
        patched.append((name, booking_reference))

    async def _record_guarantee(name, ns, pod, guaranteed_until, now, reservation, **kw):
        return None

    async def _emit_overstay(pod, name, ns, res_id, guaranteed_until):
        RELINK_EVENTS.append(("OverstayRelinked", res_id, None))

    async def _emit_relinked(pod, name, ns, res_id, guaranteed_until, *,
                             previous_reservation_id, cause):
        RELINK_EVENTS.append(("ReservationRelinked", res_id, cause))

    RELINK_EVENTS.clear()
    monkeypatch.setattr(m, "read_pod", _read)
    monkeypatch.setattr(m, "apply_toleration", _apply_toleration)
    monkeypatch.setattr(m, "_record_guarantee", _record_guarantee)
    monkeypatch.setattr(m, "emit_overstay_relinked_event", _emit_overstay)
    monkeypatch.setattr(m, "emit_reservation_relinked_event", _emit_relinked)
    return patched


# (reason, reservation id, cause) of every re-link Event the stubs saw.
RELINK_EVENTS: list[tuple] = []

# 15:15 UTC — "now".
NOW = datetime(2024, 1, 15, 15, 15, tzinfo=timezone.utc)
TWO_PM = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
HALF_PAST = datetime(2024, 1, 15, 15, 30, tzinfo=timezone.utc)  # lease end (still open)
THREE_PM = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)
FIVE_PM = datetime(2024, 1, 15, 17, 0, tzinfo=timezone.utc)


def _view(
    uid: str,
    *,
    gpu_class: str = GPU_CLASS_LABEL,
    gpu_count: int = 1,
    reservation_id: int | None = None,
    node_resident: bool = True,
    terminating: bool = False,
    namespace: str = USERNAME,
) -> PodRuntimeView:
    return PodRuntimeView(
        uid=uid,
        namespace=namespace,
        name=f"pod-{uid}",
        gpu_class=gpu_class,
        gpu_count=gpu_count,
        reservation_id=reservation_id,
        node_resident=node_resident,
        terminating=terminating,
    )


def _lease(
    res_id: int = 100,
    *,
    start_utc: datetime = TWO_PM,
    end_utc: datetime = HALF_PAST,
    gpu_count: int = 1,
):
    """A JIT on-demand lease (open at NOW, so the pod is within its guarantee)."""
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="on_demand",
        with_user=True,
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
    )


def _open_booking(
    res_id: int = 200,
    *,
    start_utc: datetime = THREE_PM,
    end_utc: datetime = FIVE_PM,
    gpu_count: int = 1,
):
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="booking",
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
    )


class TestPlanOndemandMerges:
    def test_lease_pod_merges_into_open_booking(self):
        state = _state(_lease(100), _open_booking(200))
        pod = _view("p1", reservation_id=100)
        plan = state.plan_ondemand_merges([pod], NOW)
        assert len(plan) == 1
        assigned_pod, target = plan[0]
        assert assigned_pod.uid == "p1" and target.id == 200

    def test_within_guarantee_still_merges(self):
        # The lease window (14:00–15:30) is still open at NOW, so the pod is
        # WITHIN its guarantee.  plan_pod_adoptions would leave it alone; the
        # merge planner takes it anyway — the whole point of the feature.
        state = _state(_lease(100), _open_booking(200))
        pod = _view("p1", reservation_id=100)
        assert state.guarantee_end(100, now=NOW) > NOW  # sanity: within guarantee
        assert state.plan_pod_adoptions([pod], NOW) == []
        assert len(state.plan_ondemand_merges([pod], NOW)) == 1

    def test_fewer_gpus_than_booking_still_merges(self):
        # Pod uses 1 GPU; the booking reserved 2 — differing gpu_count that
        # chaining could never bridge, but find_open_booking_for is budget-based.
        state = _state(_lease(100, gpu_count=1), _open_booking(200, gpu_count=2))
        pod = _view("p1", reservation_id=100, gpu_count=1)
        plan = state.plan_ondemand_merges([pod], NOW)
        assert len(plan) == 1 and plan[0][1].id == 200

    def test_booking_pod_not_merged(self):
        # A pod already on a booking (not a lease) is left untouched.
        state = _state(_open_booking(200), _open_booking(300))
        pod = _view("p1", reservation_id=200)
        assert state.plan_ondemand_merges([pod], NOW) == []

    def test_no_open_booking_leaves_lease_pod(self):
        state = _state(_lease(100))  # no booking to merge into
        pod = _view("p1", reservation_id=100)
        assert state.plan_ondemand_merges([pod], NOW) == []

    def test_booking_not_yet_open_no_merge(self):
        # Pre-booking usage: the booking opens in the future, so the pod stays on
        # its lease (correctly) until the window opens.
        future = _open_booking(200, start_utc=NOW + timedelta(minutes=30))
        state = _state(_lease(100), future)
        pod = _view("p1", reservation_id=100)
        assert state.plan_ondemand_merges([pod], NOW) == []

    def test_terminating_pod_skipped(self):
        state = _state(_lease(100), _open_booking(200))
        pod = _view("p1", reservation_id=100, terminating=True)
        assert state.plan_ondemand_merges([pod], NOW) == []

    def test_unknown_reservation_not_merged(self):
        # Pod's reservation is not in the active set: the controller can't confirm
        # it is an on-demand lease, so the merge planner leaves it to adoption.
        state = _state(_open_booking(200))
        pod = _view("p1", reservation_id=999)
        assert state.plan_ondemand_merges([pod], NOW) == []

    def test_budget_respected_across_pods(self):
        state = _state(_lease(100), _open_booking(200, gpu_count=1))
        p1 = _view("p1", reservation_id=100)
        p2 = _view("p2", reservation_id=100)
        plan = state.plan_ondemand_merges([p1, p2], NOW)
        # One GPU of budget on the booking → only one pod merges.
        assert len(plan) == 1

    def test_pass_spreads_across_bookings_rather_than_packing_the_largest(self):
        # Mirrors the adoption-pass regression: two equally-ending bookings, one
        # 4-GPU and one 2-GPU, with three 1-GPU lease pods.  The tie-break must
        # follow the shrinking spare capacity so both bookings are used;
        # comparing the untouched `available` packed the larger one every time.
        state = _state(
            _lease(100),
            _open_booking(200, gpu_count=4),
            _open_booking(201, gpu_count=2),
        )
        pods = [_view(f"p{i}", reservation_id=100) for i in range(3)]

        plan = state.plan_ondemand_merges(pods, NOW)

        assert len(plan) == 3
        assert {target.id for _, target in plan} == {200, 201}


# ---------------------------------------------------------------------------
# Async orchestrator: main._merge_ondemand_into_bookings / _drain_pending_merge_cancels
# ---------------------------------------------------------------------------


class TestMergeOrchestrator:
    def test_merge_relinks_and_retires_lease(self, monkeypatch):
        m = _main_module(monkeypatch)
        patched = _install_merge_stubs(monkeypatch, m)
        state = _state(_lease(100), _open_booking(200))
        state.record_placement(100, "p1", 1)
        pod = _view("p1", reservation_id=100)
        client = _FakeClient(ok=True)

        run_locked(
            state, m._merge_ondemand_into_bookings(state, client, _config(), [pod], NOW)
        )

        assert patched == [("pod-p1", "res-200")]        # re-linked to the booking
        assert client.cancels == [(100, "superseded")]   # lease retired, penalty-exempt
        assert state.occupancy.get(200, {}).get("p1") == 1  # occupancy re-homed
        assert state.occupancy.get(100, {}) == {}
        assert all(r.id != 100 for r in state.reservations)  # lease dropped locally
        assert not state.pending_ondemand_merge_cancels
        # A merge does not wait for the lease guarantee to lapse, so the pod was
        # never overstaying and must not be told it was.
        assert RELINK_EVENTS == [("ReservationRelinked", 200, "merge")]

    def test_failed_cancel_parks_for_retry(self, monkeypatch):
        m = _main_module(monkeypatch)
        _install_merge_stubs(monkeypatch, m)
        state = _state(_lease(100), _open_booking(200))
        state.record_placement(100, "p1", 1)
        pod = _view("p1", reservation_id=100)
        client = _FakeClient(ok=False)

        run_locked(
            state, m._merge_ondemand_into_bookings(state, client, _config(), [pod], NOW)
        )

        # The pod is still safely re-homed onto the booking; only the lease
        # cancel failed → parked for retry and the lease kept until it lands.
        assert state.occupancy.get(200, {}).get("p1") == 1
        assert 100 in state.pending_ondemand_merge_cancels
        assert any(r.id == 100 for r in state.reservations)

    def test_disabled_flag_no_merge(self, monkeypatch):
        m = _main_module(monkeypatch)
        patched = _install_merge_stubs(monkeypatch, m)
        state = _state(_lease(100), _open_booking(200))
        state.record_placement(100, "p1", 1)
        pod = _view("p1", reservation_id=100)
        client = _FakeClient(ok=True)

        run_locked(
            state,
            m._merge_ondemand_into_bookings(
                state, client, _config(ondemand_merge_enabled=False), [pod], NOW
            ),
        )

        assert patched == [] and client.cancels == []

    def test_drain_retries_parked_cancel(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_lease(100), _open_booking(200))
        state.pending_ondemand_merge_cancels.add(100)
        client = _FakeClient(ok=True)

        asyncio.run(m._drain_pending_merge_cancels(state, client))

        assert client.cancels == [(100, "superseded")]
        assert 100 not in state.pending_ondemand_merge_cancels
        assert all(r.id != 100 for r in state.reservations)
