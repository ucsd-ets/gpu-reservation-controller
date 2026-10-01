"""Tests for JIT on-demand lease teardown when the backing pod goes away.

A JIT lease exists only to cover one pod (its ``idempotency_key`` is the pod's
UID).  When that pod terminates on its own (Succeeded/Failed), is deleted, or is
preempted, ``pod_watch_loop`` cancels the lease via
``main._teardown_ondemand_lease`` (``reason="pod-terminated"``) instead of
letting it linger active until it expires.

The on-demand-vs-booking distinction is read live off the reservation's ``kind``
field (no in-memory tracking): the pod's ``galends/booking-reference`` annotation
resolves to the lease id, and only ``kind == "on_demand"`` rows are cancelled.
"""

from __future__ import annotations

import asyncio
import os
from types import SimpleNamespace

# app.main runs create_app() at import, which reads these from the environment.
os.environ.setdefault("RESERVATION_API_URL", "http://reservations.local")
os.environ.setdefault("RESERVATION_API_KEY", "gpures_test")

from app.config import Config
from app.controller import ControllerState
from app.k8s_client import make_booking_reference

from tests.conftest import make_config, GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME, reservation


def _config(**overrides) -> Config:
    """Thin alias for the shared builder in tests/conftest."""
    return make_config(**overrides)


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


class _FakeClient:
    def __init__(self, cancel_result=True):
        self.cancel_result = cancel_result
        self.cancel_calls: list = []

    async def cancel_reservation(self, reservation_id, reason):
        self.cancel_calls.append((reservation_id, reason))
        return self.cancel_result


def _pod(uid: str, *, reservation_id: int | None, phase: str = "Running") -> SimpleNamespace:
    annotations = None
    if reservation_id is not None:
        annotations = {"galends/booking-reference": make_booking_reference(reservation_id)}
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid,
            name="pod-" + uid,
            namespace=USERNAME,
            labels={"gpu-class": GPU_CLASS_LABEL},
            annotations=annotations,
        ),
        status=SimpleNamespace(phase=phase, conditions=None),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


def _lease_state(res_id: int, *, kind: str = "on_demand", status: str = "active") -> ControllerState:
    from datetime import datetime, timedelta, timezone

    now = datetime.now(timezone.utc)
    lease = reservation(
        res_id,
        start_utc=now - timedelta(minutes=5),
        end_utc=now + timedelta(minutes=30),
        kind=kind,
        status=status,
        gpu_class_id=GPU_CLASS_ID,
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_count=1,
        username=USERNAME,
    )
    state = ControllerState()
    state.reservations = [lease]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.record_placement(res_id, "uid-1", 1)
    return state


def _run(state, client, events):
    import app.main as main_module

    def _fake_watcher(**kw):
        return _FakeWatcher(events)

    import unittest.mock as mock

    with mock.patch.object(main_module, "PodWatcher", _fake_watcher):
        asyncio.run(main_module.pod_watch_loop(state, client, _config()))


def test_deleted_ondemand_pod_cancels_lease():
    state = _lease_state(500)
    client = _FakeClient()
    _run(state, client, [("DELETED", _pod("uid-1", reservation_id=500))])

    assert client.cancel_calls == [(500, "pod-terminated")]
    assert all(r.id != 500 for r in state.reservations)
    # Occupancy released independently of the cancel.
    assert "uid-1" not in state.occupancy.get(500, {})


def test_terminal_ondemand_pod_cancels_lease():
    state = _lease_state(500)
    client = _FakeClient()
    _run(state, client, [("MODIFIED", _pod("uid-1", reservation_id=500, phase="Succeeded"))])

    assert client.cancel_calls == [(500, "pod-terminated")]
    assert all(r.id != 500 for r in state.reservations)


def test_deleted_booking_pod_does_not_cancel():
    state = _lease_state(500, kind="booking")
    client = _FakeClient()
    _run(state, client, [("DELETED", _pod("uid-1", reservation_id=500))])

    # A user booking's pod ending must never cancel the reservation.
    assert client.cancel_calls == []
    assert any(r.id == 500 for r in state.reservations)


def test_already_cancelled_lease_not_recancelled():
    state = _lease_state(500, status="cancelled")
    client = _FakeClient()
    _run(state, client, [("DELETED", _pod("uid-1", reservation_id=500))])

    assert client.cancel_calls == []


def test_pod_without_booking_reference_no_cancel():
    state = _lease_state(500)
    client = _FakeClient()
    # A pod carrying no booking-reference annotation resolves to no lease id.
    _run(state, client, [("DELETED", _pod("uid-1", reservation_id=None))])

    assert client.cancel_calls == []
    assert any(r.id == 500 for r in state.reservations)


def test_cancel_failure_keeps_lease_in_state():
    state = _lease_state(500)
    client = _FakeClient(cancel_result=False)
    _run(state, client, [("DELETED", _pod("uid-1", reservation_id=500))])

    # The cancel was attempted, but a failed cancel leaves the lease in state
    # so the next poll can reconcile it (best-effort teardown).
    assert client.cancel_calls == [(500, "pod-terminated")]
    assert any(r.id == 500 for r in state.reservations)


# ---------------------------------------------------------------------------
# A released lease starts an admission batch
# ---------------------------------------------------------------------------
#
# Cancelling a lease returns its capacity to the app's calendar, so a candidate
# waiting on that capacity is offered it at once rather than at the next queue
# tick.  Nothing else returns app-side capacity: a booking's pod ending leaves
# the booking holding its window.


def _run_counting_batches(state, client, events) -> list:
    import unittest.mock as mock
    import app.main as main_module

    batches: list = []

    async def _fake_batch(st, cl, cfg):
        batches.append(st)

    with mock.patch.object(main_module, "_run_ondemand_admission", _fake_batch):
        _run(state, client, events)
    return batches


def test_a_deleted_lease_pod_starts_a_batch():
    state = _lease_state(500)
    batches = _run_counting_batches(
        state, _FakeClient(), [("DELETED", _pod("uid-1", reservation_id=500))],
    )
    assert len(batches) == 1


def test_a_finished_lease_pod_starts_a_batch():
    state = _lease_state(500)
    batches = _run_counting_batches(
        state, _FakeClient(),
        [("MODIFIED", _pod("uid-1", reservation_id=500, phase="Failed"))],
    )
    assert len(batches) == 1


def test_a_booking_pod_ending_starts_no_batch():
    state = _lease_state(500, kind="booking")
    batches = _run_counting_batches(
        state, _FakeClient(), [("DELETED", _pod("uid-1", reservation_id=500))],
    )
    assert batches == []


def test_a_cancel_that_did_not_land_starts_no_batch():
    state = _lease_state(500)
    batches = _run_counting_batches(
        state, _FakeClient(cancel_result=False),
        [("DELETED", _pod("uid-1", reservation_id=500))],
    )
    assert batches == []
