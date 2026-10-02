"""Unit tests for reservation adoption (owner change) detection and eviction.

Covers:
- detect_owner_changed_in_window: conditions that should and should not flag an
  in-progress reservation whose owner changed to a new teammate.
- _handle_owner_changes: evicts only the prior owner's pod (matched by
  reservation id AND prior namespace), releases its capacity, and leaves a pod
  already living in the new owner's namespace untouched.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.controller import ControllerState
from app.k8s_client import ToleratedPodInfo
from tests.conftest import make_config, run_locked, GPU_CLASS_ID, GPU_CLASS_LABEL, reservation


# ---------------------------------------------------------------------------
# Factories
# ---------------------------------------------------------------------------


def _booking(
    res_id: int,
    *,
    user_id: int = 1,
    username: str = "alice",
    status: str = "active",
    kind: str = "booking",
    start_offset_hours: float = -1,
    duration_hours: float = 2,
):
    """A now-relative reservation (open by default: now-1h … now+1h)."""
    now = datetime.now(timezone.utc)
    start = now + timedelta(hours=start_offset_hours)
    end = start + timedelta(hours=duration_hours)
    return reservation(
        res_id,
        start_utc=start,
        end_utc=end,
        kind=kind,
        user_id=user_id,
        username=username,
        status=status,
        gpu_class_label=GPU_CLASS_LABEL,
    )


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-adoption")
    import app.main as main_module

    return main_module


# ---------------------------------------------------------------------------
# detect_owner_changed_in_window
# ---------------------------------------------------------------------------


class TestDetectOwnerChangedInWindow:
    def _state(self, *prior):
        state = ControllerState()
        state.reservations = list(prior)
        return state

    def test_detects_in_window_owner_change(self):
        state = self._state(_booking(1, user_id=1, username="alice"))
        incoming = [_booking(1, user_id=2, username="bob")]
        now = datetime.now(timezone.utc)
        result = state.detect_owner_changed_in_window(incoming, now)
        assert [(r.id, prior) for r, prior in result] == [(1, "alice")]

    def test_same_owner_not_flagged(self):
        state = self._state(_booking(1, user_id=1, username="alice"))
        incoming = [_booking(1, user_id=1, username="alice")]
        now = datetime.now(timezone.utc)
        assert state.detect_owner_changed_in_window(incoming, now) == []

    def test_window_not_yet_open_not_flagged(self):
        state = self._state(_booking(1, user_id=1, username="alice", start_offset_hours=1))
        # Incoming owner differs but the window has not started yet.
        incoming = [_booking(1, user_id=2, username="bob", start_offset_hours=1)]
        now = datetime.now(timezone.utc)
        assert state.detect_owner_changed_in_window(incoming, now) == []

    def test_window_already_ended_not_flagged(self):
        state = self._state(
            _booking(1, user_id=1, username="alice", start_offset_hours=-3, duration_hours=2)
        )
        incoming = [
            _booking(1, user_id=2, username="bob", start_offset_hours=-3, duration_hours=2)
        ]
        now = datetime.now(timezone.utc)
        assert state.detect_owner_changed_in_window(incoming, now) == []

    def test_unknown_id_not_flagged(self):
        state = self._state()  # nothing stored to compare against
        incoming = [_booking(1, user_id=2, username="bob")]
        now = datetime.now(timezone.utc)
        assert state.detect_owner_changed_in_window(incoming, now) == []

    def test_cancelled_incoming_not_flagged(self):
        # A cancellation is handled by the cancellation path, not adoption.
        state = self._state(_booking(1, user_id=1, username="alice"))
        incoming = [_booking(1, user_id=2, username="bob", status="cancelled")]
        now = datetime.now(timezone.utc)
        assert state.detect_owner_changed_in_window(incoming, now) == []

    def test_reclaim_incoming_not_flagged(self):
        # Reclaim blocks carry no user; they can never be adopted.
        state = self._state(_booking(1, user_id=1, username="alice"))
        incoming = [_booking(1, kind="reclaim", user_id=2, username="bob")]
        now = datetime.now(timezone.utc)
        assert state.detect_owner_changed_in_window(incoming, now) == []

    def test_only_changed_ids_returned_in_mixed_batch(self):
        state = self._state(
            _booking(1, user_id=1, username="alice"),
            _booking(2, user_id=3, username="carol"),
        )
        incoming = [
            _booking(1, user_id=2, username="bob"),     # changed
            _booking(2, user_id=3, username="carol"),   # unchanged
        ]
        now = datetime.now(timezone.utc)
        result = state.detect_owner_changed_in_window(incoming, now)
        assert [(r.id, prior) for r, prior in result] == [(1, "alice")]


# ---------------------------------------------------------------------------
# _handle_owner_changes (eviction)
# ---------------------------------------------------------------------------


def _tolerated(namespace, name, uid, res_id):
    return ToleratedPodInfo(
        namespace=namespace, name=name, uid=uid, gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{res_id}", reservation_id=res_id, gpu_count=2,
        phase="Running", scheduled_false=False,
    )


def _install_k8s_stubs(monkeypatch, m, snapshot):
    deleted: list[tuple[str, str]] = []
    events: list[tuple[str, str, str]] = []

    async def _snapshot(_key, _group_label_key=None, _group_label_default=None, class_resources=None):
        return list(snapshot)

    async def _delete(name, ns):
        deleted.append((name, ns))

    async def _read(name, ns):
        return SimpleNamespace()

    async def _emit(pod, name, ns, desc):
        events.append((name, ns, desc))

    monkeypatch.setattr(m, "snapshot_tolerated_pods", _snapshot)
    monkeypatch.setattr(m, "delete_pod", _delete)
    monkeypatch.setattr(m, "read_pod", _read)
    monkeypatch.setattr(m, "emit_reservation_reassigned_event", _emit)
    return deleted, events


def _run_owner_changes(m, state, config, owner_changes):
    """Plan under the lock, execute outside it — the shape the reconcile uses.

    The eviction I/O deliberately runs with the lock released, so a test that
    exercises the whole path has to span both halves.
    """
    async def _go():
        snapshot = await m._snapshot_pods_for_eviction(config)
        async with state.reservation_lock:
            evictions = m._plan_owner_changes(state, owner_changes, snapshot)
        await m._execute_evictions(state, evictions)

    asyncio.run(_go())


class TestHandleOwnerChanges:
    def test_evicts_prior_owner_pod_and_frees_capacity(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("alice", "pod-1", "uid-1", 1)
        deleted, events = _install_k8s_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        new_res = _booking(1, user_id=2, username="bob")
        state.reservations = [new_res]
        state.record_placement(1, "uid-1", 2)  # prior owner's pod occupies it

        _run_owner_changes(m, state, make_config(), [(new_res, "alice")])

        assert deleted == [("pod-1", "alice")]        # prior owner's pod evicted
        assert events == [("pod-1", "alice", "to bob")]
        assert state.occupancy.get(1, {}) == {}       # budget freed for new owner

    def test_leaves_new_owner_pod_untouched(self, monkeypatch):
        m = _main_module(monkeypatch)
        # Only a pod already in the NEW owner's namespace exists (no prior-owner pod).
        new_pod = _tolerated("bob", "pod-2", "uid-2", 1)
        deleted, events = _install_k8s_stubs(monkeypatch, m, [new_pod])

        state = ControllerState()
        new_res = _booking(1, user_id=2, username="bob")
        state.reservations = [new_res]
        state.record_placement(1, "uid-2", 2)

        _run_owner_changes(m, state, make_config(), [(new_res, "alice")])

        assert deleted == []                          # nothing in prior namespace
        assert events == []
        assert state.occupancy.get(1, {}) == {"uid-2": 2}  # new owner's pod kept

    def test_no_pod_for_reservation_is_noop(self, monkeypatch):
        m = _main_module(monkeypatch)
        other = _tolerated("alice", "pod-9", "uid-9", 99)  # different reservation
        deleted, events = _install_k8s_stubs(monkeypatch, m, [other])

        state = ControllerState()
        new_res = _booking(1, user_id=2, username="bob")
        state.reservations = [new_res]

        _run_owner_changes(m, state, make_config(), [(new_res, "alice")])

        assert deleted == []
        assert events == []
