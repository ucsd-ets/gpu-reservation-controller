"""Adopt-before-evict on in-window cancellation (the Continue counterpart).

When the reservation app supersedes a still-running job's reservation via
``POST /api/reservations/{id}/continue``, it pushes the ``superseded``
cancellation together with the replacement booking.  The cancellation handler
must re-link the admitted pod onto that replacement (``POD_ADOPTION_ENABLED``)
instead of evicting it — eviction is only for pods with nowhere to go.

Drives ``main._handle_cancelled_reservations`` directly with monkeypatched
k8s seams (mirroring ``tests/test_adoption.py``'s stub shape).
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.config import Config
from app.controller import ControllerState
from app.k8s_client import ToleratedPodInfo

from tests.conftest import make_config, run_locked, GPU_CLASS_LABEL, USERNAME, reservation

NOW = datetime(2024, 1, 15, 15, 15, tzinfo=timezone.utc)


def _config(**overrides) -> Config:
    """Thin alias for the shared builder in tests/conftest."""
    return make_config(**overrides)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-cancel-adopt")
    import app.main as main_module

    return main_module


def _tolerated(uid: str, res_id: int, *, gpu_count: int = 1) -> ToleratedPodInfo:
    return ToleratedPodInfo(
        namespace=USERNAME, name=f"pod-{uid}", uid=uid, gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{res_id}", reservation_id=res_id,
        gpu_count=gpu_count, phase="Running", scheduled_false=False,
    )


def _cancelled_source(res_id: int) -> "reservation":
    """An in-window reservation the app just cancelled with reason=superseded."""
    return reservation(
        res_id,
        start_utc=NOW - timedelta(hours=1),
        end_utc=NOW + timedelta(hours=1),
        gpu_class_label=GPU_CLASS_LABEL,
        status="cancelled",
        cancelled_by_id=1,
        cancel_reason="superseded",
        kind="on_demand",
        with_user=True,
    )


def _replacement_booking(res_id: int, *, gpu_count: int = 1) -> "reservation":
    """The freshly-minted continue booking, open right now."""
    return reservation(
        res_id,
        start_utc=NOW - timedelta(minutes=1),
        end_utc=NOW + timedelta(hours=2),
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_count=gpu_count,
        kind="booking",
    )


def _install_stubs(monkeypatch, m, snapshot):
    """Stub every k8s seam the handler + adoption path touch; record calls."""
    deleted: list[tuple[str, str]] = []
    cancelled_events: list[tuple[str, str, str]] = []
    patched_refs: list[tuple[str, str]] = []  # (pod name, booking_reference)

    async def _snapshot(_key, _group_label_key=None, _group_label_default=None):
        return list(snapshot)

    async def _delete(name, ns):
        deleted.append((name, ns))

    async def _read(name, ns):
        return SimpleNamespace(status=SimpleNamespace(phase="Running", conditions=None))

    async def _emit_cancelled(pod, name, ns, desc):
        cancelled_events.append((name, ns, desc))

    async def _apply_toleration(name, ns, pod, key, value, booking_reference):
        patched_refs.append((name, booking_reference))

    async def _record_guarantee(name, ns, pod, guaranteed_until, now, reservation, **kw):
        return None

    async def _emit_overstay(pod, name, ns, res_id, guaranteed_until):
        RELINK_EVENTS.append(("OverstayRelinked", res_id, None))

    async def _emit_relinked(pod, name, ns, res_id, guaranteed_until, *,
                             previous_reservation_id, cause):
        RELINK_EVENTS.append(("ReservationRelinked", res_id, cause))

    RELINK_EVENTS.clear()

    monkeypatch.setattr(m, "snapshot_tolerated_pods", _snapshot)
    monkeypatch.setattr(m, "delete_pod", _delete)
    monkeypatch.setattr(m, "read_pod", _read)
    monkeypatch.setattr(m, "emit_reservation_cancelled_event", _emit_cancelled)
    monkeypatch.setattr(m, "apply_toleration", _apply_toleration)
    monkeypatch.setattr(m, "_record_guarantee", _record_guarantee)
    monkeypatch.setattr(m, "emit_overstay_relinked_event", _emit_overstay)
    monkeypatch.setattr(m, "emit_reservation_relinked_event", _emit_relinked)
    return deleted, cancelled_events, patched_refs


# (reason, reservation id, cause) of every re-link Event the stubs saw.
RELINK_EVENTS: list[tuple] = []


def _run_cancellations(m, state, config, cancelled, now):
    """Plan under the lock, execute outside it — the shape the reconcile uses."""
    async def _go():
        snapshot = await m._snapshot_pods_for_eviction(config)
        async with state.reservation_lock:
            evictions = await m._plan_cancelled_reservations(
                state, config, cancelled, now, snapshot
            )
        await m._execute_evictions(state, evictions)

    asyncio.run(_go())


class TestAdoptBeforeEvict:
    def test_superseded_pod_relinks_to_replacement_booking(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        state.gpu_class_labels = {10: GPU_CLASS_LABEL}
        # The caller replaces state.reservations before handling cancellations,
        # so the replacement booking is already the active set.
        state.reservations = [_replacement_booking(2)]
        state.record_placement(1, "uid-1", 1)

        _run_cancellations(m, state, _config(), [_cancelled_source(1)], NOW)

        assert patched_refs == [("pod-uid-1", "res-2")]  # re-linked, not evicted
        assert deleted == []
        assert cancelled_events == []
        assert state.occupancy.get(2, {}).get("uid-1") == 1  # occupancy re-homed
        assert state.occupancy.get(1, {}) == {}
        # Its reservation was replaced mid-window; it was never overstaying.
        assert RELINK_EVENTS == [("ReservationRelinked", 2, "replaced")]

    def test_evicts_when_adoption_disabled(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        state.reservations = [_replacement_booking(2)]
        state.record_placement(1, "uid-1", 1)

        _run_cancellations(
            m, state, _config(pod_adoption_enabled=False), [_cancelled_source(1)], NOW
        )

        assert patched_refs == []
        assert deleted == [("pod-uid-1", USERNAME)]
        assert len(cancelled_events) == 1

    def test_evicts_when_no_replacement_booking(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        state.reservations = []  # nothing to adopt into
        state.record_placement(1, "uid-1", 1)

        _run_cancellations(m, state, _config(), [_cancelled_source(1)], NOW)

        assert patched_refs == []
        assert deleted == [("pod-uid-1", USERNAME)]
        assert len(cancelled_events) == 1

    def test_replacement_without_budget_still_evicts(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _tolerated("uid-1", 1, gpu_count=2)
        deleted, cancelled_events, patched_refs = _install_stubs(monkeypatch, m, [pod])

        state = ControllerState()
        # Replacement has only 1 GPU of budget; the pod needs 2 → no adoption.
        state.reservations = [_replacement_booking(2, gpu_count=1)]
        state.record_placement(1, "uid-1", 2)

        _run_cancellations(m, state, _config(), [_cancelled_source(1)], NOW)

        assert patched_refs == []
        assert deleted == [("pod-uid-1", USERNAME)]
        assert len(cancelled_events) == 1


# ---------------------------------------------------------------------------
# REQUIRED_GROUP_LABEL must reach the cancellation/owner-change snapshots
# ---------------------------------------------------------------------------


class TestSnapshotCarriesTheGroupLabel:
    """Both handlers must capture the configured usage-group pod label.

    ``snapshot_tolerated_pods(key, group_label_key)`` only populates each view's
    ``group_label`` when told which label to read. Three of the five call sites
    passed ``config.required_group_label``; these two did not, so with the
    feature enabled every view carried ``group_label=None``, ``_group_ok``
    rejected every candidate reservation, and the adoption re-link in
    ``_handle_cancelled_reservations`` could never find a booking to carry the
    pod onto — silently turning "carry the job forward" into "evict it". That is
    the path ``POST /api/reservations/{id}/continue`` depends on when it pushes a
    ``superseded`` source alongside its replacement.

    The old stubs took a single argument, so the test doubles encoded the bug and
    could not have caught it.
    """

    def _capture(self, monkeypatch, state, build_handler):
        """Run the handler and return the group_label_key it asked the snapshot for."""
        m = _main_module(monkeypatch)
        seen = {}

        async def _snapshot(key, group_label_key=None, group_label_default=None):
            seen["key"] = key
            seen["group_label_key"] = group_label_key
            seen["group_label_default"] = group_label_default
            return []

        monkeypatch.setattr(m, "snapshot_tolerated_pods", _snapshot)
        # The snapshot is now taken once by the shared helper, before either
        # planner runs — but it must still carry the group label, which is the
        # property this whole class exists to pin.
        asyncio.run(build_handler(m))
        return seen

    def test_cancellation_path_passes_it(self, monkeypatch):
        state = ControllerState()
        config = _config(required_group_label="dsmlp/course")
        seen = self._capture(
            monkeypatch,
            state,
            lambda m: m._snapshot_pods_for_eviction(config),
        )
        assert seen["group_label_key"] == "dsmlp/course"

    def test_owner_change_path_passes_it(self, monkeypatch):
        state = ControllerState()
        config = _config(required_group_label="dsmlp/course")
        seen = self._capture(
            monkeypatch,
            state,
            lambda m: m._snapshot_pods_for_eviction(config),
        )
        assert seen["group_label_key"] == "dsmlp/course"

    def test_none_when_the_feature_is_off(self, monkeypatch):
        """The default configuration is unchanged."""
        state = ControllerState()
        seen = self._capture(
            monkeypatch,
            state,
            lambda m: m._snapshot_pods_for_eviction(_config()),
        )
        assert seen["group_label_key"] is None
