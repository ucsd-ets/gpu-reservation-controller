"""Inbound reservation-push API — POST /api/reservations/push.

Two layers of coverage:

* ``apply_push_to_active`` — the pure upsert-by-id merge helper.
* The HTTP endpoint — auth (503 when the token is unset, 401 on a bad bearer),
  an active upsert, and an in-window cancellation that evicts its admitted pod.

This is the first HTTP-surface test in the suite: it drives the real FastAPI app
through ``TestClient`` (so the auth dependency and status codes are exercised),
with the lifespan's network / Kubernetes touch-points stubbed to no-ops so
startup does no I/O.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

from app.controller import apply_push_to_active
from app.k8s_client import ToleratedPodInfo
from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME, block


def _res(res_id: int, *, status: str = "active", **kw):
    """An open (now-1h … now+1h) reservation for merge / endpoint tests."""
    now = datetime.now(timezone.utc)
    return block(res_id, now - timedelta(hours=1), now + timedelta(hours=1),
                 status=status, **kw)


def _json(*reservations):
    return {"reservations": [r.model_dump(mode="json") for r in reservations]}


# ---------------------------------------------------------------------------
# apply_push_to_active (pure)
# ---------------------------------------------------------------------------


class TestApplyPushToActive:
    def test_inserts_new_active_by_id(self):
        out = apply_push_to_active([_res(1)], [_res(2)])
        assert {r.id for r in out} == {1, 2}

    def test_replaces_existing_by_id(self):
        out = apply_push_to_active([_res(1, gpu_count=2)], [_res(1, gpu_count=8)])
        assert len(out) == 1 and out[0].gpu_count == 8

    def test_drops_non_active(self):
        out = apply_push_to_active([_res(1), _res(2)], [_res(1, status="cancelled")])
        assert {r.id for r in out} == {2}

    def test_last_occurrence_of_an_id_wins(self):
        out = apply_push_to_active([], [_res(1, gpu_count=2), _res(1, gpu_count=4)])
        assert len(out) == 1 and out[0].gpu_count == 4

    def test_does_not_mutate_inputs(self):
        current = [_res(1)]
        apply_push_to_active(current, [_res(1, status="cancelled"), _res(2)])
        assert [r.id for r in current] == [1]


# ---------------------------------------------------------------------------
# HTTP endpoint
# ---------------------------------------------------------------------------


def _patched_main(monkeypatch, token):
    """Import ``app.main`` with startup I/O stubbed and *token* installed."""
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-push")
    import app.main as main

    async def _noop(*a, **k):
        return None

    # Neutralise everything the lifespan touches so startup does no I/O.
    monkeypatch.setattr(main, "init_k8s", lambda *a, **k: None)
    monkeypatch.setattr(main, "_refresh_reservations", _noop)
    monkeypatch.setattr(main, "reservation_fetch_loop", _noop)
    monkeypatch.setattr(main, "pod_watch_loop", _noop)
    monkeypatch.setattr(main, "queue_processor_loop", _noop)
    monkeypatch.setattr(main, "preemption_loop", _noop)

    orig = main.app.state.config
    monkeypatch.setattr(
        main.app.state, "config", dataclasses.replace(orig, inbound_api_token=token)
    )
    return main


def test_push_disabled_returns_503_when_token_unset(monkeypatch):
    main = _patched_main(monkeypatch, token=None)
    with TestClient(main.app) as client:
        resp = client.post("/api/reservations/push", json=_json(_res(1)))
    assert resp.status_code == 503


@pytest.mark.parametrize("headers", [
    {},                                   # missing
    {"Authorization": "Bearer wrong"},    # mismatched
    {"Authorization": "secret"},          # no scheme
])
def test_push_bad_or_missing_bearer_returns_401(monkeypatch, headers):
    main = _patched_main(monkeypatch, token="secret")
    with TestClient(main.app) as client:
        resp = client.post("/api/reservations/push", json=_json(_res(1)), headers=headers)
    assert resp.status_code == 401


def test_push_active_upsert_updates_state(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_res(1, gpu_count=2)]
        pushed = _res(1, gpu_count=8)
        new = _res(2, gpu_count=4)

        resp = client.post(
            "/api/reservations/push",
            json=_json(pushed, new),
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body == {"applied": 2, "cancelled": 0, "adopted": 0, "total_active": 2}

        by_id = {r.id: r for r in state.reservations}
        assert by_id.keys() == {1, 2}
        assert by_id[1].gpu_count == 8   # replaced, not duplicated


def test_push_cancellation_evicts_admitted_pod(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")

    pod = ToleratedPodInfo(
        namespace=USERNAME, name="pod-1", uid="uid-1", gpu_class=GPU_CLASS_LABEL,
        booking_reference="res-1", reservation_id=1, gpu_count=2,
        phase="Running", scheduled_false=False,
    )
    deleted: list[tuple[str, str]] = []

    async def _snapshot(_key, _group_label_key=None, _group_label_default=None):
        return [pod]

    async def _delete(name, ns):
        deleted.append((name, ns))

    async def _read(name, ns):
        return SimpleNamespace()

    async def _emit(*a, **k):
        return None

    monkeypatch.setattr(main, "snapshot_tolerated_pods", _snapshot)
    monkeypatch.setattr(main, "delete_pod", _delete)
    monkeypatch.setattr(main, "read_pod", _read)
    monkeypatch.setattr(main, "emit_reservation_cancelled_event", _emit)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_res(1, gpu_count=2)]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.record_placement(1, "uid-1", 2)  # pod occupies the reservation

        resp = client.post(
            "/api/reservations/push",
            json=_json(_res(1, status="cancelled")),
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200
        assert resp.json() == {"applied": 0, "cancelled": 1, "adopted": 0, "total_active": 0}

        assert deleted == [("pod-1", USERNAME)]         # pod evicted
        assert state.occupancy.get(1, {}) == {}         # budget freed
        assert [r.id for r in state.reservations] == [] # dropped from active set


def test_push_supersede_relinks_instead_of_evicting(monkeypatch):
    """The Continue/supersede flow: a pod is carried forward, not killed.

    The app pushes the ``superseded`` cancellation **and** its replacement
    booking in one body.  The eviction planner must run *after* the merged set
    is installed, so ``find_open_booking_for`` can see the replacement — which is
    exactly the invariant the plan/execute split is most likely to break.  Get
    the ordering wrong and a user's running job is deleted instead of re-linked.
    """
    main = _patched_main(monkeypatch, token="secret")

    pod = ToleratedPodInfo(
        namespace=USERNAME, name="pod-1", uid="uid-1", gpu_class=GPU_CLASS_LABEL,
        booking_reference="res-1", reservation_id=1, gpu_count=1,
        phase="Running", scheduled_false=False,
    )
    deleted: list[tuple[str, str]] = []
    relinked: list[tuple[str, str]] = []

    async def _snapshot(_key, _group_label_key=None, _group_label_default=None):
        return [pod]

    async def _delete(name, ns):
        deleted.append((name, ns))

    async def _read(name, ns):
        return SimpleNamespace(status=SimpleNamespace(phase="Running", conditions=None))

    async def _emit(*a, **k):
        return None

    async def _apply_toleration(name, ns, pod_obj, key, value, booking_reference):
        relinked.append((name, booking_reference))

    async def _record_guarantee(*a, **k):
        return None

    monkeypatch.setattr(main, "snapshot_tolerated_pods", _snapshot)
    monkeypatch.setattr(main, "delete_pod", _delete)
    monkeypatch.setattr(main, "read_pod", _read)
    monkeypatch.setattr(main, "emit_reservation_cancelled_event", _emit)
    monkeypatch.setattr(main, "emit_overstay_relinked_event", _emit)
    monkeypatch.setattr(main, "emit_reservation_relinked_event", _emit)
    monkeypatch.setattr(main, "apply_toleration", _apply_toleration)
    monkeypatch.setattr(main, "_record_guarantee", _record_guarantee)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_booking(1, user_id=1, username=USERNAME)]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.record_placement(1, "uid-1", 1)

        # Both entries in one push, as POST /continue sends them: the superseded
        # source and its replacement booking.
        resp = client.post(
            "/api/reservations/push",
            json=_json(
                _booking(1, user_id=1, username=USERNAME, status="cancelled"),
                _booking(2, user_id=1, username=USERNAME),
            ),
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200

        assert deleted == [], "the pod was evicted instead of carried forward"
        assert relinked == [("pod-1", "res-2")]
        assert state.occupancy.get(2, {}) == {"uid-1": 1}  # occupancy re-homed
        assert state.occupancy.get(1, {}) == {}

def _booking(res_id, *, user_id, username, status="active"):
    """An open (now-1h … now+1h) booking reservation with an explicit owner."""
    from tests.conftest import reservation

    now = datetime.now(timezone.utc)
    return reservation(
        res_id,
        start_utc=now - timedelta(hours=1),
        end_utc=now + timedelta(hours=1),
        kind="booking",
        user_id=user_id,
        username=username,
        status=status,
        gpu_class_label=GPU_CLASS_LABEL,
    )


def test_push_owner_change_evicts_prior_owner_pod(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")

    # Prior owner's admitted pod lives in the old namespace ("alice").
    pod = ToleratedPodInfo(
        namespace="alice", name="pod-1", uid="uid-1", gpu_class=GPU_CLASS_LABEL,
        booking_reference="res-1", reservation_id=1, gpu_count=2,
        phase="Running", scheduled_false=False,
    )
    deleted: list[tuple[str, str]] = []
    events: list[str] = []

    async def _snapshot(_key, _group_label_key=None, _group_label_default=None):
        return [pod]

    async def _delete(name, ns):
        deleted.append((name, ns))

    async def _read(name, ns):
        return SimpleNamespace()

    async def _emit(pod_obj, name, ns, desc):
        events.append(desc)

    monkeypatch.setattr(main, "snapshot_tolerated_pods", _snapshot)
    monkeypatch.setattr(main, "delete_pod", _delete)
    monkeypatch.setattr(main, "read_pod", _read)
    monkeypatch.setattr(main, "emit_reservation_reassigned_event", _emit)

    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.reservations = [_booking(1, user_id=1, username="alice")]
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.record_placement(1, "uid-1", 2)  # prior owner's pod occupies it

        resp = client.post(
            "/api/reservations/push",
            json=_json(_booking(1, user_id=2, username="bob")),
            headers={"Authorization": "Bearer secret"},
        )
        assert resp.status_code == 200
        assert resp.json() == {"applied": 1, "cancelled": 0, "adopted": 1, "total_active": 1}

        assert deleted == [("pod-1", "alice")]          # prior owner's pod evicted
        assert events == ["to bob"]
        assert state.occupancy.get(1, {}) == {}         # budget freed for new owner
        by_id = {r.id: r for r in state.reservations}
        assert by_id[1].user.username == "bob"          # ownership transferred
