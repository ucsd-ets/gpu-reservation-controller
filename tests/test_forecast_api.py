"""Endpoint tests for ``GET /api/forecast/preemption-risk``.

Drives the real FastAPI app through ``TestClient`` with the lifespan's
startup I/O stubbed out (same harness as ``tests/test_push_api.py``) and the
two cluster snapshots monkeypatched (same shape as
``tests/test_preemption_sweep.py``).  The risk arithmetic itself is covered
by ``tests/test_forecast.py``; here we exercise auth, snapshot fail-safety,
response shape, and the namespace filter.
"""

from __future__ import annotations

import dataclasses
from datetime import datetime, timedelta, timezone

import pytest
from fastapi.testclient import TestClient

from app.controller import OnDemandCandidate
from app.k8s_client import ToleratedPodInfo

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME, reservation

FORECAST_URL = "/api/forecast/preemption-risk"
AUTH = {"Authorization": "Bearer secret"}


def _patched_main(monkeypatch, token):
    """Import ``app.main`` with startup I/O stubbed and *token* installed."""
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-forecast")
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


def _patch_snapshots(monkeypatch, main, *, pods, capacity):
    """Stub both cluster snapshots; pass an Exception to make one fail."""

    async def _snapshot_pods(_key, _group_label_key=None, _group_label_default=None):
        if isinstance(pods, Exception):
            raise pods
        return pods

    async def _snapshot_inventory(_key):
        if isinstance(capacity, Exception):
            raise capacity
        # One node per class; the pods here are unscheduled, so it is immaterial.
        return {c: {f"{c}-node": n} for c, n in capacity.items()}

    monkeypatch.setattr(main, "snapshot_tolerated_pods", _snapshot_pods)
    monkeypatch.setattr(main, "snapshot_node_gpu_inventory", _snapshot_inventory)


def _holder_pod(uid="uid-1", name="pod-1", reservation_id=1, gpu_count=1):
    return ToleratedPodInfo(
        namespace=USERNAME,
        name=name,
        uid=uid,
        gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{reservation_id}",
        reservation_id=reservation_id,
        gpu_count=gpu_count,
        phase="Running",
        scheduled_false=False,
    )


def _seed_overstay_scenario(state):
    """An overstayed 1-GPU holder plus bob's 4-GPU booking 30 min out.

    Against 4 physical GPUs with 1 in use, the boundary is short 1 GPU over a
    1-GPU eligible pool → the holder's risk reaches 1.0 in whichever bucket
    the kill window (boundary − 15 min … boundary) lands.
    """
    now = datetime.now(timezone.utc)
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.reservations = [
        reservation(
            1,
            start_utc=now - timedelta(hours=2),
            end_utc=now - timedelta(hours=1),
            gpu_count=1,
        ),
        reservation(
            2,
            start_utc=now + timedelta(minutes=30),
            end_utc=now + timedelta(minutes=90),
            username="bob",
            user_id=9,
            gpu_count=4,
        ),
    ]


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------


def test_forecast_disabled_returns_503_when_token_unset(monkeypatch):
    main = _patched_main(monkeypatch, token=None)
    with TestClient(main.app) as client:
        resp = client.get(FORECAST_URL)
    assert resp.status_code == 503


@pytest.mark.parametrize("headers", [
    {},                                   # missing
    {"Authorization": "Bearer wrong"},    # mismatched
    {"Authorization": "secret"},          # no scheme
])
def test_forecast_bad_or_missing_bearer_returns_401(monkeypatch, headers):
    main = _patched_main(monkeypatch, token="secret")
    with TestClient(main.app) as client:
        resp = client.get(FORECAST_URL, headers=headers)
    assert resp.status_code == 401


# ---------------------------------------------------------------------------
# Snapshot fail-safety
# ---------------------------------------------------------------------------


def test_pod_snapshot_failure_returns_503(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    _patch_snapshots(monkeypatch, main, pods=RuntimeError("api down"), capacity={})
    with TestClient(main.app) as client:
        resp = client.get(FORECAST_URL, headers=AUTH)
    assert resp.status_code == 503


def test_capacity_snapshot_failure_returns_503(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    _patch_snapshots(
        monkeypatch, main, pods=[], capacity=RuntimeError("api down")
    )
    with TestClient(main.app) as client:
        resp = client.get(FORECAST_URL, headers=AUTH)
    assert resp.status_code == 503


# ---------------------------------------------------------------------------
# Happy path + namespace filter
# ---------------------------------------------------------------------------


def test_forecast_happy_path_reports_overstay_risk(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    _patch_snapshots(
        monkeypatch, main, pods=[_holder_pod()], capacity={GPU_CLASS_LABEL: 4}
    )
    with TestClient(main.app) as client:
        _seed_overstay_scenario(main.app.state.controller_state)
        resp = client.get(FORECAST_URL, headers=AUTH)

    assert resp.status_code == 200
    body = resp.json()
    assert body["lead_minutes"] == 15
    assert body["selection_delegated"] is True
    assert len(body["buckets"]) == 3
    for bucket in body["buckets"]:
        assert bucket["classes"][GPU_CLASS_LABEL]["capacity"] == 4
        assert bucket["classes"][GPU_CLASS_LABEL]["free"] == 3

    (pod,) = body["pods"]
    assert pod["namespace"] == USERNAME
    assert pod["name"] == "pod-1"
    assert pod["uid"] == "uid-1"
    assert pod["reservation_id"] == 1
    assert pod["guarantee_end"] is not None
    assert [b["state"] for b in pod["buckets"]] == ["overstay"] * 3
    # The boundary 30 min out is short 1 GPU over this pod's 1-GPU pool:
    # certain preemption in whichever bucket its kill window lands.
    assert max(b["risk"] for b in pod["buckets"]) == 1.0
    assert any(
        bucket["classes"][GPU_CLASS_LABEL]["shortfall"] == 1
        for bucket in body["buckets"]
    )


def test_namespace_filter_scopes_pods_but_not_summaries(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    _patch_snapshots(
        monkeypatch, main, pods=[_holder_pod()], capacity={GPU_CLASS_LABEL: 4}
    )
    with TestClient(main.app) as client:
        _seed_overstay_scenario(main.app.state.controller_state)
        mine = client.get(FORECAST_URL, headers=AUTH, params={"namespace": USERNAME})
        other = client.get(FORECAST_URL, headers=AUTH, params={"namespace": "nosuch"})

    assert mine.status_code == other.status_code == 200
    assert [p["uid"] for p in mine.json()["pods"]] == ["uid-1"]
    # Unknown namespace: empty pod list, but the global summaries remain.
    assert other.json()["pods"] == []
    assert len(other.json()["buckets"]) == 3
    assert GPU_CLASS_LABEL in other.json()["buckets"][0]["classes"]


def test_pending_jit_candidates_surface_on_bucket_zero(monkeypatch):
    main = _patched_main(monkeypatch, token="secret")
    _patch_snapshots(monkeypatch, main, pods=[], capacity={GPU_CLASS_LABEL: 4})
    with TestClient(main.app) as client:
        state = main.app.state.controller_state
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        now = datetime.now(timezone.utc)
        state.ondemand_candidates["uid-jit"] = OnDemandCandidate(
            pod_uid="uid-jit",
            pod_name="jit-pod",
            pod_namespace=USERNAME,
            gpu_class_label=GPU_CLASS_LABEL,
            gpu_requested=3,
            min_runtime_seconds=600,
            pod_created_at=now,
            next_attempt_at=now,
        )
        resp = client.get(FORECAST_URL, headers=AUTH)

    assert resp.status_code == 200
    buckets = resp.json()["buckets"]
    assert buckets[0]["classes"][GPU_CLASS_LABEL]["pending_jit_gpus"] == 3
    assert buckets[1]["classes"][GPU_CLASS_LABEL]["pending_jit_gpus"] == 0
    assert buckets[0]["classes"][GPU_CLASS_LABEL]["shortfall"] == 0
