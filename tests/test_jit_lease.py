"""Unit tests for the JIT (just-in-time) on-demand lease path.

Covers:
- Occupancy accounting and on-demand candidate bookkeeping (pure state logic)
- ``ControllerState.find_admittable_reservation`` — the budget/horizon-aware
  routing gate between the reserved queue and a JIT lease request
- ``main._try_request_lease`` — the async orchestrator: routing re-check,
  guard 1 / guard 3 / guard 5 (per-node feasibility) gating, lease grant →
  admit, denial → cooldown, and admission-failure → compensating cancel

No Kubernetes or HTTP calls are made; ``main`` is imported after setting the
required env vars, since importing it runs ``create_app()`` at module load.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.controller import ControllerState, OnDemandCandidate, slot_start
from app.reservation_client import LeaseAttempt

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, GROUP_NAME, USERNAME, kv_fields
from tests.conftest import make_state as _state
from tests.conftest import ondemand_block as _ondemand_block
from tests.conftest import reservation, user_reservation as _user_reservation


# ---------------------------------------------------------------------------
# Occupancy — record, available, release
# ---------------------------------------------------------------------------


def _state_with_block(block):
    """Return a ControllerState pre-loaded with one reservation."""
    state = ControllerState()
    state.reservations = [block]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


class TestOccupancy:
    def test_available_decreases_on_record(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        assert state.available(block) == 4
        state.record_placement(1, "pod-a", 2)
        assert state.available(block) == 2

    def test_available_increases_on_release(self):
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 2)
        state.release_pod("pod-a")
        assert state.available(block) == 4

    def test_available_excludes_named_uid(self):
        # The reserved path passes exclude_uid so a retry doesn't count itself.
        block = _ondemand_block(1, gpu_count=4)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 2)
        state.record_placement(1, "pod-b", 1)
        assert state.available(block) == 1
        assert state.available(block, exclude_uid="pod-a") == 3

    def test_release_returns_block_id(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        result = state.release_pod("pod-a")
        assert result == 1

    def test_release_unknown_pod_returns_none(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        result = state.release_pod("ghost-pod")
        assert result is None

    def test_multi_tenant_fills_to_limit(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.record_placement(1, "pod-b", 1)
        assert state.available(block) == 0

    def test_multi_tenant_partial_release_allows_new(self):
        block = _ondemand_block(1, gpu_count=2)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.record_placement(1, "pod-b", 1)
        state.release_pod("pod-a")
        assert state.available(block) == 1

    def test_occupancy_cleaned_up_when_empty(self):
        block = _ondemand_block(1, gpu_count=1)
        state = _state_with_block(block)
        state.record_placement(1, "pod-a", 1)
        state.release_pod("pod-a")
        assert 1 not in state.occupancy


# ---------------------------------------------------------------------------
# reconcile_occupancy — rebuild from a live cluster snapshot
# ---------------------------------------------------------------------------


class TestReconcileOccupancy:
    def test_rebuilds_from_snapshot(self):
        state = ControllerState()
        state.reconcile_occupancy(
            [(1, "pod-a", 2), (1, "pod-b", 1), (2, "pod-c", 1)]
        )
        assert state.occupancy == {1: {"pod-a": 2, "pod-b": 1}, 2: {"pod-c": 1}}

    def test_empty_snapshot_clears(self):
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1}}
        state.reconcile_occupancy([])
        assert state.occupancy == {}

    def test_prunes_stale_pod(self):
        # pod-gone is no longer in the snapshot → dropped (self-heal on a missed
        # DELETE event); pod-a survives.
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1, "pod-gone": 1}}
        state.reconcile_occupancy([(1, "pod-a", 1)])
        assert state.occupancy == {1: {"pod-a": 1}}

    def test_overwrites_gpu_count(self):
        state = ControllerState()
        state.occupancy = {1: {"pod-a": 1}}
        state.reconcile_occupancy([(1, "pod-a", 3)])
        assert state.occupancy == {1: {"pod-a": 3}}


# ---------------------------------------------------------------------------
# add_ondemand_candidate / remove_ondemand_candidate
# ---------------------------------------------------------------------------


class TestCandidateManagement:
    def test_add_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert "uid-1" in state.ondemand_candidates
        c = state.ondemand_candidates["uid-1"]
        assert c.gpu_class_label == "h100"
        assert c.min_runtime_seconds == 600
        assert c.group_label is None

    def test_add_candidate_carries_group_label(self):
        state = ControllerState()
        state.add_ondemand_candidate(
            "uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc),
            group_label="CSE151",
        )
        assert state.ondemand_candidates["uid-1"].group_label == "CSE151"

    def test_add_is_idempotent(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert len(state.ondemand_candidates) == 1

    def test_already_placed_pod_not_re_added(self):
        state = ControllerState()
        state.occupancy[42] = {"uid-1": 1}
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_candidate(self):
        state = ControllerState()
        state.add_ondemand_candidate("uid-1", "pod-a", "ns-a", "h100", 1, 600, datetime.now(timezone.utc))
        state.remove_ondemand_candidate("uid-1")
        assert "uid-1" not in state.ondemand_candidates

    def test_remove_nonexistent_is_noop(self):
        state = ControllerState()
        state.remove_ondemand_candidate("ghost")  # should not raise


# ---------------------------------------------------------------------------
# find_admittable_reservation — the JIT routing gate
# ---------------------------------------------------------------------------


class TestFindAdmittableReservation:
    def test_open_now_with_budget_matches(self):
        res = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # mid-window
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is not None and got.id == 1

    def test_budget_full_no_match(self):
        res = _user_reservation(1, slot_index=0, start_time="08:00:00", duration_minutes=120, gpu_count=1)
        state = _state(res)
        state.record_placement(1, "other-pod", 1)  # fills the single GPU
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is None

    def test_within_horizon_not_yet_open_matches(self):
        res = _user_reservation(1, slot_index=0, start_time="10:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 9, 45, tzinfo=timezone.utc)  # opens in 15 min
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is not None and got.id == 1

    def test_beyond_horizon_no_match(self):
        res = _user_reservation(1, slot_index=0, start_time="12:00:00", duration_minutes=120)
        state = _state(res)
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)  # opens in 3h
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is None

    def test_no_match_at_all(self):
        state = _state()
        now = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
        got = state.find_admittable_reservation(
            USERNAME, GPU_CLASS_LABEL, 1, now, timedelta(minutes=30)
        )
        assert got is None


# ---------------------------------------------------------------------------
# main._try_request_lease
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-jit")
    import app.main as main_module

    return main_module


def _pod(*, uid="uid-1", phase="Pending", tolerations=None, conditions=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-1", namespace=USERNAME,
            annotations=None, labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(phase=phase, conditions=conditions),
        spec=SimpleNamespace(
            tolerations=tolerations if tolerations is not None else [],
            containers=[],
            scheduling_gates=None,
        ),
    )


def _gpu_only_condition():
    return SimpleNamespace(
        type="PodScheduled", status="False", reason="Unschedulable",
        message="0/10 nodes are available: 5 Insufficient nvidia.com/gpu.",
    )


def _non_gpu_condition():
    return SimpleNamespace(
        type="PodScheduled", status="False", reason="Unschedulable",
        message="0/10 nodes are available: 5 Insufficient memory.",
    )


def _taint_gated_condition():
    """The verdict a real taint-gated cluster produces for a JIT candidate.

    Every node of the pod's class carries the controller's NoSchedule taint,
    which the candidate does not tolerate yet — and TaintToleration runs ahead
    of NodeResourcesFit, so those nodes are filtered out before any GPU verdict
    exists.  Current kube-scheduler does not name the taint either.
    """
    return SimpleNamespace(
        type="PodScheduled", status="False", reason="Unschedulable",
        message=(
            "0/41 nodes are available: 12 node(s) didn't match Pod's node "
            "affinity/selector, 25 node(s) had untolerated taint(s), 4 node(s) "
            "were unschedulable."
        ),
    )


def _candidate(uid="uid-1", *, gpu_requested=1, min_runtime_seconds=600,
               group_label=None, usage_group=GROUP_NAME):
    return OnDemandCandidate(
        pod_uid=uid, pod_name="pod-1", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=gpu_requested,
        min_runtime_seconds=min_runtime_seconds,
        pod_created_at=datetime.now(timezone.utc),
        next_attempt_at=datetime.now(timezone.utc),
        group_label=group_label,
        usage_group=usage_group,
    )


def _lease(res_id=500, *, gpu_count=1, duration_seconds=1800):
    now = datetime.now(timezone.utc)
    return reservation(
        res_id,
        start_utc=now,
        end_utc=now + timedelta(seconds=duration_seconds),
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
    )


def _state_ready() -> ControllerState:
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    return state


class _FakeClient:
    def __init__(
        self,
        *,
        lease=None,
        leases=None,
        cancel_result=True,
        select_response=None,
        deny_status=409,
    ):
        self._lease = lease
        # Status accompanying a non-grant.  409 is the app's routine denial;
        # pass 403/422 to simulate a fault that waiting cannot fix, and None to
        # simulate the app never answering (network failure).
        self._deny_status = deny_status
        # Per-uid leases (keyed by idempotency_key = pod uid) so a batch grant
        # gives each pod its own reservation id; falls back to ``lease``.
        self._leases = leases or {}
        self.cancel_result = cancel_result
        # Return value for select_ondemand_admissions: a list of granted uids,
        # or None to simulate an unavailable endpoint (fallback to grant-all).
        self._select_response = select_response
        self.create_requests: list = []
        self.cancel_calls: list = []
        self.select_requests: list = []

    async def create_ondemand_reservation(self, req):
        # Mirrors the real signature: a LeaseAttempt, never a bare reservation.
        # A stub shaped around the old return type is how the caller's inability
        # to tell a 409 from a 403 stayed invisible.
        self.create_requests.append(req)
        lease = self._leases.get(req.idempotency_key, self._lease)
        if lease is None:
            return LeaseAttempt(status=self._deny_status)
        return LeaseAttempt(reservation=lease, status=201)

    async def cancel_reservation(self, reservation_id, reason):
        self.cancel_calls.append((reservation_id, reason))
        return self.cancel_result

    async def select_ondemand_admissions(self, req):
        self.select_requests.append(req)
        return self._select_response


def _patch_admission(monkeypatch, m, *, apply_error=None):
    """Wire the k8s-facing calls _try_apply_toleration makes, so only the JIT
    orchestration around it is under test."""
    async def fake_apply(*args, **kwargs):
        if apply_error is not None:
            raise apply_error

    async def fake_annotate(*args, **kwargs):
        pass

    async def fake_emit(*args, **kwargs):
        pass

    monkeypatch.setattr(m, "apply_toleration", fake_apply)
    monkeypatch.setattr(m, "annotate_runtime_guarantee", fake_annotate)
    monkeypatch.setattr(m, "emit_runtime_guaranteed_event", fake_emit)


class TestTryRequestLease:
    def test_grant_admits_pod_and_records_idempotency_key(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(500, gpu_count=1)
        client = _FakeClient(lease=lease)
        state = _state_ready()
        candidate = _candidate("uid-1", min_runtime_seconds=600)

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30,
            ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1
        req = client.create_requests[0]
        assert req.idempotency_key == "uid-1"
        assert req.gpu_class_id == GPU_CLASS_ID
        assert req.duration_seconds == 600 + 10 * 60
        assert any(r.id == 500 for r in state.reservations)
        assert state.occupancy.get(500, {}).get("uid-1") == 1
        assert client.cancel_calls == []

    def test_denial_cools_down_without_touching_reservations(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=None)  # denied
        state = _state_ready()
        candidate = _candidate("uid-1")
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None, ondemand_denial_event_enabled=False,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert candidate.next_attempt_at > before
        assert state.reservations == []
        # A 409 is routine, so nothing escalates.
        assert candidate.lease_error_count == 0

    def _run_lease_attempt(self, m, monkeypatch, client, candidate):
        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        return asyncio.run(
            m._try_request_lease(_state_ready(), client, config, candidate.pod_uid, candidate)
        )

    def test_non_409_escalates_and_backs_off_exponentially(self, monkeypatch, caplog):
        # A read-only service key (403) can never succeed by waiting.  It used
        # to log at INFO and retry at the flat 2-5 min denial cadence forever;
        # now it warns and the delay doubles per consecutive failure.
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=None, deny_status=403)
        candidate = _candidate("uid-1")

        delays = []
        for expected_count in (1, 2, 3):
            with caplog.at_level(logging.WARNING, logger=m.log.name):
                caplog.clear()
                before = datetime.now(timezone.utc)
                assert self._run_lease_attempt(m, monkeypatch, client, candidate) is False
            assert candidate.lease_error_count == expected_count
            delays.append((candidate.next_attempt_at - before).total_seconds())

            record = next(
                r for r in caplog.records if "event=lease.error" in r.getMessage()
            )
            assert record.levelno == logging.WARNING
            fields = kv_fields(record.getMessage())
            assert fields["status"] == "403"
            # Parsed, not substring-matched: "fails=1" in the message is also
            # true of fails=12.
            assert fields["fails"] == str(expected_count)

        # Each attempt waits at least as long as the previous one, and the
        # growth is real rather than jitter (the floor doubles: 120 → 240 → 480).
        assert delays[1] >= 2 * 120
        assert delays[2] >= 4 * 120

    def test_error_backoff_is_capped(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=None, deny_status=403)
        candidate = _candidate("uid-1")
        candidate.lease_error_count = 40  # far past the cap

        before = datetime.now(timezone.utc)
        assert self._run_lease_attempt(m, monkeypatch, client, candidate) is False
        delay = (candidate.next_attempt_at - before).total_seconds()
        assert delay <= m.ERROR_RETRY_CAP_SECONDS + 1

    def test_a_grant_clears_a_previous_error_streak(self, monkeypatch):
        m = _main_module(monkeypatch)
        _patch_admission(monkeypatch, m)
        client = _FakeClient(lease=_lease(501))
        candidate = _candidate("uid-1")
        candidate.lease_error_count = 3  # a fault the operator has since fixed

        assert self._run_lease_attempt(m, monkeypatch, client, candidate) is True
        assert candidate.lease_error_count == 0

    def test_admission_failure_issues_compensating_cancel(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(501)
        client = _FakeClient(lease=lease)
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m, apply_error=RuntimeError("transient patch failure"))

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        # Transient failure, pod not terminal -> candidate kept for retry.
        assert result is False
        assert client.cancel_calls == [(501, "controller-revoked")]
        assert all(r.id != 501 for r in state.reservations)

    def test_pod_terminal_after_grant_cancels_and_drops_candidate(self, monkeypatch):
        m = _main_module(monkeypatch)
        lease = _lease(502)
        client = _FakeClient(lease=lease)
        state = _state_ready()
        candidate = _candidate("uid-1")

        # First read_pod (top-of-function liveness check) sees Pending; the
        # second (inside _try_apply_toleration) sees the pod finished in the
        # meantime.
        calls = {"n": 0}

        async def fake_read_pod(name, namespace):
            calls["n"] += 1
            if calls["n"] == 1:
                return _pod(conditions=[_gpu_only_condition()])
            return _pod(phase="Succeeded")

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True  # terminal pod -> candidate dropped
        assert client.cancel_calls == [(502, "controller-revoked")]
        assert all(r.id != 502 for r in state.reservations)

    def test_gone_pod_drops_candidate_without_requesting_lease(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(600))
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(phase="Succeeded")

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert client.create_requests == []

    def test_guard1_non_gpu_constraint_drops_candidate(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(601))
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_non_gpu_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True  # dropped: our toleration cannot help
        assert client.create_requests == []
        # A definite (non-GPU) verdict is not "awaiting the scheduler".
        assert candidate.awaiting_schedule_signal is False

    def test_guard1_indeterminate_short_retries(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(602))
        state = _state_ready()
        candidate = _candidate("uid-1")
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=None)  # no PodScheduled condition yet

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert candidate.next_attempt_at > before
        assert client.create_requests == []
        # Indeterminate guard-1 parks the candidate on the scheduler's verdict;
        # the flag lets a subsequent MODIFIED re-attempt it immediately.
        assert candidate.awaiting_schedule_signal is True

    def test_guard1_taint_gated_verdict_grants(self, monkeypatch):
        """The regression this guard was rewritten for.

        A candidate whose only reported blocker is an anonymous untolerated
        taint used to classify indeterminate forever — the scheduler can never
        say "Insufficient nvidia.com/gpu" about nodes it rejected on our taint
        — so no lease was ever requested on a correctly taint-gated cluster.
        """
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(604))
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_taint_gated_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1
        # A rendered verdict is not "awaiting the scheduler".
        assert candidate.awaiting_schedule_signal is False

    def test_guard1_no_taint_bucket_drops_candidate(self, monkeypatch):
        """Nothing was rejected on a taint, so a toleration changes nothing."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(605))
        state = _state_ready()
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[SimpleNamespace(
                type="PodScheduled", status="False", reason="Unschedulable",
                message=(
                    "0/5 nodes are available: 5 node(s) didn't match Pod's "
                    "node affinity/selector."
                ),
            )])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True  # dropped
        assert client.create_requests == []

    def test_guard1b_drained_class_short_retries(self, monkeypatch):
        """A class with no schedulable nodes gets no lease — but is not dropped."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(606))
        state = _state_ready()
        state.class_node_counts = {GPU_CLASS_LABEL: 0}
        candidate = _candidate("uid-1")
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_taint_gated_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False  # held, retried later — nodes come back
        assert candidate.next_attempt_at > before
        assert client.create_requests == []

    def test_guard1b_unknown_class_fails_open(self, monkeypatch):
        """No node data yet must never wedge admission (matches guard 5)."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(607))
        state = _state_ready()
        state.class_node_counts = {}  # startup, or a failed snapshot
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_taint_gated_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1

    def test_guard1b_populated_class_grants(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(608))
        state = _state_ready()
        state.class_node_counts = {GPU_CLASS_LABEL: 1}
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_taint_gated_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1

    def test_guard3_stuck_holder_interlock_short_retries(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(603))
        state = _state_ready()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        candidate = _candidate("uid-1")
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None, ondemand_pause_event_enabled=False,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert candidate.next_attempt_at > before
        assert client.create_requests == []
        # The scheduler *has* rendered a GPU-only verdict; the candidate is held
        # by the interlock, not awaiting the scheduler — so the flag stays False
        # and its (jittered) backoff is never short-circuited by a MODIFIED.
        assert candidate.awaiting_schedule_signal is False

    def test_guard5_multi_gpu_no_single_node_holds(self, monkeypatch):
        """A >=2-GPU pod is held when no single node can host it, even though the
        class has budget in aggregate — no SU-charged lease is minted."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(605, gpu_count=2))
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 1}  # largest single-node free = 1
        candidate = _candidate("uid-1", gpu_requested=2)
        before = candidate.next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert candidate.next_attempt_at > before
        assert client.create_requests == []

    def test_guard5_multi_gpu_single_node_fits_proceeds(self, monkeypatch):
        """When a single node has enough free GPUs, the multi-GPU lease is granted."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(606, gpu_count=2))
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 2}
        candidate = _candidate("uid-1", gpu_requested=2)

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1
        assert client.create_requests[0].gpu_count == 2

    def test_guard5_single_gpu_pod_not_gated(self, monkeypatch):
        """Guard 5 only applies to >=2-GPU asks; a 1-GPU pod is never held by it,
        even when the per-node map shows zero free (the 1-GPU path is unchanged)."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(607, gpu_count=1))
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 0}
        candidate = _candidate("uid-1", gpu_requested=1)

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1

    def test_guard5_unknown_class_fails_open(self, monkeypatch):
        """No per-node data for the class (absent from the map) must not block —
        a stale/empty map never wedges multi-GPU admission."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(608, gpu_count=2))
        state = _state_ready()
        state.node_free_by_class = {}  # unknown
        candidate = _candidate("uid-1", gpu_requested=2)

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert len(client.create_requests) == 1

    def test_unresolved_gpu_class_id_retries_without_requesting(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(604))
        state = ControllerState()  # gpu_class_ids left empty
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is False
        assert client.create_requests == []

    def test_routing_recheck_prefers_admittable_reservation_over_lease(self, monkeypatch):
        """If a reservation has become admittable since the candidate was
        queued, route to the reserved path instead of requesting a lease."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(605))
        now = datetime.now(timezone.utc)
        open_res = reservation(
            9,
            start_utc=now - timedelta(minutes=10),
            end_utc=now + timedelta(hours=1),
            gpu_count=2,
            gpu_class_label=GPU_CLASS_LABEL,
            username=USERNAME,
        )
        state = _state_ready()
        state.reservations = [open_res]
        candidate = _candidate("uid-1")

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        config = SimpleNamespace(
            ondemand_horizon_minutes=30, ondemand_lease_buffer_minutes=10,
            scheduling_gate_name=None,
        )
        result = asyncio.run(m._try_request_lease(state, client, config, "uid-1", candidate))

        assert result is True
        assert client.create_requests == []  # no lease requested
        assert state.occupancy.get(9, {}).get("uid-1") == 1  # admitted under the reservation


# ---------------------------------------------------------------------------
# main._run_ondemand_admission (batch, app-delegated selection)
# ---------------------------------------------------------------------------


def _admission_config(*, placement=True, delegate=True):
    return SimpleNamespace(
        ondemand_lease_enabled=placement,
        ondemand_delegate_admission=delegate,
        ondemand_horizon_minutes=30,
        ondemand_lease_buffer_minutes=10,
        scheduling_gate_name=None,
    )


def _ready_state_with_candidates(uids):
    state = _state_ready()
    for uid in uids:
        state.ondemand_candidates[uid] = _candidate(uid)
    return state


class TestRunOndemandAdmission:
    def test_delegation_grants_only_selected_uids(self, monkeypatch):
        m = _main_module(monkeypatch)
        # Distinct lease per pod so occupancy does not collide.
        leases = {"uid-1": _lease(701), "uid-2": _lease(702), "uid-3": _lease(703)}
        client = _FakeClient(leases=leases, select_response=["uid-1", "uid-3"])
        state = _ready_state_with_candidates(["uid-1", "uid-2", "uid-3"])
        deferred_before = state.ondemand_candidates["uid-2"].next_attempt_at

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        asyncio.run(m._run_ondemand_admission(state, client, _admission_config()))

        # Exactly one selection call, offering all three.
        assert len(client.select_requests) == 1
        offered = {c.pod_uid for c in client.select_requests[0].candidates}
        assert offered == {"uid-1", "uid-2", "uid-3"}
        # Only granted pods got a lease + occupancy and were removed.
        assert {r.idempotency_key for r in client.create_requests} == {"uid-1", "uid-3"}
        assert state.occupancy.get(701, {}).get("uid-1") == 1
        assert state.occupancy.get(703, {}).get("uid-3") == 1
        assert "uid-1" not in state.ondemand_candidates
        assert "uid-3" not in state.ondemand_candidates
        # The deferred pod is kept and cooled down for a later tick.
        assert "uid-2" in state.ondemand_candidates
        assert state.ondemand_candidates["uid-2"].next_attempt_at > deferred_before

    def test_none_response_falls_back_to_grant_all(self, monkeypatch):
        m = _main_module(monkeypatch)
        leases = {"uid-1": _lease(711), "uid-2": _lease(712)}
        client = _FakeClient(leases=leases, select_response=None)  # endpoint unavailable
        state = _ready_state_with_candidates(["uid-1", "uid-2"])

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        asyncio.run(m._run_ondemand_admission(state, client, _admission_config()))

        # The app was asked, but on None every offered candidate is granted.
        assert len(client.select_requests) == 1
        assert {r.idempotency_key for r in client.create_requests} == {"uid-1", "uid-2"}
        assert state.ondemand_candidates == {}

    def test_flag_off_grants_all_without_calling_app(self, monkeypatch):
        m = _main_module(monkeypatch)
        leases = {"uid-1": _lease(721), "uid-2": _lease(722)}
        client = _FakeClient(leases=leases, select_response=["uid-1"])  # would only grant 1
        state = _ready_state_with_candidates(["uid-1", "uid-2"])

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        asyncio.run(
            m._run_ondemand_admission(state, client, _admission_config(delegate=False))
        )

        # Delegation off: the app is never consulted and everyone is granted.
        assert client.select_requests == []
        assert {r.idempotency_key for r in client.create_requests} == {"uid-1", "uid-2"}
        assert state.ondemand_candidates == {}

    def test_empty_response_grants_none(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(731), select_response=[])  # spare all
        state = _ready_state_with_candidates(["uid-1", "uid-2"])
        before = {u: c.next_attempt_at for u, c in state.ondemand_candidates.items()}

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        asyncio.run(m._run_ondemand_admission(state, client, _admission_config()))

        # Empty grant is respected: no leases, both candidates kept + cooled down.
        assert client.create_requests == []
        assert set(state.ondemand_candidates) == {"uid-1", "uid-2"}
        for uid, cand in state.ondemand_candidates.items():
            assert cand.next_attempt_at > before[uid]

    def test_unknown_granted_uid_is_ignored(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(
            leases={"uid-1": _lease(741)}, select_response=["uid-1", "ghost-uid"]
        )
        state = _ready_state_with_candidates(["uid-1"])

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        asyncio.run(m._run_ondemand_admission(state, client, _admission_config()))

        # Only the offered uid is created; the phantom uid never becomes a create.
        assert {r.idempotency_key for r in client.create_requests} == {"uid-1"}
        assert state.ondemand_candidates == {}

    def test_preflight_drop_removes_candidate_and_omits_from_offer(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(leases={"uid-1": _lease(751)}, select_response=["uid-1"])
        state = _ready_state_with_candidates(["uid-1", "gone-uid"])
        # Drive read_pod by pod name: the gone pod reads terminal (Succeeded).
        state.ondemand_candidates["gone-uid"].pod_name = "gone-pod"

        async def routing_read_pod(name, namespace):
            if name == "gone-pod":
                return _pod(phase="Succeeded")
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", routing_read_pod)
        _patch_admission(monkeypatch, m)

        asyncio.run(m._run_ondemand_admission(state, client, _admission_config()))

        # The terminal pod is dropped before the offer; only uid-1 is offered.
        assert len(client.select_requests) == 1
        assert {c.pod_uid for c in client.select_requests[0].candidates} == {"uid-1"}
        assert "gone-uid" not in state.ondemand_candidates
        assert state.ondemand_candidates == {}

    def test_placement_disabled_is_a_noop(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(761), select_response=["uid-1"])
        state = _ready_state_with_candidates(["uid-1"])

        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)

        asyncio.run(
            m._run_ondemand_admission(state, client, _admission_config(placement=False))
        )

        assert client.select_requests == []
        assert client.create_requests == []
        assert "uid-1" in state.ondemand_candidates  # untouched

    def test_reentrant_trigger_coalesces_into_one_trailing_run(self, monkeypatch):
        """A trigger arriving while a batch runs sets the rerun flag and returns
        immediately; exactly one trailing pass runs afterwards."""
        m = _main_module(monkeypatch)
        client = _FakeClient(lease=_lease(771), select_response=[])
        state = _ready_state_with_candidates(["uid-1"])

        reentered = {"count": 0}

        async def fake_read_pod(name, namespace):
            # Re-enter once while the lock is held: the nested call must not
            # start its own batch, only request a rerun.
            if reentered["count"] == 0:
                reentered["count"] += 1
                await m._run_ondemand_admission(state, client, _admission_config())
                assert state.ondemand_rerun_requested is True
            return _pod(conditions=[_gpu_only_condition()])

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        _patch_admission(monkeypatch, m)

        asyncio.run(m._run_ondemand_admission(state, client, _admission_config()))

        # The nested trigger did not launch a parallel batch; the outer loop ran
        # the trailing pass and cleared the flag.
        assert state.ondemand_rerun_requested is False


# ---------------------------------------------------------------------------
# pod_watch_loop: MODIFIED-driven fast re-attempt on the scheduler's verdict
# ---------------------------------------------------------------------------


class _FakeWatcher:
    """Yields a fixed list of (event_type, pod) events, then stops."""

    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


def _watch_config():
    """Minimal config for driving pod_watch_loop (only the attributes it reads)."""
    return SimpleNamespace(
        ondemand_horizon_minutes=30,
        required_group_label=None,
        ondemand_lease_enabled=True,
        scheduling_gate_name=None,
        best_effort_enabled=False,
        default_min_runtime_seconds=0,
        default_usage_group=None,
    )


def _pending_jit_pod(*, uid="uid-1", conditions=None):
    """A Pending, JIT-eligible pod: gpu-class label + min-runtime / usage-group
    annotations, no toleration.  *conditions* drives is_gpu_gated_pending."""
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-1", namespace=USERNAME,
            annotations={
                "galends/minimum-runtime-seconds": "600",
                "galends/usage-group": GROUP_NAME,
            },
            labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(phase="Pending", conditions=conditions),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


class TestScheduleSignalFastPath:
    """The MODIFIED carrying the scheduler's verdict re-attempts a parked JIT
    candidate immediately, instead of waiting for a periodic scan."""

    def _run_with(self, monkeypatch, m, state, events):
        """Drive pod_watch_loop over *events*, capturing _run_ondemand_admission
        calls (which are otherwise wired to the network)."""
        calls: list = []

        async def fake_admission(st, cl, cfg):
            calls.append((st, cl, cfg))

        monkeypatch.setattr(m, "_run_ondemand_admission", fake_admission)
        monkeypatch.setattr(m, "PodWatcher", lambda **kw: _FakeWatcher(events))
        asyncio.run(m.pod_watch_loop(state, None, _watch_config()))
        return calls

    def test_verdict_modified_retriggers_parked_candidate(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        candidate = _candidate("uid-1")
        candidate.awaiting_schedule_signal = True
        candidate.next_attempt_at = datetime.now(timezone.utc) + timedelta(minutes=5)
        state.ondemand_candidates["uid-1"] = candidate
        before = candidate.next_attempt_at

        # The scheduler has now recorded a GPU-only Unschedulable verdict.
        events = [("MODIFIED", _pending_jit_pod(conditions=[_gpu_only_condition()]))]
        calls = self._run_with(monkeypatch, m, state, events)

        assert len(calls) == 1  # batch was kicked
        assert candidate.awaiting_schedule_signal is False  # flag cleared
        assert candidate.next_attempt_at < before  # phantom cooldown reset to now

    def test_unflagged_candidate_is_not_retriggered(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        candidate = _candidate("uid-1")
        candidate.awaiting_schedule_signal = False  # e.g. parked on a denial backoff
        cooldown = datetime.now(timezone.utc) + timedelta(minutes=5)
        candidate.next_attempt_at = cooldown
        state.ondemand_candidates["uid-1"] = candidate

        events = [("MODIFIED", _pending_jit_pod(conditions=[_gpu_only_condition()]))]
        calls = self._run_with(monkeypatch, m, state, events)

        assert calls == []  # no batch: an unflagged candidate is left alone
        assert candidate.next_attempt_at == cooldown  # backoff untouched

    def test_verdict_still_absent_does_not_retrigger(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        candidate = _candidate("uid-1")
        candidate.awaiting_schedule_signal = True
        state.ondemand_candidates["uid-1"] = candidate

        # A reconcile MODIFIED with no PodScheduled verdict yet (conditions=None
        # → is_gpu_gated_pending returns None): keep waiting, do not kick a batch.
        events = [("MODIFIED", _pending_jit_pod(conditions=None))]
        calls = self._run_with(monkeypatch, m, state, events)

        assert calls == []
        assert candidate.awaiting_schedule_signal is True  # still parked

    def test_modified_for_untracked_pod_is_ignored(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()  # no candidates tracked

        events = [("MODIFIED", _pending_jit_pod(conditions=[_gpu_only_condition()]))]
        calls = self._run_with(monkeypatch, m, state, events)

        assert calls == []
