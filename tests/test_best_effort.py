"""Unit tests for best-effort admission (``galends/runtime-guarantee: none``).

A pod declaring it needs no runtime guarantee is admitted under a zero-length,
zero-SU ``kind="best_effort"`` reservation rather than a guaranteed JIT lease.
The point of that representation is that almost nothing downstream needed a new
branch: the stub's window is already over, so the *existing* ``guarantee_end``
path concludes the pod is past guarantee and the preemption planner treats it as
a candidate from its first tick.

What *did* change is admission, and that is most of what is covered here:

- the annotation readers (including the ``0`` minimum-runtime case that used to
  disqualify a pod silently -- the bug this feature grew out of)
- routing: a best-effort pod is JIT-eligible with no minimum runtime
- guard 4 is not applied, and guard 5 is applied at **every** GPU count with an
  in-batch tally, because nothing app-side bounds best-effort admission
- the create request built, and the ``BestEffortAdmitted`` Event that replaces a
  degenerate ``RuntimeGuaranteed`` one

Plus the two places the stub must *not* be specially handled, pinned so a later
change cannot quietly reintroduce a guarantee for it.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.controller import ControllerState, PodRuntimeView
from app.k8s_client import (
    get_pod_min_runtime_seconds,
    get_pod_runtime_guarantee_request,
)
from app.reservation_client import LeaseAttempt
from app.schemas import BestEffortReservationRequest, OnDemandReservationRequest

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, GROUP_NAME, USERNAME
from tests.conftest import reservation
from tests.test_jit_lease import (
    _candidate,
    _FakeClient,
    _gpu_only_condition,
    _main_module,
    _patch_admission,
    _state_ready,
)


# ---------------------------------------------------------------------------
# Annotation readers
# ---------------------------------------------------------------------------


def _annotated(**annotations):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid="uid-1", name="pod-1", namespace=USERNAME,
            annotations=annotations or None,
            labels={"gpu-class": GPU_CLASS_LABEL},
        ),
    )


class TestRuntimeGuaranteeAnnotation:
    def test_none_is_recognised(self):
        assert get_pod_runtime_guarantee_request(
            _annotated(**{"galends/runtime-guarantee": "none"})
        ) == "none"

    def test_value_is_case_and_space_insensitive(self):
        assert get_pod_runtime_guarantee_request(
            _annotated(**{"galends/runtime-guarantee": "  NONE  "})
        ) == "none"

    def test_absent_is_none(self):
        assert get_pod_runtime_guarantee_request(_annotated()) is None

    def test_unrecognised_value_is_none_and_warns(self, caplog):
        with caplog.at_level(logging.WARNING):
            result = get_pod_runtime_guarantee_request(
                _annotated(**{"galends/runtime-guarantee": "best-effort"})
            )
        assert result is None
        assert "pod.annotation_invalid" in caplog.text
        assert "unrecognised_value" in caplog.text


class TestZeroMinimumRuntimeNowSaysSo:
    """The original defect: a ``0`` disqualified the pod with nothing logged.

    It still returns ``None`` -- the floor is what makes the annotation mean
    "run me for at least this long" -- but it now names the annotation that
    *does* express the intent, at WARNING.
    """

    def test_zero_is_rejected_with_a_warning_naming_the_alternative(self, caplog):
        pod = _annotated(**{"galends/minimum-runtime-seconds": "0"})
        with caplog.at_level(logging.WARNING):
            assert get_pod_min_runtime_seconds(pod) is None
        assert "pod.annotation_invalid" in caplog.text
        assert "not_positive" in caplog.text
        assert "galends/runtime-guarantee" in caplog.text

    def test_negative_is_rejected_the_same_way(self, caplog):
        pod = _annotated(**{"galends/minimum-runtime-seconds": "-100"})
        with caplog.at_level(logging.WARNING):
            assert get_pod_min_runtime_seconds(pod) is None
        assert "not_positive" in caplog.text

    def test_junk_still_reports_a_parse_failure_not_a_floor_failure(self, caplog):
        pod = _annotated(**{"galends/minimum-runtime-seconds": "soon"})
        with caplog.at_level(logging.WARNING):
            assert get_pod_min_runtime_seconds(pod) is None
        assert "not_an_integer" in caplog.text

    def test_a_positive_value_is_unaffected_and_silent(self, caplog):
        pod = _annotated(**{"galends/minimum-runtime-seconds": "600"})
        with caplog.at_level(logging.WARNING):
            assert get_pod_min_runtime_seconds(pod) == 600
        assert caplog.text == ""


# ---------------------------------------------------------------------------
# The stub needs no special handling downstream
# ---------------------------------------------------------------------------


def _stub(res_id=700, *, gpu_count=1):
    """A zero-length best-effort reservation, as the app returns one."""
    now = datetime.now(timezone.utc)
    r = reservation(
        res_id,
        start_utc=now,
        end_utc=now,
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
    )
    r.kind = "best_effort"
    return r


def _view(uid="uid-1", *, res_id=700, gpu_count=1):
    return PodRuntimeView(
        uid=uid, namespace=USERNAME, name="pod-1", gpu_class=GPU_CLASS_LABEL,
        gpu_count=gpu_count, reservation_id=res_id,
        node_resident=True, terminating=False, node_name="node-a",
    )


class TestStubIsPreemptibleThroughTheOrdinaryPath:
    def test_guarantee_end_is_already_past(self):
        state = ControllerState()
        stub = _stub()
        state.reservations = [stub]
        now = datetime.now(timezone.utc) + timedelta(seconds=1)
        assert state.guarantee_end(stub.id, now=now) <= now

    def test_pod_is_past_guarantee_from_the_first_tick(self):
        # _past_guarantee is the single predicate victim eligibility, headroom
        # and adoption all share -- so this one assertion is what makes a
        # best-effort pod preemptible everywhere, with no best-effort branch.
        state = ControllerState()
        state.reservations = [_stub()]
        assert state._past_guarantee(_view(), datetime.now(timezone.utc)) is True

    def test_it_survives_the_stub_being_dropped_from_the_active_set(self):
        # The app omits best_effort rows from the poll, so the stub vanishes on
        # the next fetch. guarantee_end then resolves to None -- which reaches
        # the same conclusion, and must, or the pod's status would flap.
        state = ControllerState()
        state.reservations = []
        assert state.guarantee_end(700, now=datetime.now(timezone.utc)) is None
        assert state._past_guarantee(_view(), datetime.now(timezone.utc)) is True

    def test_a_stub_is_not_preserved_across_a_fetch(self):
        # preserve_local_ondemand_leases bridges replication lag for a *lease*
        # whose pod is still inside its guarantee. A stub has none to protect.
        state = ControllerState()
        state.reservations = [_stub()]
        preserved = state.preserve_local_ondemand_leases(
            fetched=[], now=datetime.now(timezone.utc)
        )
        assert preserved == []

    def test_a_stub_creates_no_preemption_boundary(self):
        # Boundaries come from kind == "booking" only, so a stub cannot make the
        # sweep preempt someone else on its behalf.
        state = ControllerState()
        state.reservations = [_stub()]
        now = datetime.now(timezone.utc)
        assert state.upcoming_boundaries(now, timedelta(minutes=15)) == []

    def test_merge_leaves_it_to_adoption(self):
        # plan_ondemand_merges is for a lease whose guarantee has *not* lapsed;
        # a stub's already has, so adoption handles it on the same tick.
        state = ControllerState()
        state.reservations = [_stub()]
        assert state.plan_ondemand_merges([_view()], datetime.now(timezone.utc)) == []


# ---------------------------------------------------------------------------
# Routing
# ---------------------------------------------------------------------------


def _best_effort_pod(*, uid="uid-1", min_runtime=None, conditions=None):
    annotations = {"galends/usage-group": GROUP_NAME,
                   "galends/runtime-guarantee": "none"}
    if min_runtime is not None:
        annotations["galends/minimum-runtime-seconds"] = min_runtime
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-1", namespace=USERNAME,
            annotations=annotations, labels={"gpu-class": GPU_CLASS_LABEL},
            creation_timestamp=datetime.now(timezone.utc),
        ),
        status=SimpleNamespace(phase="Pending", conditions=conditions),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


def _watch_config(**overrides):
    base = dict(
        ondemand_horizon_minutes=30,
        required_group_label=None,
        ondemand_lease_enabled=True,
        scheduling_gate_name=None,
        best_effort_enabled=True,
        default_min_runtime_seconds=0,
        default_usage_group=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _route(monkeypatch, m, state, pod, config):
    async def fake_admission(st, cl, cfg):
        pass

    monkeypatch.setattr(m, "_run_ondemand_admission", fake_admission)
    monkeypatch.setattr(m, "PodWatcher", lambda **kw: _FakeWatcher([("ADDED", pod)]))
    asyncio.run(m.pod_watch_loop(state, None, config))
    return state.ondemand_candidates


class TestRouting:
    def test_a_pod_with_no_minimum_runtime_becomes_a_candidate(self, monkeypatch):
        # The whole point: this pod had no way to be admitted before.
        m = _main_module(monkeypatch)
        state = _state_ready()
        candidates = _route(
            monkeypatch, m, state, _best_effort_pod(), _watch_config()
        )
        assert candidates["uid-1"].best_effort is True
        assert candidates["uid-1"].min_runtime_seconds == 0

    def test_a_zero_minimum_runtime_no_longer_disqualifies_it(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        candidates = _route(
            monkeypatch, m, state, _best_effort_pod(min_runtime="0"), _watch_config()
        )
        assert candidates["uid-1"].best_effort is True

    def test_the_flag_off_leaves_the_pod_pending(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        candidates = _route(
            monkeypatch, m, state, _best_effort_pod(),
            _watch_config(best_effort_enabled=False),
        )
        assert candidates == {}

    def test_a_group_is_still_required(self, monkeypatch):
        # group_name is a required natural key on the app's create, so a pod
        # naming no group cannot be admitted however little it asks for.
        m = _main_module(monkeypatch)
        state = _state_ready()
        pod = _best_effort_pod()
        pod.metadata.annotations.pop("galends/usage-group")
        candidates = _route(monkeypatch, m, state, pod, _watch_config())
        assert candidates == {}

    def test_a_declared_runtime_is_still_carried(self, monkeypatch):
        # The two annotations compose: a pod may name a runtime and still waive
        # the guarantee. The runtime just sizes nothing on this path.
        m = _main_module(monkeypatch)
        state = _state_ready()
        candidates = _route(
            monkeypatch, m, state, _best_effort_pod(min_runtime="600"), _watch_config()
        )
        assert candidates["uid-1"].best_effort is True
        assert candidates["uid-1"].min_runtime_seconds == 600


# ---------------------------------------------------------------------------
# Guards
# ---------------------------------------------------------------------------


def _preflight_config(**overrides):
    base = dict(
        ondemand_horizon_minutes=30,
        ondemand_lease_buffer_minutes=10,
        scheduling_gate_name=None,
        ondemand_denial_event_enabled=False,
        ondemand_pause_event_enabled=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _run_preflight(monkeypatch, m, state, candidate, claimed=None):
    async def fake_read_pod(name, namespace):
        return _best_effort_pod(conditions=[_gpu_only_condition()])

    monkeypatch.setattr(m, "read_pod", fake_read_pod)
    return asyncio.run(
        m._preflight_ondemand_candidate(
            state, _preflight_config(), "uid-1", candidate, claimed
        )
    )


def _be_candidate(uid="uid-1", *, gpu_requested=1):
    c = _candidate(uid, gpu_requested=gpu_requested, min_runtime_seconds=0)
    c.best_effort = True
    return c


class TestGuardFourExemption:
    def test_an_overcommitted_class_does_not_block_best_effort(self, monkeypatch):
        # Overcommit means the *app's* count exceeds physical capacity. A stub
        # consumes no app-side count, so the mismatch says nothing about it.
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        state.node_free_by_class = {GPU_CLASS_LABEL: 4}
        status, ask = _run_preflight(monkeypatch, m, state, _be_candidate())
        assert status == m._PREFLIGHT_READY
        assert ask is not None

    def test_it_still_blocks_a_guaranteed_lease(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        state.node_free_by_class = {GPU_CLASS_LABEL: 4}
        status, _ = _run_preflight(monkeypatch, m, state, _candidate("uid-1"))
        assert status == m._PREFLIGHT_RETRY


class TestGuardFiveAtEveryGpuCount:
    def test_a_single_gpu_ask_is_held_when_no_gpu_is_free(self, monkeypatch):
        # This is the only physical bound on best-effort admission: the app
        # cannot see other best-effort pods, so nothing else stops the cluster
        # being oversubscribed one free GPU at a time.
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 0}
        status, _ = _run_preflight(monkeypatch, m, state, _be_candidate())
        assert status == m._PREFLIGHT_RETRY

    def test_a_single_gpu_lease_is_unaffected(self, monkeypatch):
        # The guaranteed path is bounded app-side, so it keeps the >=2 rule.
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 0}
        status, _ = _run_preflight(monkeypatch, m, state, _candidate("uid-1"))
        assert status == m._PREFLIGHT_READY

    def test_it_fails_open_on_an_unknown_class(self, monkeypatch):
        # A snapshot gap must never wedge admission (matching guard 1b/5).
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {}
        status, _ = _run_preflight(monkeypatch, m, state, _be_candidate())
        assert status == m._PREFLIGHT_READY

    def test_a_free_gpu_admits(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 1}
        status, _ = _run_preflight(monkeypatch, m, state, _be_candidate())
        assert status == m._PREFLIGHT_READY

    def test_the_batch_tally_is_netted_off(self, monkeypatch):
        # One GPU free, one already claimed earlier in this batch -> nothing
        # left. Without the tally both candidates would clear the same opening.
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 1}
        status, _ = _run_preflight(
            monkeypatch, m, state, _be_candidate(), {GPU_CLASS_LABEL: 1}
        )
        assert status == m._PREFLIGHT_RETRY

    def test_a_tally_for_another_class_does_not_interfere(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 1}
        status, _ = _run_preflight(
            monkeypatch, m, state, _be_candidate(), {"other": 4}
        )
        assert status == m._PREFLIGHT_READY


class TestTheAskCarriesTheAnnotation:
    """The posture reaches app-side admission policy with no new field.

    ``get_pod_galends_annotations`` forwards the whole ``galends/`` namespace
    with the admission ask, so a policy deciding *which* waiting pods to admit
    can see that a candidate asked for no guarantee -- and price that however it
    likes -- without the ask needing a best-effort flag of its own.
    """

    def test_runtime_guarantee_none_is_forwarded(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 4}
        _, ask = _run_preflight(monkeypatch, m, state, _be_candidate())
        assert ask.pod_annotations["galends/runtime-guarantee"] == "none"

    def test_the_candidates_own_fifo_key_is_carried(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 4}
        candidate = _be_candidate()
        _, ask = _run_preflight(monkeypatch, m, state, candidate)
        assert ask.pod_created_at == candidate.pod_created_at


class TestTheAskCarriesNoDuration:
    def test_duration_is_zero(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        state.node_free_by_class = {GPU_CLASS_LABEL: 4}
        _, ask = _run_preflight(monkeypatch, m, state, _be_candidate())
        assert ask.duration_seconds == 0
        assert ask.gpu_class_id == GPU_CLASS_ID
        assert ask.group_name == GROUP_NAME


# ---------------------------------------------------------------------------
# Grant + admit
# ---------------------------------------------------------------------------


class _BestEffortClient(_FakeClient):
    """``_FakeClient`` plus the best-effort create, recorded separately."""

    def __init__(self, **kw):
        super().__init__(**kw)
        self.best_effort_requests: list = []

    async def create_best_effort_reservation(self, req):
        self.best_effort_requests.append(req)
        lease = self._leases.get(req.idempotency_key, self._lease)
        if lease is None:
            return LeaseAttempt(status=self._deny_status)
        return LeaseAttempt(reservation=lease, status=201)


def _grant(monkeypatch, m, state, client, candidate, ask):
    _patch_admission(monkeypatch, m)
    monkeypatch.setattr(m, "emit_best_effort_admitted_event", _noop)

    async def fake_read_pod(name, namespace):
        return _best_effort_pod(conditions=[_gpu_only_condition()])

    monkeypatch.setattr(m, "read_pod", fake_read_pod)
    return asyncio.run(
        m._grant_and_admit(
            state, client, _preflight_config(), candidate.pod_uid, candidate, ask
        )
    )


async def _noop(*args, **kwargs):
    pass


def _ask(duration_seconds=0):
    from app.schemas import OnDemandAdmissionCandidate

    return OnDemandAdmissionCandidate(
        pod_uid="uid-1", username=USERNAME, group_name=GROUP_NAME,
        gpu_class_id=GPU_CLASS_ID, gpu_count=1, duration_seconds=duration_seconds,
        # Evidence the app weighs alongside the ask; carried here so the shape
        # matches what _preflight_ondemand_candidate really builds.
        pod_created_at=datetime.now(timezone.utc),
        pod_annotations={"galends/runtime-guarantee": "none"},
    )


class TestGrant:
    def test_it_posts_the_best_effort_shape_not_a_lease(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        client = _BestEffortClient(lease=_stub())
        assert _grant(monkeypatch, m, state, client, _be_candidate(), _ask()) is True
        assert client.create_requests == []          # no lease was requested
        assert len(client.best_effort_requests) == 1
        req = client.best_effort_requests[0]
        assert isinstance(req, BestEffortReservationRequest)
        assert req.idempotency_key == "uid-1"
        assert req.best_effort is True
        assert not hasattr(req, "duration_seconds")

    def test_a_guaranteed_candidate_still_posts_a_lease(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        client = _BestEffortClient(lease=_stub())
        candidate = _candidate("uid-1")
        assert _grant(monkeypatch, m, state, client, candidate, _ask(1800)) is True
        assert client.best_effort_requests == []
        assert isinstance(client.create_requests[0], OnDemandReservationRequest)

    def test_the_pod_is_admitted_under_the_stub(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        stub = _stub(res_id=701)
        client = _BestEffortClient(lease=stub)
        _grant(monkeypatch, m, state, client, _be_candidate(), _ask())
        assert state.occupancy.get(701, {}).get("uid-1") == 1
        assert any(r.id == 701 for r in state.reservations)

    def test_a_denial_cools_down_like_any_other(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        client = _BestEffortClient(lease=None, deny_status=409)
        candidate = _be_candidate()
        before = candidate.next_attempt_at
        assert _grant(monkeypatch, m, state, client, candidate, _ask()) is False
        assert candidate.next_attempt_at > before
        assert candidate.lease_error_count == 0
        assert state.reservations == []

    def test_a_non_retryable_fault_backs_off_exponentially(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state_ready()
        client = _BestEffortClient(lease=None, deny_status=403)
        candidate = _be_candidate()
        assert _grant(monkeypatch, m, state, client, candidate, _ask()) is False
        assert candidate.lease_error_count == 1


# ---------------------------------------------------------------------------
# Admission Event and annotations
# ---------------------------------------------------------------------------


class TestAdmissionEvent:
    def _record(self, monkeypatch, m, res):
        calls = {"annotate": [], "guaranteed": [], "best_effort": []}

        async def fake_annotate(pod_name, namespace, seconds, until, facts, **kw):
            calls["annotate"].append((seconds, until, facts))

        async def fake_guaranteed(*args, **kwargs):
            calls["guaranteed"].append(args)

        async def fake_best_effort(*args, **kwargs):
            calls["best_effort"].append(args)

        monkeypatch.setattr(m, "annotate_runtime_guarantee", fake_annotate)
        monkeypatch.setattr(m, "emit_runtime_guaranteed_event", fake_guaranteed)
        monkeypatch.setattr(m, "emit_best_effort_admitted_event", fake_best_effort)
        now = datetime.now(timezone.utc)
        asyncio.run(
            m._record_guarantee(
                "pod-1", USERNAME, _best_effort_pod(),
                guaranteed_until=now, now=now, reservation=res,
                first_admission=True,
            )
        )
        return calls

    def test_best_effort_gets_its_own_event(self, monkeypatch):
        # Not RuntimeGuaranteed: that message would read "guaranteed for 0m00s,
        # until <the instant the pod started>" to the one person who reads it.
        m = _main_module(monkeypatch)
        calls = self._record(monkeypatch, m, _stub())
        assert len(calls["best_effort"]) == 1
        assert calls["guaranteed"] == []

    def test_the_stamped_duration_is_zero_not_the_one_second_floor(self, monkeypatch):
        m = _main_module(monkeypatch)
        calls = self._record(monkeypatch, m, _stub())
        assert calls["annotate"][0][0] == 0

    def test_the_reservation_kind_is_stamped_for_consumers(self, monkeypatch):
        # guarantee-status reads "overstay" for a best-effort pod's whole life,
        # which is literally true; this is what disambiguates it from a job that
        # really did outstay a guarantee.
        m = _main_module(monkeypatch)
        calls = self._record(monkeypatch, m, _stub())
        assert calls["annotate"][0][2].kind == "best_effort"

    def test_a_guaranteed_admission_is_unchanged(self, monkeypatch):
        m = _main_module(monkeypatch)
        now = datetime.now(timezone.utc)
        res = reservation(
            500, start_utc=now, end_utc=now + timedelta(hours=1),
            gpu_count=1, gpu_class_label=GPU_CLASS_LABEL, username=USERNAME,
        )
        calls = self._record(monkeypatch, m, res)
        assert len(calls["guaranteed"]) == 1
        assert calls["best_effort"] == []
        assert calls["annotate"][0][0] >= 1


# ---------------------------------------------------------------------------
# Config flag
# ---------------------------------------------------------------------------


class TestConfigFlag:
    """``BEST_EFFORT_ENABLED`` ships dark and takes the shared bool vocabulary."""

    def _from_env(self, monkeypatch, value=None):
        from app.config import Config

        monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
        monkeypatch.setenv("RESERVATION_API_KEY", "test-key")
        if value is None:
            monkeypatch.delenv("BEST_EFFORT_ENABLED", raising=False)
        else:
            monkeypatch.setenv("BEST_EFFORT_ENABLED", value)
        return Config.from_env()

    def test_defaults_off(self, monkeypatch):
        # An app that does not serve the best-effort create shape would answer
        # every such candidate with a non-retryable 4xx, so this must not be on
        # by default.
        assert self._from_env(monkeypatch).best_effort_enabled is False

    @pytest.mark.parametrize("raw", ["1", "true", "yes", "on", "TRUE", " On "])
    def test_truthy_words_enable_it(self, monkeypatch, raw):
        assert self._from_env(monkeypatch, raw).best_effort_enabled is True

    def test_junk_falls_back_to_the_default(self, monkeypatch):
        assert self._from_env(monkeypatch, "maybe").best_effort_enabled is False
