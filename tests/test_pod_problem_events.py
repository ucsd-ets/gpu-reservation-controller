"""Telling a pod's owner when the pod itself is why it is not admitted.

Four ways a ``gpu-class`` pod could be held or ignored used to reach the
controller's log and stop there, leaving its owner a pod that sat Pending with
nothing on it but kube-scheduler's "untolerated taint":

- the app answering the pod's lease ask with a **404** -- an unknown usage
  group (a mistyped ``dsmlp/course`` label), user or GPU class -- filed with the
  operator faults under ``lease.error`` although every name it can be about came
  off the pod;
- a ``gpu-class`` label naming **no class the app knows** (``class_id_unknown``);
- a ``galends/*`` annotation that was **invalid**, or asks for something the
  deployment does not offer (``pod.annotation_invalid``, or nothing at all);
- a pod **no path will admit**: no matching reservation, and not eligible for
  on-demand admission (``pod.left_pending``, at DEBUG).

Each is now a Warning Event, through the throttle ``OnDemandLeaseDenied`` and
``OnDemandAdmissionPaused`` already shared -- which now keeps what was told per
pod uid (``ControllerState.pending_status``) instead of on the candidate, so a
pod tells one story whichever path is holding it.  Nothing here touches a real
cluster or the app.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

from app import k8s_client
from app.controller import (
    PENDING_TOPIC_ANNOTATIONS,
    PENDING_TOPIC_HOLD,
    ControllerState,
    NearMiss,
    OnDemandCandidate,
    QueueEntry,
)
from app.k8s_client import (
    ANNOTATION_IGNORED_REASON,
    LEASE_REJECTED_REASON,
    NO_RESERVATION_REASON,
    UNKNOWN_GPU_CLASS_REASON,
    WAITING_FOR_RESERVATION_REASON,
    AnnotationProblem,
    emit_pending_pod_event,
    get_pod_annotation_problems,
)
from app.reservation_client import LeaseAttempt
from app.schemas import GpuClassDetail, OnDemandAdmissionCandidate

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    GROUP_NAME,
    OTHER_CLASS_ID,
    OTHER_CLASS_LABEL,
    USERNAME,
    kv_fields,
    make_config,
    reservation,
)

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
REPEAT = timedelta(minutes=30)
GROUP_LABEL_NAME = "dsmlp/course"
MIN_RUNTIME = "galends/minimum-runtime-seconds"
RUNTIME_GUARANTEE = "galends/runtime-guarantee"
USAGE_GROUP = "galends/usage-group"
NOT_FOUND = "Usage group 'cse999' not found"


def _later():
    """A booking start safely ahead of the real clock, for code under test that
    reads ``datetime.now`` itself rather than taking a ``now``."""
    return datetime.now(timezone.utc) + timedelta(days=30)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-pod-problems")
    import app.main as main_module

    return main_module


def _config(**overrides):
    return make_config(**overrides)


def _state(*reservations, required_group_label=None) -> ControllerState:
    """A state whose app lists two GPU classes, h100 and a100."""
    state = ControllerState()
    state.reservations = list(reservations)
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL, OTHER_CLASS_ID: OTHER_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID, OTHER_CLASS_LABEL: OTHER_CLASS_ID}
    state.gpu_classes_known = True    # a bulk class fetch has succeeded
    state.reservations_known = True   # and a full reservation fetch
    state.required_group_label = required_group_label
    return state


def _booking(res_id=1, *, start, hours=2, gpu_class_id=GPU_CLASS_ID, group=None,
             username=USERNAME, gpu_count=2):
    return reservation(
        res_id, start_utc=start, end_utc=start + timedelta(hours=hours),
        gpu_class_id=gpu_class_id, group=group, username=username, gpu_count=gpu_count,
    )


def _candidate(uid="uid-1", *, group_label=None, usage_group=GROUP_NAME,
               usage_group_source="annotation", gpu_class=GPU_CLASS_LABEL):
    return OnDemandCandidate(
        pod_uid=uid,
        pod_name=f"pod-{uid}",
        pod_namespace=USERNAME,
        gpu_class_label=gpu_class,
        gpu_requested=1,
        min_runtime_seconds=600,
        pod_created_at=NOW,
        next_attempt_at=NOW,
        group_label=group_label,
        usage_group=usage_group,
        usage_group_source=usage_group_source,
    )


class _ProblemRecorder:
    """Stands in for ``emit_pending_pod_event`` as ``main`` calls it."""

    def __init__(self, raises=None):
        self.calls: list[dict] = []
        self._raises = raises

    async def __call__(self, uid, name, namespace, message, *, reason,
                       gpu_class=None, gpu_count=None):
        if self._raises is not None:
            exc, self._raises = self._raises, None  # fail once, then recover
            raise exc
        self.calls.append(dict(
            uid=uid, name=name, namespace=namespace, message=message,
            reason=reason, gpu_class=gpu_class, gpu_count=gpu_count,
        ))

    @property
    def reasons(self) -> list[str]:
        return [c["reason"] for c in self.calls]


def _told(state, uid="uid-1", topic=PENDING_TOPIC_HOLD):
    return state.pending_status.get(uid, {}).get(topic)


# ---------------------------------------------------------------------------
# ControllerState: what has been told, per pod
# ---------------------------------------------------------------------------


class TestPendingStatusLedger:
    KEY = ("OnDemandLeaseDenied", "no capacity")

    def _due(self, state, key, at, topic=PENDING_TOPIC_HOLD, repeat=REPEAT):
        return state.pending_status_due("uid-1", topic, key, at, repeat)

    def test_nothing_told_yet_is_due(self):
        assert self._due(ControllerState(), self.KEY, NOW)

    def test_the_same_status_inside_the_repeat_is_not_due(self):
        state = ControllerState()
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        assert not self._due(state, self.KEY, NOW + REPEAT - timedelta(seconds=1))

    def test_the_same_status_is_restated_once_the_repeat_has_passed(self):
        state = ControllerState()
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        assert self._due(state, self.KEY, NOW + REPEAT)

    def test_a_changed_status_is_due_at_once(self):
        state = ControllerState()
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        changed = ("OnDemandLeaseDenied", "SU budget exceeded")
        assert self._due(state, changed, NOW + timedelta(seconds=1))

    def test_a_zero_repeat_is_always_due(self):
        state = ControllerState()
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        assert self._due(state, self.KEY, NOW, repeat=timedelta(0))

    def test_topics_do_not_share_a_key(self):
        # The whole reason for topics: an ignored annotation must not read as the
        # hold having changed, nor the other way round.
        state = ControllerState()
        note = ("AnnotationIgnored", "ignored")
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        state.record_pending_status("uid-1", PENDING_TOPIC_ANNOTATIONS, note, NOW)
        later = NOW + timedelta(minutes=1)
        assert not self._due(state, self.KEY, later)
        assert not self._due(state, note, later, topic=PENDING_TOPIC_ANNOTATIONS)

    def test_forgetting_a_pod_drops_every_topic(self):
        state = ControllerState()
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        state.record_pending_status("uid-1", PENDING_TOPIC_ANNOTATIONS, self.KEY, NOW)
        state.forget_pending_status("uid-1")
        state.forget_pending_status("uid-never-seen")  # no-op, no error
        assert state.pending_status == {}

    def test_prune_drops_an_untracked_pod_last_told_before_the_cutoff(self):
        # A pod deleted while the watch was down: no DELETED event ever came.
        state = ControllerState()
        state.record_pending_status("gone", PENDING_TOPIC_HOLD, self.KEY, NOW)
        state.prune_pending_status(NOW + timedelta(seconds=1))
        assert state.pending_status == {}

    def test_prune_keeps_a_pod_told_anything_since(self):
        state = ControllerState()
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        later = NOW + timedelta(hours=3)
        state.record_pending_status("uid-1", PENDING_TOPIC_ANNOTATIONS, self.KEY, later)
        state.prune_pending_status(NOW + timedelta(hours=1))
        assert "uid-1" in state.pending_status

    def test_prune_keeps_a_pod_a_path_still_tracks(self):
        state = ControllerState()
        state.ondemand_candidates["cand"] = _candidate("cand")
        state.task_queue["queued"] = QueueEntry(
            pod_uid="queued", pod_name="q", pod_namespace=USERNAME,
            gpu_class_label=GPU_CLASS_LABEL, gpu_requested=1,
            reservation=_booking(start=NOW), next_attempt_at=NOW,
        )
        for uid in ("cand", "queued"):
            state.record_pending_status(uid, PENDING_TOPIC_HOLD, self.KEY, NOW)
        state.prune_pending_status(NOW + timedelta(days=1))
        assert set(state.pending_status) == {"cand", "queued"}


# ---------------------------------------------------------------------------
# ControllerState.near_miss_bookings
# ---------------------------------------------------------------------------


class TestNearMissBookings:
    def _miss(self, state, *, gpu_class=GPU_CLASS_LABEL, group=None):
        return state.near_miss_bookings(USERNAME, gpu_class, group, NOW)

    def test_the_same_class_under_another_group(self):
        state = _state(
            _booking(start=NOW + timedelta(hours=1), group="cse151b"),
            required_group_label=GROUP_LABEL_NAME,
        )
        assert self._miss(state, group="cse999") == NearMiss(other_groups=("cse151b",))

    def test_a_pod_naming_no_group_misses_every_grouped_booking(self):
        state = _state(
            _booking(start=NOW + timedelta(hours=1), group="cse151b"),
            required_group_label=GROUP_LABEL_NAME,
        )
        assert self._miss(state, group=None).other_groups == ("cse151b",)

    def test_groups_are_not_a_match_axis_while_the_feature_is_off(self):
        state = _state(_booking(start=NOW + timedelta(hours=1), group="cse151b"))
        assert self._miss(state, group="cse999") == NearMiss()

    def test_another_class(self):
        state = _state(
            _booking(start=NOW + timedelta(hours=1), gpu_class_id=OTHER_CLASS_ID)
        )
        assert self._miss(state) == NearMiss(other_classes=(OTHER_CLASS_LABEL,))

    def test_a_booking_that_matches_is_no_miss(self):
        state = _state(
            _booking(start=NOW + timedelta(hours=1), group="cse151b"),
            required_group_label=GROUP_LABEL_NAME,
        )
        assert self._miss(state, group="cse151b") == NearMiss()

    def test_other_users_ended_and_no_show_bookings_are_ignored(self):
        state = _state(
            _booking(1, start=NOW + timedelta(hours=1), gpu_class_id=OTHER_CLASS_ID,
                     username="bob"),
            _booking(2, start=NOW - timedelta(hours=3), gpu_class_id=OTHER_CLASS_ID),
            _booking(3, start=NOW + timedelta(hours=1), gpu_class_id=OTHER_CLASS_ID),
        )
        state.noshow_reservation_ids = {3}
        assert self._miss(state) == NearMiss()

    def test_names_are_sorted_and_deduplicated(self):
        state = _state(
            _booking(1, start=NOW + timedelta(hours=1), group="zeta"),
            _booking(2, start=NOW + timedelta(hours=3), group="alpha"),
            _booking(3, start=NOW + timedelta(hours=5), group="zeta"),
            required_group_label=GROUP_LABEL_NAME,
        )
        assert self._miss(state, group="cse999").other_groups == ("alpha", "zeta")


# ---------------------------------------------------------------------------
# k8s_client.get_pod_annotation_problems
# ---------------------------------------------------------------------------


def _annotated(annotations):
    return SimpleNamespace(
        metadata=SimpleNamespace(namespace=USERNAME, name="pod-1", annotations=annotations)
    )


class TestAnnotationProblems:
    def _problems(self, annotations, *, best_effort_enabled=True):
        return get_pod_annotation_problems(
            _annotated(annotations), best_effort_enabled=best_effort_enabled
        )

    @pytest.mark.parametrize("annotations", [
        None,
        {},
        {MIN_RUNTIME: "3600", RUNTIME_GUARANTEE: "none"},
        {RUNTIME_GUARANTEE: " None "},  # case and whitespace are forgiven
        {USAGE_GROUP: "anything at all"},  # the app judges groups, not this
    ])
    def test_absent_or_valid_is_no_problem(self, annotations):
        assert self._problems(annotations) == []

    @pytest.mark.parametrize("raw,reason", [
        ("4h", "not_an_integer"),
        ("1e4", "not_an_integer"),
        ("", "not_an_integer"),
        ("0", "not_positive"),
        ("-60", "not_positive"),
    ])
    def test_a_bad_minimum_runtime(self, raw, reason):
        assert self._problems({MIN_RUNTIME: raw}) == [
            AnnotationProblem(MIN_RUNTIME, raw, reason)
        ]

    def test_an_unrecognised_runtime_guarantee(self):
        assert self._problems({RUNTIME_GUARANTEE: "no"}) == [
            AnnotationProblem(RUNTIME_GUARANTEE, "no", "unrecognised_value")
        ]

    @pytest.mark.parametrize("raw", ["none", "no"])
    def test_any_runtime_guarantee_is_not_enabled_while_best_effort_is_off(self, raw):
        # Telling someone "no" is misspelled would only send them round again to
        # learn that "none" is switched off too.
        assert self._problems({RUNTIME_GUARANTEE: raw}, best_effort_enabled=False) == [
            AnnotationProblem(RUNTIME_GUARANTEE, raw, "not_enabled")
        ]

    def test_both_are_reported_in_a_fixed_order(self):
        problems = self._problems({RUNTIME_GUARANTEE: "no", MIN_RUNTIME: "0"})
        assert [p.annotation for p in problems] == [MIN_RUNTIME, RUNTIME_GUARANTEE]

    def test_it_logs_nothing(self, caplog):
        # The getters already log each rejection; this runs beside them.
        with caplog.at_level(logging.DEBUG, logger="app.k8s_client"):
            self._problems({MIN_RUNTIME: "junk", RUNTIME_GUARANTEE: "junk"})
        assert caplog.records == []

    def test_it_agrees_with_the_logging_getter(self, caplog):
        # One parse behind both, so the log line and the Event never disagree.
        pod = _annotated({MIN_RUNTIME: "0"})
        with caplog.at_level(logging.WARNING, logger="app.k8s_client"):
            assert k8s_client.get_pod_min_runtime_seconds(pod) is None
        logged = kv_fields(caplog.records[0].getMessage())["reason"]
        [problem] = get_pod_annotation_problems(pod, best_effort_enabled=True)
        assert problem.reason == logged == "not_positive"


# ---------------------------------------------------------------------------
# k8s_client.emit_pending_pod_event — the Event write
# ---------------------------------------------------------------------------


class _CapturingCore:
    def __init__(self):
        self.events: list = []

    def create_namespaced_event(self, namespace, body):
        self.events.append((namespace, body))
        return SimpleNamespace()


def _emit(monkeypatch, *, reason, message="the pod is wrong", **kw):
    core = _CapturingCore()
    monkeypatch.setattr(k8s_client, "_core_v1", core)
    asyncio.run(emit_pending_pod_event(
        "uid-1", "pod-1", USERNAME, message, reason=reason, **kw
    ))
    return core.events[0]


class TestEmitPendingPodEvent:
    @pytest.mark.parametrize("reason,action,prefix", [
        (LEASE_REJECTED_REASON, "RequestOnDemandLease", "gpu-lease-rejected-"),
        (UNKNOWN_GPU_CLASS_REASON, "AdmitPod", "gpu-unknown-class-"),
        (NO_RESERVATION_REASON, "AdmitPod", "gpu-no-reservation-"),
        (ANNOTATION_IGNORED_REASON, "ReadAnnotations", "gpu-annotation-ignored-"),
    ])
    def test_each_is_a_warning_on_the_pod(self, monkeypatch, reason, action, prefix):
        namespace, event = _emit(monkeypatch, reason=reason)
        # The pod's namespace is its owner's username -- where they run kubectl.
        assert namespace == USERNAME
        assert event.type == "Warning"
        assert (event.reason, event.action) == (reason, action)
        assert event.metadata.generate_name == prefix
        ref = event.involved_object
        assert (ref.kind, ref.name, ref.namespace, ref.uid) == (
            "Pod", "pod-1", USERNAME, "uid-1",
        )
        assert event.message == "the pod is wrong"

    def test_a_long_message_is_capped(self, monkeypatch):
        _ns, event = _emit(monkeypatch, reason=NO_RESERVATION_REASON, message="x" * 5000)
        assert len(event.message) == 1024
        assert event.message.endswith("…")

    def test_the_emit_is_logged(self, monkeypatch, caplog):
        with caplog.at_level(logging.INFO, logger="app.k8s_client"):
            _emit(monkeypatch, reason=LEASE_REJECTED_REASON,
                  gpu_class=GPU_CLASS_LABEL, gpu_count=2)
        [record] = [r for r in caplog.records if "k8s.event_emitted" in r.getMessage()]
        fields = kv_fields(record.getMessage())
        assert fields["reason"] == LEASE_REJECTED_REASON
        assert (fields["clabel"], fields["gpus"]) == (GPU_CLASS_LABEL, "2")


# ---------------------------------------------------------------------------
# main: a 404 on the lease ask (OnDemandLeaseRejected)
# ---------------------------------------------------------------------------


class _DenyingClient:
    def __init__(self, status, detail):
        self._attempt = LeaseAttempt(status=status, detail=detail)
        self.requests = 0

    async def create_ondemand_reservation(self, req):
        self.requests += 1
        return self._attempt


def _ask(candidate):
    return OnDemandAdmissionCandidate(
        pod_uid=candidate.pod_uid,
        pod_created_at=candidate.pod_created_at,
        username=candidate.pod_namespace,
        group_name=candidate.usage_group,
        gpu_class_id=GPU_CLASS_ID,
        gpu_count=candidate.gpu_requested,
        duration_seconds=1200,
    )


def _grant(monkeypatch, m, status, detail, *, config=None, state=None, candidate=None):
    rec = _ProblemRecorder()
    monkeypatch.setattr(m, "emit_pending_pod_event", rec)
    state = state if state is not None else _state()
    candidate = candidate or _candidate()
    done = asyncio.run(m._grant_and_admit(
        state, _DenyingClient(status, detail), config or _config(),
        candidate.pod_uid, candidate, _ask(candidate),
    ))
    assert done is False  # a rejection keeps the candidate for a retry
    return rec, candidate, state


class TestLeaseRejected:
    def test_a_404_reaches_the_pod(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, _c, _s = _grant(monkeypatch, m, 404, NOT_FOUND)
        [call] = rec.calls
        assert call["reason"] == LEASE_REJECTED_REASON
        assert (call["uid"], call["namespace"]) == ("uid-1", USERNAME)
        assert (call["gpu_class"], call["gpu_count"]) == (GPU_CLASS_LABEL, 1)
        message = call["message"]
        assert NOT_FOUND in message
        # What was sent, which the app's own reason cannot say.
        assert f"user {USERNAME}" in message
        assert f"usage group '{GROUP_NAME}'" in message
        assert "Waiting will not fix this" in message

    @pytest.mark.parametrize("source,phrase", [
        ("label", f"from the pod's {GROUP_LABEL_NAME} label"),
        ("annotation", f"from the pod's {USAGE_GROUP} annotation"),
        ("default", "the cluster's default usage group"),
    ])
    def test_it_says_where_the_group_came_from(self, monkeypatch, source, phrase):
        m = _main_module(monkeypatch)
        rec, _c, _s = _grant(
            monkeypatch, m, 404, NOT_FOUND,
            config=_config(required_group_label=GROUP_LABEL_NAME),
            candidate=_candidate(usage_group_source=source),
        )
        assert phrase in rec.calls[0]["message"]

    def test_a_404_with_no_body_still_reaches_the_pod(self, monkeypatch):
        # Unlike a 409, the names that were sent say enough on their own.
        m = _main_module(monkeypatch)
        rec, _c, _s = _grant(monkeypatch, m, 404, None)
        [call] = rec.calls
        assert "does not recognise the user, usage group or GPU class" in call["message"]

    def test_it_keeps_the_error_backoff(self, monkeypatch):
        m = _main_module(monkeypatch)
        _rec, candidate, _s = _grant(monkeypatch, m, 404, NOT_FOUND)
        assert candidate.lease_error_count == 1
        assert candidate.next_attempt_at > datetime.now(timezone.utc) + timedelta(minutes=1)

    def test_a_repeat_404_is_throttled(self, monkeypatch):
        m = _main_module(monkeypatch)
        state, candidate = _state(), _candidate()
        rec, _c, _s = _grant(monkeypatch, m, 404, NOT_FOUND, state=state, candidate=candidate)
        again, _c, _s = _grant(monkeypatch, m, 404, NOT_FOUND, state=state, candidate=candidate)
        assert len(rec.calls) == 1
        assert again.calls == []
        assert _told(state).key[0] == LEASE_REJECTED_REASON

    def test_the_denial_switch_turns_it_off(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, _c, state = _grant(
            monkeypatch, m, 404, NOT_FOUND,
            config=_config(ondemand_denial_event_enabled=False),
        )
        assert rec.calls == []
        assert state.pending_status == {}

    @pytest.mark.parametrize("status", [401, 403, 422])
    def test_the_other_faults_still_reach_no_pod(self, monkeypatch, status):
        # A bad or read-only service key, a schema mismatch: nothing the pod's
        # owner wrote, so nothing they can act on.
        m = _main_module(monkeypatch)
        rec, candidate, _s = _grant(monkeypatch, m, status, "nope")
        assert rec.calls == []
        assert candidate.lease_error_count == 1

    def test_a_booking_under_another_group_is_named(self, monkeypatch):
        # The reported case: a mistyped course label, and a real booking under
        # the right course sitting unused.
        m = _main_module(monkeypatch)
        state = _state(
            _booking(1, start=_later(), group="cse151b"),
            _booking(2, start=_later(), gpu_class_id=OTHER_CLASS_ID),
            required_group_label=GROUP_LABEL_NAME,
        )
        candidate = _candidate(group_label="cse999", usage_group="cse999",
                               usage_group_source="label")
        rec, _c, _s = _grant(
            monkeypatch, m, 404, NOT_FOUND,
            config=_config(required_group_label=GROUP_LABEL_NAME),
            state=state, candidate=candidate,
        )
        message = rec.calls[0]["message"]
        assert "reservation under usage group cse151b" in message
        assert f"set its {GROUP_LABEL_NAME} label" in message
        # A booking of another class has nothing to do with a group 404.
        assert OTHER_CLASS_LABEL not in message


# ---------------------------------------------------------------------------
# main: an unknown gpu-class, in preflight (UnknownGpuClass)
# ---------------------------------------------------------------------------


def _pending_pod(uid="uid-1", *, message="0/10 nodes are available: 5 Insufficient nvidia.com/gpu."):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name=f"pod-{uid}", namespace=USERNAME,
            annotations=None, labels={"gpu-class": "h1000"},
        ),
        status=SimpleNamespace(
            phase="Pending",
            conditions=[SimpleNamespace(
                type="PodScheduled", status="False", reason="Unschedulable",
                message=message,
            )],
        ),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


def _preflight(monkeypatch, m, state, candidate, *, config=None, pod=None):
    rec = _ProblemRecorder()

    async def fake_read_pod(name, namespace):
        return pod or _pending_pod(candidate.pod_uid)

    monkeypatch.setattr(m, "read_pod", fake_read_pod)
    monkeypatch.setattr(m, "emit_pending_pod_event", rec)
    status, ask = asyncio.run(m._preflight_ondemand_candidate(
        state, config or _config(), candidate.pod_uid, candidate,
    ))
    return status, ask, rec


class TestUnknownClassInPreflight:
    def test_an_unknown_label_is_told_and_held(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate(gpu_class="h1000")
        before = candidate.next_attempt_at
        status, ask, rec = _preflight(monkeypatch, m, _state(), candidate)
        assert (status, ask) == (m._PREFLIGHT_RETRY, None)
        assert candidate.next_attempt_at > before
        [call] = rec.calls
        assert call["reason"] == UNKNOWN_GPU_CLASS_REASON
        assert call["gpu_class"] == "h1000"
        # The classes that do exist are how a typo is spotted.
        assert f"Known classes: {OTHER_CLASS_LABEL}, {GPU_CLASS_LABEL}." in call["message"]

    def test_nothing_is_told_before_the_app_has_listed_its_classes(self, monkeypatch):
        # Until a bulk class fetch succeeds, the map holds at most the classes
        # some reservation named: a label missing from it is the controller's
        # gap, not the pod's fault.
        m = _main_module(monkeypatch)
        state = _state()
        state.gpu_classes_known = False
        status, _ask, rec = _preflight(monkeypatch, m, state, _candidate(gpu_class="h1000"))
        assert status == m._PREFLIGHT_RETRY
        assert rec.calls == []

    def test_it_is_settled_before_guard_1a(self, monkeypatch):
        # A verdict guard 1a would drop on used to hide a mistyped label for
        # good: the pod left the candidate list before the class was looked at.
        m = _main_module(monkeypatch)
        candidate = _candidate(gpu_class="h1000")
        pod = _pending_pod(message="0/5 nodes are available: 5 Insufficient memory.")
        status, _ask, rec = _preflight(monkeypatch, m, _state(), candidate, pod=pod)
        assert status == m._PREFLIGHT_RETRY
        assert rec.reasons == [UNKNOWN_GPU_CLASS_REASON]

    def test_the_switch_turns_it_off(self, monkeypatch):
        m = _main_module(monkeypatch)
        status, _ask, rec = _preflight(
            monkeypatch, m, _state(), _candidate(gpu_class="h1000"),
            config=_config(pod_problem_event_enabled=False),
        )
        assert status == m._PREFLIGHT_RETRY
        assert rec.calls == []

    def test_an_app_with_no_classes_says_so(self, monkeypatch):
        m = _main_module(monkeypatch)
        message = m._unknown_class_message("h1000", (), _config())
        assert "lists no GPU classes at all." in message
        assert "Known classes" not in message

    def test_a_known_class_is_not_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        status, _ask, rec = _preflight(monkeypatch, m, _state(), _candidate())
        assert status == m._PREFLIGHT_READY
        assert rec.calls == []


# ---------------------------------------------------------------------------
# main.pod_watch_loop: pods no path will admit, and ignored annotations
# ---------------------------------------------------------------------------


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


def _pod(uid="uid-1", *, gpu_class=GPU_CLASS_LABEL, labels=None, annotations=None,
         phase="Pending", tolerations=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name=f"pod-{uid}", namespace=USERNAME,
            labels={"gpu-class": gpu_class, **(labels or {})},
            annotations=annotations,
            creation_timestamp=NOW,
            deletion_timestamp=None,
        ),
        status=SimpleNamespace(phase=phase, conditions=None),
        spec=SimpleNamespace(
            tolerations=tolerations or [],
            containers=[SimpleNamespace(
                resources=SimpleNamespace(requests={"nvidia.com/gpu": "1"})
            )],
            scheduling_gates=None,
        ),
    )


def _watch(monkeypatch, m, config, events, state=None, recorder=None):
    """Feed *events* through pod_watch_loop; admission and patching stubbed."""
    rec = recorder or _ProblemRecorder()
    batches: list[int] = []

    async def fake_admission(*args, **kwargs):
        batches.append(len(rec.calls))  # how many Events preceded the batch

    async def no_apply(*args, **kwargs):
        return False

    monkeypatch.setattr(m, "PodWatcher", lambda **kw: _FakeWatcher(events))
    monkeypatch.setattr(m, "_run_ondemand_admission", fake_admission)
    monkeypatch.setattr(m, "_try_apply_toleration", no_apply)
    monkeypatch.setattr(m, "emit_pending_pod_event", rec)
    state = state if state is not None else _state(
        required_group_label=config.required_group_label
    )
    asyncio.run(m.pod_watch_loop(state, None, config))
    return rec, state, batches


class TestTheReportedCase:
    """A pod labelled with a usage group the app does not know, end to end.

    Through the real watch loop, admission batch, preflight and grant, with only
    the Kubernetes and app I/O faked: before this change it ended in a 404 in
    the controller's log and nothing on the pod.
    """

    def test_a_mistyped_group_label_is_told_to_the_pod(self, monkeypatch):
        m = _main_module(monkeypatch)
        config = _config(required_group_label=GROUP_LABEL_NAME)
        state = _state(
            _booking(start=_later(), group="cse151b"),
            required_group_label=GROUP_LABEL_NAME,
        )
        pod = _pod(
            labels={GROUP_LABEL_NAME: "DOES_NOT_EXIST"},
            annotations={MIN_RUNTIME: "3600"},
        )
        # What a taint-gated cluster's scheduler says about an unadmitted pod.
        pod.status.conditions = [SimpleNamespace(
            type="PodScheduled", status="False", reason="Unschedulable",
            message="0/41 nodes are available: 25 node(s) had untolerated taint(s).",
        )]
        client = _DenyingClient(404, "Usage group 'DOES_NOT_EXIST' not found")
        rec = _ProblemRecorder()

        async def fake_read_pod(name, namespace):
            return pod

        monkeypatch.setattr(m, "PodWatcher", lambda **kw: _FakeWatcher([("ADDED", pod)]))
        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "emit_pending_pod_event", rec)
        asyncio.run(m.pod_watch_loop(state, client, config))

        assert client.requests == 1
        [call] = rec.calls
        assert call["reason"] == LEASE_REJECTED_REASON
        message = call["message"]
        assert "Usage group 'DOES_NOT_EXIST' not found" in message
        assert f"(from the pod's {GROUP_LABEL_NAME} label)" in message
        assert "reservation under usage group cse151b" in message
        # Still a candidate, retried on the non-retryable backoff.
        assert state.ondemand_candidates["uid-1"].lease_error_count == 1


class TestNoReservation:
    def test_a_pod_with_no_runtime_is_told_why(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME})
        rec, state, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)])
        [call] = rec.calls
        assert call["reason"] == NO_RESERVATION_REASON
        assert call["gpu_class"] == GPU_CLASS_LABEL
        message = call["message"]
        assert message.startswith(
            f"No GPU reservation matches this pod (user {USERNAME}, gpu-class "
            f"{GPU_CLASS_LABEL}), and it cannot be admitted on demand: it has no "
            f"{MIN_RUNTIME} annotation"
        )
        assert f"or book a gpu-class {GPU_CLASS_LABEL} reservation." in message
        assert state.ondemand_candidates == {}

    def test_with_on_demand_off_that_is_the_whole_story(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME})
        rec, _s, _b = _watch(
            monkeypatch, m, _config(ondemand_lease_enabled=False), [("ADDED", pod)]
        )
        message = rec.calls[0]["message"]
        assert "on-demand admission is not enabled on this cluster." in message
        assert MIN_RUNTIME not in message
        assert message.endswith(f"Book a gpu-class {GPU_CLASS_LABEL} reservation to run it.")

    def test_an_invalid_runtime_is_named_rather_than_called_missing(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "4h"})
        rec, _s, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)])
        message = rec.calls[0]["message"]
        assert f"its {MIN_RUNTIME} annotation is '4h'" in message
        assert f"has no {MIN_RUNTIME}" not in message

    def test_best_effort_asked_for_but_off_is_named(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME, RUNTIME_GUARANTEE: "none"})
        rec, _s, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)])
        message = rec.calls[0]["message"]
        assert "best-effort admission is not enabled on this cluster" in message
        assert f"has no {MIN_RUNTIME}" in message
        assert "; and " in message

    def test_a_missing_group_label_is_named(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={MIN_RUNTIME: "600", USAGE_GROUP: GROUP_NAME})
        config = _config(required_group_label=GROUP_LABEL_NAME)
        rec, _s, _b = _watch(monkeypatch, m, config, [("ADDED", pod)])
        message = rec.calls[0]["message"]
        assert f"it has no {GROUP_LABEL_NAME} label naming its usage group" in message
        # The annotation is the other mode's group source; say it is not used.
        assert f"the {USAGE_GROUP} annotation is not used on this cluster" in message

    def test_a_booking_under_another_group_is_named(self, monkeypatch):
        m = _main_module(monkeypatch)
        config = _config(required_group_label=GROUP_LABEL_NAME, ondemand_lease_enabled=False)
        state = _state(
            _booking(start=_later(), group="cse151b"),
            required_group_label=GROUP_LABEL_NAME,
        )
        pod = _pod(labels={GROUP_LABEL_NAME: "cse999"})
        rec, _s, _b = _watch(monkeypatch, m, config, [("ADDED", pod)], state=state)
        message = rec.calls[0]["message"]
        assert "usage group cse999" in message  # what it was matched on
        assert (
            f"You do have a gpu-class {GPU_CLASS_LABEL} reservation under usage group "
            f"cse151b, but this pod's usage group is 'cse999'"
        ) in message
        assert message.endswith("Otherwise, book a gpu-class h100 reservation to run it.")

    def test_a_booking_of_another_class_is_named(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_booking(start=_later(), gpu_class_id=OTHER_CLASS_ID))
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME})
        rec, _s, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)], state=state)
        assert (
            f"You do have a reservation for gpu-class {OTHER_CLASS_LABEL}, but this "
            f"pod's gpu-class label is {GPU_CLASS_LABEL}."
        ) in rec.calls[0]["message"]

    def test_an_unknown_class_is_told_as_that_instead(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(gpu_class="h1000", annotations={USAGE_GROUP: GROUP_NAME})
        rec, _s, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)])
        assert rec.reasons == [UNKNOWN_GPU_CLASS_REASON]

    def test_nothing_is_said_before_the_first_reservation_fetch(self, monkeypatch):
        # With the app down at startup the reservation list is empty, not known
        # to be empty: "no reservation matches" would be false for every pod.
        m = _main_module(monkeypatch)
        state = _state()
        state.reservations_known = False
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME})
        rec, _s, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)], state=state)
        assert rec.calls == []

    def test_a_partial_class_list_cannot_call_a_label_unknown(self, monkeypatch):
        # Only the per-id fallback has run: a label missing from the map may be
        # a real class no reservation named.  Say what is certain instead.
        m = _main_module(monkeypatch)
        state = _state()
        state.gpu_classes_known = False
        pod = _pod(gpu_class="h1000", annotations={USAGE_GROUP: GROUP_NAME})
        rec, _s, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)], state=state)
        assert rec.reasons == [NO_RESERVATION_REASON]

    def test_a_pod_that_is_not_pending_is_not_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, _s, _b = _watch(
            monkeypatch, m, _config(), [("ADDED", _pod(phase="Running"))]
        )
        assert rec.calls == []

    def test_only_first_sight_and_resyncs_tell(self, monkeypatch):
        # A MODIFIED burst (scheduler condition churn) must not drive Events;
        # the periodic re-LIST, replayed as ADDED, is what restates the status.
        m = _main_module(monkeypatch)
        rec, _s, _b = _watch(monkeypatch, m, _config(), [("MODIFIED", _pod())])
        assert rec.calls == []

    def test_a_resync_does_not_repeat_an_unchanged_status(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME})
        rec, state, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod), ("ADDED", pod)])
        assert len(rec.calls) == 1
        assert _told(state).key[0] == NO_RESERVATION_REASON

    def test_a_failed_emit_is_retried_at_the_next_sight(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME})
        rec = _ProblemRecorder(raises=RuntimeError("apiserver said no"))
        with caplog.at_level(logging.WARNING, logger="app.main"):
            _watch(monkeypatch, m, _config(), [("ADDED", pod), ("ADDED", pod)], recorder=rec)
        [failed] = [r for r in caplog.records if "k8s.event_failed" in r.getMessage()]
        assert kv_fields(failed.getMessage())["reason"] == NO_RESERVATION_REASON
        assert rec.reasons == [NO_RESERVATION_REASON]

    def test_the_switch_turns_it_off(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(gpu_class="h1000")
        rec, _s, _b = _watch(
            monkeypatch, m, _config(pod_problem_event_enabled=False),
            [("ADDED", pod), ("ADDED", _pod("uid-2"))],
        )
        assert rec.calls == []


class TestAnnotationIgnored:
    def test_a_default_runtime_standing_in_for_a_bad_one_is_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "4h"})
        config = _config(default_min_runtime_seconds=3600)
        rec, state, batches = _watch(monkeypatch, m, config, [("ADDED", pod)])
        assert state.ondemand_candidates["uid-1"].min_runtime_seconds == 3600
        [call] = rec.calls
        assert call["reason"] == ANNOTATION_IGNORED_REASON
        assert f"its {MIN_RUNTIME} annotation is '4h'" in call["message"]
        assert "default minimum runtime of 3600 seconds is used instead" in call["message"]
        # Told before the admission batch decides anything about the pod.
        assert batches == [1]
        assert _told(state, topic=PENDING_TOPIC_ANNOTATIONS) is not None
        assert _told(state) is None

    def test_best_effort_asked_for_but_off_is_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={
            USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "600", RUNTIME_GUARANTEE: "none",
        })
        rec, state, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)])
        assert state.ondemand_candidates["uid-1"].best_effort is False
        message = rec.calls[0]["message"]
        assert "best-effort admission is not enabled on this cluster" in message
        assert "charged like any on-demand lease" in message

    def test_a_best_effort_pod_is_not_told_about_a_runtime_it_does_not_use(
        self, monkeypatch
    ):
        m = _main_module(monkeypatch)
        pod = _pod(annotations={
            USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "junk", RUNTIME_GUARANTEE: "none",
        })
        rec, state, _b = _watch(
            monkeypatch, m, _config(best_effort_enabled=True), [("ADDED", pod)]
        )
        assert state.ondemand_candidates["uid-1"].best_effort is True
        assert rec.calls == []

    def test_a_pod_left_waiting_for_its_booking_is_told_in_its_wait_event(
        self, monkeypatch
    ):
        # A booking far off: with a valid runtime this pod would have been
        # admitted on demand now.  That is said inside the Event saying what it
        # waits for, where it is why it cannot start sooner -- not beside it.
        m = _main_module(monkeypatch)
        state = _state(_booking(start=_later()))
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "0"})
        rec, state, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)], state=state)
        assert "uid-1" in state.task_queue
        [call] = rec.calls
        assert call["reason"] == WAITING_FOR_RESERVATION_REASON
        assert f"its {MIN_RUNTIME} annotation is '0'" in call["message"]
        assert _told(state, topic=PENDING_TOPIC_ANNOTATIONS) is None

    def test_nothing_is_said_of_it_when_the_annotation_changed_nothing(self, monkeypatch):
        # On-demand admission is off here, so the pod would wait for its booking
        # however it was annotated.
        m = _main_module(monkeypatch)
        state = _state(_booking(start=_later()))
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "0"})
        rec, state, _b = _watch(
            monkeypatch, m, _config(ondemand_lease_enabled=False), [("ADDED", pod)],
            state=state,
        )
        assert "uid-1" in state.task_queue
        [call] = rec.calls
        assert call["reason"] == WAITING_FOR_RESERVATION_REASON
        assert MIN_RUNTIME not in call["message"]

    def test_a_pod_its_open_booking_admits_is_not_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        now = datetime.now(timezone.utc)
        state = _state(_booking(start=now - timedelta(minutes=5)))
        pod = _pod(annotations={USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "junk"})
        rec, state, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)], state=state)
        assert "uid-1" in state.task_queue
        assert rec.calls == []

    def test_it_never_flip_flops_with_the_hold(self, monkeypatch):
        # Alternating on one key, each would read as a change and be restated on
        # every retry.  On separate topics each is told once per interval.
        m = _main_module(monkeypatch)
        state, candidate = _state(), _candidate()
        problems = [AnnotationProblem(MIN_RUNTIME, "4h", "not_an_integer")]
        notes, denials = _ProblemRecorder(), []

        async def deny(uid, name, namespace, detail, *, gpu_class, gpu_count):
            denials.append(detail)

        monkeypatch.setattr(m, "emit_pending_pod_event", notes)
        monkeypatch.setattr(m, "emit_lease_denied_event", deny)
        config = _config(default_min_runtime_seconds=600)
        for minute in range(3):
            at = NOW + timedelta(minutes=minute)
            asyncio.run(m._emit_annotation_notice(
                config, state, "uid-1", "pod-uid-1", USERNAME, problems, at,
            ))
            asyncio.run(m._emit_lease_denial_event(
                config, state, "uid-1", candidate, "no capacity", at,
            ))
        assert len(notes.calls) == 1
        assert denials == ["no capacity"]


# ---------------------------------------------------------------------------
# Forgetting: a pod no longer pending leaves nothing behind
# ---------------------------------------------------------------------------


class TestForgetting:
    KEY = (NO_RESERVATION_REASON, "told")

    def _told_state(self):
        state = _state()
        state.record_pending_status("uid-1", PENDING_TOPIC_HOLD, self.KEY, NOW)
        state.record_pending_status("uid-1", PENDING_TOPIC_ANNOTATIONS, self.KEY, NOW)
        return state

    async def _noop(self, *args, **kwargs):
        return None

    def _run(self, monkeypatch, event, pod):
        m = _main_module(monkeypatch)
        monkeypatch.setattr(m, "_report_overstay_if_any", self._noop)
        monkeypatch.setattr(m, "_teardown_ondemand_lease", self._noop)
        state = self._told_state()
        _watch(monkeypatch, m, _config(), [(event, pod)], state=state)
        return state

    def test_a_deleted_pod_is_forgotten(self, monkeypatch):
        state = self._run(monkeypatch, "DELETED", _pod())
        assert state.pending_status == {}

    def test_a_finished_pod_is_forgotten(self, monkeypatch):
        state = self._run(monkeypatch, "MODIFIED", _pod(phase="Succeeded"))
        assert state.pending_status == {}

    def test_an_admitted_pod_is_forgotten(self, monkeypatch):
        tolerated = _pod(
            phase="Running",
            tolerations=[SimpleNamespace(
                key="gpu-class-reservation", value=GPU_CLASS_LABEL, effect="NoSchedule",
            )],
        )
        state = self._run(monkeypatch, "MODIFIED", tolerated)
        assert state.pending_status == {}

    def test_the_queue_tick_prunes_a_pod_it_stopped_hearing_about(self, monkeypatch):
        m = _main_module(monkeypatch)

        async def no_snapshot(*args, **kwargs):
            raise RuntimeError("apiserver unavailable")

        monkeypatch.setattr(m, "snapshot_tolerated_pods", no_snapshot)
        monkeypatch.setattr(m, "_run_ondemand_admission", self._noop)
        state = _state()
        long_ago = datetime.now(timezone.utc) - timedelta(hours=3)
        recent = datetime.now(timezone.utc)
        state.record_pending_status("gone", PENDING_TOPIC_HOLD, self.KEY, long_ago)
        state.record_pending_status("waiting", PENDING_TOPIC_HOLD, self.KEY, recent)
        asyncio.run(m._run_queue_tick(state, None, _config()))
        assert set(state.pending_status) == {"waiting"}

    def test_a_long_repeat_widens_the_prune(self, monkeypatch):
        # A pod still pending is re-told once per repeat interval, so the prune
        # waits three of them before deciding a pod is gone.
        m = _main_module(monkeypatch)

        async def no_snapshot(*args, **kwargs):
            raise RuntimeError("apiserver unavailable")

        monkeypatch.setattr(m, "snapshot_tolerated_pods", no_snapshot)
        monkeypatch.setattr(m, "_run_ondemand_admission", self._noop)
        state = _state()
        told = datetime.now(timezone.utc) - timedelta(hours=3)
        state.record_pending_status("waiting", PENDING_TOPIC_HOLD, self.KEY, told)
        asyncio.run(m._run_queue_tick(
            state, None, _config(ondemand_denial_event_repeat_minutes=120),
        ))
        assert set(state.pending_status) == {"waiting"}


# ---------------------------------------------------------------------------
# Knowing enough to blame the pod
# ---------------------------------------------------------------------------


class _AppClient:
    """The reservation app, as the fetch and push paths call it."""

    def __init__(self, *, fail_fetch=False, classes_fail=False):
        self.fail_fetch = fail_fetch
        self.classes_fail = classes_fail

    async def fetch_reservations(self):
        if self.fail_fetch:
            raise httpx.ConnectError("reservation app unreachable")
        return []

    async def fetch_gpu_classes(self):
        if self.classes_fail:
            return None
        return [GpuClassDetail(id=GPU_CLASS_ID, name="H100", label_value=GPU_CLASS_LABEL)]

    async def fetch_gpu_class(self, cid):
        return None


class TestKnowingEnoughToBlameThePod:
    """``UnknownGpuClass`` and ``NoReservation`` blame the pod, so each is said
    only once the controller holds the data that makes it true."""

    def _refresh(self, monkeypatch, state, client):
        m = _main_module(monkeypatch)
        asyncio.run(m._refresh_reservations(state, client, _config()))

    def test_a_successful_fetch_knows_both(self, monkeypatch):
        state = ControllerState()
        self._refresh(monkeypatch, state, _AppClient())
        assert state.reservations_known and state.gpu_classes_known

    def test_a_failed_fetch_knows_neither(self, monkeypatch):
        state = ControllerState()
        with pytest.raises(httpx.ConnectError):
            self._refresh(monkeypatch, state, _AppClient(fail_fetch=True))
        assert not state.reservations_known
        assert not state.gpu_classes_known

    def test_a_failed_class_list_leaves_the_classes_unknown(self, monkeypatch):
        state = ControllerState()
        self._refresh(monkeypatch, state, _AppClient(classes_fail=True))
        assert state.reservations_known
        assert not state.gpu_classes_known

    def test_a_later_failure_keeps_what_was_known(self, monkeypatch):
        # The prior maps are kept on a failed bulk fetch, and they were complete.
        state = ControllerState()
        self._refresh(monkeypatch, state, _AppClient())
        self._refresh(monkeypatch, state, _AppClient(classes_fail=True))
        assert state.gpu_classes_known
        assert state.gpu_class_ids == {GPU_CLASS_LABEL: GPU_CLASS_ID}

    def test_a_push_does_not_vouch_for_the_reservation_list(self, monkeypatch):
        # A push is a delta: before the first full fetch it proves nothing about
        # which reservations exist.
        from app.schemas import ReservationPushRequest

        m = _main_module(monkeypatch)
        state = ControllerState()
        request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(
            controller_state=state, reservation_client=_AppClient(), config=_config(),
        )))
        asyncio.run(m.push_reservations(ReservationPushRequest(reservations=[]), request))
        assert not state.reservations_known
        assert state.gpu_classes_known  # the class list itself was fetched whole


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def _from_env(self, monkeypatch, **env):
        from app.config import Config

        monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
        monkeypatch.setenv("RESERVATION_API_KEY", "test-key")
        monkeypatch.delenv("POD_PROBLEM_EVENT_ENABLED", raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return Config.from_env()

    def test_on_by_default(self, monkeypatch):
        assert self._from_env(monkeypatch).pod_problem_event_enabled is True

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", " OFF "])
    def test_falsy_words_disable_it(self, monkeypatch, raw):
        config = self._from_env(monkeypatch, POD_PROBLEM_EVENT_ENABLED=raw)
        assert config.pod_problem_event_enabled is False
