"""Unit tests for telling a pod's owner that on-demand admission is paused.

Guards 1b (no schedulable node in the class), 3 (the stuck reservation-holder
interlock) and 4 (app-side capacity overcommit) hold every on-demand candidate
of a GPU class until something outside the pod changes.  The operator hears
about it every minute (``ondemand.gated``); the pod's owner, who cannot read the
controller's log, saw only a pod that stayed Pending.  Each hold now puts an
``OnDemandAdmissionPaused`` Warning Event on the pod, on the same throttle and
cadence as the ``OnDemandLeaseDenied`` Event.  Layers, none of them touching a
real cluster or the app:

- ``k8s_client.emit_admission_paused_event`` — the Event write.
- ``main._admission_paused_message`` — what the owner reads.
- ``main._preflight_ondemand_candidate`` — which holds are told, and that
  telling never disturbs the hold itself.
- ``main._post_pending_status`` — the throttle shared with the denial Event.
- ``main._run_queue_tick`` end to end, for guard 1b: the real node snapshot
  omits a fully cordoned class rather than reporting it as zero, which is why
  guard 1b used to be unable to hold anything.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app import k8s_client
from app.controller import PENDING_TOPIC_HOLD, ControllerState, OnDemandCandidate
from app.k8s_client import emit_admission_paused_event
from app.reservation_client import LeaseAttempt

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    GROUP_NAME,
    USERNAME,
    kv_fields,
    make_config,
)

NOW = datetime(2024, 1, 15, 9, 0, tzinfo=timezone.utc)
DETAIL = "Only 2 GPU(s) available for this group at 2024-01-15 09:00 (group ceiling: 4)"


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-paused")
    import app.main as main_module

    return main_module


def _candidate(uid="uid-1", *, gpu_requested=1, best_effort=False):
    now = datetime.now(timezone.utc)
    return OnDemandCandidate(
        pod_uid=uid,
        pod_name=f"pod-{uid}",
        pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_requested=gpu_requested,
        min_runtime_seconds=0 if best_effort else 600,
        pod_created_at=now,
        next_attempt_at=now,
        usage_group=GROUP_NAME,
        best_effort=best_effort,
    )


def _config(**overrides):
    base = dict(
        ondemand_denial_event_enabled=True,
        ondemand_denial_event_repeat_minutes=30,
        ondemand_pause_event_enabled=True,
    )
    base.update(overrides)
    return make_config(**base)


def _pod(uid="uid-1"):
    """A pending pod whose scheduler verdict gets it past guard 1a."""
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name=f"pod-{uid}", namespace=USERNAME,
            annotations=None, labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(
            phase="Pending",
            conditions=[
                SimpleNamespace(
                    type="PodScheduled", status="False", reason="Unschedulable",
                    message="0/10 nodes are available: 5 Insufficient nvidia.com/gpu.",
                )
            ],
        ),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


def _state():
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    return state


class _Recorder:
    """Stands in for ``emit_admission_paused_event`` as ``main`` calls it."""

    def __init__(self, raises=None):
        self.calls: list[dict] = []
        self._raises = raises

    async def __call__(self, uid, name, namespace, message, *, gpu_class, guard):
        if self._raises is not None:
            raise self._raises
        self.calls.append(dict(
            uid=uid, name=name, namespace=namespace, message=message,
            gpu_class=gpu_class, guard=guard,
        ))


class _DenialRecorder:
    """Stands in for ``emit_lease_denied_event`` as ``main`` calls it."""

    def __init__(self):
        self.calls: list = []

    async def __call__(self, uid, name, namespace, detail, *, gpu_class, gpu_count, **_kw):
        self.calls.append(detail)


# ---------------------------------------------------------------------------
# k8s_client.emit_admission_paused_event — the Event write
# ---------------------------------------------------------------------------


class _CapturingCore:
    def __init__(self):
        self.events: list = []

    def create_namespaced_event(self, namespace, body):
        self.events.append((namespace, body))
        return SimpleNamespace()


def _emit(monkeypatch, message="paused, and here is why"):
    core = _CapturingCore()
    monkeypatch.setattr(k8s_client, "_core_v1", core)
    asyncio.run(
        emit_admission_paused_event(
            "uid-1", "pod-1", USERNAME, message, gpu_class=GPU_CLASS_LABEL, guard=4,
        )
    )
    return core


class TestEmitAdmissionPausedEvent:
    def test_event_is_a_warning_on_the_pod(self, monkeypatch):
        core = _emit(monkeypatch)
        namespace, event = core.events[0]
        # The pod's namespace is its owner's username, which is where they run
        # kubectl -- the whole reason this is an Event and not a log line.
        assert namespace == USERNAME
        assert event.metadata.namespace == USERNAME
        assert event.type == "Warning"
        assert event.reason == "OnDemandAdmissionPaused"
        assert event.action == "HoldOnDemandAdmission"
        ref = event.involved_object
        assert (ref.kind, ref.name, ref.namespace, ref.uid) == (
            "Pod", "pod-1", USERNAME, "uid-1",
        )

    def test_message_is_written_verbatim(self, monkeypatch):
        core = _emit(monkeypatch, message="exactly this")
        _ns, event = core.events[0]
        assert event.message == "exactly this"

    def test_generate_name_so_repeats_never_409(self, monkeypatch):
        core = _emit(monkeypatch)
        _ns, event = core.events[0]
        assert event.metadata.generate_name == "gpu-admission-paused-"
        assert event.metadata.name is None

    def test_emit_is_logged_with_the_guard(self, monkeypatch, caplog):
        with caplog.at_level(logging.INFO, logger="app.k8s_client"):
            _emit(monkeypatch)
        emitted = [r for r in caplog.records if "k8s.event_emitted" in r.getMessage()]
        assert len(emitted) == 1
        fields = kv_fields(emitted[0].getMessage())
        assert fields["reason"] == "OnDemandAdmissionPaused"
        assert fields["clabel"] == GPU_CLASS_LABEL
        assert fields["guard"] == "4"


# ---------------------------------------------------------------------------
# main._admission_paused_message — what the owner reads
# ---------------------------------------------------------------------------


class TestAdmissionPausedMessage:
    def test_overcommit_says_what_is_happening_and_who_to_ask(self, monkeypatch):
        m = _main_module(monkeypatch)
        msg = m._admission_paused_message(4, GPU_CLASS_LABEL, _config())
        assert f"gpu-class {GPU_CLASS_LABEL} is paused" in msg
        assert "online" in msg
        assert "Nothing about this pod needs to change" in msg
        assert msg.endswith("If this persists, contact support.")

    def test_stuck_holder_says_reserved_jobs_go_first(self, monkeypatch):
        m = _main_module(monkeypatch)
        msg = m._admission_paused_message(3, GPU_CLASS_LABEL, _config())
        assert f"gpu-class {GPU_CLASS_LABEL} is paused" in msg
        assert "hold a reservation" in msg
        assert "reserved jobs go first" in msg
        assert msg.endswith("If this persists, contact support.")

    def test_a_drained_class_says_there_is_nowhere_to_run(self, monkeypatch):
        m = _main_module(monkeypatch)
        msg = m._admission_paused_message(1, GPU_CLASS_LABEL, _config())
        assert f"gpu-class {GPU_CLASS_LABEL} is paused" in msg
        assert f"no {GPU_CLASS_LABEL} GPU nodes available" in msg
        assert "Nothing about this pod needs to change" in msg
        assert msg.endswith("If this persists, contact support.")

    def test_the_three_causes_read_differently(self, monkeypatch):
        m = _main_module(monkeypatch)
        config = _config()
        messages = {
            m._admission_paused_message(guard, GPU_CLASS_LABEL, config)
            for guard in (1, 3, 4)
        }
        assert len(messages) == 3

    def test_a_configured_contact_is_named_last(self, monkeypatch):
        m = _main_module(monkeypatch)
        config = _config(support_contact="https://support.example.edu/gpu")
        msg = m._admission_paused_message(4, GPU_CLASS_LABEL, config)
        # Nothing after the contact, so copying the URL out of kubectl describe
        # does not pick up a full stop.
        assert msg.endswith("contact support: https://support.example.edu/gpu")

    def test_it_is_stable_across_calls(self, monkeypatch):
        # The message is the throttle key: anything in it that drifts between
        # retries (a count, a timestamp) would restate the Event every retry.
        m = _main_module(monkeypatch)
        config = _config(support_contact="gpu-help@example.edu")
        assert (
            m._admission_paused_message(3, GPU_CLASS_LABEL, config)
            == m._admission_paused_message(3, GPU_CLASS_LABEL, config)
        )


# ---------------------------------------------------------------------------
# main._preflight_ondemand_candidate — which holds are told
# ---------------------------------------------------------------------------


def _preflight(monkeypatch, m, state, candidate, config=None, recorder=None):
    recorder = recorder if recorder is not None else _Recorder()

    async def fake_read_pod(name, namespace):
        return _pod(candidate.pod_uid)

    monkeypatch.setattr(m, "read_pod", fake_read_pod)
    monkeypatch.setattr(m, "emit_admission_paused_event", recorder)
    status, _ask = asyncio.run(
        m._preflight_ondemand_candidate(
            state, config or _config(), candidate.pod_uid, candidate
        )
    )
    return status, recorder


class TestPreflightTellsThePod:
    def test_an_overcommit_hold_is_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        candidate = _candidate()
        before = candidate.next_attempt_at

        status, rec = _preflight(monkeypatch, m, state, candidate)

        assert status == m._PREFLIGHT_RETRY
        assert candidate.next_attempt_at > before
        assert len(rec.calls) == 1
        call = rec.calls[0]
        assert (call["uid"], call["name"], call["namespace"]) == (
            "uid-1", "pod-uid-1", USERNAME,
        )
        assert call["guard"] == 4
        assert call["gpu_class"] == GPU_CLASS_LABEL
        assert call["message"] == m._admission_paused_message(
            4, GPU_CLASS_LABEL, _config()
        )

    def test_a_stuck_holder_hold_is_told_without_naming_anyone_elses_pod(
        self, monkeypatch
    ):
        m = _main_module(monkeypatch)
        state = _state()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        # The operator's warning names these; they are other users' pods (a
        # namespace is a username), so the owner's Event must not.
        state.stuck_holder_pods = {GPU_CLASS_LABEL: ["bob.train-7"]}

        status, rec = _preflight(monkeypatch, m, state, _candidate())

        assert status == m._PREFLIGHT_RETRY
        assert [c["guard"] for c in rec.calls] == [3]
        assert "bob" not in rec.calls[0]["message"]
        assert "train-7" not in rec.calls[0]["message"]

    def test_the_first_gate_to_hold_is_the_one_told(self, monkeypatch):
        # Guard 3 runs before guard 4, so a class under both is reported as the
        # stuck-holder pause: the Event names the gate that actually held it.
        m = _main_module(monkeypatch)
        state = _state()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        _status, rec = _preflight(monkeypatch, m, state, _candidate())
        assert [c["guard"] for c in rec.calls] == [3]

    def test_back_to_back_holds_tell_the_pod_once(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        candidate = _candidate()
        rec = _Recorder()
        _preflight(monkeypatch, m, state, candidate, recorder=rec)
        _preflight(monkeypatch, m, state, candidate, recorder=rec)
        assert len(rec.calls) == 1

    def test_a_best_effort_pod_is_not_told_about_a_gate_it_is_exempt_from(
        self, monkeypatch
    ):
        # Guard 4 does not hold best-effort candidates, so there is no pause to
        # report: telling the owner otherwise would be false.
        m = _main_module(monkeypatch)
        state = _state()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        status, rec = _preflight(
            monkeypatch, m, state, _candidate(best_effort=True)
        )
        assert status == m._PREFLIGHT_READY
        assert rec.calls == []

    def test_a_best_effort_pod_is_told_about_a_stuck_holder(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        status, rec = _preflight(
            monkeypatch, m, state, _candidate(best_effort=True)
        )
        assert status == m._PREFLIGHT_RETRY
        assert [c["guard"] for c in rec.calls] == [3]

    def test_a_drained_class_hold_is_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()
        state.class_node_counts = {GPU_CLASS_LABEL: 0}
        candidate = _candidate()
        before = candidate.next_attempt_at

        status, rec = _preflight(monkeypatch, m, state, candidate)

        assert status == m._PREFLIGHT_RETRY
        assert candidate.next_attempt_at > before
        assert [c["guard"] for c in rec.calls] == [1]
        assert rec.calls[0]["message"] == m._admission_paused_message(
            1, GPU_CLASS_LABEL, _config()
        )

    def test_a_drained_class_reports_the_drain_not_the_overcommit(self, monkeypatch):
        # A class with no nodes also reads as overcommitted whenever the app
        # counts any GPUs for it (physical 0 < app-side N).  Guard 1b runs
        # first, and "no nodes" is the more specific account of the two.
        m = _main_module(monkeypatch)
        state = _state()
        state.class_node_counts = {GPU_CLASS_LABEL: 0}
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        _status, rec = _preflight(monkeypatch, m, state, _candidate())
        assert [c["guard"] for c in rec.calls] == [1]

    def test_a_best_effort_pod_is_told_about_a_drained_class(self, monkeypatch):
        # Unlike guard 4, guard 1b has no best-effort exemption: a pod that asks
        # for no guarantee still needs a node to run on.
        m = _main_module(monkeypatch)
        state = _state()
        state.class_node_counts = {GPU_CLASS_LABEL: 0}
        status, rec = _preflight(
            monkeypatch, m, state, _candidate(best_effort=True)
        )
        assert status == m._PREFLIGHT_RETRY
        assert [c["guard"] for c in rec.calls] == [1]

    def test_an_unknown_node_count_is_not_a_pause(self, monkeypatch):
        # Fail-open: no snapshot yet, or a label the app does not know.  Nothing
        # is held, so there is nothing to tell.
        m = _main_module(monkeypatch)
        state = _state()
        state.class_node_counts = {}
        status, rec = _preflight(monkeypatch, m, state, _candidate())
        assert status == m._PREFLIGHT_READY
        assert rec.calls == []

    def test_a_fragmented_class_is_not_told(self, monkeypatch):
        # Guard 5: a full cluster, not a fault, and it clears as jobs finish.
        m = _main_module(monkeypatch)
        state = _state()
        state.node_free_by_class = {GPU_CLASS_LABEL: 1}
        status, rec = _preflight(
            monkeypatch, m, state, _candidate(gpu_requested=2)
        )
        assert status == m._PREFLIGHT_RETRY
        assert rec.calls == []

    def test_disabled_holds_just_as_before(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        candidate = _candidate()
        before = candidate.next_attempt_at
        status, rec = _preflight(
            monkeypatch, m, state, candidate,
            config=_config(ondemand_pause_event_enabled=False),
        )
        assert status == m._PREFLIGHT_RETRY
        assert candidate.next_attempt_at > before
        assert rec.calls == []
        assert state.pending_status == {}

    def test_a_failed_emit_never_disturbs_the_hold(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = _state()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        candidate = _candidate()
        before = candidate.next_attempt_at
        with caplog.at_level(logging.WARNING, logger="app.main"):
            status, _rec = _preflight(
                monkeypatch, m, state, candidate,
                recorder=_Recorder(raises=RuntimeError("apiserver said no")),
            )
        assert status == m._PREFLIGHT_RETRY
        assert candidate.next_attempt_at > before
        failed = [r for r in caplog.records if "k8s.event_failed" in r.getMessage()]
        assert len(failed) == 1
        assert kv_fields(failed[0].getMessage())["reason"] == "OnDemandAdmissionPaused"
        # Not stamped, so the next hold retries the emit instead of staying
        # silent for the whole repeat interval.
        assert state.pending_status == {}


class TestAdmissionBatchTellsEachHeldPod:
    def test_every_held_candidate_is_told_under_its_own_uid(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state()
        state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
        for uid in ("uid-1", "uid-2"):
            state.ondemand_candidates[uid] = _candidate(uid)

        async def fake_read_pod(name, namespace):
            return _pod(name.removeprefix("pod-"))

        class _NoLeaseClient:
            async def create_ondemand_reservation(self, req):  # pragma: no cover
                raise AssertionError("a paused class must not be offered a lease")

        rec = _Recorder()
        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "emit_admission_paused_event", rec)
        config = _config(ondemand_delegate_admission=False)
        asyncio.run(m._run_ondemand_admission(state, _NoLeaseClient(), config))

        assert sorted(c["uid"] for c in rec.calls) == ["uid-1", "uid-2"]
        # Held, not dropped: both wait for the pause to lift.
        assert set(state.ondemand_candidates) == {"uid-1", "uid-2"}


class TestCordonedClassEndToEnd:
    """Guard 1b through the real node snapshot, not a hand-built count.

    ``snapshot_node_gpu_inventory`` lists a class only while one of its nodes is
    schedulable, so a class with every node cordoned is *absent* from the
    inventory, never ``0``.  Guard 1b fails open on an absent class, and every
    earlier test injected the ``0`` directly — which is how the guard went
    unable to hold anything without a test noticing.
    """

    def _tick(self, monkeypatch, *, cordoned, app_knows_class=True, ready=None):
        from tests.test_k8s_capacity import TAINT_KEY, _FakeCoreV1, _node, _taint

        m = _main_module(monkeypatch)
        nodes = [
            _node(
                "gpu-1", taints=[_taint(TAINT_KEY, GPU_CLASS_LABEL)],
                allocatable={"nvidia.com/gpu": "8"}, unschedulable=cordoned,
                ready=ready,
            ),
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))

        async def no_pods(*a, **kw):
            return []

        async def fake_read_pod(name, namespace):
            return _pod(name.removeprefix("pod-"))

        class _LeaseRecorder:
            def __init__(self):
                self.requests = []

            async def create_ondemand_reservation(self, req):
                self.requests.append(req)
                return LeaseAttempt(status=409, detail="no capacity")

        rec, client = _Recorder(), _LeaseRecorder()
        monkeypatch.setattr(m, "snapshot_tolerated_pods", no_pods)
        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "emit_admission_paused_event", rec)
        monkeypatch.setattr(m, "emit_lease_denied_event", _DenialRecorder())

        state = _state()
        if not app_knows_class:
            state.gpu_class_ids = {}
        state.ondemand_candidates["uid-1"] = _candidate("uid-1")
        asyncio.run(m._run_queue_tick(state, client, _config()))
        return state, rec, client

    def test_a_fully_cordoned_class_is_held_and_the_pod_told(self, monkeypatch):
        state, rec, client = self._tick(monkeypatch, cordoned=True)
        assert state.class_node_counts == {GPU_CLASS_LABEL: 0}
        assert [c["guard"] for c in rec.calls] == [1]
        # Held before the app is asked: no lease is requested for a class with
        # nowhere to run.
        assert client.requests == []
        assert "uid-1" in state.ondemand_candidates

    def test_a_class_whose_only_node_is_not_ready_is_held_too(self, monkeypatch):
        """A crashed node is never cordoned by anyone, and still reports its
        allocatable GPUs; only its Ready condition says it is gone.  Before the
        snapshot read it, guard 1b let a lease through for a class with nowhere
        to run, and only guard 3 caught it — after the SU was charged."""
        state, rec, client = self._tick(monkeypatch, cordoned=False, ready="Unknown")
        assert state.class_node_counts == {GPU_CLASS_LABEL: 0}
        assert [c["guard"] for c in rec.calls] == [1]
        assert client.requests == []

    def test_a_schedulable_node_lets_it_through(self, monkeypatch):
        state, rec, client = self._tick(monkeypatch, cordoned=False)
        assert state.class_node_counts == {GPU_CLASS_LABEL: 1}
        assert rec.calls == []
        assert len(client.requests) == 1

    def test_a_class_the_app_does_not_know_is_never_held_by_it(self, monkeypatch):
        # Fail-open stays fail-open for a label the controller cannot vouch for.
        # (Such a candidate stops at class_id_unknown further on instead.)
        state, rec, client = self._tick(
            monkeypatch, cordoned=True, app_knows_class=False
        )
        assert state.class_node_counts == {}
        assert rec.calls == []


# ---------------------------------------------------------------------------
# main._post_pending_status — one throttle, one cadence, one story
# ---------------------------------------------------------------------------


# What each test has told so far; the throttle lives in
# ControllerState.pending_status, keyed by pod uid.  Fresh per test.
_STATE = ControllerState()


@pytest.fixture(autouse=True)
def _fresh_state():
    global _STATE
    _STATE = ControllerState()
    yield


def _told(uid="uid-1"):
    """The status last told to *uid*'s owner, or None."""
    return _STATE.pending_status.get(uid, {}).get(PENDING_TOPIC_HOLD)


def _pause(m, monkeypatch, candidate, guard, now, rec, config=None):
    monkeypatch.setattr(m, "emit_admission_paused_event", rec)
    asyncio.run(m._emit_admission_paused_event(
        config or _config(), _STATE, candidate.pod_uid, candidate, guard, now,
    ))


def _deny(m, monkeypatch, candidate, detail, now, rec):
    monkeypatch.setattr(m, "emit_lease_denied_event", rec)
    asyncio.run(m._emit_lease_denial_event(
        _config(), _STATE, candidate.pod_uid, candidate, detail, now,
    ))


class TestPauseCadence:
    """The same cadence as the denial Event: that is what "aligned" means."""

    def test_an_unchanged_pause_waits_out_the_interval(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate, rec = _candidate(), _Recorder()
        _pause(m, monkeypatch, candidate, 4, NOW, rec)
        _pause(m, monkeypatch, candidate, 4, NOW + timedelta(minutes=29), rec)
        assert len(rec.calls) == 1

    def test_an_unchanged_pause_is_restated_after_it(self, monkeypatch):
        # Events expire (an hour by default), so a pod still held must say so
        # again or kubectl describe goes blank on it.
        m = _main_module(monkeypatch)
        candidate, rec = _candidate(), _Recorder()
        _pause(m, monkeypatch, candidate, 4, NOW, rec)
        later = NOW + timedelta(minutes=30)
        _pause(m, monkeypatch, candidate, 4, later, rec)
        assert len(rec.calls) == 2
        assert _told().at == later

    def test_the_interval_is_the_denial_events(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate, rec = _candidate(), _Recorder()
        config = _config(ondemand_denial_event_repeat_minutes=5)
        _pause(m, monkeypatch, candidate, 4, NOW, rec, config)
        _pause(m, monkeypatch, candidate, 4, NOW + timedelta(minutes=5), rec, config)
        assert len(rec.calls) == 2

    def test_zero_restates_every_hold(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate, rec = _candidate(), _Recorder()
        config = _config(ondemand_denial_event_repeat_minutes=0)
        _pause(m, monkeypatch, candidate, 4, NOW, rec, config)
        _pause(m, monkeypatch, candidate, 4, NOW, rec, config)
        assert len(rec.calls) == 2

    def test_a_changed_cause_is_told_at_once(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate, rec = _candidate(), _Recorder()
        _pause(m, monkeypatch, candidate, 3, NOW, rec)
        _pause(m, monkeypatch, candidate, 4, NOW + timedelta(minutes=1), rec)
        assert [c["guard"] for c in rec.calls] == [3, 4]

    def test_pods_do_not_share_throttle_state(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec = _Recorder()
        _pause(m, monkeypatch, _candidate("uid-1"), 4, NOW, rec)
        _pause(m, monkeypatch, _candidate("uid-2"), 4, NOW, rec)
        assert len(rec.calls) == 2


class TestOneStoryPerPod:
    """A pause and a denial share one throttle, so the newest Event on the pod
    always describes what is blocking it now."""

    def test_a_denial_after_a_pause_is_told_at_once(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate, paused, denied = _candidate(), _Recorder(), _DenialRecorder()
        _pause(m, monkeypatch, candidate, 4, NOW, paused)
        _deny(m, monkeypatch, candidate, DETAIL, NOW + timedelta(minutes=1), denied)
        assert len(paused.calls) == 1
        assert denied.calls == [DETAIL]

    def test_a_pause_after_a_denial_is_told_at_once(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate, paused, denied = _candidate(), _Recorder(), _DenialRecorder()
        _deny(m, monkeypatch, candidate, DETAIL, NOW, denied)
        _pause(m, monkeypatch, candidate, 4, NOW + timedelta(minutes=1), paused)
        assert denied.calls == [DETAIL]
        assert len(paused.calls) == 1

    def test_returning_to_a_pause_is_told_again(self, monkeypatch):
        # With a throttle per Event reason, this third notice would be held back
        # for up to the whole interval, leaving the denial as the newest Event
        # on a pod the pause is holding again.
        m = _main_module(monkeypatch)
        candidate, paused, denied = _candidate(), _Recorder(), _DenialRecorder()
        _pause(m, monkeypatch, candidate, 4, NOW, paused)
        _deny(m, monkeypatch, candidate, DETAIL, NOW + timedelta(minutes=1), denied)
        _pause(m, monkeypatch, candidate, 4, NOW + timedelta(minutes=2), paused)
        assert len(paused.calls) == 2
        assert _told().key[0] == "OnDemandAdmissionPaused"


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfig:
    def _from_env(self, monkeypatch, **env):
        from app.config import Config

        monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
        monkeypatch.setenv("RESERVATION_API_KEY", "test-key")
        for name in ("ONDEMAND_PAUSE_EVENT_ENABLED", "SUPPORT_CONTACT"):
            monkeypatch.delenv(name, raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return Config.from_env()

    def test_on_by_default_with_no_contact(self, monkeypatch):
        config = self._from_env(monkeypatch)
        assert config.ondemand_pause_event_enabled is True
        assert config.support_contact is None

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", " OFF "])
    def test_falsy_words_disable_it(self, monkeypatch, raw):
        config = self._from_env(monkeypatch, ONDEMAND_PAUSE_EVENT_ENABLED=raw)
        assert config.ondemand_pause_event_enabled is False

    def test_contact_is_trimmed(self, monkeypatch):
        config = self._from_env(monkeypatch, SUPPORT_CONTACT="  gpu-help@example.edu \n")
        assert config.support_contact == "gpu-help@example.edu"

    def test_a_blank_contact_is_no_contact(self, monkeypatch):
        config = self._from_env(monkeypatch, SUPPORT_CONTACT="   ")
        assert config.support_contact is None
