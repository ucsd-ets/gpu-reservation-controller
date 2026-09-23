"""Telling the owner of a pod queued for one of their reservations what it waits on.

A pod on the reserved path -- queued for a booking that has not opened, or one
whose GPUs its owner's other pods hold -- used to get no Event at all:
kube-scheduler's ``FailedScheduling`` names only an untolerated taint, and the
newest Event the controller had put on the pod could be a ``NoReservation`` from
before its owner booked, telling them to book what they already had.  Now it is
told, through the throttle every pending-pod Event shares:

- ``WaitingForReservation`` (Normal) -- the window has not opened;
- ``ReservationFull`` (Warning) -- it is open, and the owner's other pods hold
  its GPUs, which are named;
- ``ReservationTooSmall`` (Warning) -- it holds fewer GPUs than the pod asks for.

The queue also now holds each pod on a reservation that can take it: routing's
own choice (``enqueue_pod`` used to re-match, and could pin a pod to a full
booking when routing had found one with room), a booking big enough for the pod
over a sooner, smaller one, and -- on every tick -- an open booking with room
over whatever the pod was waiting on.  Nothing here touches a real cluster.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.controller import PENDING_TOPIC_ANNOTATIONS, QueueEntry
from app.k8s_client import (
    NO_RESERVATION_REASON,
    RESERVATION_FULL_REASON,
    RESERVATION_TOO_SMALL_REASON,
    WAITING_FOR_RESERVATION_REASON,
    ToleratedPodInfo,
    local_display,
)

from tests.conftest import GPU_CLASS_LABEL, GROUP_NAME, USERNAME, kv_fields, make_config
from tests.test_pod_problem_events import (
    MIN_RUNTIME,
    USAGE_GROUP,
    _booking,
    _candidate,
    _emit,
    _FakeWatcher,
    _main_module,
    _pod,
    _preflight,
    _ProblemRecorder,
    _state,
    _told,
    _watch,
)

MISSING_RUNTIME = f"it has no {MIN_RUNTIME} annotation"


def _now() -> datetime:
    """The real clock: the watch loop and the queue tick read it themselves."""
    return datetime.now(timezone.utc)


def _open(res_id=1, *, gpu_count=1, **kw):
    """A booking whose window opened ten minutes ago and has an hour and more left."""
    return _booking(res_id, start=_now() - timedelta(minutes=10), gpu_count=gpu_count, **kw)


def _future(res_id=1, *, days=30, gpu_count=1, **kw):
    return _booking(res_id, start=_now() + timedelta(days=days), gpu_count=gpu_count, **kw)


def _entry(res, *, uid="uid-1", gpus=1, clauses=()):
    return QueueEntry(
        pod_uid=uid, pod_name=f"pod-{uid}", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=gpus, reservation=res,
        next_attempt_at=_now() - timedelta(seconds=1),
        ondemand_ineligibility=tuple(clauses),
    )


def _hold(state, res_id, uid, name, *, gpus=1, namespace=USERNAME):
    """Put a running pod of *namespace* on reservation *res_id*."""
    state.record_placement(res_id, uid, gpus)
    state.holder_names[uid] = (namespace, name)


def _wanting(gpus, **kw):
    """A pod requesting *gpus* GPUs."""
    pod = _pod(**kw)
    pod.spec.containers[0].resources.requests = {"nvidia.com/gpu": str(gpus)}
    return pod


# Neither JIT-eligible (no minimum runtime) nor ineligible for any other reason.
NOT_ON_DEMAND = {USAGE_GROUP: GROUP_NAME}
# JIT-eligible.
ON_DEMAND = {USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "3600"}


# ---------------------------------------------------------------------------
# ControllerState: which reservation a queued pod waits on
# ---------------------------------------------------------------------------


class TestFindBestReservation:
    def test_a_booking_big_enough_beats_a_sooner_smaller_one(self):
        small = _future(1, days=1, gpu_count=1)
        big = _future(2, days=2, gpu_count=4)
        state = _state(small, big)
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL, None, 2).id == 2

    def test_a_smaller_one_when_nothing_is_big_enough(self):
        # So the pod can be told its booking is too small, rather than that it
        # has none.
        state = _state(_future(1, days=1, gpu_count=1), _future(2, days=2, gpu_count=1))
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL, None, 2).id == 1

    def test_without_a_size_the_soonest_wins_as_before(self):
        state = _state(_future(1, days=1, gpu_count=1), _future(2, days=2, gpu_count=4))
        assert state.find_best_reservation(USERNAME, GPU_CLASS_LABEL).id == 1


class TestEnqueuePod:
    def test_the_reservation_routing_chose_is_the_one_queued_for(self):
        # Re-matching would pick the sooner booking, which is full.
        full, roomy = _open(1), _booking(2, start=_now() - timedelta(minutes=5), gpu_count=1)
        state = _state(full, roomy)
        _hold(state, 1, "uid-9", "jupyter-alice")
        state.enqueue_pod("uid-1", "pod-uid-1", USERNAME, GPU_CLASS_LABEL, 1, reservation=roomy)
        assert state.task_queue["uid-1"].reservation.id == 2

    def test_without_one_it_matches_a_booking_big_enough(self):
        state = _state(_future(1, days=1, gpu_count=1), _future(2, days=2, gpu_count=2))
        state.enqueue_pod("uid-1", "pod-uid-1", USERNAME, GPU_CLASS_LABEL, 2)
        assert state.task_queue["uid-1"].reservation.id == 2

    def test_moving_to_another_reservation_keeps_what_routing_recorded(self):
        first, second = _future(1, days=1), _future(2, days=2)
        state = _state(first, second)
        state.task_queue["uid-1"] = _entry(first, clauses=["why not"])
        state.enqueue_pod("uid-1", "pod-uid-1", USERNAME, GPU_CLASS_LABEL, 1, reservation=second)
        entry = state.task_queue["uid-1"]
        assert (entry.reservation.id, entry.ondemand_ineligibility) == (2, ("why not",))


class TestReconcileQueue:
    def test_a_cancelled_booking_is_replaced_by_one_big_enough(self):
        cancelled = _future(1, days=1, gpu_count=2)
        state = _state(_future(2, days=2, gpu_count=1), _future(3, days=3, gpu_count=2))
        state.task_queue["uid-1"] = _entry(cancelled, gpus=2, clauses=["why not"])
        state.reconcile_queue()
        entry = state.task_queue["uid-1"]
        assert (entry.reservation.id, entry.ondemand_ineligibility) == (3, ("why not",))


class TestReservationHolders:
    def test_the_owners_pods_are_named_and_the_rest_counted(self):
        state = _state(_open(1, gpu_count=4))
        _hold(state, 1, "u1", "zeta")
        _hold(state, 1, "u2", "alpha")
        # Left behind by an ownership change: counted, never named.
        _hold(state, 1, "u3", "bobs-pod", namespace="bob")
        # Admitted since the last snapshot by a path that records no name.
        state.record_placement(1, "u4", 1)
        assert state.reservation_holders(1, USERNAME) == (("alpha", "zeta"), 2)

    def test_the_pod_asking_is_left_out(self):
        state = _state(_open(1, gpu_count=2))
        _hold(state, 1, "u1", "zeta")
        _hold(state, 1, "uid-1", "me")
        assert state.reservation_holders(1, USERNAME, exclude_uid="uid-1") == (("zeta",), 0)

    def test_a_name_outliving_its_pod_is_inert(self):
        # The directory is looked up through occupancy, never the other way.
        state = _state(_open(1))
        state.holder_names["gone"] = (USERNAME, "long-gone")
        assert state.reservation_holders(1, USERNAME) == ((), 0)


# ---------------------------------------------------------------------------
# main: what each status says
# ---------------------------------------------------------------------------


class TestQueuedStatus:
    def _status(self, monkeypatch, state, entry, now=None):
        m = _main_module(monkeypatch)
        return m._queued_status(state, entry, now or _now())

    def test_a_booking_not_yet_open(self, monkeypatch):
        res = _future(42, days=2, gpu_count=2)
        reason, message = self._status(monkeypatch, _state(res), _entry(res))
        assert reason == WAITING_FOR_RESERVATION_REASON
        assert message == (
            f"Waiting for your GPU reservation #42 (2 x {GPU_CLASS_LABEL}, "
            f"{local_display(res.start_utc)} to {local_display(res.end_utc)}) to open; "
            f"this pod will be admitted shortly after it does."
        )

    def test_it_does_not_drift_while_the_pod_waits(self, monkeypatch):
        # The message is the throttle key: anything relative ("opens in 20
        # minutes") would restate it on every evaluation.
        res = _future(days=2)
        state, entry = _state(res), _entry(res)
        early = self._status(monkeypatch, state, entry, _now())
        later = self._status(monkeypatch, state, entry, _now() + timedelta(days=1))
        assert early == later

    def test_what_keeps_it_off_on_demand_is_said(self, monkeypatch):
        res = _future()
        entry = _entry(res, clauses=["one thing", "another"])
        _reason, message = self._status(monkeypatch, _state(res), entry)
        assert message.endswith(
            "It cannot be admitted on demand before then: one thing; and another."
        )

    def test_an_open_booking_the_owners_pods_fill(self, monkeypatch):
        res = _open(7, gpu_count=1)
        state = _state(res)
        _hold(state, 7, "u9", "jupyter-alice")
        reason, message = self._status(monkeypatch, state, _entry(res, clauses=["no runtime"]))
        assert reason == RESERVATION_FULL_REASON
        assert message.startswith("Your GPU reservation #7 (")
        assert ") is fully in use by your pod jupyter-alice." in message
        assert "This pod will be admitted as soon as 1 GPU is free." in message
        assert message.endswith("It cannot be admitted on demand meanwhile: no runtime.")

    def test_a_partly_free_booking(self, monkeypatch):
        res = _open(7, gpu_count=4)
        state = _state(res)
        for n in range(3):
            _hold(state, 7, f"u{n}", f"job-{n}")
        _reason, message = self._status(monkeypatch, state, _entry(res, gpus=2))
        assert (
            "has only 1 of its 4 GPUs free, and this pod requests 2; the rest are in "
            "use by your pods job-0, job-1, job-2." in message
        )
        assert "as soon as 2 GPUs are free." in message

    def test_a_booking_too_small_is_said_before_it_opens(self, monkeypatch):
        # Waiting for it to open would be waiting for nothing.
        res = _future(3, gpu_count=1)
        reason, message = self._status(monkeypatch, _state(res), _entry(res, gpus=2))
        assert reason == RESERVATION_TOO_SMALL_REASON
        assert message.startswith("This pod requests 2 GPUs, but your GPU reservation #3 (")
        assert "holds only 1, so the pod can never be admitted under it." in message
        assert message.endswith(
            f"Book a gpu-class {GPU_CLASS_LABEL} reservation of at least 2 GPUs, or "
            f"lower the pod's nvidia.com/gpu request and recreate it."
        )

    def test_too_small_waits_for_the_full_reservation_list(self, monkeypatch):
        # It implies the owner holds nothing larger, which a push-only view of
        # the reservations cannot show.
        res = _future(3, gpu_count=1)
        state = _state(res)
        state.reservations_known = False
        assert self._status(monkeypatch, state, _entry(res, gpus=2)) is None

    def test_an_open_booking_with_room_says_nothing(self, monkeypatch):
        # The next attempt admits it -- or an admission error is being retried,
        # which is the operator's to see, not the owner's.
        res = _open(gpu_count=2)
        state = _state(res)
        _hold(state, 1, "u9", "jupyter-alice")
        assert self._status(monkeypatch, state, _entry(res)) is None


class TestHoldersPhrase:
    @pytest.mark.parametrize("names,others,expected", [
        (("a",), 0, "your pod a"),
        (("a", "b"), 0, "your pods a, b"),
        (("a", "b", "c", "d", "e"), 0, "your pods a, b, c and 2 others"),
        (("a",), 1, "your pod a and 1 other"),
        ((), 1, "1 other pod"),
        ((), 3, "3 other pods"),
        ((), 0, None),
    ])
    def test_phrasing(self, monkeypatch, names, others, expected):
        m = _main_module(monkeypatch)
        assert m._holders_phrase(names, others) == expected

    def test_a_pod_name_is_made_fit_to_print(self, monkeypatch):
        m = _main_module(monkeypatch)
        assert m._holders_phrase(("evil\x1b[2Jname",), 0) == "your pod evil[2Jname"


class TestEmitTypes:
    @pytest.mark.parametrize("reason,event_type,prefix", [
        # Waiting for a window its owner chose is the system working as intended.
        (WAITING_FOR_RESERVATION_REASON, "Normal", "gpu-reservation-wait-"),
        (RESERVATION_FULL_REASON, "Warning", "gpu-reservation-full-"),
        (RESERVATION_TOO_SMALL_REASON, "Warning", "gpu-reservation-too-small-"),
    ])
    def test_each_is_written_on_the_pod(self, monkeypatch, reason, event_type, prefix):
        namespace, event = _emit(monkeypatch, reason=reason)
        assert namespace == USERNAME
        assert (event.type, event.reason, event.action) == (event_type, reason, "AdmitPod")
        assert event.metadata.generate_name == prefix


# ---------------------------------------------------------------------------
# main.pod_watch_loop: routing a pod onto the queue
# ---------------------------------------------------------------------------


class TestRouting:
    def test_a_pod_whose_booking_opens_soon_is_told_it_waits(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_booking(start=_now() + timedelta(minutes=10), gpu_count=1))
        pod = _pod(annotations=ON_DEMAND)
        rec, state, batches = _watch(monkeypatch, m, make_config(), [("ADDED", pod)], state=state)
        assert "uid-1" in state.task_queue and not state.ondemand_candidates
        [call] = rec.calls
        assert call["reason"] == WAITING_FOR_RESERVATION_REASON
        # It would wait for this booking however it was annotated.
        assert "on demand" not in call["message"]
        assert (call["gpu_class"], call["gpu_count"]) == (GPU_CLASS_LABEL, 1)
        assert batches == []

    def test_a_pod_waiting_for_a_far_booking_is_told_what_would_start_it_sooner(
        self, monkeypatch
    ):
        m = _main_module(monkeypatch)
        state = _state(_future())
        rec, state, _b = _watch(
            monkeypatch, m, make_config(), [("ADDED", _pod(annotations=NOT_ON_DEMAND))],
            state=state,
        )
        [call] = rec.calls
        assert call["reason"] == WAITING_FOR_RESERVATION_REASON
        assert f"It cannot be admitted on demand before then: {MISSING_RUNTIME}" in call["message"]
        assert state.task_queue["uid-1"].ondemand_ineligibility

    def test_nothing_is_offered_where_on_demand_admission_is_off(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, state, _b = _watch(
            monkeypatch, m, make_config(ondemand_lease_enabled=False),
            [("ADDED", _pod(annotations=NOT_ON_DEMAND))], state=_state(_future()),
        )
        [call] = rec.calls
        assert "on demand" not in call["message"]
        assert state.task_queue["uid-1"].ondemand_ineligibility == ()

    def test_routing_queues_the_pod_where_it_found_room(self, monkeypatch):
        # Both open; the sooner one is full.  enqueue_pod used to re-match,
        # pinning the pod to the full one until its window ended.
        m = _main_module(monkeypatch)
        full = _open(1)
        roomy = _booking(2, start=_now() - timedelta(minutes=5), gpu_count=1)
        state = _state(full, roomy)
        _hold(state, 1, "u9", "jupyter-alice")
        rec, state, _b = _watch(
            monkeypatch, m, make_config(), [("ADDED", _pod(annotations=NOT_ON_DEMAND))],
            state=state,
        )
        assert state.task_queue["uid-1"].reservation.id == 2
        # Room, so nothing to tell: the stubbed admission just did not land.
        assert rec.calls == []

    def test_a_pod_on_a_full_booking_is_told_whose_pods_hold_it(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_open(1))
        _hold(state, 1, "u9", "jupyter-alice")
        rec, state, _b = _watch(
            monkeypatch, m, make_config(), [("ADDED", _pod(annotations=NOT_ON_DEMAND))],
            state=state,
        )
        [call] = rec.calls
        assert call["reason"] == RESERVATION_FULL_REASON
        assert "fully in use by your pod jupyter-alice." in call["message"]
        assert f"on demand meanwhile: {MISSING_RUNTIME}" in call["message"]

    def test_a_pod_asking_more_than_its_booking_holds_is_told(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, state, _b = _watch(
            monkeypatch, m, make_config(),
            [("ADDED", _wanting(2, annotations=NOT_ON_DEMAND))],
            state=_state(_future(gpu_count=1)),
        )
        [call] = rec.calls
        assert call["reason"] == RESERVATION_TOO_SMALL_REASON
        assert call["gpu_count"] == 2
        assert f"It cannot be admitted on demand either: {MISSING_RUNTIME}" in call["message"]

    def test_a_queued_pod_that_goes_on_demand_leaves_the_queue(self, monkeypatch):
        # It lost the race for its booking's last GPU; the resync routes it on
        # demand, and it must not stay queued as well (two paths, two stories).
        m = _main_module(monkeypatch)
        res = _open(1)
        state = _state(res)
        state.task_queue["uid-1"] = _entry(res)
        _hold(state, 1, "u9", "jupyter-alice")
        _rec, state, batches = _watch(
            monkeypatch, m, make_config(), [("ADDED", _pod(annotations=ON_DEMAND))],
            state=state,
        )
        assert "uid-1" not in state.task_queue
        assert "uid-1" in state.ondemand_candidates
        assert batches == [0]

    def test_a_modified_event_does_not_strand_it(self, monkeypatch):
        # Only ADDED adds a candidate, so only ADDED may take the pod off the
        # queue -- or it would be on neither path until the next resync.
        m = _main_module(monkeypatch)
        res = _open(1)
        state = _state(res)
        state.task_queue["uid-1"] = _entry(res)
        _hold(state, 1, "u9", "jupyter-alice")
        _rec, state, _b = _watch(
            monkeypatch, m, make_config(), [("MODIFIED", _pod(annotations=ON_DEMAND))],
            state=state,
        )
        assert "uid-1" in state.task_queue

    def test_a_pod_back_on_the_queue_leaves_the_candidates(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_future())
        state.ondemand_candidates["uid-1"] = _candidate()
        _rec, state, _b = _watch(
            monkeypatch, m, make_config(), [("MODIFIED", _pod(annotations=NOT_ON_DEMAND))],
            state=state,
        )
        assert "uid-1" in state.task_queue
        assert "uid-1" not in state.ondemand_candidates

    def test_a_pod_that_finishes_while_queued_leaves_the_queue(self, monkeypatch):
        m = _main_module(monkeypatch)

        async def noop(*args, **kwargs):
            return None

        monkeypatch.setattr(m, "_report_overstay_if_any", noop)
        monkeypatch.setattr(m, "_teardown_ondemand_lease", noop)
        res = _future()
        state = _state(res)
        state.task_queue["uid-1"] = _entry(res)
        _rec, state, _b = _watch(
            monkeypatch, m, make_config(), [("MODIFIED", _pod(phase="Succeeded"))],
            state=state,
        )
        assert state.task_queue == {}

    def test_a_resync_inside_the_repeat_is_not_told_again(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(annotations=NOT_ON_DEMAND)
        rec, _state_, _b = _watch(
            monkeypatch, m, make_config(), [("ADDED", pod), ("ADDED", pod)],
            state=_state(_future()),
        )
        assert rec.reasons == [WAITING_FOR_RESERVATION_REASON]

    def test_a_booking_made_after_no_reservation_was_told_replaces_it(self, monkeypatch):
        # The gap this closes: the newest Event on the pod said it had no
        # reservation, and went on saying so after its owner booked one.
        m = _main_module(monkeypatch)
        rec = _ProblemRecorder()
        pod = _pod(annotations=NOT_ON_DEMAND)
        _r, state, _b = _watch(
            monkeypatch, m, make_config(), [("ADDED", pod)], state=_state(), recorder=rec,
        )
        state.reservations = [_future()]
        _watch(monkeypatch, m, make_config(), [("ADDED", pod)], state=state, recorder=rec)
        assert rec.reasons == [NO_RESERVATION_REASON, WAITING_FOR_RESERVATION_REASON]

    def test_the_switch_turns_them_off(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, state, _b = _watch(
            monkeypatch, m, make_config(reservation_wait_event_enabled=False),
            [("ADDED", _pod(annotations=NOT_ON_DEMAND))], state=_state(_future()),
        )
        assert "uid-1" in state.task_queue
        assert rec.calls == []

    def test_no_annotation_notice_rides_beside_it(self, monkeypatch):
        # An ignored annotation is said inside the wait Event, where it is why
        # the pod cannot start sooner -- not in a second Event beside it.
        m = _main_module(monkeypatch)
        rec, state, _b = _watch(
            monkeypatch, m, make_config(),
            [("ADDED", _pod(annotations={USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "4h"}))],
            state=_state(_future()),
        )
        [call] = rec.calls
        assert call["reason"] == WAITING_FOR_RESERVATION_REASON
        assert f"its {MIN_RUNTIME} annotation is '4h'" in call["message"]
        assert _told(state, topic=PENDING_TOPIC_ANNOTATIONS) is None

    def test_an_admitted_pod_is_remembered_as_a_holder(self, monkeypatch):
        m = _main_module(monkeypatch)
        tolerated = _pod(
            phase="Running",
            annotations={"galends/booking-reference": "res-1"},
            tolerations=[SimpleNamespace(
                key="gpu-class-reservation", value=GPU_CLASS_LABEL, effect="NoSchedule",
            )],
        )
        _rec, state, _b = _watch(
            monkeypatch, m, make_config(), [("MODIFIED", tolerated)], state=_state(_open()),
        )
        assert state.holder_names["uid-1"] == (USERNAME, "pod-uid-1")


class TestPreflightReroute:
    def test_a_candidate_rerouted_to_a_booking_is_told_it_waits(self, monkeypatch):
        # Its newest Event may say its lease was denied or its class paused;
        # neither applies once it waits for a booking instead.
        m = _main_module(monkeypatch)
        state = _state(_booking(start=_now() + timedelta(minutes=10), gpu_count=1))
        status, _ask, rec = _preflight(monkeypatch, m, state, _candidate())
        assert status == m._PREFLIGHT_REMOVE
        assert state.task_queue["uid-1"].reservation.id == 1
        [call] = rec.calls
        assert call["reason"] == WAITING_FOR_RESERVATION_REASON


# ---------------------------------------------------------------------------
# main._run_queue_tick: the queue, every tick
# ---------------------------------------------------------------------------


def _holder(uid, name, res_id=1, *, gpus=1, namespace=USERNAME):
    return ToleratedPodInfo(
        namespace=namespace, name=name, uid=uid, gpu_class=GPU_CLASS_LABEL,
        booking_reference=f"res-{res_id}", reservation_id=res_id, gpu_count=gpus,
        phase="Running", scheduled_false=False,
    )


class _Tick:
    """Runs ``_run_queue_tick`` against a stubbed cluster, recording what the
    reserved path attempted and what it told."""

    def __init__(self, monkeypatch, *, admits=()):
        self.m = _main_module(monkeypatch)
        self.rec = _ProblemRecorder()
        self.attempts: list[tuple[str, int]] = []
        self.snapshot: list[ToleratedPodInfo] = []
        self.during_attempt = None
        admits = set(admits)

        async def snapshot(*args, **kwargs):
            return list(self.snapshot)

        async def noop(*args, **kwargs):
            return None

        async def attempt(state, uid, entry, gate=None):
            self.attempts.append((uid, entry.reservation.id))
            if self.during_attempt is not None:
                self.during_attempt(state, uid)
            return uid in admits

        monkeypatch.setattr(self.m, "snapshot_tolerated_pods", snapshot)
        monkeypatch.setattr(self.m, "_apply_guarantee_status", noop)
        monkeypatch.setattr(self.m, "_apply_reservation_facts", noop)
        monkeypatch.setattr(self.m, "_run_ondemand_admission", noop)
        monkeypatch.setattr(self.m, "_try_apply_toleration", attempt)
        monkeypatch.setattr(self.m, "emit_pending_pod_event", self.rec)

    def __call__(self, state, **overrides):
        config = make_config(
            ondemand_lease_enabled=False, pod_adoption_enabled=False,
            ondemand_merge_enabled=False, **overrides,
        )
        asyncio.run(self.m._run_queue_tick(state, None, config))


class TestQueueTick:
    def test_a_pod_waiting_for_its_window_is_told_so(self, monkeypatch):
        tick = _Tick(monkeypatch)
        res = _future()
        state = _state(res)
        state.task_queue["uid-1"] = _entry(res)
        tick(state)
        assert tick.attempts == []
        assert tick.rec.reasons == [WAITING_FOR_RESERVATION_REASON]

    def test_a_pod_whose_booking_is_taken_is_told_by_whom(self, monkeypatch):
        tick = _Tick(monkeypatch)
        res = _open(1)
        state = _state(res)
        state.task_queue["uid-1"] = _entry(res)
        tick.snapshot = [_holder("u9", "jupyter-alice")]
        tick(state)
        assert tick.attempts == [("uid-1", 1)]
        [call] = tick.rec.calls
        assert call["reason"] == RESERVATION_FULL_REASON
        assert "fully in use by your pod jupyter-alice." in call["message"]

    def test_a_holder_changing_is_told_at_once_and_nothing_else_is(self, monkeypatch):
        tick = _Tick(monkeypatch)
        res = _open(1)
        state = _state(res)
        state.task_queue["uid-1"] = _entry(res)
        tick.snapshot = [_holder("u9", "jupyter-alice")]
        tick(state)
        state.task_queue["uid-1"].next_attempt_at = _now()
        tick(state)  # unchanged: inside the repeat, not restated
        tick.snapshot = [_holder("u8", "train-alice")]
        state.task_queue["uid-1"].next_attempt_at = _now()
        tick(state)
        messages = [c["message"] for c in tick.rec.calls]
        assert len(messages) == 2
        assert "jupyter-alice" in messages[0] and "train-alice" in messages[1]

    def test_the_tick_rebuilds_the_holder_directory(self, monkeypatch):
        tick = _Tick(monkeypatch)
        state = _state(_open(1))
        state.holder_names = {"gone": (USERNAME, "long-gone")}
        tick.snapshot = [_holder("u9", "jupyter-alice")]
        tick(state)
        assert state.holder_names == {"u9": (USERNAME, "jupyter-alice")}

    def test_an_open_booking_with_room_takes_the_pod_from_a_full_one(self, monkeypatch, caplog):
        # e.g. booked in answer to ReservationTooSmall, or left by a holder:
        # admitted on this tick, not after the next watch resync.
        tick = _Tick(monkeypatch, admits={"uid-1"})
        full = _open(1)
        roomy = _booking(2, start=_now() - timedelta(minutes=5), gpu_count=1)
        state = _state(full, roomy)
        state.task_queue["uid-1"] = _entry(full)
        tick.snapshot = [_holder("u9", "jupyter-alice")]
        with caplog.at_level(logging.INFO, logger="app.main"):
            tick(state)
        assert tick.attempts == [("uid-1", 2)]
        assert "uid-1" not in state.task_queue
        [line] = [r.getMessage() for r in caplog.records if "pod.requeued" in r.getMessage()]
        fields = kv_fields(line)
        assert (fields["reason"], fields["old.rid"], fields["new.rid"]) == (
            "open_reservation_with_room", "1", "2",
        )

    def test_a_pod_its_booking_can_take_is_not_moved(self, monkeypatch):
        tick = _Tick(monkeypatch)
        first = _open(1)
        other = _booking(2, start=_now() - timedelta(minutes=20), gpu_count=1)
        state = _state(first, other)
        state.task_queue["uid-1"] = _entry(first)
        tick(state)
        assert tick.attempts == [("uid-1", 1)]
        assert state.task_queue["uid-1"].reservation.id == 1

    def test_a_pod_waiting_for_its_window_moves_to_one_open_now(self, monkeypatch):
        tick = _Tick(monkeypatch, admits={"uid-1"})
        later, now_open = _future(1, days=1), _open(2)
        state = _state(later, now_open)
        state.task_queue["uid-1"] = _entry(later)
        tick(state)
        assert tick.attempts == [("uid-1", 2)]

    def test_an_ended_booking_hands_the_pod_to_an_open_one(self, monkeypatch):
        tick = _Tick(monkeypatch)
        ended = _booking(1, start=_now() - timedelta(hours=3), gpu_count=1)
        state = _state(ended, _open(2))
        state.task_queue["uid-1"] = _entry(ended)
        tick(state)
        assert tick.attempts == [("uid-1", 2)]
        assert state.task_queue["uid-1"].reservation.id == 2

    def test_an_ended_booking_with_nothing_open_drops_the_pod_as_before(self, monkeypatch):
        tick = _Tick(monkeypatch)
        ended = _booking(1, start=_now() - timedelta(hours=3), gpu_count=1)
        state = _state(ended, _future(2))
        state.task_queue["uid-1"] = _entry(ended)
        tick(state)
        assert state.task_queue == {}
        assert tick.rec.calls == []

    def test_a_pod_the_watch_dequeued_mid_tick_is_left_alone(self, monkeypatch):
        # Deleted while the tick awaited an earlier pod's admission: moving it
        # to an open booking would put a deleted pod back on the queue.
        tick = _Tick(monkeypatch)
        full, roomy = _open(1), _open(2, gpu_count=2)
        ended = _booking(3, start=_now() - timedelta(hours=3), gpu_count=1)
        state = _state(full, roomy, ended)
        state.task_queue["uid-1"] = _entry(roomy, uid="uid-1")
        state.task_queue["uid-2"] = _entry(ended, uid="uid-2")

        def watch_deletes_uid_2(state_, uid):
            state_.dequeue_pod("uid-2")
            state_.forget_pending_status("uid-2")

        tick.during_attempt = watch_deletes_uid_2
        tick(state)
        assert tick.attempts == [("uid-1", 2)]
        assert "uid-2" not in state.task_queue
        assert "uid-2" not in state.pending_status


# ---------------------------------------------------------------------------
# main._retry_waiters: a holder going frees its GPUs for a waiter at once
# ---------------------------------------------------------------------------


class TestRetryWaiters:
    """ReservationFull promises admission as soon as a GPU is free; the tick
    alone kept that promise only to within two intervals."""

    def _run(self, monkeypatch, state, events, *, admits=True):
        m = _main_module(monkeypatch)
        attempts: list[tuple[str, int]] = []
        rec = _ProblemRecorder()

        async def attempt(state_, uid, entry, gate=None):
            attempts.append((uid, entry.reservation.id))
            return admits

        async def noop(*args, **kwargs):
            return None

        monkeypatch.setattr(m, "PodWatcher", lambda **kw: _FakeWatcher(events))
        monkeypatch.setattr(m, "_run_ondemand_admission", noop)
        monkeypatch.setattr(m, "_try_apply_toleration", attempt)
        monkeypatch.setattr(m, "emit_pending_pod_event", rec)
        monkeypatch.setattr(m, "_report_overstay_if_any", noop)
        monkeypatch.setattr(m, "_teardown_ondemand_lease", noop)
        asyncio.run(m.pod_watch_loop(state, None, make_config()))
        return attempts, rec

    def _waiting_behind(self, res, *, cooldown=True):
        state = _state(res)
        _hold(state, res.id, "uid-9", "jupyter-alice")
        entry = _entry(res)
        if cooldown:  # retried at the tick after next, left to itself
            entry.next_attempt_at = _now() + timedelta(minutes=5)
        state.task_queue["uid-1"] = entry
        return state

    def test_a_deleted_holder_admits_the_waiter_at_once(self, monkeypatch):
        state = self._waiting_behind(_open(1))
        attempts, rec = self._run(monkeypatch, state, [("DELETED", _pod("uid-9"))])
        assert attempts == [("uid-1", 1)]
        assert "uid-1" not in state.task_queue
        assert rec.calls == []

    def test_a_finished_holder_admits_the_next_job_at_once(self, monkeypatch):
        state = self._waiting_behind(_open(1))
        attempts, _rec = self._run(
            monkeypatch, state, [("MODIFIED", _pod("uid-9", phase="Succeeded"))]
        )
        assert attempts == [("uid-1", 1)]

    def test_only_waiters_on_that_reservation_whose_window_is_open(self, monkeypatch):
        held, other = _open(1), _open(2)
        later = _future(3)
        state = self._waiting_behind(held)
        state.reservations += [other, later]
        state.task_queue["uid-2"] = _entry(other, uid="uid-2")
        state.task_queue["uid-3"] = _entry(later, uid="uid-3")
        attempts, _rec = self._run(monkeypatch, state, [("DELETED", _pod("uid-9"))])
        assert attempts == [("uid-1", 1)]

    def test_a_waiter_already_being_admitted_is_left_to_it(self, monkeypatch):
        # The queue tick is mid-patch for it: its placement is recorded first.
        state = self._waiting_behind(_open(1, gpu_count=2))
        state.record_placement(1, "uid-1", 1)
        attempts, _rec = self._run(monkeypatch, state, [("DELETED", _pod("uid-9"))])
        assert attempts == []

    def test_a_pod_that_held_nothing_retries_nothing(self, monkeypatch):
        state = self._waiting_behind(_open(1))
        attempts, _rec = self._run(monkeypatch, state, [("DELETED", _pod("uid-5"))])
        assert attempts == []

    def test_a_waiter_that_still_does_not_fit_is_not_told_from_here(self, monkeypatch):
        # The tick re-tells it; telling it on every release would let a busy
        # reservation's churn restate it without bound.
        state = self._waiting_behind(_open(1))
        attempts, rec = self._run(
            monkeypatch, state, [("DELETED", _pod("uid-9"))], admits=False,
        )
        assert attempts == [("uid-1", 1)]
        assert "uid-1" in state.task_queue
        assert rec.calls == []


class TestConfig:
    def _from_env(self, monkeypatch, **env):
        from app.config import Config

        monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
        monkeypatch.setenv("RESERVATION_API_KEY", "test-key")
        monkeypatch.delenv("RESERVATION_WAIT_EVENT_ENABLED", raising=False)
        for name, value in env.items():
            monkeypatch.setenv(name, value)
        return Config.from_env()

    def test_on_by_default(self, monkeypatch):
        assert self._from_env(monkeypatch).reservation_wait_event_enabled is True

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", " OFF "])
    def test_falsy_words_disable_it(self, monkeypatch, raw):
        config = self._from_env(monkeypatch, RESERVATION_WAIT_EVENT_ENABLED=raw)
        assert config.reservation_wait_event_enabled is False
