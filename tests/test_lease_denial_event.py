"""Unit tests for surfacing a JIT lease's 409 denial reason on the waiting pod.

The app answers an infeasible on-demand ask with a 409 whose ``detail`` says
why.  Until this feature that reason reached the controller's log and stopped
there, so the pod's owner — who has ``kubectl`` on their own namespace but no
access to the controller's logs — saw an indefinitely Pending pod with nothing
explaining it.  Three layers, none of them touching a real cluster or the app:

- ``ReservationClient.create_ondemand_reservation`` — the detail is now carried
  on the ``LeaseAttempt``, not merely logged.
- ``k8s_client.emit_lease_denied_event`` — the Warning Event write, against a
  capturing fake ``_core_v1``.
- ``main._emit_lease_denial_event`` — the throttle: which denials emit, which
  are suppressed as a restatement, and what a failed emit leaves behind.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

from app import k8s_client
from app.controller import PENDING_TOPIC_HOLD, ControllerState, OnDemandCandidate
from app.k8s_client import emit_lease_denied_event
from app.reservation_client import LeaseAttempt, ReservationClient
from app.schemas import OnDemandAdmissionCandidate, OnDemandReservationRequest

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
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-denial")
    import app.main as main_module

    return main_module


def _candidate(uid="uid-1"):
    return OnDemandCandidate(
        pod_uid=uid,
        pod_name="pod-1",
        pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_requested=2,
        min_runtime_seconds=600,
        pod_created_at=NOW,
        next_attempt_at=NOW,
        usage_group=GROUP_NAME,
    )


def _config(**overrides):
    base = dict(
        ondemand_denial_event_enabled=True,
        ondemand_denial_event_repeat_minutes=30,
        support_contact=None,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


# ---------------------------------------------------------------------------
# ReservationClient — the detail reaches the caller, not just the log
# ---------------------------------------------------------------------------


def _client_with_handler(handler):
    config = make_config()
    client = ReservationClient(config)
    client._client = httpx.AsyncClient(
        base_url=config.reservation_api_url,
        transport=httpx.MockTransport(handler),
    )
    return client


def _client_answering(status: int, body):
    """A ReservationClient whose POST /api/reservations answers *status*."""

    def handler(request: httpx.Request) -> httpx.Response:
        if isinstance(body, dict):
            return httpx.Response(status, json=body)
        return httpx.Response(status, text=body)

    return _client_with_handler(handler)


def _ask():
    return OnDemandReservationRequest(
        username=USERNAME,
        group_name=GROUP_NAME,
        gpu_class_id=GPU_CLASS_ID,
        gpu_count=2,
        duration_seconds=1200,
        idempotency_key="uid-1",
    )


class TestLeaseAttemptCarriesDetail:
    def test_409_detail_is_returned_not_only_logged(self):
        client = _client_answering(409, {"detail": DETAIL})
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert attempt.granted is False
        assert attempt.status == 409
        assert attempt.detail == DETAIL

    def test_409_detail_is_also_logged(self, caplog):
        client = _client_answering(409, {"detail": DETAIL})
        with caplog.at_level(logging.INFO, logger="app.reservation_client"):
            asyncio.run(client.create_ondemand_reservation(_ask()))
        denied = [r for r in caplog.records if "api.lease_denied" in r.getMessage()]
        assert len(denied) == 1
        assert kv_fields(denied[0].getMessage())["detail"] == DETAIL

    def test_non_409_detail_is_carried_too(self):
        # The event path ignores it, but the LeaseAttempt shape stays uniform.
        client = _client_answering(403, {"detail": "read-only service key"})
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert attempt.status == 403
        assert attempt.detail == "read-only service key"

    def test_non_json_body_degrades_to_text(self):
        client = _client_answering(409, "<html>gateway said no</html>")
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert attempt.detail == "<html>gateway said no</html>"

    def test_long_detail_is_truncated(self):
        client = _client_answering(409, {"detail": "x" * 500})
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        # 200 chars plus the ellipsis marker — a full HTML error page must not
        # become a 5 KB Event message.
        assert len(attempt.detail) == 201
        assert attempt.detail.endswith("…")

    def test_network_failure_carries_no_detail(self):
        def handler(request):
            raise httpx.ConnectError("boom")

        client = _client_with_handler(handler)
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert attempt.status is None
        assert attempt.detail is None


# ---------------------------------------------------------------------------
# k8s_client.emit_lease_denied_event — the Event write
# ---------------------------------------------------------------------------


class _CapturingCore:
    def __init__(self, raises=None):
        self.events: list = []
        self._raises = raises

    def create_namespaced_event(self, namespace, body):
        if self._raises is not None:
            raise self._raises
        self.events.append((namespace, body))
        return SimpleNamespace()


def _emit(core, monkeypatch, *, detail=DETAIL):
    monkeypatch.setattr(k8s_client, "_core_v1", core)
    asyncio.run(
        emit_lease_denied_event(
            "uid-1", "pod-1", USERNAME, detail,
            gpu_class=GPU_CLASS_LABEL, gpu_count=2,
        )
    )


class TestEmitLeaseDeniedEvent:
    def test_event_is_a_warning_in_the_pods_namespace(self, monkeypatch):
        core = _CapturingCore()
        _emit(core, monkeypatch)
        namespace, event = core.events[0]
        # The namespace is what makes it visible to the user: a pod's namespace
        # is its owner's username, and that is where they run kubectl.
        assert namespace == USERNAME
        assert event.metadata.namespace == USERNAME
        assert event.type == "Warning"
        assert event.reason == "OnDemandLeaseDenied"
        assert event.action == "RequestOnDemandLease"

    def test_involved_object_points_at_the_pod(self, monkeypatch):
        core = _CapturingCore()
        _emit(core, monkeypatch)
        _ns, event = core.events[0]
        ref = event.involved_object
        assert (ref.kind, ref.name, ref.namespace, ref.uid) == (
            "Pod", "pod-1", USERNAME, "uid-1",
        )

    def test_message_carries_the_apps_reason_verbatim(self, monkeypatch):
        core = _CapturingCore()
        _emit(core, monkeypatch)
        _ns, event = core.events[0]
        assert DETAIL in event.message
        assert "2 x " + GPU_CLASS_LABEL in event.message

    def test_generate_name_so_repeats_never_409(self, monkeypatch):
        core = _CapturingCore()
        _emit(core, monkeypatch)
        _ns, event = core.events[0]
        assert event.metadata.generate_name == "gpu-lease-denied-"
        assert event.metadata.name is None

    def test_emit_is_logged(self, monkeypatch, caplog):
        core = _CapturingCore()
        with caplog.at_level(logging.INFO, logger="app.k8s_client"):
            _emit(core, monkeypatch)
        emitted = [r for r in caplog.records if "k8s.event_emitted" in r.getMessage()]
        assert len(emitted) == 1
        fields = kv_fields(emitted[0].getMessage())
        assert fields["reason"] == "OnDemandLeaseDenied"
        assert fields["pod"] == "pod-1"

    def test_normal_emitters_still_emit_normal(self, monkeypatch):
        # event_type defaults to Normal, so the five pre-existing emitters are
        # unchanged by the parameterisation this feature added.
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        pod = SimpleNamespace(metadata=SimpleNamespace(uid="uid-1"))
        asyncio.run(
            k8s_client.emit_preempted_event(pod, "pod-1", USERNAME, "over guarantee")
        )
        _ns, event = core.events[0]
        assert event.type == "Normal"
        assert event.involved_object.uid == "uid-1"


# ---------------------------------------------------------------------------
# main._emit_lease_denial_event — the throttle
# ---------------------------------------------------------------------------


class _Recorder:
    def __init__(self, raises=None):
        self.calls: list = []
        self.kwargs: list[dict] = []
        self._raises = raises

    async def __call__(self, uid, name, namespace, detail, *, gpu_class, gpu_count, **kw):
        if self._raises is not None:
            raise self._raises
        self.calls.append((uid, name, namespace, detail, gpu_class, gpu_count))
        self.kwargs.append(kw)


def _run(m, monkeypatch, config, candidate, detail, now, recorder=None, state=None):
    recorder = recorder or _Recorder()
    state = state if state is not None else _STATE
    monkeypatch.setattr(m, "emit_lease_denied_event", recorder)
    asyncio.run(m._emit_lease_denial_event(
        config, state, candidate.pod_uid, candidate, detail, now,
    ))
    return recorder


# What each test has told so far.  Reset per test (see the autouse fixture):
# the throttle lives in ControllerState.pending_status, keyed by pod uid.
_STATE = ControllerState()


@pytest.fixture(autouse=True)
def _fresh_state():
    global _STATE
    _STATE = ControllerState()
    yield


def _told(uid="uid-1"):
    """The status last told to *uid*'s owner, or None."""
    return _STATE.pending_status.get(uid, {}).get(PENDING_TOPIC_HOLD)


class TestDenialEventThrottle:
    def test_first_denial_emits(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _run(m, monkeypatch, _config(), candidate, DETAIL, NOW)
        assert len(rec.calls) == 1
        assert rec.calls[0][3] == DETAIL
        assert _told().at == NOW

    def test_same_reason_within_the_interval_is_suppressed(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _Recorder()
        _run(m, monkeypatch, _config(), candidate, DETAIL, NOW, rec)
        # The retry cadence is 2-5 min; restating an unchanged reason that often
        # would bury the pod's real events.
        _run(m, monkeypatch, _config(), candidate, DETAIL,
             NOW + timedelta(minutes=4), rec)
        assert len(rec.calls) == 1

    def test_same_reason_after_the_interval_is_restated(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _Recorder()
        _run(m, monkeypatch, _config(), candidate, DETAIL, NOW, rec)
        # Events expire, so a pod still blocked must say so again or kubectl
        # describe goes blank on a pod that is still stuck.
        later = NOW + timedelta(minutes=31)
        _run(m, monkeypatch, _config(), candidate, DETAIL, later, rec)
        assert len(rec.calls) == 2
        assert _told().at == later

    def test_a_changed_reason_emits_immediately(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _Recorder()
        _run(m, monkeypatch, _config(), candidate, DETAIL, NOW, rec)
        other = "SU budget exceeded for this window"
        _run(m, monkeypatch, _config(), candidate, other,
             NOW + timedelta(seconds=30), rec)
        assert len(rec.calls) == 2
        assert rec.calls[1][3] == other

    def test_zero_interval_emits_every_time(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        config = _config(ondemand_denial_event_repeat_minutes=0)
        rec = _Recorder()
        _run(m, monkeypatch, config, candidate, DETAIL, NOW, rec)
        _run(m, monkeypatch, config, candidate, DETAIL, NOW, rec)
        assert len(rec.calls) == 2

    def test_disabled_emits_nothing(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _run(m, monkeypatch, _config(ondemand_denial_event_enabled=False),
                   candidate, DETAIL, NOW)
        assert rec.calls == []
        assert _told() is None

    @pytest.mark.parametrize("detail", [None, ""])
    def test_no_detail_emits_nothing(self, monkeypatch, detail):
        # A 409 with an empty body says nothing worth putting on the pod.
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _run(m, monkeypatch, _config(), candidate, detail, NOW)
        assert rec.calls == []

    def test_a_failed_emit_is_swallowed_and_not_remembered(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _Recorder(raises=RuntimeError("apiserver said no"))
        with caplog.at_level(logging.WARNING, logger="app.main"):
            _run(m, monkeypatch, _config(), candidate, DETAIL, NOW, rec)
        failed = [r for r in caplog.records if "k8s.event_failed" in r.getMessage()]
        assert len(failed) == 1
        assert kv_fields(failed[0].getMessage())["reason"] == "OnDemandLeaseDenied"
        # Not stamped, so the next denial retries rather than being suppressed
        # for the whole repeat interval.
        assert _told() is None
        ok = _Recorder()
        _run(m, monkeypatch, _config(), candidate, DETAIL,
             NOW + timedelta(seconds=30), ok)
        assert len(ok.calls) == 1

    def test_pods_do_not_share_throttle_state(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec = _Recorder()
        _run(m, monkeypatch, _config(), _candidate("uid-1"), DETAIL, NOW, rec)
        _run(m, monkeypatch, _config(), _candidate("uid-2"), DETAIL, NOW, rec)
        assert len(rec.calls) == 2

    def test_the_story_survives_the_candidate(self, monkeypatch):
        # Told state is keyed by pod uid, not held on the candidate: a pod that
        # leaves the candidate list and comes back (rerouted to a booking that
        # then fell through, say) is not re-told an unchanged reason early.
        m = _main_module(monkeypatch)
        rec = _Recorder()
        _run(m, monkeypatch, _config(), _candidate("uid-1"), DETAIL, NOW, rec)
        _run(m, monkeypatch, _config(), _candidate("uid-1"), DETAIL,
             NOW + timedelta(minutes=5), rec)
        assert len(rec.calls) == 1


# ---------------------------------------------------------------------------
# main._grant_and_admit — which non-grants reach the pod
# ---------------------------------------------------------------------------


class _DenyingClient:
    """A client whose lease request always fails with *status* / *detail*."""

    def __init__(self, status, detail, **envelope):
        self._attempt = LeaseAttempt(status=status, detail=detail, **envelope)

    async def create_ondemand_reservation(self, req):
        return self._attempt


def _grant_and_admit(m, monkeypatch, status, detail, config=None, candidate=None,
                     **envelope):
    rec = _Recorder()
    monkeypatch.setattr(m, "emit_lease_denied_event", rec)
    candidate = candidate or _candidate()
    ask = OnDemandAdmissionCandidate(
        pod_uid="uid-1",
        pod_created_at=datetime.now(timezone.utc),
        username=USERNAME,
        group_name=GROUP_NAME,
        gpu_class_id=GPU_CLASS_ID,
        gpu_count=2,
        duration_seconds=1200,
    )
    done = asyncio.run(
        m._grant_and_admit(
            ControllerState(), _DenyingClient(status, detail, **envelope),
            config or _config(), "uid-1", candidate, ask,
        )
    )
    assert done is False  # every non-grant keeps the candidate for a retry
    return rec, candidate


class TestGrantAndAdmitDenialWiring:
    def test_409_reaches_the_pod(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, _candidate_out = _grant_and_admit(m, monkeypatch, 409, DETAIL)
        assert len(rec.calls) == 1
        assert rec.calls[0][3] == DETAIL

    def test_409_detail_is_logged_alongside(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        monkeypatch.setattr(m, "emit_lease_denied_event", _Recorder())
        with caplog.at_level(logging.INFO, logger="app.main"):
            _grant_and_admit(m, monkeypatch, 409, DETAIL)
        denied = [r for r in caplog.records if "event=lease.denied" in r.getMessage()]
        assert len(denied) == 1
        assert kv_fields(denied[0].getMessage())["detail"] == DETAIL

    def test_a_network_failure_reaches_no_pod(self, monkeypatch):
        # Retryable like a 409, but it says nothing about the ask — the app
        # never answered, so there is no reason to report.
        m = _main_module(monkeypatch)
        rec, _c = _grant_and_admit(m, monkeypatch, None, None)
        assert rec.calls == []

    def test_a_5xx_reaches_no_pod(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, _c = _grant_and_admit(m, monkeypatch, 503, "upstream unavailable")
        assert rec.calls == []

    def test_an_operator_fault_reaches_no_pod(self, monkeypatch):
        # A read-only service key is not the pod owner's problem and they could
        # not act on it; it already gets a WARNING log line of its own.
        m = _main_module(monkeypatch)
        rec, candidate = _grant_and_admit(m, monkeypatch, 403, "read-only service key")
        assert rec.calls == []
        assert candidate.lease_error_count == 1


class TestEmptyBodyCarriesNoReason:
    def test_an_empty_409_body_yields_no_detail(self):
        # "no body" is the log's placeholder for an unquotable response, not
        # something the app said -- it must never reach a user as a reason.
        client = _client_answering(409, "")
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert attempt.status == 409
        assert attempt.detail is None

    def test_the_placeholder_still_reaches_the_log(self, caplog):
        client = _client_answering(409, "")
        with caplog.at_level(logging.INFO, logger="app.reservation_client"):
            asyncio.run(client.create_ondemand_reservation(_ask()))
        denied = [r for r in caplog.records if "api.lease_denied" in r.getMessage()]
        assert kv_fields(denied[0].getMessage())["detail"] == "no body"

    def test_and_so_no_event_is_emitted(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, _c = _grant_and_admit(m, monkeypatch, 409, None)
        assert rec.calls == []


# ---------------------------------------------------------------------------
# The admission-denial envelope: retryable / not_before are honoured
# ---------------------------------------------------------------------------

STRUCTURAL = "Requested 8 GPU(s) but this class allows at most 4 per reservation."


class TestDenialEnvelopeIsRead:
    def test_structural_denial_is_not_retryable(self):
        client = _client_answering(409, {
            "detail": STRUCTURAL, "code": "gpu_count_over_class_cap", "retryable": False,
        })
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert (attempt.code, attempt.app_retryable) == ("gpu_count_over_class_cap", False)
        assert attempt.structural is True
        assert attempt.retryable is False

    def test_contended_denial_carries_not_before(self):
        client = _client_answering(409, {
            "detail": DETAIL, "code": "su_budget_member", "retryable": True,
            "not_before": "2024-01-16T00:00:00Z",
        })
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert attempt.structural is False
        assert attempt.retryable is True
        assert attempt.not_before == datetime(2024, 1, 16, tzinfo=timezone.utc)

    def test_absent_envelope_is_read_as_retryable(self):
        # An older app, or a 409 outside the admission gates: the contract says
        # read an absent flag as retryable -- the behaviour before it existed.
        client = _client_answering(409, {"detail": DETAIL})
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert (attempt.code, attempt.app_retryable, attempt.not_before) == (None, None, None)
        assert attempt.retryable is True

    @pytest.mark.parametrize("body", [
        {"detail": DETAIL, "retryable": "false"},       # string, not bool
        {"detail": DETAIL, "not_before": "next tuesday"},
        {"detail": DETAIL, "code": 7},
    ])
    def test_a_malformed_envelope_degrades_to_status_only(self, body):
        client = _client_answering(409, body)
        attempt = asyncio.run(client.create_ondemand_reservation(_ask()))
        assert attempt.retryable is True
        assert attempt.not_before is None

    def test_the_code_and_verdict_are_logged(self, caplog):
        client = _client_answering(409, {
            "detail": STRUCTURAL, "code": "gpu_count_over_class_cap", "retryable": False,
        })
        with caplog.at_level(logging.INFO, logger="app.reservation_client"):
            asyncio.run(client.create_ondemand_reservation(_ask()))
        denied = [r for r in caplog.records if "api.lease_denied" in r.getMessage()]
        fields = kv_fields(denied[0].getMessage())
        assert (fields["reason"], fields["retryable"]) == ("gpu_count_over_class_cap", "false")


class TestStructuralDenialIsNotRetriedForever:
    def test_it_backs_off_instead_of_the_ordinary_cadence(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        for expected in (1, 2, 3):
            _grant_and_admit(
                m, monkeypatch, 409, STRUCTURAL, candidate=candidate,
                code="gpu_count_over_class_cap", app_retryable=False,
            )
            assert candidate.lease_error_count == expected
        # Third consecutive structural denial: at least 4x the 2-min floor.
        delay = candidate.next_attempt_at - datetime.now(timezone.utc)
        assert delay >= timedelta(minutes=7)

    def test_it_is_still_told_to_the_pod_as_structural(self, monkeypatch):
        m = _main_module(monkeypatch)
        rec, _c = _grant_and_admit(
            m, monkeypatch, 409, STRUCTURAL,
            code="gpu_count_over_class_cap", app_retryable=False,
        )
        assert len(rec.calls) == 1
        assert rec.kwargs[0]["structural"] is True

    def test_it_logs_at_info_with_the_verdict(self, monkeypatch, caplog):
        # The pod's owner, not the operator, can act on it: it must not trip the
        # lease.error WARNING that announces a misconfigured deployment.
        m = _main_module(monkeypatch)
        with caplog.at_level(logging.INFO, logger="app.main"):
            _grant_and_admit(
                m, monkeypatch, 409, STRUCTURAL,
                code="gpu_count_over_class_cap", app_retryable=False,
            )
        assert not [r for r in caplog.records if "event=lease.error" in r.getMessage()]
        denied = [r for r in caplog.records if "event=lease.denied" in r.getMessage()]
        fields = kv_fields(denied[0].getMessage())
        assert fields["reason"] == "gpu_count_over_class_cap"
        assert fields["retryable"] == "false"
        assert int(fields["retry_s"]) >= 120

    def test_a_contended_denial_resets_the_backoff(self, monkeypatch):
        m = _main_module(monkeypatch)
        candidate = _candidate()
        _grant_and_admit(m, monkeypatch, 409, STRUCTURAL, candidate=candidate,
                         app_retryable=False)
        _grant_and_admit(m, monkeypatch, 409, DETAIL, candidate=candidate,
                         app_retryable=True)
        assert candidate.lease_error_count == 0


class TestNotBeforeIsWaitedFor:
    def test_the_retry_waits_for_a_near_not_before(self, monkeypatch):
        m = _main_module(monkeypatch)
        not_before = datetime.now(timezone.utc) + timedelta(minutes=20)
        _rec, candidate = _grant_and_admit(
            m, monkeypatch, 409, DETAIL, app_retryable=True, not_before=not_before,
        )
        assert candidate.next_attempt_at == not_before

    def test_a_distant_not_before_is_capped(self, monkeypatch):
        # Advisory: it can clear sooner, so the pod is rechecked at the cap
        # rather than left a week behind a stale estimate.
        m = _main_module(monkeypatch)
        before = datetime.now(timezone.utc)
        _rec, candidate = _grant_and_admit(
            m, monkeypatch, 409, DETAIL, app_retryable=True,
            not_before=before + timedelta(days=7),
        )
        assert candidate.next_attempt_at - before <= timedelta(
            seconds=m.ERROR_RETRY_CAP_SECONDS + 5
        )

    def test_a_past_not_before_keeps_the_ordinary_cadence(self, monkeypatch):
        m = _main_module(monkeypatch)
        before = datetime.now(timezone.utc)
        _rec, candidate = _grant_and_admit(
            m, monkeypatch, 409, DETAIL, app_retryable=True,
            not_before=before - timedelta(hours=1),
        )
        assert timedelta(minutes=2) <= candidate.next_attempt_at - before <= timedelta(
            minutes=5, seconds=5
        )


class TestDenialMessage:
    def _message(self, monkeypatch, detail, **kw):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(emit_lease_denied_event(
            "uid-1", "pod-1", USERNAME, detail,
            gpu_class=GPU_CLASS_LABEL, gpu_count=2, **kw,
        ))
        return core.events[0][1].message

    def test_a_detail_ending_in_a_full_stop_is_not_doubled(self, monkeypatch):
        msg = self._message(monkeypatch, "User 'jsmith' has only 20 of 200 SU remaining.")
        assert "remaining. The pod stays Pending" in msg
        assert ".." not in msg

    def test_a_structural_denial_does_not_promise_a_retry(self, monkeypatch):
        msg = self._message(
            monkeypatch, STRUCTURAL, structural=True, support="contact support: help@x",
        )
        assert "keep retrying" not in msg
        assert "Waiting will not change this" in msg
        assert msg.endswith("contact support: help@x")

    def test_a_contended_denial_names_when_it_clears(self, monkeypatch):
        msg = self._message(
            monkeypatch, DETAIL, not_before=datetime(2024, 1, 16, tzinfo=timezone.utc),
        )
        assert "keep retrying" in msg
        assert "expects this to clear by" in msg


class TestStructuralFlipIsNews:
    def test_the_same_reason_turning_structural_emits_at_once(self, monkeypatch):
        # "Retrying" and "waiting will not help" ask different things of the
        # owner, so the same detail changing verdict is not a restatement.
        m = _main_module(monkeypatch)
        candidate = _candidate()
        rec = _Recorder()
        monkeypatch.setattr(m, "emit_lease_denied_event", rec)
        for structural in (False, True):
            asyncio.run(m._emit_lease_denial_event(
                _config(), _STATE, candidate.pod_uid, candidate, DETAIL,
                NOW + timedelta(minutes=1), structural=structural,
            ))
        assert [kw["structural"] for kw in rec.kwargs] == [False, True]
