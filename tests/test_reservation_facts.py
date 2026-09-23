"""Unit tests for the reservation-fact annotations stamped on an admitted pod.

``galends/guaranteed-until`` says when a pod's GPU access stops being protected,
but nothing said *what* it is running under — a booking the user made, or a
just-in-time lease the controller minted for them — nor what that reservation's
own window and size are.  Five keys fill that gap, riding the runtime-guarantee
patch so they cost no extra API call:

  reservation-kind / -start / -end / -gpu-count, gpu-class-name

plus ``galends/admitted-at``, which is write-once by design.

Three layers, all without a real cluster:

- ``utc_iso`` — the single definition of the timestamp wire format.
- ``annotate_runtime_guarantee`` — the write patch, against a capturing fake
  ``_core_v1``.
- ``main._record_guarantee`` and the re-link paths — that the facts follow the
  pod to its *new* reservation, and that a re-link does not restamp
  ``admitted-at``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app import k8s_client
from app.k8s_client import (
    ADMITTED_AT,
    GPU_CLASS_NAME,
    RESERVATION_END,
    RESERVATION_GPU_COUNT,
    RESERVATION_KIND,
    RESERVATION_START,
    ReservationFacts,
    annotate_runtime_guarantee,
    utc_iso,
)

from tests.conftest import GPU_CLASS_LABEL, USERNAME, reservation, run_locked

NOW = datetime(2024, 1, 15, 15, 15, tzinfo=timezone.utc)
TWO_PM = datetime(2024, 1, 15, 14, 0, tzinfo=timezone.utc)
HALF_PAST = datetime(2024, 1, 15, 15, 30, tzinfo=timezone.utc)
THREE_PM = datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc)
FIVE_PM = datetime(2024, 1, 15, 17, 0, tzinfo=timezone.utc)

FACTS = ReservationFacts(
    kind="booking",
    start_utc=TWO_PM,
    end_utc=FIVE_PM,
    gpu_count=4,
    gpu_class_name="H100",
)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://reservation.test")
    monkeypatch.setenv("RESERVATION_API_KEY", "gpures_test")
    import app.main

    return app.main


class _CapturingCore:
    def __init__(self):
        self.patches: list[tuple[str, str, dict]] = []

    def patch_namespaced_pod(self, name, namespace, body):
        self.patches.append((name, namespace, body))
        return SimpleNamespace()


# ---------------------------------------------------------------------------
# utc_iso — the wire format
# ---------------------------------------------------------------------------


class TestUtcIso:
    def test_renders_the_documented_format(self):
        assert utc_iso(TWO_PM) == "2024-01-15T14:00:00Z"

    def test_subsecond_precision_is_dropped(self):
        """The format has no fractional part; a consumer parsing it must not
        have to cope with one appearing."""
        assert utc_iso(TWO_PM.replace(microsecond=123456)) == "2024-01-15T14:00:00Z"

    def test_non_utc_offset_is_converted_not_mislabelled(self):
        """A value that reached us on another offset is normalised, so the ``Z``
        suffix never claims a wall-clock reading that is not UTC."""
        berlin = timezone(timedelta(hours=2))
        assert utc_iso(TWO_PM.astimezone(berlin)) == "2024-01-15T14:00:00Z"

    def test_naive_datetime_is_treated_as_already_utc(self):
        """Defensive fallback — nothing in this codebase builds naive datetimes,
        and the local zone must not leak in if one ever arrives."""
        assert utc_iso(datetime(2024, 1, 15, 14, 0)) == "2024-01-15T14:00:00Z"


# ---------------------------------------------------------------------------
# annotate_runtime_guarantee — the write patch
# ---------------------------------------------------------------------------


class TestAnnotateReservationFacts:
    def test_facts_ride_the_guarantee_patch(self, monkeypatch):
        """One patch carries both, so the descriptive set is free of an extra
        API round-trip."""
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(
            annotate_runtime_guarantee("pod-x", USERNAME, 7200, FIVE_PM, FACTS)
        )

        assert len(core.patches) == 1
        annotations = core.patches[0][2]["metadata"]["annotations"]
        assert annotations[RESERVATION_KIND] == "booking"
        assert annotations[RESERVATION_START] == "2024-01-15T14:00:00Z"
        assert annotations[RESERVATION_END] == "2024-01-15T17:00:00Z"
        assert annotations[RESERVATION_GPU_COUNT] == "4"
        assert annotations[GPU_CLASS_NAME] == "H100"
        # The guarantee keys are untouched by the addition.
        assert annotations["galends/guaranteed-until"] == "2024-01-15T17:00:00Z"

    def test_every_value_is_a_string(self, monkeypatch):
        """Kubernetes rejects a non-string annotation value; the integer count
        is the one that would slip through untyped."""
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(
            annotate_runtime_guarantee("pod-x", USERNAME, 7200, FIVE_PM, FACTS, NOW)
        )
        annotations = core.patches[0][2]["metadata"]["annotations"]
        assert all(isinstance(v, str) for v in annotations.values())

    def test_admitted_at_is_written_when_given(self, monkeypatch):
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(
            annotate_runtime_guarantee("pod-x", USERNAME, 7200, FIVE_PM, FACTS, NOW)
        )
        annotations = core.patches[0][2]["metadata"]["annotations"]
        assert annotations[ADMITTED_AT] == "2024-01-15T15:15:00Z"

    def test_admitted_at_is_absent_when_omitted(self, monkeypatch):
        """The key must be *absent*, not present-and-empty: a re-link patch
        that carried it would overwrite the pod's original admission instant."""
        core = _CapturingCore()
        monkeypatch.setattr(k8s_client, "_core_v1", core)
        asyncio.run(
            annotate_runtime_guarantee("pod-x", USERNAME, 7200, FIVE_PM, FACTS)
        )
        annotations = core.patches[0][2]["metadata"]["annotations"]
        assert ADMITTED_AT not in annotations


# ---------------------------------------------------------------------------
# main._record_guarantee — digesting a reservation into facts
# ---------------------------------------------------------------------------


def _capture_annotate(monkeypatch, m):
    """Record what ``_record_guarantee`` passes down to the annotation writer."""
    calls: list[dict] = []

    async def _annotate(pod_name, namespace, seconds, guaranteed_until, facts,
                        admitted_at=None):
        calls.append({"facts": facts, "admitted_at": admitted_at})

    async def _emit(*args, **kwargs):
        return None

    monkeypatch.setattr(m, "annotate_runtime_guarantee", _annotate)
    monkeypatch.setattr(m, "emit_runtime_guaranteed_event", _emit)
    return calls


def _booking(res_id=200, *, start_utc=THREE_PM, end_utc=FIVE_PM, gpu_count=1,
             kind="booking"):
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind=kind,
        with_user=True,
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=USERNAME,
    )


class TestRecordGuarantee:
    def test_facts_are_digested_from_the_reservation(self, monkeypatch):
        m = _main_module(monkeypatch)
        calls = _capture_annotate(monkeypatch, m)
        res = _booking(gpu_count=4)

        asyncio.run(
            m._record_guarantee("pod-1", USERNAME, object(), FIVE_PM, NOW, res)
        )

        facts = calls[0]["facts"]
        assert facts.kind == "booking"
        assert facts.start_utc == THREE_PM
        assert facts.end_utc == FIVE_PM
        assert facts.gpu_count == 4
        assert facts.gpu_class_name == res.gpu_class.name

    def test_lease_reports_its_own_kind(self, monkeypatch):
        """The whole point of the key: a JIT lease must be distinguishable from
        a booking the user made themselves."""
        m = _main_module(monkeypatch)
        calls = _capture_annotate(monkeypatch, m)

        asyncio.run(
            m._record_guarantee(
                "pod-1", USERNAME, object(), HALF_PAST, NOW,
                _booking(100, start_utc=TWO_PM, end_utc=HALF_PAST, kind="on_demand"),
            )
        )

        assert calls[0]["facts"].kind == "on_demand"

    def test_first_admission_stamps_admitted_at(self, monkeypatch):
        m = _main_module(monkeypatch)
        calls = _capture_annotate(monkeypatch, m)

        asyncio.run(
            m._record_guarantee(
                "pod-1", USERNAME, object(), FIVE_PM, NOW, _booking(),
                first_admission=True,
            )
        )

        assert calls[0]["admitted_at"] == NOW

    def test_relink_does_not_stamp_admitted_at(self, monkeypatch):
        """A re-link is not a new admission — restamping would restart a
        session-elapsed clock mid-job."""
        m = _main_module(monkeypatch)
        calls = _capture_annotate(monkeypatch, m)

        asyncio.run(
            m._record_guarantee("pod-1", USERNAME, object(), FIVE_PM, NOW, _booking())
        )

        assert calls[0]["admitted_at"] is None

    def test_annotation_failure_stays_best_effort(self, monkeypatch):
        """Recording facts must not become a new way to fail an admission that
        has already applied its toleration."""
        m = _main_module(monkeypatch)

        async def _boom(*args, **kwargs):
            raise RuntimeError("api down")

        monkeypatch.setattr(m, "annotate_runtime_guarantee", _boom)
        asyncio.run(
            m._record_guarantee("pod-1", USERNAME, object(), FIVE_PM, NOW, _booking())
        )  # does not raise


# ---------------------------------------------------------------------------
# Re-link: the facts must follow the pod to its new reservation
# ---------------------------------------------------------------------------


class TestFactsFollowARelink:
    def test_merge_restamps_facts_from_the_booking(self, monkeypatch):
        """A pod merged from a JIT lease onto its user's now-open booking must
        stop describing the retired lease — its kind, window and size all change.
        This runs the real ``_record_guarantee`` (only the k8s seams are faked)
        because the invariant is that the merge path passes the *new*
        reservation, not that the digest works in isolation.
        """
        m = _main_module(monkeypatch)
        calls = _capture_annotate(monkeypatch, m)

        async def _read(name, ns):
            return SimpleNamespace(
                status=SimpleNamespace(phase="Running", conditions=None)
            )

        async def _noop(*args, **kwargs):
            return None

        monkeypatch.setattr(m, "read_pod", _read)
        monkeypatch.setattr(m, "apply_toleration", _noop)
        monkeypatch.setattr(m, "emit_overstay_relinked_event", _noop)
        monkeypatch.setattr(m, "emit_reservation_relinked_event", _noop)

        from app.controller import ControllerState, PodRuntimeView
        from tests.conftest import make_config

        lease = _booking(100, start_utc=TWO_PM, end_utc=HALF_PAST, kind="on_demand")
        booking = _booking(200, start_utc=THREE_PM, end_utc=FIVE_PM, gpu_count=2)
        state = ControllerState()
        state.reservations = [lease, booking]
        state.record_placement(100, "p1", 1)
        pod = PodRuntimeView(
            uid="p1", namespace=USERNAME, name="pod-p1", gpu_class=GPU_CLASS_LABEL,
            gpu_count=1, reservation_id=100, node_resident=True, terminating=False,
        )

        class _Client:
            async def cancel_reservation(self, reservation_id, reason):
                return True

        run_locked(
            state,
            m._merge_ondemand_into_bookings(
                state, _Client(), make_config(), [pod], NOW
            ),
        )

        assert len(calls) == 1
        facts = calls[0]["facts"]
        assert facts.kind == "booking"          # no longer the lease
        assert facts.start_utc == THREE_PM      # the booking's window
        assert facts.end_utc == FIVE_PM
        assert facts.gpu_count == 2             # the booking's reserved count
        # ... and the pod's original admission instant is left alone.
        assert calls[0]["admitted_at"] is None
