"""Pod evidence carried alongside a JIT on-demand admission ask.

``OnDemandAdmissionCandidate`` offers the app two things beyond the "ask" a
``POST /api/reservations`` create would carry: the pod's **creation time** and
its **``galends/`` annotations**.  Neither reaches the create — they exist so
admission policy in the app can price a candidate on how long it has waited and
on what its owner declared about the job, which the ask itself does not say.

The bounds are the interesting part.  Annotation values are whatever the pod's
creator wrote, and Kubernetes lets an object carry 256 KiB of them; an admission
batch would otherwise ship that per candidate every 2-5 minutes for as long as
the pod stayed Pending.  ``get_pod_galends_annotations`` caps both the key count
and each value, and does the capping **here** rather than app-side because the
app's schema is deliberately lenient (a candidate it refuses 422s the whole
batch).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.controller import ControllerState, OnDemandCandidate
from app.k8s_client import (
    GALENDS_ANNOTATION_MAX_KEYS,
    GALENDS_ANNOTATION_MAX_VALUE,
    get_pod_galends_annotations,
)

from tests.conftest import make_config, GPU_CLASS_LABEL, GROUP_NAME, USERNAME


def _pod(annotations: dict | None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid="uid-1",
            name="pod-uid-1",
            namespace=USERNAME,
            labels={"gpu-class": GPU_CLASS_LABEL},
            annotations=annotations,
            creation_timestamp=datetime.now(timezone.utc),
        ),
        status=SimpleNamespace(phase="Pending", conditions=None),
        spec=SimpleNamespace(
            tolerations=[],
            containers=[
                SimpleNamespace(
                    resources=SimpleNamespace(requests={"nvidia.com/gpu": "1"})
                )
            ],
            scheduling_gates=None,
        ),
    )


# ---------------------------------------------------------------------------
# Which annotations are selected
# ---------------------------------------------------------------------------


class TestPrefixSelection:
    def test_only_galends_keys_are_taken(self):
        got = get_pod_galends_annotations(_pod({
            "galends/minimum-runtime-seconds": "600",
            "galends/usage-group": GROUP_NAME,
            "kubernetes.io/psp": "restricted",
            "hub.jupyter.org/username": "alice",
        }))
        assert got == {
            "galends/minimum-runtime-seconds": "600",
            "galends/usage-group": GROUP_NAME,
        }

    def test_a_lookalike_prefix_is_not_ours(self):
        # The trailing slash is what distinguishes the namespace we own from a
        # DNS-qualified one that merely starts with the same letters.
        got = get_pod_galends_annotations(_pod({
            "galends.example.com/usage-group": "someone-elses",
            "galends-usage-group": "not-a-prefix-either",
            "galends/usage-group": GROUP_NAME,
        }))
        assert got == {"galends/usage-group": GROUP_NAME}

    def test_controller_written_keys_are_forwarded_too(self):
        # No judgement is made about which keys matter: a re-queued pod may
        # carry the controller's own annotations, and the app is the consumer.
        got = get_pod_galends_annotations(_pod({
            "galends/booking-reference": "res-42",
            "galends/guarantee-status": "overstay",
        }))
        assert got == {
            "galends/booking-reference": "res-42",
            "galends/guarantee-status": "overstay",
        }

    def test_no_annotations_is_an_empty_map_not_an_error(self):
        # Legitimate: a pod admitted under DEFAULT_MINIMUM_RUNTIME_SECONDS and
        # DEFAULT_USAGE_GROUP declares nothing at all.
        assert get_pod_galends_annotations(_pod(None)) == {}
        assert get_pod_galends_annotations(_pod({})) == {}
        assert get_pod_galends_annotations(_pod({"only/other": "x"})) == {}


# ---------------------------------------------------------------------------
# Bounds
# ---------------------------------------------------------------------------


class TestBounds:
    def test_a_long_value_is_truncated_not_dropped(self):
        got = get_pod_galends_annotations(_pod({
            "galends/notes": "x" * (GALENDS_ANNOTATION_MAX_VALUE + 5000),
        }))
        value = got["galends/notes"]
        assert len(value) == GALENDS_ANNOTATION_MAX_VALUE
        assert value == "x" * GALENDS_ANNOTATION_MAX_VALUE

    def test_a_value_at_the_limit_is_untouched(self):
        exact = "y" * GALENDS_ANNOTATION_MAX_VALUE
        assert get_pod_galends_annotations(_pod({"galends/notes": exact})) == {
            "galends/notes": exact
        }

    def test_keys_are_capped_in_sorted_order(self):
        # Sorted, so a pod offered on two consecutive attempts presents the same
        # subset rather than flapping with dict iteration order.
        many = {f"galends/k{i:03d}": str(i) for i in range(GALENDS_ANNOTATION_MAX_KEYS + 10)}
        got = get_pod_galends_annotations(_pod(many))
        assert len(got) == GALENDS_ANNOTATION_MAX_KEYS
        assert sorted(got) == sorted(many)[:GALENDS_ANNOTATION_MAX_KEYS]

    def test_the_cap_is_deterministic_across_calls(self):
        many = {f"galends/k{i:03d}": str(i) for i in range(GALENDS_ANNOTATION_MAX_KEYS + 10)}
        first = get_pod_galends_annotations(_pod(many))
        # A different insertion order must not change which keys survive.
        shuffled = dict(reversed(list(many.items())))
        assert get_pod_galends_annotations(_pod(shuffled)) == first

    def test_dropped_keys_say_so_at_debug(self, caplog):
        many = {f"galends/k{i:03d}": str(i) for i in range(GALENDS_ANNOTATION_MAX_KEYS + 3)}
        with caplog.at_level(logging.DEBUG, logger="app.k8s_client"):
            get_pod_galends_annotations(_pod(many))
        line = next(r.getMessage() for r in caplog.records
                    if "pod.annotations_truncated" in r.getMessage())
        assert "count=3" in line
        assert f"pod=pod-uid-1" in line

    def test_a_pod_within_the_cap_is_silent(self, caplog):
        with caplog.at_level(logging.DEBUG, logger="app.k8s_client"):
            get_pod_galends_annotations(_pod({"galends/usage-group": GROUP_NAME}))
        assert not any("pod.annotations_truncated" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# What the preflighted ask carries
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-candidate-data")
    import app.main as main_module

    return main_module


def _preflight(monkeypatch, pod_annotations, *, created_at):
    m = _main_module(monkeypatch)
    now = datetime.now(timezone.utc)
    candidate = OnDemandCandidate(
        pod_uid="uid-1",
        pod_name="pod-uid-1",
        pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL,
        gpu_requested=1,
        min_runtime_seconds=600,
        pod_created_at=created_at,
        next_attempt_at=now,
        group_label=None,
        usage_group=GROUP_NAME,
    )

    async def _read(name, ns):
        return _pod(pod_annotations)

    monkeypatch.setattr(m, "read_pod", _read)
    monkeypatch.setattr(m, "is_gpu_gated_pending", lambda pod, taint_key, resources=(): True)

    state = ControllerState()
    state.gpu_class_ids = {GPU_CLASS_LABEL: 10}
    outcome, ask = asyncio.run(
        m._preflight_ondemand_candidate(state, make_config(), "uid-1", candidate)
    )
    assert outcome == m._PREFLIGHT_READY
    return ask


class TestPreflightAskCarriesPodEvidence:
    def test_creation_time_is_the_candidates_own_fifo_key(self, monkeypatch):
        # Not "now", and not the pod object's timestamp: the app must order by
        # exactly what the controller orders its batch by, so an age computed
        # from it is real queueing delay.
        created = datetime.now(timezone.utc) - timedelta(minutes=42)
        ask = _preflight(monkeypatch, {"galends/usage-group": GROUP_NAME}, created_at=created)
        assert ask.pod_created_at == created

    def test_annotations_come_from_the_freshly_read_pod(self, monkeypatch):
        # Preflight re-reads the pod, so a pod re-annotated while it waited is
        # offered as it is now rather than as it was when first seen.
        ask = _preflight(
            monkeypatch,
            {
                "galends/minimum-runtime-seconds": "600",
                "galends/usage-group": GROUP_NAME,
                "galends/added-while-waiting": "yes",
                "unrelated/key": "dropped",
            },
            created_at=datetime.now(timezone.utc),
        )
        assert ask.pod_annotations == {
            "galends/added-while-waiting": "yes",
            "galends/minimum-runtime-seconds": "600",
            "galends/usage-group": GROUP_NAME,
        }

    def test_an_unannotated_pod_still_produces_a_valid_ask(self, monkeypatch):
        ask = _preflight(monkeypatch, None, created_at=datetime.now(timezone.utc))
        assert ask.pod_annotations == {}
        assert ask.username == USERNAME
        assert ask.group_name == GROUP_NAME

    def test_the_ask_itself_is_unchanged(self, monkeypatch):
        # The evidence rides *alongside* the ask; nothing about the create moved.
        ask = _preflight(monkeypatch, None, created_at=datetime.now(timezone.utc))
        assert (ask.gpu_class_id, ask.gpu_count) == (10, 1)
        assert ask.duration_seconds == 600 + make_config().ondemand_lease_buffer_minutes * 60
