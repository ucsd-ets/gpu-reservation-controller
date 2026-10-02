"""JIT lease group sourcing: the lease ask's required ``group_name``.

``group_name`` is a **required** natural key on the app's on-demand create
endpoint (RESERVATION-API.md §"Creating on-demand reservations"), so a pod is
JIT-eligible only when it names its usage group:

- ``REQUIRED_GROUP_LABEL`` on → the group label value (also the match axis);
- feature off → the ``galends/usage-group`` pod annotation;
- neither → not JIT-eligible (previously such a pod became a candidate whose
  lease request carried ``group_name: null`` and was 422-rejected app-side,
  forever).

Drives ``pod_watch_loop`` with a fake watcher and asserts what lands in
``state.ondemand_candidates`` and in the preflighted ask.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from types import SimpleNamespace

from app.config import Config
from app.controller import ControllerState

from tests.conftest import make_config, GPU_CLASS_LABEL, GROUP_NAME, USERNAME

GROUP_LABEL_NAME = "dsmlp/course"


def _config(**overrides) -> Config:
    """Thin alias for the shared builder in tests/conftest."""
    return make_config(**overrides)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-jit-group")
    import app.main as main_module

    return main_module


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


def _pod(uid: str, *, labels: dict | None = None, annotations: dict | None = None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid,
            name=f"pod-{uid}",
            namespace=USERNAME,
            labels={"gpu-class": GPU_CLASS_LABEL, **(labels or {})},
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


def _run_watch(monkeypatch, m, config, pod):
    """Feed one ADDED event through pod_watch_loop with the admission batch stubbed."""
    batches: list[bool] = []

    async def _no_admission(*args, **kwargs):
        batches.append(True)

    monkeypatch.setattr(m, "PodWatcher", lambda **kw: _FakeWatcher([("ADDED", pod)]))
    monkeypatch.setattr(m, "_run_ondemand_admission", _no_admission)

    state = ControllerState()
    state.required_group_label = config.required_group_label
    asyncio.run(m.pod_watch_loop(state, None, config))
    return state, batches


class TestJitGroupSource:
    def test_annotation_supplies_group_when_label_feature_off(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(
            "uid-1",
            annotations={
                "galends/minimum-runtime-seconds": "600",
                "galends/usage-group": GROUP_NAME,
            },
        )
        state, batches = _run_watch(monkeypatch, m, _config(), pod)

        assert "uid-1" in state.ondemand_candidates
        candidate = state.ondemand_candidates["uid-1"]
        assert candidate.usage_group == GROUP_NAME
        assert candidate.group_label is None  # match axis stays disabled
        assert batches  # a fresh candidate kicks an admission batch

    def test_no_group_source_means_not_jit_eligible(self, monkeypatch):
        # min-runtime is present but no group label feature and no
        # galends/usage-group annotation: the lease ask could not carry the
        # required group_name, so the pod must not become a candidate.
        m = _main_module(monkeypatch)
        pod = _pod("uid-2", annotations={"galends/minimum-runtime-seconds": "600"})
        state, batches = _run_watch(monkeypatch, m, _config(), pod)

        assert state.ondemand_candidates == {}
        assert batches == []

    def test_label_supplies_group_when_feature_on(self, monkeypatch):
        # With REQUIRED_GROUP_LABEL on, the label is both match axis and group
        # source; a conflicting annotation is ignored.
        m = _main_module(monkeypatch)
        pod = _pod(
            "uid-3",
            labels={GROUP_LABEL_NAME: "cse251a"},
            annotations={
                "galends/minimum-runtime-seconds": "600",
                "galends/usage-group": "something-else",
            },
        )
        config = _config(required_group_label=GROUP_LABEL_NAME)
        state, _ = _run_watch(monkeypatch, m, config, pod)

        candidate = state.ondemand_candidates["uid-3"]
        assert candidate.usage_group == "cse251a"
        assert candidate.group_label == "cse251a"

    def test_labelless_pod_not_eligible_when_feature_on(self, monkeypatch):
        # Feature on + annotation but no label: unchanged from before — the
        # pod matches no grouped reservation and is not JIT-eligible.
        m = _main_module(monkeypatch)
        pod = _pod(
            "uid-4",
            annotations={
                "galends/minimum-runtime-seconds": "600",
                "galends/usage-group": GROUP_NAME,
            },
        )
        config = _config(required_group_label=GROUP_LABEL_NAME)
        state, _ = _run_watch(monkeypatch, m, config, pod)

        assert state.ondemand_candidates == {}


class TestPreflightAskCarriesUsageGroup:
    def test_ask_group_name_comes_from_usage_group(self, monkeypatch):
        from app.controller import OnDemandCandidate

        m = _main_module(monkeypatch)
        now = datetime.now(timezone.utc)
        candidate = OnDemandCandidate(
            pod_uid="uid-9",
            pod_name="pod-uid-9",
            pod_namespace=USERNAME,
            gpu_class_label=GPU_CLASS_LABEL,
            gpu_requested=1,
            min_runtime_seconds=600,
            pod_created_at=now,
            next_attempt_at=now,
            group_label=None,
            usage_group=GROUP_NAME,
        )

        async def _read(name, ns):
            return _pod(
                "uid-9",
                annotations={
                    "galends/minimum-runtime-seconds": "600",
                    "galends/usage-group": GROUP_NAME,
                },
            )

        monkeypatch.setattr(m, "read_pod", _read)
        # Make guard 1 pass deterministically without PodScheduled conditions.
        monkeypatch.setattr(m, "is_gpu_gated_pending", lambda pod, taint_key, resources=(): True)

        state = ControllerState()
        state.gpu_class_ids = {GPU_CLASS_LABEL: 10}

        outcome, ask = asyncio.run(
            m._preflight_ondemand_candidate(state, _config(), "uid-9", candidate)
        )
        assert outcome == m._PREFLIGHT_READY
        assert ask is not None
        assert ask.group_name == GROUP_NAME
        assert ask.username == USERNAME
