"""Deployment-wide stand-ins for the two things a pod must declare itself.

A pod is JIT-eligible only when it names a minimum runtime
(``galends/minimum-runtime-seconds``) *and* a usage group (the
``REQUIRED_GROUP_LABEL`` pod label, else the ``galends/usage-group``
annotation).  A pod that names neither is left Pending forever, which is right
for a cluster where users are expected to annotate their workloads and wrong for
one where the operator would rather every unannotated pod fall into a house
default.

``DEFAULT_MINIMUM_RUNTIME_SECONDS`` and ``DEFAULT_USAGE_GROUP`` supply those
stand-ins.  Both ship disabled (``0`` / unset), so an unconfigured deployment
behaves exactly as before — which the "no default" cases below pin.

``DEFAULT_USAGE_GROUP`` is an ordered list resolved per user against the
listed groups' rosters; here the pod's owner is a member of every listed group,
so the stand-in behaviour is what is under test.  The per-user walk itself is
``tests/test_default_group_fallback.py``.

Drives ``pod_watch_loop`` with a fake watcher, mirroring
``tests/test_jit_group_source.py``.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from app.config import Config
from app.controller import ControllerState, DefaultGroupRoster

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    GROUP_NAME,
    USERNAME,
    make_config,
    reservation,
)

GROUP_LABEL_NAME = "dsmlp/course"
MIN_RUNTIME = "galends/minimum-runtime-seconds"
USAGE_GROUP = "galends/usage-group"


def _config(**overrides) -> Config:
    return make_config(**overrides)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-pod-defaults")
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


def _roster(name: str = GROUP_NAME, *, members=(USERNAME,), auto_join=False, active=True):
    """One DEFAULT_USAGE_GROUP entry as the app would describe it."""
    return DefaultGroupRoster(
        name=name, group_id=1, is_active=active, auto_join=auto_join,
        members=frozenset(members),
    )


def _run_watch(monkeypatch, m, config, pod, state=None, rosters=None):
    """Feed one ADDED event through pod_watch_loop with the admission batch stubbed.

    *rosters* stand in for the last GET /api/groups: by default the pod's owner
    is a member of every configured default group, which is the case the
    stand-in tests below are about (the per-user walk has its own tests).
    """

    async def _no_admission(*args, **kwargs):
        pass

    monkeypatch.setattr(m, "PodWatcher", lambda **kw: _FakeWatcher([("ADDED", pod)]))
    monkeypatch.setattr(m, "_run_ondemand_admission", _no_admission)

    state = state if state is not None else ControllerState()
    state.required_group_label = config.required_group_label
    state.default_usage_groups = config.default_usage_groups
    if rosters is None:
        rosters = [_roster(name) for name in config.default_usage_groups]
    state.default_group_rosters = {r.name: r for r in rosters}
    asyncio.run(m.pod_watch_loop(state, None, config))
    return state


# ---------------------------------------------------------------------------
# DEFAULT_MINIMUM_RUNTIME_SECONDS
# ---------------------------------------------------------------------------


class TestDefaultMinimumRuntime:
    def test_unannotated_pod_is_not_eligible_without_a_default(self, monkeypatch):
        # The shipped default (0 = off): unchanged behaviour.
        m = _main_module(monkeypatch)
        pod = _pod("uid-1", annotations={USAGE_GROUP: GROUP_NAME})
        state = _run_watch(monkeypatch, m, _config(), pod)

        assert state.ondemand_candidates == {}

    def test_default_stands_in_for_a_missing_annotation(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod("uid-2", annotations={USAGE_GROUP: GROUP_NAME})
        config = _config(default_min_runtime_seconds=900)
        state = _run_watch(monkeypatch, m, config, pod)

        assert state.ondemand_candidates["uid-2"].min_runtime_seconds == 900

    def test_a_pod_with_no_annotations_at_all_is_covered(self, monkeypatch):
        # metadata.annotations is None, not {} — the common shape for a pod
        # nobody annotated.  Both defaults have to survive that.
        m = _main_module(monkeypatch)
        pod = _pod("uid-3")
        config = _config(
            default_min_runtime_seconds=900, default_usage_groups=(GROUP_NAME,)
        )
        state = _run_watch(monkeypatch, m, config, pod)

        candidate = state.ondemand_candidates["uid-3"]
        assert candidate.min_runtime_seconds == 900
        assert candidate.usage_group == GROUP_NAME

    def test_the_pods_own_annotation_wins(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(
            "uid-4",
            annotations={MIN_RUNTIME: "300", USAGE_GROUP: GROUP_NAME},
        )
        config = _config(default_min_runtime_seconds=900)
        state = _run_watch(monkeypatch, m, config, pod)

        assert state.ondemand_candidates["uid-4"].min_runtime_seconds == 300

    def test_an_unusable_annotation_falls_back_to_the_default(self, monkeypatch):
        # get_pod_min_runtime_seconds already rejects junk and non-positive
        # values; the default is what a rejected value now degrades to, matching
        # config.py's own "junk falls back" posture.
        m = _main_module(monkeypatch)
        pod = _pod(
            "uid-5",
            annotations={MIN_RUNTIME: "not-a-number", USAGE_GROUP: GROUP_NAME},
        )
        config = _config(default_min_runtime_seconds=900)
        state = _run_watch(monkeypatch, m, config, pod)

        assert state.ondemand_candidates["uid-5"].min_runtime_seconds == 900


# ---------------------------------------------------------------------------
# DEFAULT_USAGE_GROUP
# ---------------------------------------------------------------------------


class TestDefaultUsageGroupWithLabelFeatureOn:
    def test_labelless_pod_is_not_eligible_without_a_default(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod("uid-6", annotations={MIN_RUNTIME: "600"})
        config = _config(required_group_label=GROUP_LABEL_NAME)
        state = _run_watch(monkeypatch, m, config, pod)

        assert state.ondemand_candidates == {}

    def test_default_stands_in_for_a_missing_label(self, monkeypatch):
        # The default substitutes for the label wholesale: it is both the match
        # axis carried on the candidate and the lease ask's group_name.
        m = _main_module(monkeypatch)
        pod = _pod("uid-7", annotations={MIN_RUNTIME: "600"})
        config = _config(
            required_group_label=GROUP_LABEL_NAME, default_usage_groups=(GROUP_NAME,)
        )
        state = _run_watch(monkeypatch, m, config, pod)

        candidate = state.ondemand_candidates["uid-7"]
        assert candidate.group_label == GROUP_NAME
        assert candidate.usage_group == GROUP_NAME

    def test_the_pods_own_label_wins(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(
            "uid-8",
            labels={GROUP_LABEL_NAME: "cse251a"},
            annotations={MIN_RUNTIME: "600"},
        )
        config = _config(
            required_group_label=GROUP_LABEL_NAME, default_usage_groups=(GROUP_NAME,)
        )
        state = _run_watch(monkeypatch, m, config, pod)

        candidate = state.ondemand_candidates["uid-8"]
        assert candidate.group_label == "cse251a"
        assert candidate.usage_group == "cse251a"

    def test_default_also_matches_on_the_reserved_path(self, monkeypatch):
        # The point of defaulting the *label* rather than only the lease ask:
        # a labelless pod now matches the default group's booking and is queued
        # for it instead of being routed to a JIT lease.
        m = _main_module(monkeypatch)
        now = datetime.now(timezone.utc)
        state = ControllerState()
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.reservations = [
            reservation(
                1,
                start_utc=now - timedelta(minutes=5),
                end_utc=now + timedelta(hours=2),
                group=GROUP_NAME,
            )
        ]
        pod = _pod("uid-9", annotations={MIN_RUNTIME: "600"})
        config = _config(
            required_group_label=GROUP_LABEL_NAME, default_usage_groups=(GROUP_NAME,)
        )

        # The window is already open, so the fast path fires; stub the patch.
        async def _no_apply(*args, **kwargs):
            return False

        monkeypatch.setattr(m, "_try_apply_toleration", _no_apply)
        _run_watch(monkeypatch, m, config, pod, state=state)

        assert "uid-9" in state.task_queue
        assert state.task_queue["uid-9"].reservation.id == 1
        assert state.ondemand_candidates == {}

    def test_a_mismatched_default_still_matches_nothing(self, monkeypatch):
        # The default is not a wildcard: it names one group, and a booking held
        # by a different group stays unmatched.
        m = _main_module(monkeypatch)
        now = datetime.now(timezone.utc)
        state = ControllerState()
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.reservations = [
            reservation(
                1,
                start_utc=now - timedelta(minutes=5),
                end_utc=now + timedelta(hours=2),
                group="some-other-group",
            )
        ]
        pod = _pod("uid-10", annotations={MIN_RUNTIME: "600"})
        config = _config(
            required_group_label=GROUP_LABEL_NAME, default_usage_groups=(GROUP_NAME,)
        )
        _run_watch(monkeypatch, m, config, pod, state=state)

        assert state.task_queue == {}
        assert state.ondemand_candidates["uid-10"].usage_group == GROUP_NAME


class TestDefaultUsageGroupWithLabelFeatureOff:
    def test_default_stands_in_for_a_missing_annotation(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod("uid-11", annotations={MIN_RUNTIME: "600"})
        config = _config(default_usage_groups=(GROUP_NAME,))
        state = _run_watch(monkeypatch, m, config, pod)

        candidate = state.ondemand_candidates["uid-11"]
        assert candidate.usage_group == GROUP_NAME
        # The match axis stays disabled — only REQUIRED_GROUP_LABEL turns it on.
        assert candidate.group_label is None

    def test_the_pods_own_annotation_wins(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _pod(
            "uid-12",
            annotations={MIN_RUNTIME: "600", USAGE_GROUP: "annotated-group"},
        )
        config = _config(default_usage_groups=(GROUP_NAME,))
        state = _run_watch(monkeypatch, m, config, pod)

        assert state.ondemand_candidates["uid-12"].usage_group == "annotated-group"


# ---------------------------------------------------------------------------
# The snapshot must read a defaulted pod back the same way routing wrote it
# ---------------------------------------------------------------------------


class TestSnapshotAppliesTheGroupDefault:
    """A pod admitted under a default group must read back carrying it.

    ``ToleratedPodInfo.group_label`` feeds ``_group_ok`` through
    ``PodRuntimeView``.  If the snapshot reported ``None`` for a pod routing had
    treated as a default group, every reservation would be rejected for it —
    adoption and the JIT-lease merge could never find a booking to carry it onto,
    and it would be evicted instead.

    The fallback is per user, so what the snapshot is handed is a resolver,
    called with each labelless pod's namespace -- ``state.default_usage_group_for``
    in production.
    """

    def _snapshot(self, monkeypatch, pods, *, key, default):
        from app import k8s_client

        monkeypatch.setattr(
            k8s_client,
            "_core_v1",
            SimpleNamespace(
                list_pod_for_all_namespaces=lambda **kw: SimpleNamespace(items=pods)
            ),
        )
        return asyncio.run(
            k8s_client.snapshot_tolerated_pods(
                "gpu-class-reservation", key, default
            )
        )

    def _tolerated_pod(self, labels: dict, *, namespace: str = USERNAME, uid: str = "uid-20"):
        return SimpleNamespace(
            metadata=SimpleNamespace(
                uid=uid,
                name=f"pod-{uid}",
                namespace=namespace,
                labels={"gpu-class": GPU_CLASS_LABEL, **labels},
                annotations={"galends/booking-reference": "res-1"},
                deletion_timestamp=None,
            ),
            status=SimpleNamespace(phase="Running", conditions=None),
            spec=SimpleNamespace(
                node_name="node-a",
                tolerations=[
                    SimpleNamespace(
                        key="gpu-class-reservation",
                        operator="Equal",
                        value=GPU_CLASS_LABEL,
                        effect="NoSchedule",
                    )
                ],
                containers=[
                    SimpleNamespace(
                        resources=SimpleNamespace(requests={"nvidia.com/gpu": "1"})
                    )
                ],
            ),
        )

    def test_default_fills_in_for_a_labelless_pod(self, monkeypatch):
        out = self._snapshot(
            monkeypatch,
            [self._tolerated_pod({})],
            key=GROUP_LABEL_NAME,
            default=lambda namespace: GROUP_NAME,
        )
        assert out[0].group_label == GROUP_NAME

    def test_each_pod_gets_its_own_owners_default(self, monkeypatch):
        # Two labelless pods, two users, two different fallbacks: the resolver
        # is asked per pod, with that pod's namespace.
        state = ControllerState()
        state.default_usage_groups = ("research-a", "dsmlp-public")
        state.default_group_rosters = {
            "research-a": _roster("research-a", members=("bob",)),
            "dsmlp-public": _roster("dsmlp-public", members=(), auto_join=True),
        }
        out = self._snapshot(
            monkeypatch,
            [
                self._tolerated_pod({}, namespace="bob", uid="uid-b"),
                self._tolerated_pod({}, namespace="carol", uid="uid-c"),
            ],
            key=GROUP_LABEL_NAME,
            default=state.default_usage_group_for,
        )
        assert {p.namespace: p.group_label for p in out} == {
            "bob": "research-a",
            "carol": "dsmlp-public",
        }

    def test_the_pods_own_label_wins(self, monkeypatch):
        asked: list[str] = []

        def _default(namespace):
            asked.append(namespace)
            return GROUP_NAME

        out = self._snapshot(
            monkeypatch,
            [self._tolerated_pod({GROUP_LABEL_NAME: "cse251a"})],
            key=GROUP_LABEL_NAME,
            default=_default,
        )
        assert out[0].group_label == "cse251a"
        assert asked == []  # a pod with its own label never needs the fallback

    def test_no_default_leaves_it_none(self, monkeypatch):
        out = self._snapshot(
            monkeypatch, [self._tolerated_pod({})], key=GROUP_LABEL_NAME, default=None
        )
        assert out[0].group_label is None

    def test_a_user_no_default_admits_reads_back_none(self, monkeypatch):
        out = self._snapshot(
            monkeypatch,
            [self._tolerated_pod({})],
            key=GROUP_LABEL_NAME,
            default=lambda namespace: None,
        )
        assert out[0].group_label is None

    def test_feature_off_ignores_the_default(self, monkeypatch):
        # The group *match axis* is off, so the field stays None regardless.
        out = self._snapshot(
            monkeypatch,
            [self._tolerated_pod({})],
            key=None,
            default=lambda namespace: GROUP_NAME,
        )
        assert out[0].group_label is None
