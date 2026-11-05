"""Guard 5 against a live per-node map, and what that map is built from.

Guard 5 holds an on-demand ask no single node it may use has room for.  It used
to read a node map rebuilt only on the queue tick (every 300 s), which leaked
both ways:

- **late**: a GPU coming free was invisible until the next tick, so a held pod
  waited up to an interval while anything else could take the GPU;
- **early**: the tally stopping two grants from landing on one opening lived
  only inside one admission batch, so the next batch -- any new pod's ADDED --
  granted the same opening again, and the loser sat Pending under a charged
  lease;
- **at startup**: nothing filled the map before the pod watch replayed every
  pending pod, so every guard failed open and each was granted a lease.

Layers, none touching a real cluster or the app:

- ``controller.gpu_pod_record`` / ``capacity_frees_up`` and the
  ``ControllerState`` methods behind ``gpu_pods`` -- pure.
- ``main._hold_on_fit`` / ``_run_ondemand_admission_once`` -- claims netted off
  across batches.
- ``main.pod_watch_loop`` -- a pod ending re-attempts a held candidate at once.
- ``main._seed_node_feasibility`` and ``main._run_queue_tick`` -- the seed, the
  rebuild, and ``lease.unbound``.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from kubernetes.client import (
    V1Container,
    V1ObjectMeta,
    V1Pod,
    V1PodCondition,
    V1PodSpec,
    V1PodStatus,
    V1ResourceRequirements,
    V1Toleration,
)

from app import k8s_client
from app.controller import (
    TOLERATION_KEY,
    ControllerState,
    GpuPod,
    OnDemandCandidate,
    capacity_frees_up,
    gpu_pod_record,
)
from app.k8s_client import WAITING_FOR_NODE_REASON, NodePlacement
from app.reservation_client import LeaseAttempt

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    GROUP_NAME,
    USERNAME,
    kv_fields,
    make_config,
    reservation,
)

HOST = "kubernetes.io/hostname"
_TAINTED = "0/2 nodes are available: 2 node(s) had untolerated taint(s)."
_LABELS = {"h1": {HOST: "h1"}, "h2": {HOST: "h2"}}
_ON_H1 = NodePlacement(node_selector=((HOST, "h1"),))
_ON_H2 = NodePlacement(node_selector=((HOST, "h2"),))


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-node-fit")
    import app.main as main_module

    return main_module


def _now():
    return datetime.now(timezone.utc)


def _record(name="p", *, node=None, gpus=1, placement=None, rid=None, ns=USERNAME):
    return GpuPod(
        namespace=ns, name=name, gpu_class=GPU_CLASS_LABEL, gpu_count=gpus,
        node_name=node, placement=placement, reservation_id=rid,
    )


def _state(h1=1, h2=4) -> ControllerState:
    """Class h100 on h1 and h2, with an inventory installed (as a tick would)."""
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    state.install_node_inventory({GPU_CLASS_LABEL: {"h1": h1, "h2": h2}}, dict(_LABELS))
    return state


def _pod(
    uid,
    *,
    ns=USERNAME,
    node_selector=None,
    gpus=1,
    tolerated=False,
    phase="Pending",
    node_name=None,
    booking_ref=None,
):
    """A real ``kubernetes.client`` pod, so attribute names are the client's own."""
    tolerations = (
        [V1Toleration(key=TOLERATION_KEY, operator="Equal", value=GPU_CLASS_LABEL,
                      effect="NoSchedule")]
        if tolerated else None
    )
    conditions = None
    if node_name is None and phase == "Pending":
        conditions = [V1PodCondition(
            type="PodScheduled", status="False", reason="Unschedulable", message=_TAINTED,
        )]
    return V1Pod(
        metadata=V1ObjectMeta(
            uid=uid, name=f"pod-{uid}", namespace=ns,
            labels={"gpu-class": GPU_CLASS_LABEL},
            annotations={"galends/booking-reference": booking_ref} if booking_ref else None,
            creation_timestamp=_now(),
        ),
        spec=V1PodSpec(
            containers=[V1Container(
                name="main",
                resources=V1ResourceRequirements(requests={"nvidia.com/gpu": str(gpus)}),
            )],
            node_selector=node_selector,
            tolerations=tolerations,
            node_name=node_name,
        ),
        status=V1PodStatus(phase=phase, conditions=conditions),
    )


def _candidate(uid, *, ns=USERNAME, gpus=1):
    now = _now()
    return OnDemandCandidate(
        pod_uid=uid, pod_name=f"pod-{uid}", pod_namespace=ns,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=gpus,
        min_runtime_seconds=600, pod_created_at=now, next_attempt_at=now,
        usage_group=GROUP_NAME,
    )


def _lease(rid, ns, *, started=None, gpus=1):
    start = started or _now()
    return reservation(
        rid, kind="on_demand", with_user=True, username=ns,
        start_utc=start, end_utc=start + timedelta(hours=1),
        gpu_count=gpus, gpu_class_label=GPU_CLASS_LABEL,
    )


class _Client:
    """Grants a lease per pod (keyed by its uid, the idempotency key)."""

    def __init__(self, leases):
        self._leases = leases
        self.create_requests: list = []
        self.cancel_calls: list = []

    async def create_ondemand_reservation(self, req):
        self.create_requests.append(req)
        return LeaseAttempt(reservation=self._leases[req.idempotency_key], status=201)

    async def cancel_reservation(self, reservation_id, reason):
        self.cancel_calls.append((reservation_id, reason))
        return True


class _Events:
    def __init__(self):
        self.reasons: list[str] = []

    async def __call__(self, uid, name, namespace, message, *, reason, **_kw):
        self.reasons.append(reason)


def _wire(monkeypatch, m, pods):
    """Fake the Kubernetes calls admission makes; *pods* is uid -> pod."""
    by_name = {p.metadata.name: p for p in pods.values()}

    async def read_pod(name, namespace):
        return by_name[name]

    async def _noop(*_a, **_kw):
        return None

    events = _Events()
    monkeypatch.setattr(m, "read_pod", read_pod)
    monkeypatch.setattr(m, "apply_toleration", _noop)
    monkeypatch.setattr(m, "annotate_runtime_guarantee", _noop)
    monkeypatch.setattr(m, "emit_runtime_guaranteed_event", _noop)
    monkeypatch.setattr(m, "emit_pending_pod_event", events)
    return events


# ---------------------------------------------------------------------------
# Pure: classifying a pod, and which changes make room
# ---------------------------------------------------------------------------


class TestGpuPodRecord:
    BASE = dict(namespace=USERNAME, name="p", gpu_class=GPU_CLASS_LABEL, gpu_count=2)

    def _rec(self, **kw):
        return gpu_pod_record(**{**self.BASE, **dict(
            phase="Running", scheduled_false=False, terminating=False, node_name=None,
        ), **kw})

    def test_a_running_pod_occupies_its_node(self):
        assert self._rec(node_name="h1").node_name == "h1"

    def test_a_bound_pending_pod_occupies_its_node_too(self):
        # Bound, pulling its image: the GPUs are already its.
        assert self._rec(phase="Pending", node_name="h1").node_name == "h1"

    def test_an_unbound_pending_pod_is_a_claim(self):
        rec = self._rec(phase="Pending", scheduled_false=True)
        assert rec is not None and rec.node_name is None

    @pytest.mark.parametrize("kw", [
        dict(terminating=True, node_name="h1"),
        dict(phase="Succeeded", node_name="h1"),
        dict(phase="Failed", node_name="h1"),
        dict(phase="Unknown", node_name="h1"),
        dict(phase="Running"),             # not bound, not Pending
        dict(gpu_count=0, node_name="h1"),
        dict(terminating=True, phase="Pending"),
    ])
    def test_these_hold_nothing(self, kw):
        assert self._rec(**kw) is None

    def test_it_agrees_with_the_snapshot_rule(self):
        # free_gpus_by_node_class leaves out a bound pod the scheduler marked
        # unschedulable; the record must too, or the two maps disagree.
        assert self._rec(phase="Pending", scheduled_false=True, node_name="h1") is None


class TestCapacityFreesUp:
    @pytest.mark.parametrize("previous,current,expected", [
        (None, _record(node="h1"), False),              # appeared, bound
        (None, _record(), False),                        # appeared, a claim
        (_record(), _record(node="h1"), True),           # a claim bound
        (_record(), None, True),                         # a claim went away
        (_record(), _record(rid=7), False),              # still a claim (re-linked)
        (_record(node="h1"), None, True),                # left its node
        (_record(node="h1"), _record(node="h1", rid=7), False),  # re-linked in place
        (_record(node="h1"), _record(node="h1"), False),
        (None, None, False),
    ])
    def test_only_a_release_or_a_resolved_claim_counts(self, previous, current, expected):
        assert capacity_frees_up(previous, current) is expected


# ---------------------------------------------------------------------------
# ControllerState: the map, its rebuild, and claims
# ---------------------------------------------------------------------------


class TestLiveMap:
    def test_a_bound_pod_takes_its_nodes_gpus_and_gives_them_back(self):
        state = _state(h1=4, h2=4)
        state.set_gpu_pod("a", _record(node="h1", gpus=3), _now())
        assert state.node_free_by_node_class[GPU_CLASS_LABEL] == {"h1": 1, "h2": 4}
        assert state.node_free_by_class[GPU_CLASS_LABEL] == 4
        state.set_gpu_pod("b", _record(node="h2", gpus=4), _now())
        assert state.node_free_by_class[GPU_CLASS_LABEL] == 1
        state.set_gpu_pod("a", None, _now())
        assert state.node_free_by_node_class[GPU_CLASS_LABEL] == {"h1": 4, "h2": 0}

    def test_a_claim_takes_no_node(self):
        state = _state(h1=1, h2=0)
        state.set_gpu_pod("a", _record(), _now())
        assert state.node_free_by_node_class[GPU_CLASS_LABEL] == {"h1": 1, "h2": 0}

    def test_the_same_change_twice_counts_once(self):
        state = _state(h1=4, h2=4)
        for _ in range(2):
            state.set_gpu_pod("a", _record(node="h1", gpus=2), _now())
        assert state.node_free_by_node_class[GPU_CLASS_LABEL]["h1"] == 2

    def test_a_class_with_no_inventory_is_left_alone(self):
        # Without allocatable counts there is nothing to derive from: whatever
        # the map holds stays (unknown still never blocks).
        state = ControllerState()
        state.node_free_by_class = {GPU_CLASS_LABEL: 3}
        state.set_gpu_pod("a", _record(node="h1"), _now())
        assert state.node_free_by_class == {GPU_CLASS_LABEL: 3}

    def test_an_inventory_replaces_the_maps_wholesale(self):
        state = _state()
        state.install_node_inventory({"a100": {"a1": 2}}, {"a1": {}})
        assert state.node_free_by_node_class == {"a100": {"a1": 2}}
        assert state.node_free_by_class == {"a100": 2}


class TestRebuild:
    def test_the_snapshot_replaces_what_the_watch_knew(self):
        state = _state(h1=4, h2=4)
        state.set_gpu_pod("stale", _record(node="h1", gpus=4), _now() - timedelta(minutes=5))
        state.rebuild_gpu_pods({"x": _record(node="h2", gpus=1)}, taken_at=_now())
        assert set(state.gpu_pods) == {"x"}
        assert state.node_free_by_node_class[GPU_CLASS_LABEL] == {"h1": 4, "h2": 3}

    def test_a_change_after_the_snapshot_was_taken_survives_it(self):
        state = _state(h1=4, h2=4)
        taken_at = _now()
        # The LIST was in flight: one pod bound, another was deleted.
        state.set_gpu_pod("bound", _record(node="h1", gpus=2), taken_at + timedelta(seconds=1))
        state.set_gpu_pod("gone", None, taken_at + timedelta(seconds=1))
        state.rebuild_gpu_pods(
            {"bound": _record(gpus=2), "gone": _record(node="h2", gpus=4)}, taken_at,
        )
        assert state.gpu_pods == {"bound": _record(node="h1", gpus=2)}
        assert state.node_free_by_node_class[GPU_CLASS_LABEL] == {"h1": 2, "h2": 4}

    def test_older_changes_are_pruned(self):
        state = _state()
        state.set_gpu_pod("a", None, _now() - timedelta(minutes=1))
        state.rebuild_gpu_pods({}, taken_at=_now())
        assert state._gpu_pod_changes == {}


class TestNodeClaims:
    def test_an_unconstrained_claim_counts_against_any_node(self):
        state = _state()
        state.set_gpu_pod("a", _record(gpus=2), _now())
        assert state.node_claims(GPU_CLASS_LABEL, frozenset({"h1"})) == 2
        assert state.node_claims(GPU_CLASS_LABEL, None) == 2

    def test_a_claim_pinned_elsewhere_does_not(self):
        state = _state()
        state.set_gpu_pod("a", _record(placement=_ON_H2), _now())
        assert state.node_claims(GPU_CLASS_LABEL, frozenset({"h1"})) == 0
        assert state.node_claims(GPU_CLASS_LABEL, frozenset({"h2"})) == 1
        assert state.node_claims(GPU_CLASS_LABEL, None) == 1

    def test_a_claim_allowing_no_class_node_counts_against_nothing(self):
        state = _state()
        nowhere = NodePlacement(node_selector=((HOST, "gpu-99"),))
        state.set_gpu_pod("a", _record(placement=nowhere), _now())
        assert state.node_claims(GPU_CLASS_LABEL, None) == 0

    def test_bound_pods_another_class_and_the_asker_are_not_claims(self):
        state = _state()
        state.set_gpu_pod("bound", _record(node="h1"), _now())
        state.set_gpu_pod("other", GpuPod(
            namespace=USERNAME, name="o", gpu_class="a100", gpu_count=1,
        ), _now())
        state.set_gpu_pod("self", _record(), _now())
        assert state.node_claims(GPU_CLASS_LABEL, None, exclude_uid="self") == 0


# ---------------------------------------------------------------------------
# main: claims across batches, and the watch re-attempting a hold
# ---------------------------------------------------------------------------


class TestNoDoubleGrantAcrossBatches:
    """Two pods pinned to h1, which has one free GPU, arriving in two batches."""

    def _setup(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(h1=1, h2=4)
        pods = {
            "a": _pod("a", ns="alice", node_selector={HOST: "h1"}),
            "b": _pod("b", ns="bob", node_selector={HOST: "h1"}),
        }
        events = _wire(monkeypatch, m, pods)
        client = _Client({"a": _lease(501, "alice"), "b": _lease(502, "bob")})
        return m, state, client, events

    def _batch(self, m, state, client):
        asyncio.run(m._run_ondemand_admission_once(state, client, make_config()))

    def test_the_second_batch_does_not_grant_the_claimed_opening(self, monkeypatch, caplog):
        m, state, client, events = self._setup(monkeypatch)
        state.ondemand_candidates["a"] = _candidate("a", ns="alice")
        self._batch(m, state, client)
        assert [r.idempotency_key for r in client.create_requests] == ["a"]
        # Admitted, not bound yet: a claim on h1, outliving the batch.
        assert state.gpu_pods["a"].node_name is None

        state.ondemand_candidates["b"] = _candidate("b", ns="bob")
        with caplog.at_level(logging.INFO, logger="app.main"):
            self._batch(m, state, client)
        assert [r.idempotency_key for r in client.create_requests] == ["a"]
        assert state.ondemand_candidates["b"].held_by_node_fit is True
        held = [kv_fields(r.getMessage()) for r in caplog.records
                if "ondemand.candidate_held" in r.getMessage()]
        assert held[-1]["guard"] == "5" and held[-1]["claimed"] == "1"
        assert events.reasons == [WAITING_FOR_NODE_REASON]

    def test_the_held_pod_is_retried_when_the_opening_comes_free(self, monkeypatch):
        m, state, client, _events = self._setup(monkeypatch)
        state.ondemand_candidates["a"] = _candidate("a", ns="alice")
        self._batch(m, state, client)
        state.ondemand_candidates["b"] = _candidate("b", ns="bob")
        self._batch(m, state, client)
        b = state.ondemand_candidates["b"]
        retry_at = b.next_attempt_at

        # a binds to h1: it resolves its claim but takes the GPU, so b still
        # does not fit and is not woken.
        now = _now()
        assert m._note_gpu_pod(state, "a", _record(node="h1", rid=501), now) is False
        assert b.next_attempt_at == retry_at

        # a ends: h1's GPU is free, b fits, and is made due at once.
        assert m._note_gpu_pod(state, "a", None, now) is True
        assert b.next_attempt_at <= now and b.held_by_node_fit is False
        self._batch(m, state, client)
        assert [r.idempotency_key for r in client.create_requests] == ["a", "b"]


class TestWatchRetriesAHold:
    """Through ``pod_watch_loop``: the pod on h1 finishing grants the held pod."""

    class _Watcher:
        def __init__(self, events):
            self._events = events

        async def events(self):
            for ev in self._events:
                yield ev

    def test_a_pod_ending_on_a_held_pods_node_grants_it_at_once(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(h1=1, h2=4)
        now = _now()
        booking = reservation(
            700, start_utc=now - timedelta(hours=1), end_utc=now + timedelta(hours=1),
            username="carol", gpu_count=1,
        )
        state.reservations = [booking]
        running = _pod("r", ns="carol", tolerated=True, phase="Running",
                       node_name="h1", booking_ref="res-700")
        state.set_gpu_pod("r", m._gpu_pod_of_watched(running, GPU_CLASS_LABEL), now)
        waiting = _pod("b", ns="bob", node_selector={HOST: "h1"})
        _wire(monkeypatch, m, {"b": waiting})
        client = _Client({"b": _lease(502, "bob")})

        # Held: h1 is full.
        state.ondemand_candidates["b"] = _candidate("b", ns="bob")
        asyncio.run(m._run_ondemand_admission_once(state, client, make_config()))
        assert client.create_requests == [] and "b" in state.ondemand_candidates

        done = _pod("r", ns="carol", tolerated=True, phase="Succeeded",
                    node_name="h1", booking_ref="res-700")
        monkeypatch.setattr(
            m, "PodWatcher", lambda **kw: self._Watcher([("MODIFIED", done)]),
        )
        asyncio.run(m.pod_watch_loop(state, client, make_config()))
        assert [r.idempotency_key for r in client.create_requests] == ["b"]
        assert "b" not in state.ondemand_candidates

    def test_a_pod_ending_elsewhere_starts_no_batch(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(h1=1, h2=1)
        now = _now()
        on_h2 = _pod("r", ns="carol", tolerated=True, phase="Running",
                     node_name="h2", booking_ref="res-700")
        state.set_gpu_pod("r", m._gpu_pod_of_watched(on_h2, GPU_CLASS_LABEL), now)
        state.set_gpu_pod("h1-holder", _record("x", node="h1"), now)
        candidate = _candidate("b", ns="bob")
        candidate.held_by_node_fit = True
        candidate.placement = _ON_H1
        candidate.next_attempt_at = now + timedelta(seconds=30)
        state.ondemand_candidates["b"] = candidate

        batches = []

        async def _batch(*_a, **_kw):
            batches.append(1)

        monkeypatch.setattr(m, "_run_ondemand_admission", _batch)
        done = _pod("r", ns="carol", tolerated=True, phase="Succeeded",
                    node_name="h2", booking_ref="res-700")
        monkeypatch.setattr(
            m, "PodWatcher", lambda **kw: self._Watcher([("MODIFIED", done)]),
        )
        asyncio.run(m.pod_watch_loop(state, _Client({}), make_config()))
        assert batches == []
        assert candidate.held_by_node_fit is True

    def test_a_tolerated_pod_binding_is_recorded(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(h1=4, h2=4)
        bound = _pod("r", ns="carol", tolerated=True, phase="Pending",
                     node_name="h1", booking_ref="res-700", gpus=3)
        monkeypatch.setattr(
            m, "PodWatcher", lambda **kw: self._Watcher([("MODIFIED", bound)]),
        )
        asyncio.run(m.pod_watch_loop(state, _Client({}), make_config()))
        assert state.gpu_pods["r"].node_name == "h1"
        assert state.node_free_by_node_class[GPU_CLASS_LABEL] == {"h1": 1, "h2": 4}


# ---------------------------------------------------------------------------
# Startup seed, tick rebuild, and lease.unbound
# ---------------------------------------------------------------------------


def _node(name, gpus):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, deletion_timestamp=None, annotations=None,
                                 labels={HOST: name}),
        spec=SimpleNamespace(
            taints=[SimpleNamespace(key=TOLERATION_KEY, value=GPU_CLASS_LABEL,
                                    effect="NoSchedule")],
            unschedulable=False,
        ),
        status=SimpleNamespace(allocatable={"nvidia.com/gpu": str(gpus)}),
    )


class _FakeCoreV1:
    def __init__(self, nodes=(), pods=(), fail=False):
        self._nodes, self._pods, self._fail = list(nodes), list(pods), fail

    def list_node(self):
        if self._fail:
            raise RuntimeError("node LIST failed")
        return SimpleNamespace(items=self._nodes)

    def list_pod_for_all_namespaces(self, label_selector=None):
        return SimpleNamespace(items=self._pods)


class TestStartupSeed:
    def test_the_maps_are_filled_before_the_watch_starts(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        running = _pod("r", tolerated=True, phase="Running", node_name="h1",
                       booking_ref="res-1")
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(
            nodes=[_node("h1", 1), _node("h2", 4)], pods=[running],
        ))
        state = ControllerState()
        state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
        with caplog.at_level(logging.INFO, logger="app.main"):
            asyncio.run(m._seed_node_feasibility(state, make_config()))
        assert state.node_free_by_node_class == {GPU_CLASS_LABEL: {"h1": 0, "h2": 4}}
        assert state.node_labels == {"h1": {HOST: "h1"}, "h2": {HOST: "h2"}}
        assert state.class_node_counts == {GPU_CLASS_LABEL: 2}
        assert set(state.gpu_pods) == {"r"}
        seeded = [f for f in (kv_fields(r.getMessage()) for r in caplog.records)
                  if f.get("event") == "startup.node_feasibility"]
        assert len(seeded) == 1
        assert seeded[0]["classes"] == "1" and seeded[0]["nodes"] == "2"

    def test_a_replayed_pod_pinned_to_a_full_node_is_then_held(self, monkeypatch):
        """The restart burst: with the seed, the replay is judged against data."""
        m = _main_module(monkeypatch)
        running = _pod("r", tolerated=True, phase="Running", node_name="h1",
                       booking_ref="res-1")
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(
            nodes=[_node("h1", 1), _node("h2", 4)], pods=[running],
        ))
        state = ControllerState()
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
        asyncio.run(m._seed_node_feasibility(state, make_config()))

        _wire(monkeypatch, m, {"b": _pod("b", ns="bob", node_selector={HOST: "h1"})})
        client = _Client({"b": _lease(502, "bob")})
        state.ondemand_candidates["b"] = _candidate("b", ns="bob")
        asyncio.run(m._run_ondemand_admission_once(state, client, make_config()))
        assert client.create_requests == []

    def test_a_failed_seed_logs_and_leaves_the_maps_empty(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(fail=True))
        state = ControllerState()
        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._seed_node_feasibility(state, make_config()))
        assert state.node_free_by_class == {} and state.node_free_by_node_class == {}
        assert any("event=startup.node_feasibility_failed" in r.getMessage()
                   for r in caplog.records)

    def test_nothing_is_seeded_with_on_demand_off(self, monkeypatch):
        m = _main_module(monkeypatch)
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(fail=True))
        state = ControllerState()
        asyncio.run(m._seed_node_feasibility(
            state, make_config(ondemand_lease_enabled=False),
        ))
        assert state.node_free_by_class == {}

    def test_lifespan_seeds_before_starting_the_loops(self):
        """Ordering is the whole fix: a seed after the watch started is too late."""
        import inspect

        from app import main

        source = inspect.getsource(main.lifespan)
        assert source.index("_seed_node_feasibility(") < source.index("pod_watch_loop(")


def _tick(monkeypatch, state, pods, nodes):
    m = _main_module(monkeypatch)
    monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes=nodes, pods=pods))

    async def _noop(*_a, **_kw):
        return None

    monkeypatch.setattr(m, "_apply_guarantee_status", _noop)
    monkeypatch.setattr(m, "_apply_reservation_facts", _noop)
    config = make_config(pod_adoption_enabled=False, ondemand_merge_enabled=False)
    asyncio.run(m._run_queue_tick(state, None, config))
    return m


class TestTickRebuild:
    def test_the_tick_rebuilds_from_its_snapshot(self, monkeypatch):
        state = _state(h1=4, h2=4)
        state.set_gpu_pod("ghost", _record(node="h1", gpus=4), _now() - timedelta(hours=1))
        running = _pod("r", tolerated=True, phase="Running", node_name="h2",
                       booking_ref="res-1", gpus=3)
        _tick(monkeypatch, state, [running], [_node("h1", 4), _node("h2", 4)])
        assert set(state.gpu_pods) == {"r"}
        assert state.node_free_by_node_class == {GPU_CLASS_LABEL: {"h1": 4, "h2": 1}}

    def test_a_change_the_watch_made_since_the_snapshot_survives_the_tick(self, monkeypatch):
        state = _state(h1=4, h2=4)
        # The watch saw the pod end after the tick's LIST went out.
        state.set_gpu_pod("r", None, _now() + timedelta(minutes=1))
        running = _pod("r", tolerated=True, phase="Running", node_name="h2",
                       booking_ref="res-1", gpus=3)
        _tick(monkeypatch, state, [running], [_node("h1", 4), _node("h2", 4)])
        assert "r" not in state.gpu_pods
        assert state.node_free_by_node_class == {GPU_CLASS_LABEL: {"h1": 4, "h2": 4}}


class TestLeaseUnbound:
    def _unbound(self, uid, rid, *, node_selector=None):
        return _pod(uid, tolerated=True, booking_ref=f"res-{rid}", node_selector=node_selector)

    def _lines(self, caplog):
        return [kv_fields(r.getMessage()) for r in caplog.records
                if "event=lease.unbound" in r.getMessage()]

    def test_a_pod_unbound_under_a_charged_lease_is_reported(self, monkeypatch, caplog):
        state = _state()
        state.reservations = [_lease(500, USERNAME, started=_now() - timedelta(minutes=10))]
        pod = self._unbound("u", 500, node_selector={HOST: "h1"})
        busy = _pod("busy", tolerated=True, phase="Running", node_name="h1",
                    booking_ref="res-9")
        with caplog.at_level(logging.WARNING, logger="app.main"):
            _tick(monkeypatch, state, [pod, busy], [_node("h1", 1), _node("h2", 4)])
        lines = self._lines(caplog)
        assert len(lines) == 1
        line = lines[0]
        assert line["rid"] == "500" and line["pod"] == "pod-u" and line["gpus"] == "1"
        assert 590 <= int(line["unbound_s"]) <= 700
        # Its own placement explains the wait: h1 is full, h2 has room.
        assert line["reason"] == "matching_nodes_full"
        assert line["detail"].startswith("node selector")

    @pytest.mark.parametrize("case", ["young", "booking", "bound", "best_effort"])
    def test_these_are_not(self, monkeypatch, caplog, case):
        state = _state()
        started = _now() - timedelta(minutes=10)
        lease = _lease(500, USERNAME, started=started)
        pod = self._unbound("u", 500)
        if case == "young":
            lease = _lease(500, USERNAME, started=_now() - timedelta(seconds=30))
        elif case == "booking":
            lease = reservation(500, start_utc=started, end_utc=started + timedelta(hours=1),
                                gpu_count=1)
        elif case == "bound":
            pod = _pod("u", tolerated=True, phase="Running", node_name="h2",
                       booking_ref="res-500")
        elif case == "best_effort":
            lease = reservation(500, kind="best_effort", with_user=True,
                                start_utc=started, end_utc=started, gpu_count=1)
        state.reservations = [lease]
        with caplog.at_level(logging.WARNING, logger="app.main"):
            _tick(monkeypatch, state, [pod], [_node("h1", 1), _node("h2", 4)])
        assert self._lines(caplog) == []
