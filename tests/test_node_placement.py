"""A pod's own node selector / required node affinity, and what it does to JIT.

Users narrow where a GPU pod may run beyond its class: a node selector naming
one host, or a required affinity on a hardware label.  Before admission the
scheduler cannot report on that -- every node of the pod's class is rejected on
our untolerated taint first, and TaintToleration runs ahead of NodeAffinity -- so
guard 1a read such a pod as leasable.  It was granted an SU-charged lease, sat
Pending under it, and once admitted tripped guard 3, pausing on-demand admission
for every other pod of the class for as long as it waited.

Layers, none touching a real cluster or the app:

- ``k8s_client.get_pod_node_placement`` / ``NodePlacement`` -- digesting the
  constraints off real ``kubernetes.client`` models and matching them with
  Kubernetes' semantics.
- ``k8s_client.snapshot_node_gpu_inventory(labels_out=)`` and
  ``snapshot_tolerated_pods`` -- carrying node labels and pod placement.
- ``controller.placement_stall_reason`` and ``main._placement_nodes`` -- pure.
- ``main._preflight_ondemand_candidate`` -- the new guard-1 hold
  (``NoMatchingNode``) and guard 5 over only the allowed nodes (``WaitingForNode``).
- ``main._run_queue_tick`` end to end -- guard 3 leaving such a holder out.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from kubernetes.client import (
    V1Affinity,
    V1Container,
    V1NodeAffinity,
    V1NodeSelector,
    V1NodeSelectorRequirement,
    V1NodeSelectorTerm,
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
    OnDemandCandidate,
    placement_stall_reason,
)
from app.k8s_client import (
    NO_MATCHING_NODE_REASON,
    WAITING_FOR_NODE_REASON,
    NodePlacement,
    get_pod_node_placement,
)

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    GROUP_NAME,
    OTHER_CLASS_ID,
    OTHER_CLASS_LABEL,
    USERNAME,
    kv_fields,
    make_config,
)

HOST = "kubernetes.io/hostname"
PRODUCT = "nvidia.com/gpu.product"
_TAINTED = "0/3 nodes are available: 3 node(s) had untolerated taint(s)."


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-placement")
    import app.main as main_module

    return main_module


def _req(key, operator, *values):
    return V1NodeSelectorRequirement(key=key, operator=operator, values=list(values) or None)


def _pod(
    uid="uid-1",
    *,
    node_selector=None,
    terms=None,
    gpus=1,
    tolerated=False,
    phase="Pending",
    message=_TAINTED,
):
    """A real ``kubernetes.client`` pod, so attribute names are the client's own."""
    affinity = None
    if terms is not None:
        affinity = V1Affinity(node_affinity=V1NodeAffinity(
            required_during_scheduling_ignored_during_execution=V1NodeSelector(
                node_selector_terms=terms,
            ),
        ))
    tolerations = (
        [V1Toleration(key=TOLERATION_KEY, operator="Equal", value=GPU_CLASS_LABEL,
                      effect="NoSchedule")]
        if tolerated else None
    )
    return V1Pod(
        metadata=V1ObjectMeta(
            uid=uid, name=f"pod-{uid}", namespace=USERNAME,
            labels={"gpu-class": GPU_CLASS_LABEL},
            annotations={"galends/booking-reference": "res-1"} if tolerated else None,
        ),
        spec=V1PodSpec(
            containers=[V1Container(
                name="main",
                resources=V1ResourceRequirements(requests={"nvidia.com/gpu": str(gpus)}),
            )],
            node_selector=node_selector,
            affinity=affinity,
            tolerations=tolerations,
        ),
        status=V1PodStatus(
            phase=phase,
            conditions=[V1PodCondition(
                type="PodScheduled", status="False", reason="Unschedulable",
                message=message,
            )],
        ),
    )


# ---------------------------------------------------------------------------
# get_pod_node_placement / NodePlacement — Kubernetes' matching semantics
# ---------------------------------------------------------------------------


class TestGetPodNodePlacement:
    def test_a_pod_with_neither_has_no_placement(self):
        assert get_pod_node_placement(_pod()) is None

    def test_a_plain_test_double_without_the_fields_has_none(self):
        # Every other suite builds pods from SimpleNamespace without these
        # attributes; reading them must not raise.
        pod = SimpleNamespace(spec=SimpleNamespace(tolerations=[], containers=[]))
        assert get_pod_node_placement(pod) is None

    def test_node_selector_needs_every_label_equal(self):
        p = get_pod_node_placement(_pod(node_selector={HOST: "h1", "zone": "a"}))
        assert p.matches("h1", {HOST: "h1", "zone": "a", "extra": "x"})
        assert not p.matches("h1", {HOST: "h1", "zone": "b"})
        assert not p.matches("h1", {HOST: "h1"})

    @pytest.mark.parametrize("req,labels,expected", [
        (_req(PRODUCT, "In", "H100", "A100"), {PRODUCT: "A100"}, True),
        (_req(PRODUCT, "In", "H100"), {}, False),
        (_req(PRODUCT, "NotIn", "H100"), {PRODUCT: "A100"}, True),
        # NotIn and DoesNotExist hold for a node without the label at all.
        (_req(PRODUCT, "NotIn", "H100"), {}, True),
        (_req(PRODUCT, "Exists"), {PRODUCT: ""}, True),
        (_req(PRODUCT, "Exists"), {}, False),
        (_req(PRODUCT, "DoesNotExist"), {}, True),
        (_req(PRODUCT, "DoesNotExist"), {PRODUCT: "H100"}, False),
        (_req("gpu.memory", "Gt", "40000"), {"gpu.memory": "81920"}, True),
        (_req("gpu.memory", "Gt", "81920"), {"gpu.memory": "81920"}, False),
        (_req("gpu.memory", "Lt", "81920"), {"gpu.memory": "40960"}, True),
        # Gt/Lt need an integer on both sides; anything else fails the node.
        (_req("gpu.memory", "Gt", "40000"), {"gpu.memory": "80Gi"}, False),
        # Stricter than Python's int(), as Go's ParseInt is.
        (_req("gpu.memory", "Gt", "40000"), {"gpu.memory": "81_920"}, False),
        (_req("gpu.memory", "Gt", "-1"), {"gpu.memory": "+0"}, True),
        (_req("gpu.memory", "Gt", "40000"), {}, False),
    ])
    def test_match_expression_operators(self, req, labels, expected):
        p = get_pod_node_placement(_pod(terms=[V1NodeSelectorTerm(match_expressions=[req])]))
        assert p.matches("n1", labels) is expected

    def test_terms_are_ored_and_expressions_within_a_term_anded(self):
        p = get_pod_node_placement(_pod(terms=[
            V1NodeSelectorTerm(match_expressions=[
                _req(PRODUCT, "In", "H100"), _req("zone", "In", "a"),
            ]),
            V1NodeSelectorTerm(match_expressions=[_req(HOST, "In", "special")]),
        ]))
        assert p.matches("n1", {PRODUCT: "H100", "zone": "a"})
        assert not p.matches("n1", {PRODUCT: "H100", "zone": "b"})
        assert p.matches("n1", {HOST: "special"})

    def test_an_empty_term_matches_no_node(self):
        p = get_pod_node_placement(_pod(terms=[V1NodeSelectorTerm()]))
        assert not p.matches("n1", {PRODUCT: "H100"})

    def test_match_fields_on_node_name(self):
        p = get_pod_node_placement(_pod(terms=[V1NodeSelectorTerm(
            match_fields=[_req("metadata.name", "In", "gpu-7")],
        )]))
        assert p.matches("gpu-7", {})
        assert not p.matches("gpu-8", {})

    def test_selector_and_affinity_must_both_hold(self):
        p = get_pod_node_placement(_pod(
            node_selector={"zone": "a"},
            terms=[V1NodeSelectorTerm(match_expressions=[_req(PRODUCT, "In", "H100")])],
        ))
        assert p.matches("n1", {"zone": "a", PRODUCT: "H100"})
        assert not p.matches("n1", {"zone": "b", PRODUCT: "H100"})
        assert not p.matches("n1", {"zone": "a", PRODUCT: "A100"})

    @pytest.mark.parametrize("term", [
        V1NodeSelectorTerm(match_expressions=[_req(PRODUCT, "Matches", "H.*")]),
        V1NodeSelectorTerm(match_fields=[_req("metadata.labels", "In", "x")]),
        V1NodeSelectorTerm(match_fields=[_req("metadata.name", "Exists")]),
    ])
    def test_something_it_cannot_evaluate_reads_as_no_placement(self, term):
        # Fail open: a placement the controller cannot read never holds a pod.
        assert get_pod_node_placement(_pod(terms=[term])) is None

    def test_describe(self):
        p = get_pod_node_placement(_pod(
            node_selector={HOST: "h1"},
            terms=[
                V1NodeSelectorTerm(match_expressions=[_req(PRODUCT, "In", "H100", "A100")]),
                V1NodeSelectorTerm(match_expressions=[_req("zone", "DoesNotExist")]),
            ],
        ))
        assert p.describe() == (
            "node selector kubernetes.io/hostname=h1; required node affinity "
            "(nvidia.com/gpu.product in (H100, A100)) or (zone absent)"
        )

    def test_matching_nodes(self):
        p = get_pod_node_placement(_pod(node_selector={PRODUCT: "H100"}))
        labels = {"n1": {PRODUCT: "H100"}, "n2": {PRODUCT: "A100"}}
        assert p.matching_nodes(["n1", "n2", "n3"], labels) == frozenset({"n1"})


# ---------------------------------------------------------------------------
# Snapshots carry node labels and pod placement
# ---------------------------------------------------------------------------


def _node(name, gpu_class, gpus, labels=None, *, unschedulable=False):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name, deletion_timestamp=None, annotations=None, labels=labels,
        ),
        spec=SimpleNamespace(
            taints=[SimpleNamespace(key=TOLERATION_KEY, value=gpu_class, effect="NoSchedule")],
            unschedulable=unschedulable,
        ),
        status=SimpleNamespace(allocatable={"nvidia.com/gpu": str(gpus)}),
    )


class _FakeCoreV1:
    def __init__(self, nodes=(), pods=()):
        self._nodes, self._pods = list(nodes), list(pods)

    def list_node(self):
        return SimpleNamespace(items=self._nodes)

    def list_pod_for_all_namespaces(self, label_selector=None):
        return SimpleNamespace(items=self._pods)


class TestSnapshots:
    def test_inventory_fills_labels_for_the_nodes_it_counts(self, monkeypatch):
        nodes = [
            _node("h1", GPU_CLASS_LABEL, 4, {HOST: "h1"}),
            _node("h2", GPU_CLASS_LABEL, 4, {HOST: "h2"}, unschedulable=True),
            _node("h3", GPU_CLASS_LABEL, 4, None),
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))
        labels: dict = {}
        inventory = asyncio.run(
            k8s_client.snapshot_node_gpu_inventory(TOLERATION_KEY, labels_out=labels)
        )
        assert inventory == {GPU_CLASS_LABEL: {"h1": 4, "h3": 4}}
        # A cordoned node is not counted, so it has no labels here either; a
        # node with no labels is recorded as having none, not left out.
        assert labels == {"h1": {HOST: "h1"}, "h3": {}}

    def test_inventory_without_labels_out_is_unchanged(self, monkeypatch):
        monkeypatch.setattr(
            k8s_client, "_core_v1", _FakeCoreV1([_node("h1", GPU_CLASS_LABEL, 4)])
        )
        assert asyncio.run(k8s_client.snapshot_node_gpu_inventory(TOLERATION_KEY)) == {
            GPU_CLASS_LABEL: {"h1": 4}
        }

    def test_tolerated_pods_carry_their_placement(self, monkeypatch):
        pods = [
            _pod("a", node_selector={HOST: "h1"}, tolerated=True),
            _pod("b", tolerated=True),
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(pods=pods))
        snap = asyncio.run(k8s_client.snapshot_tolerated_pods(TOLERATION_KEY))
        by_uid = {p.uid: p for p in snap}
        assert by_uid["a"].placement == NodePlacement(node_selector=((HOST, "h1"),))
        assert by_uid["b"].placement is None


# ---------------------------------------------------------------------------
# Pure helpers
# ---------------------------------------------------------------------------


class TestPlacementStallReason:
    FREE = {"h1": 0, "h2": 4}

    def test_unconstrained_is_never_excused(self):
        assert placement_stall_reason(1, self.FREE, None) is None

    def test_allowing_no_node_is_excused(self):
        assert placement_stall_reason(1, self.FREE, frozenset()) == "no_matching_node"

    def test_allowed_nodes_full_while_another_has_room_is_excused(self):
        assert placement_stall_reason(1, self.FREE, frozenset({"h1"})) == "matching_nodes_full"

    def test_an_allowed_node_with_room_is_not_excused(self):
        # Stuck for some reason nothing here explains: guard 3's business.
        assert placement_stall_reason(1, self.FREE, frozenset({"h2"})) is None

    def test_a_class_full_everywhere_is_not_excused(self):
        # A pod without the constraint would be stuck too.
        assert placement_stall_reason(1, {"h1": 0, "h2": 0}, frozenset({"h1"})) is None

    def test_room_is_measured_against_the_pods_own_gpu_count(self):
        free = {"h1": 2, "h2": 4}
        assert placement_stall_reason(4, free, frozenset({"h1"})) == "matching_nodes_full"
        assert placement_stall_reason(2, free, frozenset({"h1"})) is None


class TestPlacementNodes:
    LABELS = {"h1": {HOST: "h1", PRODUCT: "H100"}, "h2": {HOST: "h2", PRODUCT: "H100"}}

    def _nodes(self, monkeypatch, placement, class_nodes=("h1", "h2"), labels=None):
        m = _main_module(monkeypatch)
        return m._placement_nodes(
            placement, class_nodes, self.LABELS if labels is None else labels
        )

    def test_no_placement_or_unknown_class_is_unconstrained(self, monkeypatch):
        assert self._nodes(monkeypatch, None) is None
        p = NodePlacement(node_selector=((HOST, "h1"),))
        assert self._nodes(monkeypatch, p, class_nodes=None) is None

    def test_a_subset_is_returned(self, monkeypatch):
        p = NodePlacement(node_selector=((HOST, "h1"),))
        assert self._nodes(monkeypatch, p) == frozenset({"h1"})

    def test_allowing_none_is_an_empty_set_not_none(self, monkeypatch):
        p = NodePlacement(node_selector=((HOST, "elsewhere"),))
        assert self._nodes(monkeypatch, p) == frozenset()

    def test_allowing_every_node_of_the_class_is_unconstrained(self, monkeypatch):
        # A node selector restating the class's hardware narrows nothing.
        p = NodePlacement(node_selector=((PRODUCT, "H100"),))
        assert self._nodes(monkeypatch, p) is None

    def test_a_node_without_recorded_labels_makes_it_unknown(self, monkeypatch):
        p = NodePlacement(node_selector=((HOST, "elsewhere"),))
        assert self._nodes(monkeypatch, p, labels={"h1": self.LABELS["h1"]}) is None


# ---------------------------------------------------------------------------
# main._preflight_ondemand_candidate
# ---------------------------------------------------------------------------


class _EventRecorder:
    """Stands in for ``emit_pending_pod_event`` as ``main`` calls it."""

    def __init__(self):
        self.calls: list[dict] = []

    async def __call__(self, uid, name, namespace, message, *, reason,
                       gpu_class=None, gpu_count=None):
        self.calls.append(dict(uid=uid, message=message, reason=reason,
                               gpu_class=gpu_class, gpu_count=gpu_count))

    @property
    def reasons(self):
        return [c["reason"] for c in self.calls]


def _candidate(uid="uid-1", gpus=1):
    now = datetime.now(timezone.utc)
    return OnDemandCandidate(
        pod_uid=uid, pod_name=f"pod-{uid}", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=gpus,
        min_runtime_seconds=600, pod_created_at=now, next_attempt_at=now,
        usage_group=GROUP_NAME,
    )


def _state(*, h1_free=0, h2_free=4, labels=True):
    """Class h100 on h1 (full by default) and h2; class a100 on a1."""
    state = ControllerState()
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL, OTHER_CLASS_ID: OTHER_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID, OTHER_CLASS_LABEL: OTHER_CLASS_ID}
    state.node_free_by_node_class = {
        GPU_CLASS_LABEL: {"h1": h1_free, "h2": h2_free},
        OTHER_CLASS_LABEL: {"a1": 4},
    }
    state.node_free_by_class = {GPU_CLASS_LABEL: max(h1_free, h2_free), OTHER_CLASS_LABEL: 4}
    state.class_node_counts = {GPU_CLASS_LABEL: 2, OTHER_CLASS_LABEL: 1}
    if labels:
        state.node_labels = {
            "h1": {HOST: "h1", PRODUCT: "H100"},
            "h2": {HOST: "h2", PRODUCT: "H100"},
            "a1": {HOST: "a1", PRODUCT: "A100"},
        }
    return state


def _preflight(monkeypatch, state, pod, *, candidate=None, config=None, claimed=None,
               recorder=None):
    m = _main_module(monkeypatch)
    candidate = candidate or _candidate(gpus=int(
        pod.spec.containers[0].resources.requests["nvidia.com/gpu"]
    ))
    recorder = recorder if recorder is not None else _EventRecorder()

    async def fake_read_pod(name, namespace):
        return pod

    monkeypatch.setattr(m, "read_pod", fake_read_pod)
    monkeypatch.setattr(m, "emit_pending_pod_event", recorder)
    status, ask = asyncio.run(m._preflight_ondemand_candidate(
        state, config or make_config(), candidate.pod_uid, candidate, claimed,
    ))
    return m, status, ask, candidate, recorder


class TestPreflightNoMatchingNode:
    def test_a_selector_matching_no_class_node_is_held_and_told(self, monkeypatch, caplog):
        with caplog.at_level(logging.INFO, logger="app.main"):
            m, status, ask, candidate, rec = _preflight(
                monkeypatch, _state(), _pod(node_selector={HOST: "a1"})
            )
        assert status == m._PREFLIGHT_RETRY and ask is None
        # The jittered 2-5 min retry, not the 30 s one: waiting is on a person.
        assert candidate.next_attempt_at >= datetime.now(timezone.utc) + timedelta(seconds=100)
        assert rec.reasons == [NO_MATCHING_NODE_REASON]
        message = rec.calls[0]["message"]
        assert "node selector kubernetes.io/hostname=a1" in message
        assert "none of the 2 schedulable node(s) of GPU class h100" in message
        # It points at the class whose nodes the pod actually asked for.
        assert "It does match nodes of GPU class a100" in message
        held = [kv_fields(r.getMessage()) for r in caplog.records
                if "ondemand.candidate_held" in r.getMessage()]
        assert held and held[0]["guard"] == "1" and held[0]["reason"] == "no_matching_node"
        assert held[0]["nodes"] == "2"

    def test_a_selector_matching_nothing_anywhere_names_no_other_class(self, monkeypatch):
        _m, _s, _a, _c, rec = _preflight(
            monkeypatch, _state(), _pod(node_selector={HOST: "gpu-99"})
        )
        assert rec.reasons == [NO_MATCHING_NODE_REASON]
        assert "It does match nodes" not in rec.calls[0]["message"]

    def test_hardware_the_class_does_not_have(self, monkeypatch):
        pod = _pod(terms=[V1NodeSelectorTerm(match_expressions=[_req(PRODUCT, "In", "A100")])])
        _m, _s, _a, _c, rec = _preflight(monkeypatch, _state(), pod)
        assert rec.reasons == [NO_MATCHING_NODE_REASON]
        assert "required node affinity nvidia.com/gpu.product in (A100)" in rec.calls[0]["message"]

    def test_it_is_told_before_a_class_wide_pause(self, monkeypatch):
        # The pause would not help this pod even once lifted; its own
        # placement is what its owner needs to hear about.
        state = _state()
        state.stuck_holder_gpu_classes = {GPU_CLASS_LABEL}
        _m, _s, _a, _c, rec = _preflight(monkeypatch, state, _pod(node_selector={HOST: "x"}))
        assert rec.reasons == [NO_MATCHING_NODE_REASON]

    def test_the_event_can_be_turned_off_without_releasing_the_hold(self, monkeypatch):
        m, status, _a, _c, rec = _preflight(
            monkeypatch, _state(), _pod(node_selector={HOST: "x"}),
            config=make_config(pod_problem_event_enabled=False),
        )
        assert status == m._PREFLIGHT_RETRY
        assert rec.calls == []

    def test_an_unchanged_hold_is_told_once_per_repeat_interval(self, monkeypatch):
        state, rec = _state(), _EventRecorder()
        pod = _pod(node_selector={HOST: "x"})
        _preflight(monkeypatch, state, pod, recorder=rec)
        _preflight(monkeypatch, state, pod, recorder=rec)
        assert rec.reasons == [NO_MATCHING_NODE_REASON]

    @pytest.mark.parametrize("state_kw", [dict(labels=False)])
    def test_unknown_node_labels_never_hold(self, monkeypatch, state_kw):
        m, status, ask, _c, rec = _preflight(
            monkeypatch, _state(**state_kw), _pod(node_selector={HOST: "x"})
        )
        assert status == m._PREFLIGHT_READY and ask is not None
        assert rec.calls == []

    def test_no_inventory_for_the_class_never_holds(self, monkeypatch):
        state = _state()
        state.node_free_by_node_class = {}
        m, status, _a, _c, _r = _preflight(monkeypatch, state, _pod(node_selector={HOST: "x"}))
        assert status == m._PREFLIGHT_READY


class TestPreflightWaitingForNode:
    def test_a_one_gpu_pod_pinned_to_a_full_node_is_held(self, monkeypatch, caplog):
        with caplog.at_level(logging.INFO, logger="app.main"):
            m, status, _a, candidate, rec = _preflight(
                monkeypatch, _state(h1_free=0, h2_free=4), _pod(node_selector={HOST: "h1"})
            )
        assert status == m._PREFLIGHT_RETRY
        assert candidate.next_attempt_at <= datetime.now(timezone.utc) + timedelta(seconds=60)
        assert rec.reasons == [WAITING_FOR_NODE_REASON]
        assert "kubernetes.io/hostname=h1" in rec.calls[0]["message"]
        held = [kv_fields(r.getMessage()) for r in caplog.records
                if "ondemand.candidate_held" in r.getMessage()]
        assert held[0]["guard"] == "5" and held[0]["node_free"] == "0"
        assert held[0]["detail"].startswith("node selector")

    def test_a_pod_pinned_to_a_node_with_room_proceeds(self, monkeypatch):
        m, status, ask, _c, rec = _preflight(
            monkeypatch, _state(h1_free=0, h2_free=4), _pod(node_selector={HOST: "h2"})
        )
        assert status == m._PREFLIGHT_READY and ask.gpu_count == 1
        assert rec.calls == []

    def test_a_multi_gpu_ask_is_measured_on_the_allowed_nodes_only(self, monkeypatch):
        # h2 has room for 4 but the pod allows only h1, which has 2.
        m, status, _a, _c, rec = _preflight(
            monkeypatch, _state(h1_free=2, h2_free=4),
            _pod(node_selector={HOST: "h1"}, gpus=4),
        )
        assert status == m._PREFLIGHT_RETRY
        assert rec.reasons == [WAITING_FOR_NODE_REASON]

    def test_a_selector_allowing_every_class_node_changes_nothing(self, monkeypatch):
        # Restating the class's hardware narrows nothing: a 1-GPU ask still
        # skips guard 5 exactly as an unconstrained one does, full class or not.
        m, status, _a, _c, rec = _preflight(
            monkeypatch, _state(h1_free=0, h2_free=0), _pod(node_selector={PRODUCT: "H100"})
        )
        assert status == m._PREFLIGHT_READY
        assert rec.calls == []

    def test_the_batch_tally_is_netted_off_the_allowed_nodes(self, monkeypatch):
        # Conservative by design: the tally is per class, so a GPU another
        # candidate claimed this batch counts against these nodes too.
        m, status, _a, _c, rec = _preflight(
            monkeypatch, _state(h1_free=1, h2_free=4), _pod(node_selector={HOST: "h1"}),
            claimed={GPU_CLASS_LABEL: 1},
        )
        assert status == m._PREFLIGHT_RETRY
        assert rec.reasons == [WAITING_FOR_NODE_REASON]


class _LeaseRecorder:
    """Stands in for the ``ReservationClient`` a batch would ask for leases."""

    def __init__(self):
        self.requests: list = []

    async def create_ondemand_reservation(self, req):
        self.requests.append(req)
        from app.reservation_client import LeaseAttempt
        return LeaseAttempt(status=409, detail="no capacity")


class TestNoLeaseIsRequested:
    """Both holds sit in preflight, ahead of the grant: the app is never asked."""

    def _batch(self, monkeypatch, pod, state):
        m = _main_module(monkeypatch)

        async def fake_read_pod(name, namespace):
            return pod

        async def _no_event(*_a, **_kw):
            return None

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "emit_pending_pod_event", _EventRecorder())
        monkeypatch.setattr(m, "emit_lease_denied_event", _no_event)
        client = _LeaseRecorder()
        state.ondemand_candidates["uid-1"] = _candidate()
        asyncio.run(m._run_ondemand_admission_once(state, client, make_config()))
        return client

    def test_not_for_a_pod_allowing_no_class_node(self, monkeypatch):
        client = self._batch(monkeypatch, _pod(node_selector={HOST: "a1"}), _state())
        assert client.requests == []

    def test_not_for_a_pod_whose_allowed_nodes_are_full(self, monkeypatch):
        client = self._batch(monkeypatch, _pod(node_selector={HOST: "h1"}), _state(h1_free=0))
        assert client.requests == []

    def test_but_for_one_whose_allowed_node_has_room(self, monkeypatch):
        client = self._batch(monkeypatch, _pod(node_selector={HOST: "h2"}), _state())
        assert len(client.requests) == 1


# ---------------------------------------------------------------------------
# main._run_queue_tick — guard 3 leaves out a holder stuck on its placement
# ---------------------------------------------------------------------------


def _tick(monkeypatch, pods, nodes, *, inventory_fails=False):
    m = _main_module(monkeypatch)
    fake = _FakeCoreV1(nodes=nodes, pods=pods)
    if inventory_fails:
        def _boom():
            raise RuntimeError("node LIST failed")
        fake.list_node = _boom
    monkeypatch.setattr(k8s_client, "_core_v1", fake)

    async def _noop(*_a, **_kw):
        return None

    # Annotation reconciles are covered elsewhere; this tick is about guard 3.
    monkeypatch.setattr(m, "_apply_guarantee_status", _noop)
    monkeypatch.setattr(m, "_apply_reservation_facts", _noop)
    state = ControllerState()
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    config = make_config(
        ondemand_lease_enabled=True, pod_adoption_enabled=False, ondemand_merge_enabled=False,
    )
    asyncio.run(m._run_queue_tick(state, None, config))
    return state


_NODES = [
    _node("h1", GPU_CLASS_LABEL, 1, {HOST: "h1"}),
    _node("h2", GPU_CLASS_LABEL, 4, {HOST: "h2"}),
]


def _running_on(node, gpus, uid):
    pod = _pod(uid, tolerated=True, gpus=gpus, phase="Running")
    pod.spec.node_name = node
    pod.status.conditions = None
    return pod


class TestQueueTickGuard3:
    def test_a_holder_allowing_no_class_node_does_not_pause_the_class(self, monkeypatch, caplog):
        stuck = _pod("s", tolerated=True, node_selector={HOST: "gpu-99"})
        with caplog.at_level(logging.DEBUG, logger="app.main"):
            state = _tick(monkeypatch, [stuck], _NODES)
        assert state.stuck_holder_gpu_classes == set()
        excluded = [kv_fields(r.getMessage()) for r in caplog.records
                    if "interlock.holder_excluded" in r.getMessage()]
        assert excluded and excluded[0]["reason"] == "no_matching_node"
        assert excluded[0]["pod"] == "pod-s"

    def test_a_holder_pinned_to_a_full_node_while_another_has_room(self, monkeypatch):
        busy = _running_on("h1", 1, "busy")
        stuck = _pod("s", tolerated=True, node_selector={HOST: "h1"})
        state = _tick(monkeypatch, [busy, stuck], _NODES)
        assert state.stuck_holder_gpu_classes == set()

    def test_a_pinned_holder_on_a_class_full_everywhere_still_pauses_it(self, monkeypatch):
        pods = [
            _running_on("h1", 1, "b1"), _running_on("h2", 4, "b2"),
            _pod("s", tolerated=True, node_selector={HOST: "h1"}),
        ]
        state = _tick(monkeypatch, pods, _NODES)
        assert state.stuck_holder_gpu_classes == {GPU_CLASS_LABEL}

    def test_a_pinned_holder_whose_node_has_room_still_pauses_it(self, monkeypatch):
        stuck = _pod("s", tolerated=True, node_selector={HOST: "h2"})
        state = _tick(monkeypatch, [stuck], _NODES)
        assert state.stuck_holder_gpu_classes == {GPU_CLASS_LABEL}
        assert state.stuck_holder_pods == {GPU_CLASS_LABEL: [f"{USERNAME}.pod-s"]}

    def test_an_unconstrained_stuck_holder_pauses_it_as_before(self, monkeypatch):
        state = _tick(monkeypatch, [_pod("s", tolerated=True)], _NODES)
        assert state.stuck_holder_gpu_classes == {GPU_CLASS_LABEL}

    def test_without_an_inventory_every_stuck_holder_counts(self, monkeypatch):
        stuck = _pod("s", tolerated=True, node_selector={HOST: "gpu-99"})
        state = _tick(monkeypatch, [stuck], _NODES, inventory_fails=True)
        assert state.stuck_holder_gpu_classes == {GPU_CLASS_LABEL}

    def test_the_tick_records_what_the_preflight_reads(self, monkeypatch):
        state = _tick(monkeypatch, [_running_on("h2", 3, "r")], _NODES)
        assert state.node_free_by_node_class == {GPU_CLASS_LABEL: {"h1": 1, "h2": 1}}
        assert state.node_labels == {"h1": {HOST: "h1"}, "h2": {HOST: "h2"}}
        assert state.node_free_by_class == {GPU_CLASS_LABEL: 1}


# ---------------------------------------------------------------------------
# With the readiness rule: a pod pinned to a NotReady node
# ---------------------------------------------------------------------------


class TestPinnedToNotReadyNode:
    """The inventory drops a NotReady node (``node_exclusion_reason``), so a pod
    pinned to one allows no node of its class: it is told ``NoMatchingNode``
    ("out of service"), not ``WaitingForNode``, and no lease is requested.
    Through the real queue tick and node snapshot, not a hand-built map."""

    def _tick(self, monkeypatch, *, h2_ready):
        m = _main_module(monkeypatch)
        ready = lambda ok: SimpleNamespace(type="Ready", status="True" if ok else "False")
        h1 = _node("h1", GPU_CLASS_LABEL, 4, {HOST: "h1"})
        h2 = _node("h2", GPU_CLASS_LABEL, 4, {HOST: "h2"})
        h1.status.conditions = [ready(True)]
        h2.status.conditions = [ready(h2_ready)]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes=[h1, h2]))

        pod = _pod(node_selector={HOST: "h2"})

        async def fake_read_pod(name, namespace):
            return pod

        async def _noop(*_a, **_kw):
            return None

        rec = _EventRecorder()
        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "emit_pending_pod_event", rec)
        monkeypatch.setattr(m, "emit_lease_denied_event", _noop)
        monkeypatch.setattr(m, "_apply_guarantee_status", _noop)
        monkeypatch.setattr(m, "_apply_reservation_facts", _noop)
        state = ControllerState()
        state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
        state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
        state.ondemand_candidates["uid-1"] = _candidate()
        client = _LeaseRecorder()
        asyncio.run(m._run_queue_tick(state, client, make_config(
            pod_adoption_enabled=False, ondemand_merge_enabled=False,
        )))
        return state, rec, client

    def test_a_notready_pinned_node_is_no_matching_node(self, monkeypatch):
        state, rec, client = self._tick(monkeypatch, h2_ready=False)
        assert state.node_labels == {"h1": {HOST: "h1"}}
        assert rec.reasons == [NO_MATCHING_NODE_REASON]
        assert client.requests == []

    def test_once_it_is_ready_the_lease_is_requested(self, monkeypatch):
        _state_, rec, client = self._tick(monkeypatch, h2_ready=True)
        assert rec.calls == []
        assert len(client.requests) == 1
