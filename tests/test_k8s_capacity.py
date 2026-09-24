"""Tests for the k8s_client node/pod capacity snapshots.

Covers ``snapshot_node_gpu_capacity`` (per-class totals), its per-node primitive
``snapshot_node_gpu_inventory``, the ``galends/force-node-capacity`` operator
override those read, and the ``node_name`` capture added to
``snapshot_tolerated_pods``.  Uses SimpleNamespace stubs and a fake ``_core_v1``
(no real Kubernetes client), matching the ``monkeypatch`` / ``asyncio.run``
convention used for the other async k8s_client wrappers.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import app.k8s_client as k8s_client

TAINT_KEY = "gpu-class-reservation"


def _taint(key: str, value: str) -> SimpleNamespace:
    return SimpleNamespace(key=key, value=value)


def _node(
    name: str,
    *,
    taints: list | None = None,
    allocatable: dict | None = None,
    unschedulable: bool = False,
    deleting: bool = False,
    annotations: dict | None = None,
    ready: str | None = None,
) -> SimpleNamespace:
    """A node stub.  *ready* is the ``Ready`` condition's status (``"True"``,
    ``"False"``, ``"Unknown"``); ``None`` reports no conditions at all, which the
    snapshot keeps, so the stubs that predate readiness still read as schedulable.
    """
    conditions = None if ready is None else [SimpleNamespace(type="Ready", status=ready)]
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name,
            deletion_timestamp="2024-01-01T00:00:00Z" if deleting else None,
            annotations=annotations,
        ),
        spec=SimpleNamespace(taints=taints, unschedulable=unschedulable),
        status=SimpleNamespace(allocatable=allocatable, conditions=conditions),
    )


class _FakeCoreV1:
    def __init__(self, nodes: list):
        self._nodes = nodes

    def list_node(self):
        return SimpleNamespace(items=self._nodes)


def _run_snapshot(monkeypatch, nodes: list) -> dict[str, int]:
    monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))
    return asyncio.run(k8s_client.snapshot_node_gpu_capacity(TAINT_KEY))


class TestSnapshotNodeGpuCapacity:
    def test_sums_allocatable_by_taint_value(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
            _node(
                "n2",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 8}

    def test_multiple_classes_bucketed_separately(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
            _node(
                "n2",
                taints=[_taint(TAINT_KEY, "a100")],
                allocatable={"nvidia.com/gpu": "2"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 4, "a100": 2}

    def test_node_without_taint_ignored(self, monkeypatch):
        nodes = [
            _node("n1", taints=[], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=None, allocatable={"nvidia.com/gpu": "4"}),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_other_taint_keys_ignored(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint("some-other-taint", "x")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_unschedulable_node_excluded(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
                unschedulable=True,
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_deleting_node_excluded(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "4"},
                deleting=True,
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_missing_allocatable_treated_as_zero(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable=None),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 0}

    def test_garbage_allocatable_treated_as_zero(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "h100")],
                allocatable={"nvidia.com/gpu": "not-a-number"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {"h100": 0}

    def test_empty_taint_value_ignored(self, monkeypatch):
        nodes = [
            _node(
                "n1",
                taints=[_taint(TAINT_KEY, "")],
                allocatable={"nvidia.com/gpu": "4"},
            ),
        ]
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {}

    def test_no_nodes(self, monkeypatch):
        assert _run_snapshot(monkeypatch, []) == {}


# ---------------------------------------------------------------------------
# snapshot_node_gpu_inventory — the per-node primitive
# ---------------------------------------------------------------------------


def _run_inventory(monkeypatch, nodes: list) -> dict[str, dict[str, int]]:
    monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1(nodes))
    return asyncio.run(k8s_client.snapshot_node_gpu_inventory(TAINT_KEY))


class TestSnapshotNodeGpuInventory:
    def test_per_node_breakdown(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "8"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 4, "n2": 8}}

    def test_multiple_classes_bucketed_by_node(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=[_taint(TAINT_KEY, "a100")], allocatable={"nvidia.com/gpu": "2"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {
            "h100": {"n1": 4},
            "a100": {"n2": 2},
        }

    def test_unschedulable_and_deleting_nodes_excluded(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}, unschedulable=True),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}, deleting=True),
            _node("n3", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n3": 4}}

    def test_garbage_and_missing_allocatable_treated_as_zero(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "not-a-number"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable={}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 0, "n2": 0}}

    def test_untainted_node_ignored(self, monkeypatch):
        nodes = [_node("n1", taints=[], allocatable={"nvidia.com/gpu": "4"})]
        assert _run_inventory(monkeypatch, nodes) == {}

    def test_capacity_is_the_per_class_sum_of_the_inventory(self, monkeypatch):
        """``snapshot_node_gpu_capacity`` must equal the collapsed inventory."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "4"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")], allocatable={"nvidia.com/gpu": "8"}),
            _node("n3", taints=[_taint(TAINT_KEY, "a100")], allocatable={"nvidia.com/gpu": "2"}),
        ]
        inventory = _run_inventory(monkeypatch, nodes)
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {
            gpu_class: sum(per_node.values())
            for gpu_class, per_node in inventory.items()
        }
        assert capacity == {"h100": 12, "a100": 2}


# ---------------------------------------------------------------------------
# Node readiness — a crashed node stops counting without anyone cordoning it
# ---------------------------------------------------------------------------


class TestNodeExclusionReason:
    """The pure predicate, exercised without a snapshot around it."""

    def test_ready_node_is_kept(self):
        assert k8s_client.node_exclusion_reason(_node("n1", ready="True")) is None

    def test_ready_false_is_not_ready(self):
        # The kubelet is up and says the node is not ready (runtime down, PLEG).
        assert k8s_client.node_exclusion_reason(_node("n1", ready="False")) == "not_ready"

    def test_ready_unknown_is_not_ready(self):
        # The kubelet stopped heartbeating: the crash case.
        assert k8s_client.node_exclusion_reason(_node("n1", ready="Unknown")) == "not_ready"

    def test_no_conditions_is_kept(self):
        """Missing data never removes capacity — it would pause admission or
        start a preemption on the strength of nothing."""
        assert k8s_client.node_exclusion_reason(_node("n1")) is None

    def test_other_conditions_without_ready_are_kept(self):
        node = _node("n1")
        node.status.conditions = [SimpleNamespace(type="MemoryPressure", status="True")]
        assert k8s_client.node_exclusion_reason(node) is None

    def test_status_without_a_conditions_attribute_is_kept(self):
        node = _node("n1")
        node.status = SimpleNamespace(allocatable={})
        assert k8s_client.node_exclusion_reason(node) is None

    def test_cordoned_and_deleting_keep_their_reasons(self):
        assert k8s_client.node_exclusion_reason(
            _node("n1", unschedulable=True, ready="True")
        ) == "cordoned"
        assert k8s_client.node_exclusion_reason(
            _node("n1", deleting=True, ready="True")
        ) == "deleting"


class TestNotReadyNodesInInventory:
    def test_not_ready_nodes_excluded(self, monkeypatch):
        """Neither ``spec.unschedulable`` nor ``status.allocatable`` moves when a
        node dies, so the Ready condition is the only thing that can drop it."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, ready="Unknown"),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, ready="False"),
            _node("n3", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, ready="True"),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n3": 8}}
        # And so the per-class total the capacity audit (guard 4) compares.
        assert _run_snapshot(monkeypatch, nodes) == {"h100": 8}

    def test_a_class_whose_nodes_are_all_down_is_absent(self, monkeypatch):
        """The same shape a fully cordoned class takes, which guard 1b reads."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, ready="Unknown"),
        ]
        assert _run_inventory(monkeypatch, nodes) == {}

    def test_forced_capacity_does_not_resurrect_a_not_ready_node(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"},
                  annotations={k8s_client.FORCE_NODE_CAPACITY: "4"}, ready="Unknown"),
        ]
        assert _run_inventory(monkeypatch, nodes) == {}

    def test_exclusions_are_logged_for_gpu_nodes_only(self, monkeypatch, caplog):
        """At DEBUG, naming the node and why — the per-class inventory line
        shows a smaller total but not which node went missing."""
        nodes = [
            _node("gpu-down", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, ready="Unknown"),
            _node("gpu-cordoned", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, unschedulable=True),
            _node("cpu-down", taints=[], ready="Unknown"),
        ]
        with caplog.at_level(logging.DEBUG, logger="app.k8s_client"):
            _run_inventory(monkeypatch, nodes)
        lines = sorted(
            r.getMessage() for r in caplog.records
            if "event=k8s.node_excluded " in r.getMessage() + " "
        )
        assert len(lines) == 2, lines
        assert "node=gpu-cordoned" in lines[0] and "reason=cordoned" in lines[0]
        assert "node=gpu-down" in lines[1] and "reason=not_ready" in lines[1]


# ---------------------------------------------------------------------------
# galends/force-node-capacity — the operator override
# ---------------------------------------------------------------------------

FORCE = k8s_client.FORCE_NODE_CAPACITY


class TestGetNodeForcedGpuCapacity:
    """The pure annotation reader, exercised without a snapshot around it."""

    def test_absent_annotation_is_none(self):
        assert k8s_client.get_node_forced_gpu_capacity(_node("n1")) is None
        assert k8s_client.get_node_forced_gpu_capacity(
            _node("n1", annotations={})
        ) is None
        assert k8s_client.get_node_forced_gpu_capacity(
            _node("n1", annotations={"galends/other": "4"})
        ) is None

    def test_positive_value_parsed(self):
        node = _node("n1", annotations={FORCE: "6"})
        assert k8s_client.get_node_forced_gpu_capacity(node) == 6

    def test_zero_is_a_valid_override(self):
        """0 masks a node's GPUs without cordoning it — not a rejection."""
        node = _node("n1", annotations={FORCE: "0"})
        assert k8s_client.get_node_forced_gpu_capacity(node) == 0

    def test_surrounding_whitespace_tolerated(self):
        node = _node("n1", annotations={FORCE: " 3\n"})
        assert k8s_client.get_node_forced_gpu_capacity(node) == 3

    def test_empty_value_is_none(self):
        assert k8s_client.get_node_forced_gpu_capacity(
            _node("n1", annotations={FORCE: ""})
        ) is None
        assert k8s_client.get_node_forced_gpu_capacity(
            _node("n1", annotations={FORCE: "   "})
        ) is None

    def test_negative_rejected_with_warning(self, caplog):
        node = _node("n1", annotations={FORCE: "-2"})
        with caplog.at_level(logging.WARNING, logger="app.k8s_client"):
            assert k8s_client.get_node_forced_gpu_capacity(node) is None
        assert any(
            "event=k8s.node_capacity_forced_invalid" in r.getMessage()
            and "node=n1" in r.getMessage()
            for r in caplog.records
        )

    def test_garbage_rejected_with_warning(self, caplog):
        node = _node("n1", annotations={FORCE: "four"})
        with caplog.at_level(logging.WARNING, logger="app.k8s_client"):
            assert k8s_client.get_node_forced_gpu_capacity(node) is None
        assert any(
            "event=k8s.node_capacity_forced_invalid" in r.getMessage()
            and "value=four" in r.getMessage()
            for r in caplog.records
        )

    def test_fractional_rejected(self):
        """A GPU count is whole; "2.5" is a typo, not a request for 2."""
        assert k8s_client.get_node_forced_gpu_capacity(
            _node("n1", annotations={FORCE: "2.5"})
        ) is None


class TestForcedCapacityInInventory:
    def test_forced_value_replaces_allocatable(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "2"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 2, "n2": 8}}
        assert _run_snapshot(monkeypatch, nodes) == {"h100": 10}

    def test_forced_value_may_exceed_allocatable(self, monkeypatch):
        """The override is a replacement, not a cap — raising is the operator's call."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}, annotations={FORCE: "16"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 16}}

    def test_forced_zero_keeps_the_node_at_zero(self, monkeypatch):
        """Same shape as a node reporting no GPUs — present, contributing none.

        Keeping the entry matters: ``node_counts_by_class`` (guard 1b) counts
        nodes, so dropping it would read as "this class has no nodes at all".
        """
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "0"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 0}}
        assert _run_snapshot(monkeypatch, nodes) == {"h100": 0}

    def test_invalid_forced_value_falls_back_to_allocatable(self, monkeypatch):
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "-1"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "4"}, annotations={FORCE: "lots"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 8, "n2": 4}}

    def test_forced_value_overrides_unparseable_allocatable(self, monkeypatch):
        """The main repair case: a device plugin reporting nonsense."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "not-a-number"},
                  annotations={FORCE: "4"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable=None, annotations={FORCE: "4"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {"h100": {"n1": 4, "n2": 4}}

    def test_forced_value_applies_to_every_class_the_node_serves(self, monkeypatch):
        """The annotation is per node, so a multi-taint node forces both buckets."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100"), _taint(TAINT_KEY, "a100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "3"}),
        ]
        assert _run_inventory(monkeypatch, nodes) == {
            "h100": {"n1": 3},
            "a100": {"n1": 3},
        }

    def test_forced_node_still_excluded_when_cordoned_or_deleting(self, monkeypatch):
        """The override sets a node's capacity; it does not resurrect the node."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "4"},
                  unschedulable=True),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "4"},
                  deleting=True),
        ]
        assert _run_inventory(monkeypatch, nodes) == {}

    def test_forced_node_without_the_taint_still_ignored(self, monkeypatch):
        """The taint is what enrols a node; an annotation alone enrols nothing."""
        nodes = [_node("n1", taints=[], allocatable={"nvidia.com/gpu": "8"},
                       annotations={FORCE: "4"})]
        assert _run_inventory(monkeypatch, nodes) == {}

    def test_applied_override_is_logged(self, monkeypatch, caplog):
        """At DEBUG, so an operator can confirm the annotation took effect.

        The per-class ``k8s.node_inventory`` line shows the total but not which
        node was overridden, and nothing else in the controller reports it.
        """
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "2"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}),
        ]
        with caplog.at_level(logging.DEBUG, logger="app.k8s_client"):
            _run_inventory(monkeypatch, nodes)
        forced_lines = [
            r.getMessage() for r in caplog.records
            if "event=k8s.node_capacity_forced " in r.getMessage() + " "
        ]
        assert len(forced_lines) == 1, forced_lines
        assert "node=n1" in forced_lines[0]
        assert "total=2" in forced_lines[0]

    def test_capacity_remains_the_collapse_of_the_forced_inventory(self, monkeypatch):
        """``snapshot_node_gpu_capacity``'s contract survives the override."""
        nodes = [
            _node("n1", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}, annotations={FORCE: "2"}),
            _node("n2", taints=[_taint(TAINT_KEY, "h100")],
                  allocatable={"nvidia.com/gpu": "8"}),
            _node("n3", taints=[_taint(TAINT_KEY, "a100")],
                  allocatable={"nvidia.com/gpu": "2"}, annotations={FORCE: "0"}),
        ]
        inventory = _run_inventory(monkeypatch, nodes)
        capacity = _run_snapshot(monkeypatch, nodes)
        assert capacity == {
            gpu_class: sum(per_node.values())
            for gpu_class, per_node in inventory.items()
        }
        assert capacity == {"h100": 10, "a100": 0}


# ---------------------------------------------------------------------------
# snapshot_tolerated_pods — node_name capture
# ---------------------------------------------------------------------------


class _FakeCoreV1Pods:
    def __init__(self, pods: list):
        self._pods = pods

    def list_pod_for_all_namespaces(self, label_selector=None):
        return SimpleNamespace(items=self._pods)


def _tolerated_pod(
    name: str, *, uid: str, node_name, gpu_class: str = "h100", gpu_count: int = 1,
    annotations: dict | None = None
) -> SimpleNamespace:
    return SimpleNamespace(
        metadata=SimpleNamespace(
            namespace="alice",
            name=name,
            uid=uid,
            labels={"gpu-class": gpu_class},
            annotations=annotations or {"galends/booking-reference": "res-1"},
            deletion_timestamp=None,
        ),
        status=SimpleNamespace(phase="Running", conditions=None),
        spec=SimpleNamespace(
            node_name=node_name,
            tolerations=[
                SimpleNamespace(key=TAINT_KEY, value=gpu_class, effect="NoSchedule")
            ],
            containers=[
                SimpleNamespace(
                    resources=SimpleNamespace(
                        requests={"nvidia.com/gpu": str(gpu_count)}
                    )
                )
            ],
        ),
    )


class TestSnapshotToleratedPodsNodeName:
    def test_captures_node_name_and_none_when_unscheduled(self, monkeypatch):
        pods = [
            _tolerated_pod("p1", uid="u1", node_name="n1"),
            _tolerated_pod("p2", uid="u2", node_name=None),  # scheduled nowhere yet
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1Pods(pods))
        out = asyncio.run(k8s_client.snapshot_tolerated_pods(TAINT_KEY))
        by_uid = {p.uid: p for p in out}
        assert by_uid["u1"].node_name == "n1"
        assert by_uid["u2"].node_name is None

    def test_captures_termination_warning_annotations(self, monkeypatch):
        pods = [
            _tolerated_pod(
                "p1", uid="u1", node_name="n1",
                annotations={
                    "galends/booking-reference": "res-1",
                    "galends/termination-warning-at": "2024-01-15T10:00:00Z",
                    "galends/termination-warning-risk": "0.50",
                },
            ),
            _tolerated_pod("p2", uid="u2", node_name="n1"),  # no warning annotations
        ]
        monkeypatch.setattr(k8s_client, "_core_v1", _FakeCoreV1Pods(pods))
        out = asyncio.run(k8s_client.snapshot_tolerated_pods(TAINT_KEY))
        by_uid = {p.uid: p for p in out}
        assert by_uid["u1"].termination_warning_at == "2024-01-15T10:00:00Z"
        assert by_uid["u1"].termination_warning_risk == "0.50"
        assert by_uid["u2"].termination_warning_at is None
        assert by_uid["u2"].termination_warning_risk is None
