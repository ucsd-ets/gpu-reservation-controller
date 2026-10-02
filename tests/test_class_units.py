"""Classes whose unit is not one ``nvidia.com/gpu``: AMD GPUs, and memory blocks.

A class may define what one unit is (RESERVATION-API.md, "What one unit of a
class is").  Everything downstream of the conversion already counted integers,
so what is tested here is the conversion itself and every place a hard-coded
``nvidia.com/gpu`` used to stand:

- pods and nodes are counted in the same units, so free capacity means
  something -- including the half-done case the change had to avoid, where a
  class's pods were counted in its unit and its nodes in ``nvidia.com/gpu`` and
  every booking boundary looked like a shortfall;
- guard 1a treats a shortage of what the class counts as the class's business;
- a pod is only told it requests nothing once its class's unit is known;
- a memory class refuses a pod whose memory limit is not its request;
- a node's units come after what pods outside the reservation system request.

Nothing here touches a real cluster or the app.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timedelta
from decimal import Decimal
from types import SimpleNamespace

import pytest

from app import k8s_client
from app.controller import ControllerState, PodRuntimeView, QueueEntry, free_capacity_by_class
from app.k8s_client import (
    MEMORY_LIMIT_MISMATCH_REASON,
    NO_GPU_REQUEST_REASON,
    UNKNOWN_GPU_CLASS_REASON,
    get_pod_effective_requests,
    get_pod_gpu_count,
    get_pod_memory_limit_problem,
    is_gpu_gated_pending,
)
from app.resources import (
    DEFAULT_CLASS_RESOURCES,
    class_resources,
    is_native_resource,
    node_units,
    plural,
    pod_units,
    to_quantity,
)
from app.schemas import GpuClassDetail

from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, GROUP_NAME, USERNAME, reservation
from tests.test_pod_problem_events import (
    MIN_RUNTIME,
    NOW,
    USAGE_GROUP,
    _config,
    _main_module,
    _state,
    _watch,
)

TAINT_KEY = "gpu-class-reservation"
GI = 2**30
AMD = class_resources({"amd.com/gpu": "1"})
BLOCK = class_resources({"memory": "16Gi", "cpu": "2"}, "block")


def _q(text) -> Decimal:
    value = to_quantity(text)
    assert value is not None, text
    return value


# ---------------------------------------------------------------------------
# resources: the unit arithmetic
# ---------------------------------------------------------------------------


class TestQuantities:
    @pytest.mark.parametrize("text, expected", [
        ("16Gi", 16 * GI),
        ("2113498716Ki", 2113498716 * 1024),
        ("126500m", Decimal("126.5")),
        ("1E", Decimal(10**18)),      # exa, not an exponent
        ("1e3", Decimal(1000)),
        (".5", Decimal("0.5")),
        (4, Decimal(4)),
    ])
    def test_parses_what_kubernetes_writes(self, text, expected):
        assert to_quantity(text) == expected

    @pytest.mark.parametrize("text", ["", "Gi", "16GB", "lots", None, True])
    def test_junk_is_none(self, text):
        assert to_quantity(text) is None

    def test_native_resources_are_the_ones_kubernetes_defines(self):
        assert is_native_resource("memory") and is_native_resource("cpu")
        assert is_native_resource("hugepages-1Gi")
        assert not is_native_resource("nvidia.com/gpu")
        assert not is_native_resource("amd.com/gpu")


class TestClassResources:
    def test_none_is_the_nvidia_default(self):
        assert class_resources(None).is_default
        assert class_resources({}, "card").unit_name == "card"
        assert class_resources(None) == DEFAULT_CLASS_RESOURCES

    def test_an_amd_class_counts_its_own_gpus(self):
        assert AMD.names == ("amd.com/gpu",)
        assert not AMD.is_default and not AMD.native

    def test_a_memory_block_lists_both_and_says_so(self):
        assert BLOCK.names == ("memory", "cpu")
        assert BLOCK.native == ("memory", "cpu")
        assert BLOCK.counts_memory
        assert BLOCK.describe() == "memory 16Gi + cpu 2"
        assert BLOCK.amount(1) == "1 block" and BLOCK.amount(3) == "3 blocks"

    @pytest.mark.parametrize("bad", [{"memory": "0"}, {"memory": "-1Gi"}, {"memory": "lots"}])
    def test_an_unusable_quantity_raises(self, bad):
        with pytest.raises(ValueError):
            class_resources(bad)

    def test_plural(self):
        assert plural("GPU", 2) == "GPUs" and plural("box", 2) == "boxes"
        assert plural("block", 1) == "block"


class TestUnits:
    def test_a_pod_needs_its_largest_share_rounded_up(self):
        # 40 GiB is 2.5 blocks of memory; 2 cores is 1 block of cpu.
        assert pod_units({"memory": _q("40Gi"), "cpu": _q("2")}, BLOCK) == 3
        # A CPU-heavy pod is charged for the memory it locks out.
        assert pod_units({"memory": _q("16Gi"), "cpu": _q("13")}, BLOCK) == 7

    def test_a_pod_requesting_none_of_it_needs_nothing(self):
        assert pod_units({"cpu": _q("4")}, DEFAULT_CLASS_RESOURCES) == 0
        assert pod_units({}, BLOCK) == 0

    def test_a_node_offers_its_scarcest_share_rounded_down(self):
        alloc = {"memory": _q("2113498716Ki"), "cpu": _q("127")}   # ~2015 GiB
        assert node_units(alloc, BLOCK) == 63                     # cpu-bound
        used = {"memory": _q("3Gi"), "cpu": _q("1500m")}
        assert node_units(alloc, BLOCK, used) == 62

    def test_a_node_missing_a_listed_resource_offers_nothing(self):
        assert node_units({"memory": _q("2Ti")}, BLOCK) == 0

    def test_a_unit_is_atomic_in_every_resource(self):
        """Pods whose units fit a node fit it in memory and cpu alike."""
        alloc = {"memory": _q("256Gi"), "cpu": _q("32")}
        pods = [
            {"memory": _q("100Gi"), "cpu": _q("2")},   # 7 units
            {"memory": _q("8Gi"), "cpu": _q("10")},    # 5 units
            {"memory": _q("64Gi"), "cpu": _q("8")},    # 4 units
        ]
        needed = sum(pod_units(p, BLOCK) for p in pods)
        assert needed <= node_units(alloc, BLOCK) == 16
        for name in ("memory", "cpu"):
            assert sum(p[name] for p in pods) <= alloc[name]


# ---------------------------------------------------------------------------
# k8s_client: what a pod requests, and what a memory class requires of it
# ---------------------------------------------------------------------------


def _container(name="main", *, requests=None, limits=None, restart_policy=None):
    return SimpleNamespace(
        name=name, restart_policy=restart_policy,
        resources=SimpleNamespace(requests=requests, limits=limits),
    )


def _pod_spec(*containers, init=(), overhead=None, pod_resources=None):
    return SimpleNamespace(spec=SimpleNamespace(
        containers=list(containers), init_containers=list(init),
        overhead=overhead, resources=pod_resources,
    ))


class TestEffectiveRequests:
    def test_containers_sum(self):
        pod = _pod_spec(
            _container(requests={"memory": "8Gi", "amd.com/gpu": "1"}),
            _container("side", requests={"memory": "1Gi"}),
        )
        assert get_pod_effective_requests(pod) == {
            "memory": 9 * GI, "amd.com/gpu": Decimal(1),
        }

    def test_an_init_container_counts_only_when_it_is_the_larger(self):
        pod = _pod_spec(
            _container(requests={"memory": "4Gi"}),
            init=[_container("setup", requests={"memory": "32Gi"})],
        )
        assert get_pod_effective_requests(pod)["memory"] == 32 * GI

    def test_a_sidecar_adds_to_the_running_total(self):
        pod = _pod_spec(
            _container(requests={"memory": "4Gi"}),
            init=[_container("proxy", requests={"memory": "1Gi"}, restart_policy="Always")],
        )
        assert get_pod_effective_requests(pod)["memory"] == 5 * GI

    def test_runtime_class_overhead_is_added(self):
        pod = _pod_spec(_container(requests={"memory": "4Gi"}), overhead={"memory": "1Gi"})
        assert get_pod_effective_requests(pod)["memory"] == 5 * GI

    def test_pod_level_resources_replace_the_container_sum(self):
        pod = _pod_spec(
            _container(requests={"memory": "4Gi"}),
            pod_resources=SimpleNamespace(requests={"memory": "64Gi"}, limits=None),
        )
        assert get_pod_effective_requests(pod)["memory"] == 64 * GI

    def test_a_limit_stands_in_for_an_absent_request(self):
        pod = _pod_spec(_container(limits={"amd.com/gpu": "2"}))
        assert get_pod_gpu_count(pod, AMD) == 2

    def test_the_default_still_counts_nvidia_gpus(self):
        pod = _pod_spec(_container(requests={"nvidia.com/gpu": "2", "memory": "500Gi"}))
        assert get_pod_gpu_count(pod) == 2
        assert get_pod_gpu_count(pod, AMD) == 0
        assert get_pod_gpu_count(pod, BLOCK) == 32   # memory-bound


class TestMemoryLimitProblem:
    def test_limit_equal_to_request_is_fine(self):
        pod = _pod_spec(_container(requests={"memory": "64Gi"}, limits={"memory": "64Gi"}))
        assert get_pod_memory_limit_problem(pod) is None

    def test_a_limit_alone_is_fine(self):
        # The API server defaults the request to the limit.
        pod = _pod_spec(_container(limits={"memory": "64Gi"}))
        assert get_pod_memory_limit_problem(pod) is None

    def test_no_limit_is_named(self):
        pod = _pod_spec(_container("nb", requests={"memory": "64Gi"}))
        assert get_pod_memory_limit_problem(pod) == ("nb", "no_limit")

    def test_a_limit_above_the_request_is_named(self):
        pod = _pod_spec(_container("nb", requests={"memory": "64Gi"}, limits={"memory": "1Ti"}))
        assert get_pod_memory_limit_problem(pod) == ("nb", "limit_not_request")

    def test_a_sidecar_is_checked_but_a_plain_init_container_is_not(self):
        main = _container(requests={"memory": "8Gi"}, limits={"memory": "8Gi"})
        setup = _container("setup")                    # runs to completion first
        assert get_pod_memory_limit_problem(_pod_spec(main, init=[setup])) is None
        proxy = _container("proxy", restart_policy="Always")
        assert get_pod_memory_limit_problem(_pod_spec(main, init=[proxy])) == (
            "proxy", "no_limit",
        )

    def test_a_pod_level_limit_bounds_the_whole_pod(self):
        pod = _pod_spec(
            _container("nb"),
            pod_resources=SimpleNamespace(requests=None, limits={"memory": "64Gi"}),
        )
        assert get_pod_memory_limit_problem(pod) is None


# ---------------------------------------------------------------------------
# Guard 1a: a shortage of what the class counts is the class's business
# ---------------------------------------------------------------------------


def _unschedulable(message):
    cond = SimpleNamespace(
        type="PodScheduled", status="False", reason="Unschedulable", message=message,
    )
    return SimpleNamespace(status=SimpleNamespace(phase="Pending", conditions=[cond]))


class TestGuard1a:
    AMD_MSG = ("0/30 nodes are available: 5 node(s) had untolerated taint(s), "
               "25 Insufficient amd.com/gpu.")
    BIGMEM_MSG = ("0/30 nodes are available: 2 node(s) had untolerated taint(s), "
                  "20 Insufficient memory, 8 Insufficient cpu.")

    def test_an_amd_shortage_was_dropped_by_the_nvidia_reading(self):
        assert is_gpu_gated_pending(_unschedulable(self.AMD_MSG), TAINT_KEY) is False

    def test_an_amd_class_proceeds(self):
        assert is_gpu_gated_pending(_unschedulable(self.AMD_MSG), TAINT_KEY, AMD.names) is True

    def test_a_memory_class_proceeds_on_memory_and_cpu(self):
        pod = _unschedulable(self.BIGMEM_MSG)
        assert is_gpu_gated_pending(pod, TAINT_KEY, BLOCK.names) is True

    def test_a_shortage_the_class_does_not_count_still_drops(self):
        pod = _unschedulable(self.BIGMEM_MSG + " 3 Insufficient ephemeral-storage.")
        assert is_gpu_gated_pending(pod, TAINT_KEY, BLOCK.names) is False


# ---------------------------------------------------------------------------
# The node inventory: units per class, after what other pods request
# ---------------------------------------------------------------------------


def _node(name, label, allocatable, annotations=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(name=name, deletion_timestamp=None,
                                 annotations=annotations, labels={}),
        spec=SimpleNamespace(
            taints=[SimpleNamespace(key=TAINT_KEY, value=label, effect="NoSchedule")],
            unschedulable=False,
        ),
        status=SimpleNamespace(allocatable=allocatable, conditions=None),
    )


def _cluster_pod(name, requests, *, admitted=False, phase="Running", deleting=False):
    tolerations = [SimpleNamespace(key=TAINT_KEY)] if admitted else [
        SimpleNamespace(key=None)            # a DaemonSet's blanket toleration
    ]
    return SimpleNamespace(
        metadata=SimpleNamespace(
            name=name, labels={"gpu-class": "bigmem"} if admitted else {},
            deletion_timestamp="2024-01-01T00:00:00Z" if deleting else None,
        ),
        status=SimpleNamespace(phase=phase),
        spec=SimpleNamespace(
            tolerations=tolerations,
            containers=[_container(requests=requests)], init_containers=None,
            overhead=None, resources=None,
        ),
    )


class _FakeCoreV1:
    def __init__(self, nodes, pods_by_node=None):
        self._nodes = nodes
        self._pods = pods_by_node or {}
        self.pod_lists: list[str] = []

    def list_node(self):
        return SimpleNamespace(items=self._nodes)

    def list_pod_for_all_namespaces(self, field_selector=None, **_kw):
        node = field_selector.split("=", 1)[1]
        self.pod_lists.append(node)
        return SimpleNamespace(items=self._pods.get(node, []))


def _inventory(monkeypatch, fake, specs):
    monkeypatch.setattr(k8s_client, "_core_v1", fake)
    return asyncio.run(k8s_client.snapshot_node_gpu_inventory(TAINT_KEY, specs))


BIGMEM_ALLOC = {"memory": "2113498716Ki", "cpu": "128", "pods": "110"}


class TestInventory:
    def test_an_amd_node_is_counted_in_amd_gpus(self, monkeypatch):
        fake = _FakeCoreV1([
            _node("amd-1", "mi300x", {"amd.com/gpu": "8"}),
            _node("nv-1", "h100", {"nvidia.com/gpu": "4"}),
        ])
        assert _inventory(monkeypatch, fake, {"mi300x": AMD}) == {
            "mi300x": {"amd-1": 8}, "h100": {"nv-1": 4},
        }
        assert fake.pod_lists == []   # vendor resources: no other pod holds them

    def test_without_the_unit_an_amd_node_reads_as_no_gpus(self, monkeypatch):
        fake = _FakeCoreV1([_node("amd-1", "mi300x", {"amd.com/gpu": "8"})])
        assert _inventory(monkeypatch, fake, {}) == {"mi300x": {"amd-1": 0}}

    def test_a_memory_node_subtracts_what_other_pods_request(self, monkeypatch):
        pods = {"big-1": [
            _cluster_pod("node-exporter", {"memory": "512Mi", "cpu": "250m"}),
            _cluster_pod("cni", {"memory": "1Gi", "cpu": "1"}),
            # Counted by the planners, not here.
            _cluster_pod("job", {"memory": "512Gi", "cpu": "16"}, admitted=True),
            # Finished, and being torn down: no longer holding anything.
            _cluster_pod("old", {"memory": "64Gi", "cpu": "8"}, phase="Succeeded"),
            _cluster_pod("leaving", {"memory": "64Gi", "cpu": "8"}, deleting=True),
        ]}
        fake = _FakeCoreV1([_node("big-1", "bigmem", BIGMEM_ALLOC)], pods)
        # cpu: (128 - 1.25) / 2 = 63.4 -> 63; memory: (~2015.6 - 1.5) / 16 -> 125.
        assert _inventory(monkeypatch, fake, {"bigmem": BLOCK}) == {"bigmem": {"big-1": 63}}
        assert fake.pod_lists == ["big-1"]

    def test_a_forced_capacity_skips_the_subtraction(self, monkeypatch):
        node = _node("big-1", "bigmem", BIGMEM_ALLOC,
                     annotations={"galends/force-node-capacity": "60"})
        fake = _FakeCoreV1([node], {"big-1": [_cluster_pod("cni", {"cpu": "100"})]})
        assert _inventory(monkeypatch, fake, {"bigmem": BLOCK}) == {"bigmem": {"big-1": 60}}
        assert fake.pod_lists == []

    def test_gpu_classes_never_list_pods(self, monkeypatch):
        fake = _FakeCoreV1([_node("nv-1", "h100", {"nvidia.com/gpu": "4"})])
        _inventory(monkeypatch, fake, {"h100": DEFAULT_CLASS_RESOURCES})
        assert fake.pod_lists == []


class TestPodsAndNodesInTheSameUnits:
    """The hazard a half-done change would have shipped.

    Counting an AMD class's pods in ``amd.com/gpu`` but its nodes in
    ``nvidia.com/gpu`` reads the class as zero capacity with pods using it --
    negative free capacity, which the preemption sweep reads as a shortfall at
    every booking boundary and kills overstayers to cover.
    """

    @staticmethod
    def _view(gpu_count, node="amd-1"):
        return PodRuntimeView(
            uid="u", namespace="ns", name="p", gpu_class="mi300x", gpu_count=gpu_count,
            reservation_id=1, node_resident=True, terminating=False, node_name=node,
        )

    def test_consistent_units_give_real_free_capacity(self, monkeypatch):
        fake = _FakeCoreV1([_node("amd-1", "mi300x", {"amd.com/gpu": "8"})])
        inventory = _inventory(monkeypatch, fake, {"mi300x": AMD})
        pod = _pod_spec(_container(requests={"amd.com/gpu": "2"}))
        view = self._view(get_pod_gpu_count(pod, AMD))
        capacity = {c: sum(n.values()) for c, n in inventory.items()}
        assert free_capacity_by_class(capacity, [view]) == {"mi300x": 6}

    def test_mixed_units_are_what_goes_negative(self, monkeypatch):
        fake = _FakeCoreV1([_node("amd-1", "mi300x", {"amd.com/gpu": "8"})])
        inventory = _inventory(monkeypatch, fake, {})          # nodes: nvidia
        pod = _pod_spec(_container(requests={"amd.com/gpu": "2"}))
        view = self._view(get_pod_gpu_count(pod, AMD))          # pods: amd
        capacity = {c: sum(n.values()) for c, n in inventory.items()}
        assert free_capacity_by_class(capacity, [view]) == {"mi300x": -2}

    def test_the_tolerated_snapshot_counts_each_pod_in_its_class_unit(self, monkeypatch):
        pod = SimpleNamespace(
            metadata=SimpleNamespace(
                name="p", namespace="ns", uid="u", labels={"gpu-class": "mi300x"},
                annotations={"galends/booking-reference": "res-1"},
                deletion_timestamp=None,
            ),
            status=SimpleNamespace(phase="Running", conditions=None),
            spec=SimpleNamespace(
                tolerations=[SimpleNamespace(key=TAINT_KEY)], node_name="amd-1",
                containers=[_container(requests={"amd.com/gpu": "2"})],
                init_containers=None, overhead=None, resources=None,
                node_selector=None, affinity=None,
            ),
        )

        class _Fake:
            def list_pod_for_all_namespaces(self, **_kw):
                return SimpleNamespace(items=[pod])

        monkeypatch.setattr(k8s_client, "_core_v1", _Fake())
        [info] = asyncio.run(k8s_client.snapshot_tolerated_pods(
            TAINT_KEY, class_resources={"mi300x": AMD}
        ))
        assert info.gpu_count == 2
        [info] = asyncio.run(k8s_client.snapshot_tolerated_pods(TAINT_KEY))
        assert info.gpu_count == 0


# ---------------------------------------------------------------------------
# ControllerState: which unit a label is counted in
# ---------------------------------------------------------------------------


class TestStateLookup:
    def test_a_listed_label_without_a_unit_record_is_the_default(self):
        state = ControllerState()
        state.gpu_class_ids = {"h100": 1}
        assert state.class_resources("h100") == DEFAULT_CLASS_RESOURCES

    def test_an_unlisted_or_unparseable_label_is_unknown_but_counted_as_nvidia(self):
        state = ControllerState()
        state.gpu_class_ids = {"bigmem": 2}
        state.gpu_class_resources = {"bigmem": None}
        assert state.class_resources("bigmem") is None
        assert state.class_resources("nope") is None
        assert state.resources_for("bigmem") == DEFAULT_CLASS_RESOURCES
        assert state.counting_resources() == {}

    def test_a_reroute_takes_the_count_afresh(self):
        """A redefined unit reaches pods already queued at their next resync."""
        state = ControllerState()
        state.add_ondemand_candidate("u", "p", "ns", "bigmem", 4, 600, NOW)
        state.add_ondemand_candidate("u", "p", "ns", "bigmem", 2, 600, NOW)
        assert state.ondemand_candidates["u"].gpu_requested == 2


class TestClassMapResolution:
    def test_the_class_list_carries_each_unit_and_flags_a_bad_one(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)

        class _Client:
            async def fetch_gpu_classes(self):
                return [
                    GpuClassDetail(id=1, name="H100", label_value="h100"),
                    GpuClassDetail(id=2, name="MI300X", label_value="mi300x",
                                   k8s_resources={"amd.com/gpu": "1"}),
                    GpuClassDetail(id=3, name="Bigmem", label_value="bigmem",
                                   k8s_resources={"memory": "16Gi", "cpu": 2},
                                   unit_name="block"),
                    GpuClassDetail(id=4, name="Broken", label_value="broken",
                                   k8s_resources={"memory": "lots"}),
                ]

            async def fetch_gpu_class(self, cid):  # pragma: no cover
                return None

        with caplog.at_level(logging.WARNING, logger="app.main"):
            maps = asyncio.run(m._resolve_gpu_class_maps(
                _Client(), set(), m.GpuClassMaps({}, {}, {})
            ))
        assert maps.resources["h100"] == DEFAULT_CLASS_RESOURCES
        assert maps.resources["mi300x"] == AMD
        assert maps.resources["bigmem"] == BLOCK
        assert maps.resources["broken"] is None
        assert maps.ids["broken"] == 4          # still a class the app knows
        [line] = [r.getMessage() for r in caplog.records
                  if "event=class.unresolvable" in r.getMessage()]
        assert "reason=invalid_unit" in line and "clabel=broken" in line


# ---------------------------------------------------------------------------
# The watch loop: routing a pod of a class with its own unit
# ---------------------------------------------------------------------------


def _watch_pod(uid="uid-1", *, gpu_class="mi300x", requests=None, limits=None,
               annotations=None, phase="Pending"):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name=f"pod-{uid}", namespace=USERNAME,
            labels={"gpu-class": gpu_class},
            annotations=annotations if annotations is not None else {
                USAGE_GROUP: GROUP_NAME, MIN_RUNTIME: "3600",
            },
            creation_timestamp=NOW, deletion_timestamp=None,
        ),
        status=SimpleNamespace(phase=phase, conditions=None),
        spec=SimpleNamespace(
            tolerations=[], scheduling_gates=None,
            containers=[_container("nb", requests=requests, limits=limits)],
        ),
    )


def _unit_state(**units):
    """The test state's two GPU classes, plus *units* (label → spec) as more."""
    state = _state()
    for i, (label, spec) in enumerate(units.items(), start=50):
        state.gpu_class_labels[i] = label
        state.gpu_class_ids[label] = i
        state.gpu_class_resources[label] = spec
    return state


class TestRouting:
    def test_an_amd_pod_goes_on_demand_for_its_amd_gpus(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _watch_pod(requests={"amd.com/gpu": "2"})
        rec, state, batches = _watch(
            monkeypatch, m, _config(), [("ADDED", pod)], state=_unit_state(mi300x=AMD),
        )
        assert state.ondemand_candidates["uid-1"].gpu_requested == 2
        assert rec.calls == [] and batches == [0]

    def test_an_amd_pod_is_not_blamed_before_its_class_unit_is_known(self, monkeypatch):
        """Counted in nvidia.com/gpu, an AMD pod reads as requesting nothing --
        which is the controller's gap, not the pod's fault."""
        m = _main_module(monkeypatch)
        state = _unit_state(mi300x=AMD)
        state.gpu_class_resources["mi300x"] = None        # its unit did not parse
        pod = _watch_pod(requests={"amd.com/gpu": "2"})
        rec, state, batches = _watch(monkeypatch, m, _config(), [("ADDED", pod)], state=state)
        assert rec.calls == []
        assert state.ondemand_candidates == {} and batches == []

    def test_an_unlisted_class_is_still_told_it_is_unknown(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _watch_pod(gpu_class="mi300", requests={"amd.com/gpu": "2"})
        rec, _state_, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)])
        assert rec.reasons == [UNKNOWN_GPU_CLASS_REASON]

    def test_a_memory_pod_is_counted_in_blocks(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _watch_pod(gpu_class="bigmem",
                         requests={"memory": "40Gi", "cpu": "2"},
                         limits={"memory": "40Gi"})
        rec, state, _b = _watch(
            monkeypatch, m, _config(), [("ADDED", pod)], state=_unit_state(bigmem=BLOCK),
        )
        assert state.ondemand_candidates["uid-1"].gpu_requested == 3
        assert rec.calls == []

    def test_a_memory_pod_without_a_matching_limit_is_refused(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _watch_pod(gpu_class="bigmem",
                         requests={"memory": "64Gi", "cpu": "2"},
                         limits={"memory": "1Ti"})
        rec, state, batches = _watch(
            monkeypatch, m, _config(), [("ADDED", pod)], state=_unit_state(bigmem=BLOCK),
        )
        assert state.ondemand_candidates == {} and state.task_queue == {}
        assert batches == []
        [call] = rec.calls
        assert call["reason"] == MEMORY_LIMIT_MISMATCH_REASON
        assert call["gpu_count"] == 4
        message = call["message"]
        assert "blocks of memory 16Gi + cpu 2" in message
        assert "container nb sets a memory limit different from its memory request" in message
        assert "resources.limits.memory equal to resources.requests.memory" in message

    def test_the_memory_rule_waits_for_a_booking_too(self, monkeypatch):
        """A booking does not exempt the pod: what it reserves still bounds nothing."""
        m = _main_module(monkeypatch)
        state = _unit_state(bigmem=BLOCK)
        state.reservations = [reservation(
            1, start_utc=NOW - timedelta(minutes=5), end_utc=NOW + timedelta(hours=2),
            gpu_class_id=state.gpu_class_ids["bigmem"], gpu_count=8,
        )]
        pod = _watch_pod(gpu_class="bigmem", requests={"memory": "64Gi"})
        rec, state, _b = _watch(monkeypatch, m, _config(), [("ADDED", pod)], state=state)
        assert state.task_queue == {}
        assert rec.reasons == [MEMORY_LIMIT_MISMATCH_REASON]

    def test_a_memory_pod_requesting_none_of_it_names_what_to_set(self, monkeypatch):
        m = _main_module(monkeypatch)
        pod = _watch_pod(gpu_class="bigmem", requests={"nvidia.com/gpu": "1"})
        rec, _s, _b = _watch(
            monkeypatch, m, _config(), [("ADDED", pod)], state=_unit_state(bigmem=BLOCK),
        )
        [call] = rec.calls
        assert call["reason"] == NO_GPU_REQUEST_REASON
        assert "Set resources.limits memory or cpu" in call["message"]
        assert "nvidia.com/gpu" not in call["message"]


# ---------------------------------------------------------------------------
# What the pod's owner and the operator read
# ---------------------------------------------------------------------------


class TestWording:
    def _entry(self, gpu_requested, gpu_count):
        r = reservation(1, start_utc=NOW, end_utc=NOW + timedelta(hours=2),
                        gpu_class_id=GPU_CLASS_ID, gpu_count=gpu_count)
        return QueueEntry(
            pod_uid="u", pod_name="p", pod_namespace=USERNAME,
            gpu_class_label="bigmem", gpu_requested=gpu_requested, reservation=r,
            next_attempt_at=NOW,
        )

    def test_reservation_too_small_names_the_class_resources(self, monkeypatch):
        m = _main_module(monkeypatch)
        message = m._reservation_too_small_message(self._entry(5, 4), BLOCK)
        assert "requests 5 blocks" in message
        assert "lower the pod's memory and cpu request" in message
        assert "nvidia.com/gpu" not in message

    def test_the_default_wording_is_unchanged(self, monkeypatch):
        m = _main_module(monkeypatch)
        message = m._reservation_too_small_message(self._entry(2, 1))
        assert "requests 2 GPUs" in message
        assert "lower the pod's nvidia.com/gpu request" in message

    def test_the_guard_4_detail_suspects_system_pods_on_a_memory_class(self, monkeypatch):
        m = _main_module(monkeypatch)
        from app.controller import OnDemandGate
        gate = OnDemandGate(label="bigmem", guard=4, reason="class_overcommitted",
                            since=NOW, waiting=1, app_gpus=128, phys_gpus=120)
        detail = m._ondemand_gate_detail(gate, _config(), BLOCK)
        assert "has 128 blocks today" in detail
        assert "allocatable memory and cpu in blocks of memory 16Gi + cpu 2" in detail
        assert "DaemonSets" in detail
        assert "NVIDIA" not in detail
