"""Unit tests for the per-node GPU accounting helpers in controller.py.

Pure functions — no Kubernetes or HTTP.  Cover:

- ``free_gpus_by_node_class`` — per-(class, node) free GPUs = allocatable minus
  the GPUs of *bound, live, non-terminating* pods on that node.
- ``largest_node_free_by_class`` — the largest single-node opening per class, the
  number a multi-GPU admission feasibility check (guard 5) compares against.
- ``node_counts_by_class`` — how many schedulable nodes back each class, which
  guard 1 reads to tell "class is full" apart from "class has no nodes left".

The final class contrasts per-node reality with the class-global total, which is
the whole reason this accounting exists: a class can show free GPUs in aggregate
while no single node can host a multi-GPU pod.
"""

from __future__ import annotations

from app.controller import (
    PodRuntimeView,
    free_capacity_by_class,
    free_gpus_by_node_class,
    largest_node_free_by_class,
    node_counts_by_class,
)


def _view(
    uid: str,
    *,
    gpu_class: str = "h100",
    gpu_count: int = 1,
    node_name=None,
    node_resident: bool = True,
    terminating: bool = False,
) -> PodRuntimeView:
    return PodRuntimeView(
        uid=uid,
        namespace="alice",
        name=f"pod-{uid}",
        gpu_class=gpu_class,
        gpu_count=gpu_count,
        reservation_id=1,
        node_resident=node_resident,
        terminating=terminating,
        node_name=node_name,
    )


class TestFreeGpusByNodeClass:
    def test_subtracts_bound_live_pods_per_node(self):
        cap = {"h100": {"n1": 4, "n2": 4}}
        pods = [
            _view("a", gpu_count=3, node_name="n1"),
            _view("b", gpu_count=1, node_name="n2"),
        ]
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": 1, "n2": 3}}

    def test_unscheduled_pod_counts_against_no_node(self):
        cap = {"h100": {"n1": 4}}
        pods = [_view("a", gpu_count=2, node_name=None)]  # not yet scheduled
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": 4}}

    def test_terminating_pod_frees_its_gpus(self):
        cap = {"h100": {"n1": 4}}
        pods = [_view("a", gpu_count=2, node_name="n1", terminating=True)]
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": 4}}

    def test_non_resident_pod_ignored(self):
        cap = {"h100": {"n1": 4}}
        pods = [_view("a", gpu_count=2, node_name="n1", node_resident=False)]
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": 4}}

    def test_pod_on_node_absent_from_inventory_ignored(self):
        cap = {"h100": {"n1": 4}}
        pods = [_view("a", gpu_count=2, node_name="n-ghost")]
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": 4}}

    def test_pod_of_class_absent_from_inventory_ignored(self):
        cap = {"h100": {"n1": 4}}
        pods = [_view("a", gpu_class="a100", gpu_count=2, node_name="n1")]
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": 4}}

    def test_overcommit_goes_negative(self):
        cap = {"h100": {"n1": 2}}
        pods = [_view("a", gpu_count=3, node_name="n1")]
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": -1}}

    def test_multiple_pods_on_one_node_accumulate(self):
        cap = {"h100": {"n1": 8}}
        pods = [
            _view("a", gpu_count=2, node_name="n1"),
            _view("b", gpu_count=3, node_name="n1"),
        ]
        assert free_gpus_by_node_class(cap, pods) == {"h100": {"n1": 3}}

    def test_does_not_mutate_input(self):
        cap = {"h100": {"n1": 4}}
        free_gpus_by_node_class(cap, [_view("a", gpu_count=1, node_name="n1")])
        assert cap == {"h100": {"n1": 4}}  # input untouched


class TestLargestNodeFreeByClass:
    def test_returns_max_over_nodes(self):
        assert largest_node_free_by_class({"h100": {"n1": 1, "n2": 3}}) == {"h100": 3}

    def test_empty_class_is_zero(self):
        assert largest_node_free_by_class({"h100": {}}) == {"h100": 0}

    def test_all_consumed_is_zero(self):
        assert largest_node_free_by_class({"h100": {"n1": 0, "n2": 0}}) == {"h100": 0}

    def test_negative_nodes_report_max(self):
        # Over-committed everywhere: the "largest" is the least-negative node.
        assert largest_node_free_by_class({"h100": {"n1": -2, "n2": -1}}) == {"h100": -1}


class TestNodeCountsByClass:
    def test_counts_nodes_not_gpus(self):
        assert node_counts_by_class({"h100": {"n1": 8, "n2": 8}}) == {"h100": 2}

    def test_zero_gpu_node_still_counts(self):
        # A node carrying the class taint but reporting no allocatable GPUs is
        # still a node: the class is not drained, it is empty of capacity, and
        # those want different treatment from guard 1 vs guard 5.
        assert node_counts_by_class({"h100": {"n1": 0}}) == {"h100": 1}

    def test_drained_class_is_zero(self):
        assert node_counts_by_class({"h100": {}}) == {"h100": 0}

    def test_empty_inventory(self):
        assert node_counts_by_class({}) == {}

    def test_a_known_class_missing_from_the_inventory_is_zero(self):
        # The real snapshot never produces {"a100": {}}: a class whose nodes are
        # all cordoned or gone is simply absent.  Without the known classes that
        # absence read as "no data", and guard 1b could never hold anything.
        assert node_counts_by_class({"h100": {"n1": 8}}, ["h100", "a100"]) == {
            "h100": 1, "a100": 0,
        }

    def test_the_inventory_wins_over_the_zero(self):
        assert node_counts_by_class({"h100": {"n1": 8, "n2": 8}}, ["h100"]) == {"h100": 2}

    def test_an_unknown_label_stays_absent(self):
        # A class the app does not know is not vouched for either way, so guard
        # 1b keeps failing open on it.
        assert node_counts_by_class({}, []) == {}
        assert "xtra" not in node_counts_by_class({"h100": {"n1": 8}}, ["h100"])


class TestFragmentationVsClassTotal:
    """Per-node reality can be strictly smaller than the class-global total."""

    def test_fragmented_free_below_class_total(self):
        # Two 4-GPU nodes, one 3-GPU pod on each → 1 free per node, 2 free total.
        cap_by_node = {"h100": {"n1": 4, "n2": 4}}
        cap_by_class = {"h100": 8}
        pods = [
            _view("a", gpu_count=3, node_name="n1"),
            _view("b", gpu_count=3, node_name="n2"),
        ]
        # The class-global free says 2 GPUs are available...
        assert free_capacity_by_class(cap_by_class, pods) == {"h100": 2}
        # ...but no single node has more than 1 free, so a 2-GPU pod cannot fit.
        largest = largest_node_free_by_class(
            free_gpus_by_node_class(cap_by_node, pods)
        )
        assert largest == {"h100": 1}
