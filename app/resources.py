"""What one unit of a GPU class is, and how pods and nodes are counted in units.

Every count the reservation app keeps for a class -- its total, a reservation's
``gpu_count``, every ceiling -- is a whole number of **units**.  For an ordinary
class a unit is one ``nvidia.com/gpu``.  A class may instead define its unit as
any bundle of Kubernetes resources (RESERVATION-API.md, "What one unit of a class
is"): one ``amd.com/gpu``, or a ``{"memory": "16Gi", "cpu": "2"}`` block of a
large-memory node.  This module is the one place the controller converts between
Kubernetes quantities and units, so the rest of it -- budgets, occupancy, free
capacity, preemption, the guards -- keeps counting integers exactly as before.

The conversion is deliberately lopsided, which is what makes a unit atomic:

* a **pod** needs, per listed resource, its request divided by the unit's
  quantity rounded **up**, and its units are the **largest** of those;
* a **node** offers, per listed resource, what it can allocate divided by the
  unit's quantity rounded **down**, and its units are the **smallest** of those.

So if the units of the pods on a node fit the node's units, the pods fit the node
in *every* listed resource -- the same reasoning as Slurm's ``MAX_TRES`` billing.

Pure, and free of the ``kubernetes`` package like ``controller.py``, which
imports it: ``k8s_client`` digests pods and nodes into the plain quantity maps
these functions take.
"""

from __future__ import annotations

import math
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Mapping, Optional

#: The resource an ordinary class counts, one per unit.
DEFAULT_RESOURCE = "nvidia.com/gpu"
DEFAULT_UNIT_NAME = "GPU"

# Resource names Kubernetes itself defines.  Every pod on a node may request
# them, including pods this controller never admitted (DaemonSets, static pods),
# so a node's free share of them is only what those pods leave over.  A vendor
# resource like nvidia.com/gpu is exclusive to the pods that request it, and on a
# reservation-tainted node only admitted pods do.
_NATIVE = frozenset({"cpu", "memory", "ephemeral-storage"})


def is_native_resource(name: str) -> bool:
    """True for a resource Kubernetes defines (cpu, memory, hugepages-*, ...)."""
    if "/" not in name:
        return name in _NATIVE or name.startswith("hugepages-")
    return name.split("/", 1)[0].endswith("kubernetes.io")


# Kubernetes' quantity grammar: a decimal number, then a binary-SI suffix, a
# decimal-SI suffix or a decimal exponent.  ``1E`` is an exa, ``1E3`` an exponent.
_QUANTITY = re.compile(
    r"^(?P<num>[+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))"
    r"(?P<suffix>Ki|Mi|Gi|Ti|Pi|Ei|m|k|M|G|T|P|E|[eE][+-]?[0-9]+)?$"
)
_BINARY = {"Ki": 2**10, "Mi": 2**20, "Gi": 2**30, "Ti": 2**40, "Pi": 2**50, "Ei": 2**60}
_DECIMAL = {"m": Decimal("0.001"), "k": 10**3, "M": 10**6, "G": 10**9,
            "T": 10**12, "P": 10**15, "E": 10**18}


def to_quantity(raw: object) -> Optional[Decimal]:
    """Parse one Kubernetes quantity (``"16Gi"``, ``"500m"``, ``2``), or ``None``."""
    if raw is None or isinstance(raw, bool):
        return None
    if isinstance(raw, (int, float, Decimal)):
        try:
            return Decimal(str(raw))
        except InvalidOperation:
            return None
    m = _QUANTITY.match(str(raw).strip())
    if not m:
        return None
    value = Decimal(m.group("num"))
    suffix = m.group("suffix") or ""
    if suffix in _BINARY:
        return value * _BINARY[suffix]
    if suffix in _DECIMAL:
        return value * _DECIMAL[suffix]
    if suffix:
        return value.scaleb(int(suffix[1:]))
    return value


@dataclass(frozen=True)
class ClassResources:
    """One unit of a class: ``(resource, quantity per unit)`` pairs, and its name."""

    units: tuple[tuple[str, Decimal], ...]
    unit_name: str = DEFAULT_UNIT_NAME

    @property
    def names(self) -> tuple[str, ...]:
        """The resources a unit is made of, in the order the class lists them."""
        return tuple(name for name, _ in self.units)

    @property
    def is_default(self) -> bool:
        """One ``nvidia.com/gpu`` -- a class that predates units, and still most."""
        return self.units == ((DEFAULT_RESOURCE, Decimal(1)),)

    @property
    def native(self) -> tuple[str, ...]:
        """The listed resources that pods outside the reservation system consume too."""
        return tuple(name for name in self.names if is_native_resource(name))

    @property
    def counts_memory(self) -> bool:
        return "memory" in self.names

    def describe(self) -> str:
        """``memory 16Gi + cpu 2`` -- one unit, for an Event or an operator line."""
        return " + ".join(f"{name} {_fmt(name, qty)}" for name, qty in self.units)

    def amount(self, count: int) -> str:
        """``1 GPU``, ``3 blocks``: *count* units in the class's own word."""
        return f"{count} {plural(self.unit_name, count)}"


_BYTE_SUFFIXES = (
    ("Ti", 2**40), ("Gi", 2**30), ("Mi", 2**20),
    ("T", 10**12), ("G", 10**9), ("M", 10**6), ("k", 10**3), ("Ki", 2**10),
)


def _fmt(name: str, qty: Decimal) -> str:
    """A quantity as a person would write it back: ``16Gi``, not ``17179869184``.

    Only a byte-valued resource gets a suffix -- ``cpu 2`` must not become ``2``
    of something else, and a vendor resource is a plain count.
    """
    if name in ("memory", "ephemeral-storage") or name.startswith("hugepages-"):
        for suffix, factor in _BYTE_SUFFIXES:
            if qty >= factor and qty % factor == 0:
                return f"{qty // factor}{suffix}"
    return format(qty.normalize(), "f")


def plural(word: str, count: int) -> str:
    """English plural for a unit name: ``GPU`` -> ``GPUs``, ``box`` -> ``boxes``."""
    if count == 1 or not word:
        return word
    lower = word.lower()
    if lower.endswith(("s", "x", "z", "ch", "sh")):
        return word + "es"
    return word + "s"


DEFAULT_CLASS_RESOURCES = ClassResources(((DEFAULT_RESOURCE, Decimal(1)),))


def class_resources(
    k8s_resources: Optional[Mapping[str, object]], unit_name: Optional[str] = None,
) -> ClassResources:
    """Build a class's unit from the app's ``k8s_resources`` / ``unit_name``.

    ``None`` (or an empty mapping) is the NVIDIA default.  Raises ``ValueError``
    for a quantity that does not parse or is not positive -- the app refuses
    both on write, so this is a contract violation the caller should log and
    treat as "unit unknown" rather than guess at.
    """
    name = (unit_name or "").strip() or DEFAULT_UNIT_NAME
    if not k8s_resources:
        return ClassResources(DEFAULT_CLASS_RESOURCES.units, name)
    units: list[tuple[str, Decimal]] = []
    for resource, raw in k8s_resources.items():
        qty = to_quantity(raw)
        if qty is None or qty <= 0:
            raise ValueError(f"{resource}: {raw!r} is not a positive quantity")
        units.append((str(resource), qty))
    return ClassResources(tuple(units), name)


def pod_units(requests: Mapping[str, Decimal], spec: ClassResources) -> int:
    """Units a pod needs: the largest of its per-resource needs, each rounded up.

    *requests* is the pod's effective request per resource
    (``k8s_client.get_pod_effective_requests``).  A resource it does not request
    needs no units, so a pod requesting none of the class's resources needs ``0``
    -- which the caller treats as "not a pod of this class at all".
    """
    need = 0
    for name, unit in spec.units:
        req = requests.get(name)
        if req is not None and req > 0:
            need = max(need, math.ceil(req / unit))
    return need


def node_units(
    allocatable: Mapping[str, Decimal],
    spec: ClassResources,
    used_by_others: Optional[Mapping[str, Decimal]] = None,
) -> int:
    """Units a node offers: the smallest of its per-resource offers, rounded down.

    *used_by_others* is what pods the controller did not admit request of each
    resource on this node; it is subtracted first, and only matters for a native
    resource (nothing else requests a vendor resource on a reservation node).  A
    listed resource the node does not advertise offers nothing, so the node
    offers ``0`` -- which is right: a pod needing it could never schedule there.
    """
    offer: Optional[int] = None
    for name, unit in spec.units:
        free = allocatable.get(name, Decimal(0)) - (used_by_others or {}).get(name, Decimal(0))
        units = max(0, math.floor(free / unit))
        offer = units if offer is None else min(offer, units)
    return offer or 0
