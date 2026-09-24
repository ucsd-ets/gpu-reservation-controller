"""Core controller state machine.

This module owns the in-memory state shared by the three background loops:

``ControllerState``    — reservations list, GPU-class label map, task queue,
                         on-demand candidates, on-demand occupancy map
``QueueEntry``         — one pod waiting for its reservation window (reserved path)
``OnDemandCandidate``  — one pod searching for an on-demand block (on-demand path)
``slot_start / slot_end`` — UTC time-window accessors
``TOLERATION_KEY``     — the taint/toleration key used by the reservation system

No Kubernetes or HTTP I/O is done here; those live in k8s_client.py and
reservation_client.py respectively.
"""

from __future__ import annotations

import asyncio
import logging
import math
import random
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Collection, Iterable, Literal, NamedTuple, Optional

from .log_fields import kv
from .schemas import ReservationResponse

log = logging.getLogger(__name__)


def canceller_description(r: ReservationResponse) -> str:
    """Return the 'by X' fragment for a ReservationCancelled event message.

    The API carries only ``cancelled_by_id`` (RESERVATION-API.md §6 — there is
    no nested canceller object), so the best available distinction is
    self-vs-other; a machine-readable ``cancel_reason`` (controller cancel,
    supersede) is appended when present.

    Priority:
    1. ``cancelled_by_id`` set and differs from the owner → "by another user"
    2. Fallback → "by user" (self-cancellation or unknown canceller)
    """
    if r.cancelled_by_id is not None and (
        r.user_id is None or r.cancelled_by_id != r.user_id
    ):
        desc = "by another user"
    else:
        desc = "by user"
    if r.cancel_reason:
        desc += f" (reason: {r.cancel_reason})"
    return desc

# Toleration key applied by the controller.  The full toleration is:
#   gpu-class-reservation=<gpu-class-label>:NoSchedule
TOLERATION_KEY = "gpu-class-reservation"


# ---------------------------------------------------------------------------
# Time-window helpers
# ---------------------------------------------------------------------------


def slot_start(r: ReservationResponse) -> datetime:
    """Return the UTC start of *r*'s reserved window."""
    return r.start_utc


def slot_end(r: ReservationResponse) -> datetime:
    """Return the UTC end of *r*'s reserved window."""
    return r.end_utc


def apply_push_to_active(
    current: list[ReservationResponse],
    pushed: list[ReservationResponse],
) -> list[ReservationResponse]:
    """Upsert *pushed* entries into the *current* active reservation list by id.

    Used by the inbound push API to apply a partial delta:
    - a pushed entry with ``status == "active"`` replaces any existing entry of
      the same id, or is inserted if new (upsert);
    - a pushed entry with any other ``status`` (e.g. ``"cancelled"``) removes
      that id from the active set.

    Pure and I/O-free so it is unit-testable in isolation; the eviction of any
    already-admitted pod for a cancelled entry is handled separately by the
    caller via ``detect_cancelled_in_window`` + the cancellation handler.  When
    the same id appears more than once in *pushed*, the last occurrence wins.
    """
    by_id: dict[int, ReservationResponse] = {r.id: r for r in current}
    for r in pushed:
        if r.status == "active":
            by_id[r.id] = r
        else:
            by_id.pop(r.id, None)
    return list(by_id.values())


def free_capacity_by_class(
    capacity_by_class: dict[str, int],
    pods: "list[PodRuntimeView]",
) -> dict[str, int]:
    """Return free GPUs per class: physical capacity minus node-resident use.

    Only pods that are ``node_resident`` and not ``terminating`` count against
    capacity — a terminating pod's GPUs are already being recovered by
    Kubernetes and are treated as free for planning purposes.  A class absent
    from *capacity_by_class* (unknown physical capacity) is not included in
    the result.  A returned value may be negative, signalling an
    over-committed class (more GPUs in use than the snapshot says exist).
    """
    used: dict[str, int] = {}
    for p in pods:
        if not p.node_resident or p.terminating:
            continue
        if p.gpu_class not in capacity_by_class:
            continue
        used[p.gpu_class] = used.get(p.gpu_class, 0) + p.gpu_count
    return {
        gpu_class: capacity - used.get(gpu_class, 0)
        for gpu_class, capacity in capacity_by_class.items()
    }


def free_gpus_by_node_class(
    capacity_by_node_class: dict[str, dict[str, int]],
    pods: "list[PodRuntimeView]",
) -> dict[str, dict[str, int]]:
    """Return free GPUs per node, per class: allocatable minus node-resident use.

    *capacity_by_node_class* is ``{gpu_class: {node_name: allocatable}}`` (from
    ``k8s_client.snapshot_node_gpu_inventory``).  A pod is subtracted from the
    node it is bound to only when it is actually occupying a GPU there —
    ``node_name`` is set (scheduled), ``node_resident`` (not terminal), and not
    ``terminating`` (its GPUs are being recovered).  An unscheduled pod
    (``node_name is None``) belongs to no node and counts against none.  A pod on
    a node/class absent from the inventory is ignored (its physical capacity is
    unknown).  A returned value may be negative (more in use than the snapshot
    says exists).  Pure — no I/O, no mutation of the inputs.
    """
    free = {gpu_class: dict(nodes) for gpu_class, nodes in capacity_by_node_class.items()}
    for p in pods:
        if not p.node_name or not p.node_resident or p.terminating:
            continue
        per_node = free.get(p.gpu_class)
        if per_node is not None and p.node_name in per_node:
            per_node[p.node_name] -= p.gpu_count
    return free


def largest_node_free_by_class(
    free_by_node_class: dict[str, dict[str, int]],
) -> dict[str, int]:
    """Collapse per-node free GPUs to the largest *single-node* opening per class.

    ``{gpu_class: max over nodes of free}``.  A multi-GPU pod requesting ``g``
    GPUs of a class can only schedule if some single node has ``g`` free, so this
    is the number an admission feasibility check compares against.  A class with
    no nodes maps to ``0``.  Pure.
    """
    return {
        gpu_class: max(nodes.values(), default=0)
        for gpu_class, nodes in free_by_node_class.items()
    }


def placement_stall_reason(
    gpu_count: int,
    class_free_by_node: dict[str, int],
    placement_nodes: Optional[Collection[str]],
) -> Optional[str]:
    """Why an admitted pod's being stuck Pending is its own placement's doing.

    Guard 3 pauses on-demand admission for a whole class while any admitted pod
    of it is stuck Pending, because that normally means the class has less room
    than the controller's accounting believes.  A pod whose node selector or
    affinity (*placement_nodes*: the nodes of its class it allows) keeps it off
    the nodes that do have room says nothing of the sort, and must not pause
    the class for everyone else -- one user's pod pinned to a busy host, or to
    hardware the class does not have, would otherwise hold every on-demand job
    of the class for as long as it waited:

    - ``"no_matching_node"`` -- it allows none of the class's nodes, so it
      cannot start here at any occupancy.
    - ``"matching_nodes_full"`` -- no node it allows has *gpu_count* GPUs free,
      but another node of the class does: without the constraint it would run.

    ``None`` otherwise, and the pod counts towards guard 3 as before: it is
    unconstrained (*placement_nodes* is ``None``), or the class is short on
    every node (a pod without the constraint would be stuck too), or a node it
    allows has room (it is stuck for a reason nothing here explains).  Pure.
    """
    if placement_nodes is None:
        return None
    if not placement_nodes:
        return "no_matching_node"
    if any(class_free_by_node.get(n, 0) >= gpu_count for n in placement_nodes):
        return None
    if any(
        free >= gpu_count
        for n, free in class_free_by_node.items()
        if n not in placement_nodes
    ):
        return "matching_nodes_full"
    return None


def gpu_capacity_by_class(
    capacity_by_node_class: dict[str, dict[str, int]],
) -> dict[str, int]:
    """Collapse a per-node inventory to total GPUs per class.

    Identical to what ``k8s_client.snapshot_node_gpu_capacity`` returns for the
    same inventory, so the queue tick can re-check guard 4 from the inventory it
    already holds without a second node LIST.  Pure.
    """
    return {
        gpu_class: sum(per_node.values())
        for gpu_class, per_node in capacity_by_node_class.items()
    }


def node_counts_by_class(
    capacity_by_node_class: dict[str, dict[str, int]],
    known_classes: Iterable[str] = (),
) -> dict[str, int]:
    """Collapse a per-node inventory to the number of nodes backing each class.

    ``{gpu_class: node count}``.  Guard 1b reads this: a lease minted for a
    class with nowhere to run is an SU charge against a pod that cannot start.

    ``snapshot_node_gpu_inventory`` excludes cordoned and terminating nodes and
    lists a class only when at least one node survives that, so a class whose
    nodes are all cordoned or gone is not ``0`` in the inventory — it is
    *absent*.  Read as-is, that is indistinguishable from "no data", which guard
    1b fails open on, so the guard could never fire.  Every class in
    *known_classes* (the labels the reservation app knows) is therefore recorded
    explicitly, as ``0`` when the inventory has no node for it.  A label outside
    it stays absent — unknown, never blocking — as does everything before the
    first successful snapshot.  Pure.
    """
    counts = dict.fromkeys(known_classes, 0)
    counts.update(
        (gpu_class, len(nodes)) for gpu_class, nodes in capacity_by_node_class.items()
    )
    return counts


class LockContractError(RuntimeError):
    """A ``reservation_lock`` contract violation — re-entry, or a missing hold.

    Deliberately a real exception rather than an ``assert``: asserts vanish under
    ``python -O``, and this guards an invariant whose violation is otherwise
    *silent*.  Raising turns a permanent hang into a `task.crashed` CRITICAL and
    a 503 from ``GET /health``, which is the whole point.
    """


class _OwnedLock:
    """An ``asyncio.Lock`` that knows which task holds it.

    ``asyncio.Lock`` is **not reentrant**, so a coroutine that takes
    ``reservation_lock`` while already holding it does not raise — it waits on
    itself, forever.  Nothing notices: ``_on_task_done`` records only
    *exceptions*, so the deadlocked loop is never marked dead and ``GET /health``
    keeps returning 200 while the controller quietly stops admitting pods.

    Tracking the owning task converts that into an immediate, traceable
    exception, and gives :meth:`held_by_current_task` exact semantics — plain
    ``Lock.locked()`` is true when *anyone* holds it, so it can neither detect
    re-entry nor tell a helper whether *its own* caller took the lock.

    Drop-in for the ``async with`` sites (there are no direct
    ``acquire``/``release`` callers, and none should be added).
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._owner: Optional[asyncio.Task] = None

    async def __aenter__(self) -> "_OwnedLock":
        task = asyncio.current_task()
        if task is not None and task is self._owner:
            raise LockContractError(
                "reservation_lock is not reentrant: this task already holds it. "
                "A helper documented 'caller must hold the lock' must not take it "
                "itself — see require_reservation_lock."
            )
        await self._lock.acquire()
        self._owner = task
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        self._owner = None
        self._lock.release()

    def held_by_current_task(self) -> bool:
        """Whether the calling task is the one holding this lock."""
        task = asyncio.current_task()
        return task is not None and task is self._owner

    def locked(self) -> bool:
        """Whether anyone holds the lock (``asyncio.Lock`` parity)."""
        return self._lock.locked()


class CapacityDiff(NamedTuple):
    """One class where the app-side and physical GPU counts disagree.

    ``physical`` is the total allocatable GPUs observed from Kubernetes node
    taints (``snapshot_node_gpu_capacity``); ``app_side`` is the reservation
    app's ``total_gpus`` for the same class.  ``overcommitted`` is
    ``app_side > physical`` — the app believes there are more GPUs than
    physically exist, so on-demand leases it hands out may never schedule.
    """

    label: str
    app_side: int
    physical: int

    @property
    def overcommitted(self) -> bool:
        return self.app_side > self.physical


def reconcile_capacity(
    app_side: dict[str, int],
    physical: dict[str, int],
) -> tuple[list[CapacityDiff], set[str]]:
    """Compare app-side vs physical per-class GPU capacity.

    Walks the union of class labels in both maps and returns:

    - a list of :class:`CapacityDiff` for every class whose two counts differ
      (in either direction), including a class present in only one map — the
      other side is treated as ``0`` (no physical nodes carry the taint, or the
      app does not know the class); and
    - the set of labels that are **over-committed** (``app_side > physical``),
      the per-class on-demand admission pause gate.

    A class whose app-side count is *unknown* must not appear in ``app_side``
    at all (the caller omits classes with no ``total_gpus``); such a class can
    still surface as a diff via the physical side but is never treated as
    over-committed, since overcommit cannot be concluded from missing data.
    """
    diffs: list[CapacityDiff] = []
    overcommitted: set[str] = set()
    for label in sorted(set(app_side) | set(physical)):
        a = app_side.get(label, 0)
        p = physical.get(label, 0)
        if a == p:
            continue
        diff = CapacityDiff(label=label, app_side=a, physical=p)
        diffs.append(diff)
        # Only a *known* app-side count (label present in ``app_side``) can
        # be over-committed; a class seen only physically is not.
        if label in app_side and diff.overcommitted:
            overcommitted.add(label)
    return diffs, overcommitted


class OnDemandGate(NamedTuple):
    """One GPU class on which JIT on-demand admission is currently paused.

    Produced by :meth:`ControllerState.plan_ondemand_gates` for the periodic
    ``ondemand.gated`` WARNING (``main.ondemand_gate_warning_loop``).  Only the
    **class-wide** gates appear — guard 1b (no schedulable node), guard 3 (a
    stuck reservation holder) and guard 4 (app-side overcommit).  These hold
    every candidate of the class regardless of its ask and persist until an
    operator (or the cluster) changes something.  Guard 5 is deliberately
    absent: it is per-candidate fragmentation that clears as pods finish, which
    is the cluster being full rather than a fault anyone must act on.

    ``since`` is when this controller first observed the gate (at most one
    warning interval late, and reset by a restart); ``waiting`` counts the
    pending candidates it is holding back right now.  The remaining fields are
    the evidence a sysadmin needs for *this* guard and are ``None`` otherwise.
    """

    label: str
    guard: int
    reason: str
    since: datetime
    waiting: int
    app_gpus: Optional[int] = None    # guard 4: the app's effective_gpus_today
    phys_gpus: Optional[int] = None   # guard 4: last observed physical capacity
    stuck_pods: tuple[str, ...] = ()  # guard 3: "ns.name" of the stuck holders


# ---------------------------------------------------------------------------
# What a still-pending pod's owner has been told
# ---------------------------------------------------------------------------

# The two independent things a pending pod's owner is told about, each with its
# own throttle (see ControllerState.pending_status).  "hold" is what is keeping
# the pod Pending right now -- one current answer, so moving from one cause to
# another is news and is reported at once.  "annotations" is a galends/*
# annotation the controller had to ignore: true for as long as the pod exists,
# whatever holds it, so it must not share the hold's key -- two alternating
# statuses would each read as a change and be restated on every retry.
PENDING_TOPIC_HOLD = "hold"
PENDING_TOPIC_ANNOTATIONS = "annotations"


@dataclass(frozen=True)
class PendingStatus:
    """One status Event put on a still-pending pod: what it said, and when.

    ``key`` is the Event's reason plus the text that varies with it; see
    ``main._post_pending_status``, the only writer.
    """

    key: tuple[str, str]
    at: datetime


class NearMiss(NamedTuple):
    """A user's live bookings that one of their pods narrowly failed to match.

    What a pod left without a reservation is told when the user *does* hold
    one -- for the right class under another usage group, or for another class
    -- because "you have no reservation" is the wrong thing to tell someone
    looking at one.  Produced by :meth:`ControllerState.near_miss_bookings`.
    """

    other_groups: tuple[str, ...] = ()   # same GPU class, a different usage group
    other_classes: tuple[str, ...] = ()  # a different GPU class altogether


# ---------------------------------------------------------------------------
# Task queue entry
# ---------------------------------------------------------------------------


@dataclass
class QueueEntry:
    """A pod that has been matched to a reservation and is waiting its turn."""

    pod_uid: str
    pod_name: str
    pod_namespace: str
    gpu_class_label: str   # value of pod label "gpu-class" (e.g. "h100")
    gpu_requested: int     # nvidia.com/gpu units requested by this pod
    reservation: ReservationResponse
    next_attempt_at: datetime  # earliest time to try applying the toleration
    group_label: Optional[str] = None  # value of REQUIRED_GROUP_LABEL pod label; None when disabled
    # Why this pod waits for its booking rather than being admitted on demand:
    # main._ondemand_ineligibility's clauses, recorded by routing when it queued
    # the pod only because it could not go on demand (the any-match fallback --
    # a booking beyond ONDEMAND_HORIZON_MINUTES, full, or too small).  Quoted in
    # the pod's reservation-wait Event, so its owner can see what keeps it from
    # starting sooner.  Empty when on-demand admission is off, or routing queued
    # the pod for a booking it would wait for anyway.  Kept when the entry is
    # re-pointed at another reservation: it describes the pod, not the booking.
    ondemand_ineligibility: tuple[str, ...] = ()


@dataclass
class OnDemandCandidate:
    """A pod eligible for a just-in-time (JIT) on-demand reservation lease.

    Distinct from QueueEntry: a QueueEntry is bound to a specific reservation;
    an OnDemandCandidate has none yet and requests one from the reservation
    app (``main._try_request_lease``).
    """

    pod_uid: str
    pod_name: str
    pod_namespace: str
    gpu_class_label: str   # value of pod label "gpu-class" (e.g. "h100")
    gpu_requested: int     # nvidia.com/gpu units requested by this pod
    # galends/minimum-runtime-seconds annotation value (or the configured
    # default).  0 for a best-effort candidate, which asks for no runtime and
    # therefore sizes nothing -- see best_effort below.
    min_runtime_seconds: int
    pod_created_at: datetime  # metadata.creationTimestamp; used for FIFO ordering
    next_attempt_at: datetime  # earliest time to try requesting a lease
    group_label: Optional[str] = None  # value of REQUIRED_GROUP_LABEL pod label; None when disabled
    # Usage group the lease ask carries as its required group_name: the group
    # label value when REQUIRED_GROUP_LABEL is on, else the pod's
    # galends/usage-group annotation.  JIT eligibility requires it, so it is only
    # Optional to keep dataclass field ordering.
    usage_group: Optional[str] = None
    # Where usage_group came from -- "label" (the REQUIRED_GROUP_LABEL pod
    # label), "annotation" (galends/usage-group) or "default"
    # (DEFAULT_USAGE_GROUP) -- so a pod whose group the app does not recognise
    # can be told what to fix: its own label or annotation, or nothing it
    # controls at all.  None when the candidate was not built by routing.
    usage_group_source: Optional[str] = None
    # Set True only while the candidate is parked on an indeterminate guard-1
    # result (the scheduler has not yet recorded a PodScheduled verdict).  A
    # MODIFIED watch event carrying that verdict re-attempts immediately instead
    # of waiting for a periodic scan; other cooldowns (denial, guard-3/4) leave
    # this False so they are never short-circuited.  See main.pod_watch_loop.
    awaiting_schedule_signal: bool = False
    # The pod declared galends/runtime-guarantee: none -- it wants no runtime
    # guarantee at all, and is admitted under a zero-length, zero-SU
    # kind="best_effort" reservation rather than a guaranteed lease.  It is
    # therefore preemptible from its first tick, through the ordinary
    # guarantee_end path (the stub's window is already over), with no
    # best-effort branch anywhere in the preemption planner.  What it *does*
    # change is admission: guard 4 does not apply (an app-side overcommit says
    # nothing about a row that consumes no app-side capacity) and guard 5 is
    # tightened to every GPU count, because nothing app-side bounds how many
    # best-effort pods are admitted -- see main._preflight_ondemand_candidate.
    best_effort: bool = False
    # Consecutive non-retryable lease failures (a 4xx that is not 409: a
    # read-only service key, a schema mismatch, an unknown group).  Waiting does
    # not fix these, so the backoff grows with the count instead of retrying at
    # the flat 2-5 min denial cadence forever.  Reset on any grant or routine
    # denial.  See main._grant_and_admit and _error_retry_at.
    lease_error_count: int = 0
    # What this pod's owner has already been told is *not* kept here: it lives
    # in ControllerState.pending_status, keyed by pod uid, so the story survives
    # the pod moving off the candidate list (onto the reserved queue, or onto no
    # path at all) and back.


@dataclass(frozen=True)
class PodRuntimeView:
    """Preemption-planning view of one live tolerated pod.

    Built by main.py from a ``k8s_client.ToleratedPodInfo`` (digesting the
    Kubernetes-shaped snapshot into the plain fields the pure planning
    functions below need) — keeping this module free of Kubernetes imports.
    """

    uid: str
    namespace: str
    name: str
    gpu_class: str
    gpu_count: int
    reservation_id: Optional[int]  # parsed booking-reference id; None = not ours
    node_resident: bool            # Running, or (Pending and not scheduled_false)
    terminating: bool              # metadata.deletionTimestamp is set
    group_label: Optional[str] = None  # value of REQUIRED_GROUP_LABEL pod label; None when disabled
    node_name: Optional[str] = None    # spec.nodeName; None until the pod is scheduled onto a node
    # Parsed value of the pod's ``galends/termination-warning-at`` annotation, or
    # None when it carries none.  The headroom planners read it as *state*: it is
    # both the notice deadline they gate kills on and the value they keep stable
    # across ticks, which is what makes the notice survive a controller restart
    # (the pod's own annotation is the only record that it was warned).
    termination_warning_at: Optional[datetime] = None


@dataclass
class PreemptionPlan:
    """Outcome of ``ControllerState.plan_boundary_preemption`` for one boundary/phase."""

    boundary: datetime
    phase: str                       # "A" (lead-time) or "B" (at-boundary)
    demand_by_class: dict[str, int]
    free_by_class: dict[str, int]
    victims: list[PodRuntimeView]
    unmet_by_class: dict[str, int]    # shortfall remaining after all eligible kills


@dataclass
class BoundaryPreemptionNeed:
    """What must be reclaimed at one boundary/phase, *before* victims are chosen.

    ``ControllerState.plan_boundary_candidates`` computes this: per GPU class,
    how many GPUs still have to be freed (``kills_needed_by_class``) and which
    live, past-guarantee, controller-admitted pods are *eligible* to supply them
    (``candidates_by_class``).  It deliberately stops short of picking victims —
    that choice is delegated to the reservation app (which can prioritise by
    owner/group/kind), with a local uniform-random fallback
    (``select_victims_locally``) when the app is unreachable or delegation is
    disabled.  Only classes with a non-zero shortfall appear in either map.
    """

    boundary: datetime
    phase: str                       # "A" (lead-time) or "B" (at-boundary)
    demand_by_class: dict[str, int]
    free_by_class: dict[str, int]
    kills_needed_by_class: dict[str, int]
    candidates_by_class: dict[str, list[PodRuntimeView]]


def select_victims_locally(
    need: BoundaryPreemptionNeed,
    rng: Optional[random.Random] = None,
) -> dict[str, list[PodRuntimeView]]:
    """Pick victims from *need*'s candidate pool with a uniform-random policy.

    The local fallback for what the reservation app does when victim selection
    is delegated to it: per GPU class, shuffle the eligible candidates and
    greedily accumulate until ``kills_needed`` GPUs are covered (which may
    overshoot when a victim is larger than the residual shortfall).  Returns the
    chosen victims grouped by GPU-class label.  Pure — no I/O, no mutation.
    """
    rng = rng or random.Random()
    selected: dict[str, list[PodRuntimeView]] = {}
    for gpu_class, kills_needed in need.kills_needed_by_class.items():
        eligible = list(need.candidates_by_class.get(gpu_class, []))
        rng.shuffle(eligible)
        picked: list[PodRuntimeView] = []
        killed_gpus = 0
        for p in eligible:
            if killed_gpus >= kills_needed:
                break
            picked.append(p)
            killed_gpus += p.gpu_count
        selected[gpu_class] = picked
    return selected


def build_preemption_plan(
    need: BoundaryPreemptionNeed,
    selected_by_class: dict[str, list[PodRuntimeView]],
) -> PreemptionPlan:
    """Assemble a ``PreemptionPlan`` from a boundary's need + the chosen victims.

    ``selected_by_class`` is whatever the selector (app or local fallback)
    returned — the victims are taken as-is, and ``unmet_by_class`` records any
    per-class shortfall the selection did not cover (fewer GPUs chosen than
    ``kills_needed`` — e.g. the pool was too small, or the app deliberately
    spared pods).  Victim order follows ``kills_needed_by_class`` iteration.
    Pure — no I/O, no mutation.
    """
    victims: list[PodRuntimeView] = []
    unmet: dict[str, int] = {}
    for gpu_class, kills_needed in need.kills_needed_by_class.items():
        picked = selected_by_class.get(gpu_class, [])
        victims.extend(picked)
        killed_gpus = sum(p.gpu_count for p in picked)
        if killed_gpus < kills_needed:
            unmet[gpu_class] = kills_needed - killed_gpus
    return PreemptionPlan(
        boundary=need.boundary,
        phase=need.phase,
        demand_by_class=need.demand_by_class,
        free_by_class=need.free_by_class,
        victims=victims,
        unmet_by_class=unmet,
    )


# ---------------------------------------------------------------------------
# Preemption-risk forecast (pure planning)
# ---------------------------------------------------------------------------

# Number of hourly forecast buckets: the remainder of the current wall-clock
# hour plus the next two full hours.  A module constant, not an environment
# variable, by design — the forecast surface is fixed so its consumers (the
# reservation app UI) can rely on the shape.
FORECAST_BUCKET_COUNT = 3


def forecast_buckets(
    now: datetime, count: int = FORECAST_BUCKET_COUNT
) -> list[tuple[datetime, datetime]]:
    """Return *count* calendar-aligned forecast buckets starting at *now*.

    Bucket 0 runs from *now* to the top of the next hour (possibly much
    shorter than an hour; exactly one hour when *now* is already on the
    hour); each subsequent bucket is one full wall-clock hour.  Buckets are
    half-open ``[start, end)``.  Calendar alignment lives only here — the
    rest of the forecast consumes whatever intervals this returns.
    """
    first_end = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    buckets = [(now, first_end)]
    for i in range(1, count):
        buckets.append(
            (first_end + timedelta(hours=i - 1), first_end + timedelta(hours=i))
        )
    return buckets


def overlapping_bucket_indices(
    interval_start: datetime,
    interval_end: datetime,
    buckets: list[tuple[datetime, datetime]],
) -> list[int]:
    """Indices of *buckets* the closed interval [start, end] intersects.

    Buckets are half-open ``[s, e)``, so an interval ending exactly at a
    bucket's start still counts (a kill landing at instant ``s`` happens
    inside that bucket) while one starting exactly at a bucket's end does
    not.
    """
    return [
        i
        for i, (s, e) in enumerate(buckets)
        if interval_start < e and interval_end >= s
    ]


def combine_risks(risks: Iterable[float]) -> float:
    """Combine independent per-boundary risks into one probability.

    ``1 − Π(1 − r)`` — treats each boundary as an independent chance of being
    preempted.  In reality boundaries share candidate pools and an earlier
    kill frees capacity for later boundaries, so this errs on the high
    (conservative) side.  Inputs and result are clamped to [0, 1].
    """
    survival = 1.0
    for r in risks:
        survival *= 1.0 - min(1.0, max(0.0, r))
    return min(1.0, max(0.0, 1.0 - survival))


@dataclass(frozen=True)
class ForecastBoundaryNeed:
    """Projected shortfall and at-risk pool for one *future* boundary.

    The forecast sibling of ``BoundaryPreemptionNeed`` with two deliberate
    differences: eligibility is evaluated *as of the boundary instant*
    (``guarantee_end <= boundary``, where the guarantee itself is computed at
    the real "now" so back-to-back chains stay intact — see
    ``ControllerState.forecast_boundary_need``), and ``eligible_by_class``
    covers every class with at least one eligible pod, not only shortfall
    classes, because bucket summaries want the pool size even at zero
    shortfall.
    """

    boundary: datetime
    demand_by_class: dict[str, int]
    kills_needed_by_class: dict[str, int]     # max(0, demand − free); only > 0 kept
    eligible_by_class: dict[str, list[PodRuntimeView]]
    pool_gpus_by_class: dict[str, int]        # Σ gpu_count over eligible_by_class


@dataclass(frozen=True)
class ForecastBucketSummary:
    """Per-GPU-class aggregates for one forecast bucket.

    All six maps share one identical, sorted key set (the union of physical
    capacity classes, admitted-pod classes, boundary-demand classes, and
    pending-JIT classes) so consumers can iterate any one of them.
    ``free_by_class`` may be negative (over-committed class).
    ``pending_jit_gpus_by_class`` is informational pressure only — never
    folded into ``shortfall_by_class`` (a granted JIT lease has
    ``kind="on_demand"`` and therefore never creates a preemption boundary
    under the sweep's ``kind == "booking"`` filters).
    """

    start: datetime
    end: datetime
    capacity_by_class: dict[str, int]
    free_by_class: dict[str, int]
    demand_by_class: dict[str, int]
    shortfall_by_class: dict[str, int]
    eligible_pool_gpus_by_class: dict[str, int]
    pending_jit_gpus_by_class: dict[str, int]


@dataclass(frozen=True)
class PodBucketRisk:
    """One admitted pod's preemption outlook within one forecast bucket."""

    risk: float   # 0.0–1.0; 0 whenever the guarantee covers the whole bucket
    state: str    # "guaranteed" | "overstay" | "mixed" (guarantee ends mid-bucket)


@dataclass(frozen=True)
class PodForecast:
    """Per-pod forecast: the pod's view, its live guarantee, per-bucket risk."""

    view: PodRuntimeView
    guarantee_end: Optional[datetime]  # at the real now; None = unresolvable (overstay)
    buckets: list[PodBucketRisk]       # index-aligned with the forecast's buckets


@dataclass(frozen=True)
class PreemptionForecast:
    """Full output of ``ControllerState.forecast_preemption_risk``."""

    generated_at: datetime
    lead_minutes: int
    buckets: list[ForecastBucketSummary]
    pods: list[PodForecast]


@dataclass(frozen=True)
class TerminationWarning:
    """One at-risk pod's projected preemption, for the warning annotations.

    Produced by ``ControllerState.plan_termination_warnings``.  ``terminate_at``
    is the projected *kill instant* — ``max(boundary − lead, guarantee_end)`` —
    at the *soonest* upcoming booking boundary where the pod is an eligible
    preemption victim in a class that is short on capacity.  This is the
    kill-window start the sweep would actually delete the pod at (phase A runs
    ``lead`` minutes *before* the boundary; a pod already past its guarantee is
    killed then, not at the boundary), matching the forecast API's model.  For a
    phase-B victim whose guarantee ends exactly at the boundary it degrades to
    the boundary itself.  ``risk`` is that boundary's
    ``min(1, shortfall / pool_gpus)`` (the uniform-random local selection model —
    pool *membership* is exact, the number models the fallback).  Purely
    informational: the controller stamps these onto the pod and never reads them
    back to make a decision.
    """

    view: PodRuntimeView
    terminate_at: datetime   # projected kill instant at the soonest at-risk boundary
    risk: float              # (0, 1] at that boundary
    # The booking boundary the pod is at risk at, or None for a headroom notice
    # (no booking involved: capacity held free for on-demand jobs).  Carried so
    # the message can say which, rather than implying a reservation starts at
    # ``terminate_at`` -- true only for a pod whose guarantee ends exactly at
    # the boundary.
    boundary: Optional[datetime] = None


@dataclass(frozen=True)
class GuaranteeStatus:
    """One admitted pod's live guarantee status, for the status annotations.

    Produced by ``ControllerState.plan_guarantee_status``.  ``status`` is
    ``"guaranteed"`` while the pod is inside its live (chain-aware) runtime
    guarantee, else ``"overstay"``.  ``guaranteed_until`` is the live guarantee
    end when guaranteed (written to ``galends/guaranteed-until``), or ``None`` when
    overstay — the reconcile then leaves the pod's existing (now-past)
    ``guaranteed-until`` frozen and flips only the status.  Purely informational:
    the controller stamps these onto the pod and never reads them back to make a
    decision.
    """

    view: PodRuntimeView
    # The two literals mirror k8s_client.GUARANTEE_STATUS_GUARANTEED /
    # _OVERSTAY (kept as bare strings here so controller.py stays free of
    # k8s_client imports); annotate_runtime_guarantee stamps the same values at
    # admission, so they must not drift or the diff-and-skip reconcile would
    # re-patch every pod every tick.
    status: Literal["guaranteed", "overstay"]
    guaranteed_until: Optional[datetime]  # live end when guaranteed; None when overstay


# ---------------------------------------------------------------------------
# Shared controller state
# ---------------------------------------------------------------------------


class ControllerState:
    """In-memory state accessed by all background loops.

    All mutations happen inside the asyncio event loop, so no locking is
    required for the background loops (asyncio is single-threaded).  The one
    exception is ``reservation_lock``, which serialises reservation-state
    reconciliation between the fetch loop and the inbound push endpoint — two
    coroutines that both mutate ``reservations`` across ``await`` points.
    """

    def __init__(self) -> None:
        # Current snapshot of active reservations from the API.
        self.reservations: list[ReservationResponse] = []

        # Mapping from gpu_class_id → Kubernetes label value (e.g. "h100").
        # Refreshed each reconcile from GET /api/gpu-classes (full list), with a
        # per-id GET /api/gpu-classes/{id} fallback for a class referenced by a
        # reservation but missing from the bulk list.
        self.gpu_class_labels: dict[int, str] = {}

        # Reverse of gpu_class_labels (label → id).  A pod carries only its
        # gpu-class label, but requesting a JIT lease needs the numeric
        # gpu_class_id, so this map is what main._try_request_lease resolves
        # against.  Refreshed alongside gpu_class_labels.
        self.gpu_class_ids: dict[str, int] = {}

        # Whether the controller has the whole picture a pod's owner may be
        # told is wrong with *their pod*: every class the app has (a successful
        # bulk class fetch, not just the classes some reservation named), and
        # the full reservation list (a successful fetch, not merely an empty
        # list or a pushed delta).  Until then "your gpu-class is unknown" and
        # "no reservation matches" could be the controller's gap rather than
        # the pod's fault, so neither is said.  Set by the reconcile; never
        # reset, because a later failed fetch keeps the last good data.
        self.gpu_classes_known: bool = False
        self.reservations_known: bool = False

        # App-side per-class GPU capacity: label → effective_gpus_today (the
        # count the reservation app actually admits against, i.e. total_gpus
        # after any date-span override covering today), refreshed alongside the
        # label maps.  Keyed by label so it shares a key space with the physical
        # snapshot (snapshot_node_gpu_capacity).  Populated only for classes
        # whose count is known (see schemas.GpuClassDetail.audit_gpus); the
        # hourly capacity audit compares it against physical capacity (see
        # main._run_capacity_audit).
        self.gpu_class_capacity: dict[str, int] = {}

        # GPU class labels found over-committed (app-side effective count >
        # physical capacity).  New on-demand admissions are paused for any class
        # in this set (mirrors stuck_holder_gpu_classes above); recomputed by the
        # hourly audit *and* every queue-processor tick (main.
        # _refresh_overcommit_pause), so a class clears within one tick once the
        # deficiency is resolved.  Empty = no pause.
        self.overcommitted_gpu_classes: set[str] = set()

        # Physical per-class GPU capacity from the last *successful* snapshot
        # that recomputed overcommitted_gpu_classes (label → allocatable GPUs),
        # kept so the ondemand.gated warning can state both sides of a guard-4
        # overcommit, not just that one exists.  Written only alongside
        # overcommitted_gpu_classes, so the two always describe one snapshot.
        self.physical_gpu_capacity: dict[str, int] = {}

        # Name of the pod label naming the usage group to match (REQUIRED_GROUP_LABEL),
        # or None when the feature is disabled.  Set once from config at startup.
        # When set, the reserved-path matchers additionally require a pod's group
        # label value to equal the reservation's group.name (see _group_ok).
        self.required_group_label: Optional[str] = None

        # Active work queue keyed by pod UID (reserved path).
        self.task_queue: dict[str, QueueEntry] = {}

        # Pods eligible for on-demand placement, keyed by pod UID.
        # These have the galends/minimum-runtime-seconds annotation and are
        # Pending, but do not match any user reservation.
        self.ondemand_candidates: dict[str, OnDemandCandidate] = {}

        # What each still-pending pod's owner has been told: pod uid → topic
        # (PENDING_TOPIC_HOLD / PENDING_TOPIC_ANNOTATIONS) → the last status
        # Event and when it went out.  The throttle behind
        # main._post_pending_status, which is every status Event addressed to
        # the owner of a pod that is not running.  Keyed by uid rather than
        # carried on an OnDemandCandidate so the pod tells one story whichever
        # path holds it -- a candidate, the reserved queue, or no path at all --
        # and a move between them is not mistaken for news.  In-memory: after a
        # restart each pod is re-told once, the right side to err on, because
        # Events expire.  Dropped when the pod is admitted, finishes or is
        # deleted; prune_pending_status catches a pod whose DELETED event was
        # never seen (a watch re-LIST replays only the pods that still exist).
        self.pending_status: dict[str, dict[str, PendingStatus]] = {}

        # Unified occupancy map for ALL admitted pods (reserved, on-demand, and
        # no-show alike): reservation id → { pod_uid: gpu_count }.  Available
        # capacity on any reservation = reservation.gpu_count - sum(values).
        #
        # Kept warm incrementally (record_placement on admission, release_pod on
        # vacate) and rebuilt from a live cluster snapshot each queue-processor
        # tick (reconcile_occupancy), so a missed watch event self-heals within
        # one tick.  Each pod is bucketed by the reservation id parsed from its
        # galends/booking-reference, so no separate annotation is needed.
        self.occupancy: dict[int, dict[str, int]] = {}

        # Who the pods in ``occupancy`` are: pod uid → (namespace, name).  A
        # directory, not a second occupancy map -- ``reservation_holders`` looks
        # up the uids ``occupancy`` lists and never the reverse, so an entry
        # outliving its pod is inert.  Rebuilt from the same snapshot each
        # queue-processor tick, and added to wherever a pod is admitted or seen
        # admitted in between, so a pod waiting on a full reservation can be
        # told which of its owner's pods hold it (``ReservationFull``).
        self.holder_names: dict[str, tuple[str, str]] = {}

        # Safety interlock (guard 3): set of GPU class labels that currently
        # have at least one reservation-holder pod stuck Pending.  Updated each
        # queue_processor_loop tick.  On-demand placement is held for any class
        # in this set; other classes are unaffected.  Empty = no interlock.
        self.stuck_holder_gpu_classes: set[str] = set()
        # The stuck holder pods behind each class in stuck_holder_gpu_classes
        # ("ns.name"), refreshed on the same tick, so the periodic
        # ondemand.gated warning can name the pods an operator must inspect.
        self.stuck_holder_pods: dict[str, list[str]] = {}

        # When each class-wide on-demand gate — keyed (class label, guard) —
        # was first observed, for the ondemand.gated warning's dur_s.  Owned by
        # plan_ondemand_gates, which adds and prunes it; in-memory, so a
        # restart re-dates a standing gate from the restart.
        self.ondemand_gate_since: dict[tuple[str, int], datetime] = {}

        # Per-node feasibility (guard 5): the largest number of free GPUs on any
        # *single* node, per GPU-class label (max over nodes of allocatable minus
        # node-resident use — see controller.largest_node_free_by_class).  A
        # multi-GPU on-demand candidate is held when no single node can host it,
        # so the controller does not mint an SU-charged lease that can never
        # schedule.  Refreshed each queue_processor_loop tick from a node-inventory
        # + tolerated-pod snapshot; a class absent from the map is "unknown" and
        # never blocks (fail-open); a failed snapshot leaves the prior map intact
        # (fail-safe).  Empty = no data yet.
        self.node_free_by_class: dict[str, int] = {}

        # Node existence (guard 1): number of schedulable nodes carrying each
        # GPU class's reservation taint, from the same inventory snapshot that
        # feeds node_free_by_class.  A candidate whose class is *known* to have
        # no nodes is held rather than granted a lease it could never run
        # under; a class absent from the map is "unknown" and never blocks
        # (fail-open), and a failed snapshot leaves the prior map intact
        # (fail-safe).  Empty = no data yet.
        self.class_node_counts: dict[str, int] = {}

        # Node placement (guards 1 and 5 for a pod with a node selector or
        # required node affinity): free GPUs per node, per class, and each
        # node's labels, from the same inventory snapshot.  node_free_by_class
        # is the collapse of the first; a pod that may run on only some of a
        # class's nodes is measured against those nodes instead.  Same
        # lifecycle: refreshed each tick, left intact on a failed snapshot, and
        # a class or node absent from either is unknown and never blocks.
        self.node_free_by_node_class: dict[str, dict[str, int]] = {}
        self.node_labels: dict[str, dict[str, str]] = {}

        # No-show tracking:
        # Maps reservation_id → deadline by which a matching pod must appear.
        # Cleared when a pod is matched; moved to noshow_reservation_ids on expiry.
        self.noshow_deadlines: dict[int, datetime] = {}

        # Reservations permanently declared no-show for this controller lifetime.
        # Excluded from reserved-path matching and chaining from the moment of
        # declaration (see find_best_reservation / _chain_for).
        self.noshow_reservation_ids: set[int] = set()

        # Declared no-shows still awaiting a durable controller-issued cancel
        # (POST /api/reservations/{id}/cancel, reason="no-show").  Populated by
        # check_noshow_deadlines; drained by queue_processor_loop once the
        # cancel succeeds.  Retried every tick until it does, since a durable
        # app-side cancel is what actually frees the window for re-booking.
        self.pending_noshow_cancels: set[int] = set()

        # On-demand lease ids whose pod has been merged into a now-open matching
        # booking but whose penalty-exempt cancel (reason="superseded") has not
        # yet landed.  Populated by _merge_ondemand_into_bookings when the cancel
        # fails; drained every queue-processor tick until it succeeds so a merged
        # lease never lingers holding capacity / accruing SU.  In-memory only
        # (a restart re-derives the merge from the pod's booking-reference).
        self.pending_ondemand_merge_cancels: set[int] = set()

        # Reservation ids currently "claimed" by a live reserved-path holder pod
        # — each holder's booking reservation plus the back-to-back chain its
        # runtime cap extends across (see reservations_claimed_by).  A reservation
        # in this set must not be declared a no-show or lent out as on-demand
        # capacity, because a holder is actively occupying its window via a
        # chained deadline even though no pod is booked directly under it.
        # Recomputed from the live pod snapshot each queue-processor tick.
        self.claimed_reservation_ids: set[int] = set()

        # Preemption sweep bookkeeping: for each upcoming slot boundary,
        # which phase(s) ("A" = lead-time, "B" = at-boundary) have already
        # been evaluated.  Prevents a flapping snapshot from re-planning (and
        # potentially re-selecting fresh victims) within the same phase
        # window; a restart simply loses the marks and re-evaluates once,
        # which is safe because recomputation is idempotent (a killed pod
        # shows as Terminating or gone).  Pruned by ``prune_preemption_marks``.
        self.preemption_fired: dict[datetime, set[str]] = {}

        # When the anticipatory-headroom *kill* evaluation last ran.  Headroom
        # rides the preemption sweep rather than a loop of its own (one pair of
        # cluster snapshots, and one writer for the termination-warning
        # annotations), so this is what throttles it to
        # HEADROOM_CHECK_INTERVAL instead of the sweep's own faster cadence.
        # None = never evaluated, so the first sweep after startup runs it.
        # Deliberately *not* used to throttle headroom *warnings*: those are
        # recomputed on every sweep (see main._run_preemption_sweep).
        self.headroom_last_eval: Optional[datetime] = None

        # Whether some pod may still carry a ``galends/termination-warning-*``
        # annotation.  The preemption sweep skips its cluster snapshots on a tick
        # with nothing in scope -- and, before this existed, skipped the warning
        # reconcile with them, so a warning whose boundary was cancelled or had
        # passed stayed on the pod until some other booking came within range
        # (hours, on a quiet night).  While this is set, the sweep runs anyway so
        # the reconcile can clear what is stale.  ``True`` at startup: the pods'
        # own annotations are the state, and a previous lifetime may have left
        # some behind.
        self.termination_warnings_outstanding: bool = True

        # Serialises reservation-state reconciliation between the fetch loop and
        # the inbound push endpoint.  Both mutate ``reservations`` and evict pods
        # across ``await`` points; without this lock a push landing mid-fetch
        # could be clobbered by the fetch's wholesale replace, or double-evict.
        # (This is the one place the "no locking" note above is relaxed, now that
        # an inbound HTTP handler runs concurrently with the background loops.)
        #
        # _OwnedLock rather than a bare asyncio.Lock so a re-entry raises instead
        # of deadlocking silently, and so require_reservation_lock below can ask
        # "does *this task* hold it" rather than "does anyone".
        self.reservation_lock: _OwnedLock = _OwnedLock()

        # Serialises the batch on-demand admission run (``_run_ondemand_admission``
        # in main): the pod-watch loop fires it immediately on a newly-discovered
        # candidate while the queue-processor tick also drives it.  Only one batch
        # runs at a time; a trigger arriving mid-batch sets ``ondemand_rerun_requested``
        # so exactly one trailing batch runs afterwards, collapsing an ADDED burst
        # into at most one in-flight + one trailing pass.  Distinct from
        # ``reservation_lock`` (which the grant/admit step still takes briefly).
        self.ondemand_admission_lock: asyncio.Lock = asyncio.Lock()
        self.ondemand_rerun_requested: bool = False

    # ------------------------------------------------------------------
    # Lock contract
    # ------------------------------------------------------------------

    def require_reservation_lock(self, who: str) -> None:
        """Assert that the calling task holds ``reservation_lock``.

        Call this at the top of any helper whose docstring says "caller must
        hold the lock".  That contract used to live only in prose, and prose
        cannot be checked: a helper that silently ran unlocked would corrupt the
        reservation set under a concurrent fetch, with no symptom at the call
        site.

        Note the asymmetry with the *other* direction.  A helper that **takes**
        the lock (``_drain_pending_merge_cancels``, ``_teardown_ondemand_lease``,
        …) must **not** call this — it is precisely the function that is allowed
        to acquire, and ``_OwnedLock`` already raises if it is re-entered.
        """
        if not self.reservation_lock.held_by_current_task():
            raise LockContractError(
                f"{who} requires reservation_lock, but the calling task does not "
                "hold it"
            )

    # ------------------------------------------------------------------
    # No-show tracking
    # ------------------------------------------------------------------

    def update_noshow_tracking(
        self,
        now: datetime,
        timeout_minutes: int,
        grace_minutes: int,
        reason: str = "new",
    ) -> None:
        """Arm no-show deadlines for active user reservations not yet tracked.

        Called once after the initial reservation fetch (``reason="init"``) and
        after each subsequent refresh (``reason="new"``, the default).  For each
        active user reservation not already tracked, not already declared a
        no-show, and not claimed by a live chained holder:
        - Window not yet open: deadline = slot_start + timeout_minutes
        - Window already open (mid-window startup): deadline = now + grace_minutes
        - Window already expired: skipped

        Does not overwrite existing deadlines or resurrect declared no-shows.  At
        startup the ``noshow_reservation_ids`` / ``claimed_reservation_ids``
        guards are provably empty, so this is a superset of the old
        ``initialize_noshow_tracking`` — which was deleted so a guard added here
        can never be forgotten in a parallel copy (CODE-REVIEW D7).
        """
        for r in self.reservations:
            if r.kind != "booking" or r.user is None:
                continue
            if r.id in self.noshow_deadlines:
                continue
            if r.id in self.noshow_reservation_ids:
                continue
            if r.id in self.claimed_reservation_ids:
                continue  # a live chained holder is occupying this window
            end = slot_end(r)
            start = slot_start(r)
            if end <= now:
                continue
            if start > now:
                deadline = start + timedelta(minutes=timeout_minutes)
            else:
                deadline = now + timedelta(minutes=grace_minutes)
            self.noshow_deadlines[r.id] = deadline
            log.debug("%s", kv(
                event="noshow.armed", rid=r.id, reason=reason, deadline=deadline,
            ))

    def reconcile_noshow(self) -> None:
        """Prune no-show state for reservations no longer in the active list.

        Called after each reservation refresh alongside reconcile_queue.  An id
        leaves the active list once its controller-issued cancel has actually
        landed (or the app removed it some other way), so ``pending_noshow_cancels``
        is pruned the same way as ``noshow_reservation_ids`` — no further retry
        is needed for an id the next fetch no longer reports as active.
        """
        active_ids = {r.id for r in self.reservations}
        stale = [rid for rid in self.noshow_deadlines if rid not in active_ids]
        for rid in stale:
            self.noshow_deadlines.pop(rid)
            log.debug("%s", kv(event="noshow.deadline_pruned", rid=rid, reason="left_active_list"))
        stale_noshow = [rid for rid in self.noshow_reservation_ids if rid not in active_ids]
        for rid in stale_noshow:
            self.noshow_reservation_ids.discard(rid)
            log.info("%s", kv(event="noshow.removed", rid=rid, reason="left_active_list"))
        stale_pending = [rid for rid in self.pending_noshow_cancels if rid not in active_ids]
        for rid in stale_pending:
            self.pending_noshow_cancels.discard(rid)
            log.debug("%s", kv(event="noshow.cancel_pruned", rid=rid, reason="left_active_list"))

    def check_noshow_deadlines(self, now: datetime) -> None:
        """Declare no-shows for any reservation whose deadline has passed.

        Called at the start of each queue_processor_loop tick.  Moves expired
        entries from noshow_deadlines into noshow_reservation_ids and queues
        them in pending_noshow_cancels for a durable controller-issued cancel
        (``queue_processor_loop`` sends ``POST /api/reservations/{id}/cancel``,
        reason="no-show", later in the same tick) — that cancel is what
        actually frees the window app-side for re-booking.  A reservation
        currently claimed by a live chained holder is never declared a no-show —
        ``refresh_claimed_reservations`` clears its deadline, and this guard is a
        belt-and-suspenders check against tick ordering.
        """
        expired = [
            rid
            for rid, deadline in self.noshow_deadlines.items()
            if now >= deadline and rid not in self.claimed_reservation_ids
        ]
        for rid in expired:
            del self.noshow_deadlines[rid]
            res = next((r for r in self.reservations if r.id == rid), None)
            # Don't declare a no-show for a window that has already ended: it can
            # never be re-booked anyway, so "capacity opened" would be a
            # misleading log line (CODE-REVIEW D9).  Just drop the deadline.
            if res is not None and slot_end(res) <= now:
                log.debug("%s", kv(
                    event="noshow.skipped", rid=rid, reason="window_already_ended",
                ))
                continue
            self.noshow_reservation_ids.add(rid)
            self.pending_noshow_cancels.add(rid)
            user = res.user.username if (res and res.user) else "unknown"
            gpu_class = self.gpu_class_labels.get(res.gpu_class_id, "unknown") if res else "unknown"
            log.info("%s", kv(
                event="noshow.declared", rid=rid, user=user, clabel=gpu_class,
                reason="no_pod_before_deadline",
            ))

    def mark_pod_seen_for_noshow(
        self,
        namespace: str,
        gpu_class_label: str,
        booking_reservation_id: Optional[int] = None,
        group_label: Optional[str] = None,
    ) -> None:
        """Clear the no-show deadline(s) vouched for by an already-admitted pod.

        Called when a pod carrying the toleration is detected.  Only a
        reserved-path holder (booking ``res-<id>``) vouches for a reservation;
        when *booking_reservation_id* is that holder's id we clear the deadline
        for **every** window the holder's chained session spans
        (``reservations_claimed_by``), closing the booked-id-vs-occupied-id gap.
        On-demand / no-show squatters (other prefixes) vouch for nothing.

        Falls back — when no booking id is available (e.g. a pod tolerated by
        something other than this controller) — to clearing the soonest matching
        user reservation by ``slot_start``, the historical behaviour.
        """
        if booking_reservation_id is not None:
            res = next(
                (r for r in self.reservations if r.id == booking_reservation_id),
                None,
            )
            if res is None or res.kind != "booking" or res.user is None:
                return  # on-demand / no-show booking — not a holder
            cleared = [
                rid
                for rid in self.reservations_claimed_by(booking_reservation_id)
                if self.noshow_deadlines.pop(rid, None) is not None
            ]
            if cleared:
                log.debug("%s", kv(
                    event="noshow.cleared", ids=sorted(cleared), ns=namespace,
                    clabel=gpu_class_label, reason="holder_pod_admitted",
                ))
            return

        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and self._group_ok(r, group_label)
            and r.id in self.noshow_deadlines
        ]
        if not candidates:
            return
        best = min(candidates, key=slot_start)
        self.noshow_deadlines.pop(best.id, None)
        log.debug("%s", kv(
            event="noshow.cleared", rid=best.id, ns=namespace,
            clabel=gpu_class_label, reason="matching_pod_admitted",
        ))

    def refresh_claimed_reservations(
        self,
        holder_reservation_ids: list[int],
        now: Optional[datetime] = None,
    ) -> None:
        """Recompute ``claimed_reservation_ids`` from live reserved-path holders.

        *holder_reservation_ids* are the booking ids (``res-<id>``) of the
        Running/Pending holder pods currently in the cluster.  Each expands to
        its back-to-back chain, so every window a holder occupies — directly or
        via a chained deadline — is marked claimed.  Claimed reservations also
        have their no-show deadline cleared: a chained holder is actively using
        the window, so it must not count down toward no-show.  When the holder
        later vacates, the reservation drops out of the claimed set and the next
        ``update_noshow_tracking`` re-arms it with the grace timeout (the same
        path that recycles any vacated window).

        Called each queue-processor tick, before ``check_noshow_deadlines``.
        """
        now = now or datetime.now(timezone.utc)
        claimed: set[int] = set()
        for rid in holder_reservation_ids:
            claimed |= self.reservations_claimed_by(rid, now)
        self.claimed_reservation_ids = claimed
        for rid in claimed:
            self.noshow_deadlines.pop(rid, None)

    # ------------------------------------------------------------------
    # Reservation matching
    # ------------------------------------------------------------------

    def find_best_reservation(
        self,
        namespace: str,
        gpu_class_label: str,
        group_label: Optional[str] = None,
        gpu_requested: Optional[int] = None,
    ) -> Optional[ReservationResponse]:
        """Find the nearest upcoming (or active) reservation for *namespace* / *gpu_class_label*.

        Matching rules:
        - reservation.user.username == namespace
        - gpu_class_labels[reservation.gpu_class_id] == gpu_class_label
        - usage group satisfies REQUIRED_GROUP_LABEL (see _group_ok); *group_label*
          is the pod's group label value, ignored when the feature is disabled
        - reservation window has not yet expired

        When multiple matches exist, return the one whose window starts soonest
        -- among those big enough for the pod, when *gpu_requested* is given: a
        booking holding fewer GPUs than the pod asks for can never admit it, so
        a pod is queued for a smaller one only when the user holds nothing
        bigger (and is then told so, ``ReservationTooSmall``).
        """
        now = datetime.now(timezone.utc)
        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and self._group_ok(r, group_label)
            and slot_end(r) > now  # still has time left
            and r.id not in self.noshow_reservation_ids
        ]
        if not candidates:
            return None
        if gpu_requested is not None:
            big_enough = [r for r in candidates if r.gpu_count >= gpu_requested]
            if big_enough:
                candidates = big_enough
        return min(candidates, key=slot_start)

    def find_admittable_reservation(
        self,
        namespace: str,
        gpu_class_label: str,
        gpu_requested: int,
        now: datetime,
        horizon: timedelta,
        group_label: Optional[str] = None,
    ) -> Optional[ReservationResponse]:
        """Find the nearest reservation admittable soon, with room, for a pod.

        The JIT-routing-aware sibling of ``find_best_reservation``: that method
        answers "does any future reservation match at all" (used to preserve
        the existing wait-for-window behaviour for a pod that is not
        JIT-eligible); this one answers "is one admittable soon enough, with
        budget" — the trigger for routing a pod to the reserved queue instead
        of requesting a JIT on-demand lease. Matching rules are the same as
        ``find_best_reservation`` plus two additional requirements:
        - the window opens now or within *horizon* (``slot_start(r) <= now +
          horizon``)
        - it has spare budget for *gpu_requested* (``available(r) >=
          gpu_requested``)

        When multiple match, the one whose window starts soonest wins.
        """
        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self.gpu_class_labels.get(r.gpu_class_id) == gpu_class_label
            and self._group_ok(r, group_label)
            and slot_end(r) > now
            and r.id not in self.noshow_reservation_ids
            and slot_start(r) <= now + horizon
            and self.available(r) >= gpu_requested
        ]
        if not candidates:
            return None
        return min(candidates, key=slot_start)

    def find_open_booking_for(
        self,
        namespace: str,
        gpu_class_label: str,
        now: datetime,
        gpu_requested: int,
        extra_used: Optional[dict[int, int]] = None,
        group_label: Optional[str] = None,
    ) -> Optional[ReservationResponse]:
        """Find a *currently-open* booking *namespace* holds that can adopt a pod.

        Used to re-link an overstay pod (one running past its runtime guarantee)
        to a reservation the same user has since booked, so it is no longer a
        preemption victim.  Unlike ``find_best_reservation`` this requires the
        window to be **open right now** (``slot_start <= now < slot_end``) and to
        have spare budget for *gpu_requested*.  *extra_used* lets a caller
        planning several adoptions in one pass reserve budget it has already
        assigned but not yet recorded (mirrors the ``available``-minus-running-tally
        shape ``plan_boundary_preemption`` uses).  Among candidates the
        latest-ending window wins (longest guarantee), ties broken by most free
        capacity — *net of ``extra_used``*, so a pass spreads its pods over the
        reservations that still have room rather than packing them all onto
        whichever one started out largest.  ``r.id`` breaks a remaining tie so
        the pass is reproducible rather than resolved by list order.

        Filter and tie-break share one ``_spare`` expression deliberately: when
        they disagreed, the filter honoured the running tally while the key read
        the untouched ``available``, which is constant across a pass because the
        planners never mutate ``occupancy``.
        """
        extra_used = extra_used or {}

        def _spare(r: ReservationResponse) -> int:
            return self.available(r) - extra_used.get(r.id, 0)

        candidates = [
            r
            for r in self.reservations
            if r.kind == "booking"
            and r.user is not None
            and r.user.username == namespace
            and self._effective_label(r) == gpu_class_label
            and self._group_ok(r, group_label)
            and slot_start(r) <= now < slot_end(r)
            and r.id not in self.noshow_reservation_ids
            and _spare(r) >= gpu_requested
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda r: (slot_end(r), _spare(r), r.id))

    def near_miss_bookings(
        self,
        namespace: str,
        gpu_class_label: str,
        group_label: Optional[str],
        now: datetime,
    ) -> NearMiss:
        """*namespace*'s live bookings that a pod of *gpu_class_label* /
        *group_label* does not match, by the axis it misses on.

        Considers the bookings ``find_best_reservation`` would -- this user's,
        with time left, not a declared no-show -- and reports each that fails
        one of the two axes the pod itself supplies: the same class under a
        different usage group (only while REQUIRED_GROUP_LABEL is on), or a
        different class.  A booking whose class label cannot be resolved is
        skipped, since it cannot be described in terms the user could set.
        Names are de-duplicated and sorted, so the result -- which ends up in
        a throttled Event message -- does not flap between calls.  Pure.
        """
        other_groups: set[str] = set()
        other_classes: set[str] = set()
        for r in self.reservations:
            if (
                r.kind != "booking"
                or r.user is None
                or r.user.username != namespace
                or slot_end(r) <= now
                or r.id in self.noshow_reservation_ids
            ):
                continue
            label = self._effective_label(r)
            if label is None:
                continue
            if label != gpu_class_label:
                other_classes.add(label)
            elif not self._group_ok(r, group_label) and r.group is not None:
                other_groups.add(r.group.name)
        return NearMiss(tuple(sorted(other_groups)), tuple(sorted(other_classes)))

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    def enqueue_pod(
        self,
        pod_uid: str,
        pod_name: str,
        pod_namespace: str,
        gpu_class_label: str,
        gpu_requested: int,
        group_label: Optional[str] = None,
        reservation: Optional[ReservationResponse] = None,
    ) -> None:
        """Match pod to a reservation and add to the work queue if eligible.

        *reservation* is the one routing chose; without it the pod is matched
        here (``find_best_reservation``, preferring a booking big enough for
        it).  Routing must pass its choice: re-matching here used to pick the
        soonest booking regardless of room, so a pod routed to a booking with
        space could be pinned to a full one instead and wait there until its
        window ended.

        If the pod is already queued for the same reservation, this is a no-op
        (idempotent).  If the reservation changes (e.g. old one cancelled), the
        entry is replaced, keeping what routing recorded about it
        (``ondemand_ineligibility``).  *group_label* is the pod's
        REQUIRED_GROUP_LABEL value (ignored when the feature is disabled); it is
        carried on the QueueEntry so reconcile_queue can re-match with the same
        group constraint.
        """
        if reservation is None:
            reservation = self.find_best_reservation(
                pod_namespace, gpu_class_label, group_label, gpu_requested
            )
        if reservation is None:
            return  # no matching reservation; nothing to do

        # A pod appeared for this reservation — clear its no-show deadline.
        self.noshow_deadlines.pop(reservation.id, None)

        existing = self.task_queue.get(pod_uid)
        if existing and existing.reservation.id == reservation.id:
            return  # already queued for the same reservation

        entry = QueueEntry(
            pod_uid=pod_uid,
            pod_name=pod_name,
            pod_namespace=pod_namespace,
            gpu_class_label=gpu_class_label,
            gpu_requested=gpu_requested,
            reservation=reservation,
            next_attempt_at=datetime.now(timezone.utc),
            group_label=group_label,
            ondemand_ineligibility=existing.ondemand_ineligibility if existing else (),
        )
        self.task_queue[pod_uid] = entry
        log.info("%s", kv(
            event="pod.enqueued", ns=pod_namespace, pod=pod_name, rid=reservation.id,
            start=slot_start(reservation), end=slot_end(reservation),
            reserved=reservation.gpu_count, gpus=gpu_requested,
        ))

    def dequeue_pod(self, pod_uid: str) -> None:
        """Remove a pod from the work queue (e.g. pod deleted, or toleration applied)."""
        if pod_uid in self.task_queue:
            entry = self.task_queue.pop(pod_uid)
            log.debug("%s", kv(
                event="pod.dequeued", ns=entry.pod_namespace, pod=entry.pod_name,
                poduid=pod_uid,
            ))

    def _chain_for(
        self,
        reservation: ReservationResponse,
        now: datetime,
    ) -> list[ReservationResponse]:
        """Return the back-to-back chain *following* *reservation*, in window order.

        A reservation extends the chain when it shares the same namespace, GPU
        class, and gpu_count, has not yet expired, is not a no-show, and starts
        exactly when the previous window ends (``slot_start(next) ==
        slot_end(previous)``).  When REQUIRED_GROUP_LABEL is enabled it must also
        share the same usage group (``group_id``), so a guarantee never chains
        across a different group's window.  *reservation* itself is **not**
        included.

        Shared by ``compute_guaranteed_until`` (runtime-guarantee arithmetic) and
        ``reservations_claimed_by`` (no-show protection) so both agree on what a
        single holder pod's session spans.
        """
        if reservation.user is None:
            return []
        namespace = reservation.user.username
        # Both sides of the class comparison are resolved here, unlike the
        # ``find_*`` matchers where the left side is the pod's own label (always
        # a real string).  A bare ``gpu_class_labels.get()`` therefore yields
        # ``None == None`` for two *different* classes whose labels are both
        # unresolvable, chaining a guarantee across them.  ``_effective_label``
        # is the resolver that falls back to the label embedded in the
        # reservation; an unresolvable anchor chains nothing at all, matching
        # how ``boundary_demand`` skips a reservation it cannot classify.
        gpu_class_label = self._effective_label(reservation)
        if gpu_class_label is None:
            log.debug("%s", kv(
                event="guarantee.chain_skipped", rid=reservation.id,
                reason="no_resolvable_class_label",
            ))
            return []
        gpu_count = reservation.gpu_count

        candidates = sorted(
            [
                r
                for r in self.reservations
                if r.id != reservation.id
                and r.kind == "booking"
                and r.user is not None
                and r.user.username == namespace
                and self._effective_label(r) == gpu_class_label
                and r.gpu_count == gpu_count
                and (
                    self.required_group_label is None
                    or r.group_id == reservation.group_id
                )
                and slot_end(r) > now
                and r.id not in self.noshow_reservation_ids
            ],
            key=slot_start,
        )

        chain: list[ReservationResponse] = []
        prev_end = slot_end(reservation)
        visited: set[int] = set()
        while True:
            # If several reservations abut the same instant (shouldn't happen in
            # this data model), take the longest, mirroring the same tiebreak
            # find_open_booking_for uses.
            abutting = [
                r
                for r in candidates
                if r.id not in visited and slot_start(r) == prev_end
            ]
            if not abutting:
                break
            next_res = max(abutting, key=slot_end)
            chain.append(next_res)
            prev_end = slot_end(next_res)
            visited.add(next_res.id)
        return chain

    def compute_guaranteed_until(
        self,
        now: datetime,
        current_reservation: ReservationResponse,
    ) -> datetime:
        """Return the absolute UTC end of the runtime guarantee for a reserved-path
        admission under *current_reservation*.

        The guarantee is the end of *current_reservation*'s window, extended
        across any back-to-back future reservations that share the same
        namespace, GPU class, and gpu_count (see ``_chain_for``).  *now* is
        used only to select which future reservations still qualify as
        "future" — the result is an absolute instant, which callers write to
        the pod as an informational annotation.  No flooring: an absolute end
        may equal or even (in a stale-data race) precede *now*; callers
        convert to a duration in seconds where a positive value is required.
        """
        chain = self._chain_for(current_reservation, now)
        return slot_end(chain[-1]) if chain else slot_end(current_reservation)

    def guarantee_end(
        self,
        reservation_id: Optional[int],
        *,
        now: datetime,
    ) -> Optional[datetime]:
        """Return the absolute UTC end of the runtime guarantee for a pod admitted
        under *reservation_id*, or ``None`` if no live guarantee can be resolved.

        The guarantee is live and restart-safe: it is recomputed from current
        window arithmetic on every call rather than frozen at admission or read
        back from the pod, so a pod's guarantee can grow after admission (an
        abutting follow-on booking), which a fixed Kubernetes deadline cannot
        express.

        Looks up *reservation_id* in ``self.reservations`` and returns the end
        of its back-to-back chain (``compute_guaranteed_until``).  ``None`` if
        the reservation is no longer in the active list — its window is over
        and the pod is unconditionally past guarantee — or if
        *reservation_id* is ``None`` (a pod not admitted by this controller, or
        whose booking-reference does not parse): it is never "ours" to
        guarantee, and it is never a preemption victim.
        """
        if reservation_id is None:
            return None
        res = next((r for r in self.reservations if r.id == reservation_id), None)
        if res is None:
            return None
        return self.compute_guaranteed_until(now, res)

    def reservations_claimed_by(
        self,
        reservation_id: int,
        now: Optional[datetime] = None,
    ) -> set[int]:
        """Return the set of reservation ids a holder booked under *reservation_id*
        occupies: the reservation itself plus its back-to-back chain.

        Used to protect every window a single chained holder pod spans from being
        declared a no-show or lent out as on-demand capacity, closing the
        booked-id-vs-occupied-id gap.
        """
        now = now or datetime.now(timezone.utc)
        res = next((r for r in self.reservations if r.id == reservation_id), None)
        claimed = {reservation_id}
        if res is not None and res.kind == "booking" and res.user is not None:
            claimed |= {r.id for r in self._chain_for(res, now)}
        return claimed

    def reconcile_queue(self) -> None:
        """Re-validate queue entries against the current reservation list.

        Called after each reservation refresh.  Entries whose reservation was
        cancelled are removed and, if a new matching reservation exists, the pod
        is re-queued for it.  Surviving entries have their captured reservation
        object swapped for the freshly loaded one of the same id: ``reservations``
        is replaced wholesale each cycle, and this is the only place a
        wholesale-replaced object would otherwise be retained stale (CODE-REVIEW
        D9).
        """
        by_id = {r.id: r for r in self.reservations}
        active_ids = set(by_id)
        stale_uids = [
            uid
            for uid, entry in self.task_queue.items()
            if entry.reservation.id not in active_ids
        ]
        # Re-point surviving entries at the fresh reservation object.
        for entry in self.task_queue.values():
            fresh = by_id.get(entry.reservation.id)
            if fresh is not None:
                entry.reservation = fresh
        for uid in stale_uids:
            entry = self.task_queue.pop(uid)
            new_res = self.find_best_reservation(
                entry.pod_namespace, entry.gpu_class_label, entry.group_label,
                entry.gpu_requested,
            )
            if new_res:
                self.task_queue[uid] = QueueEntry(
                    pod_uid=uid,
                    pod_name=entry.pod_name,
                    pod_namespace=entry.pod_namespace,
                    gpu_class_label=entry.gpu_class_label,
                    gpu_requested=entry.gpu_requested,
                    reservation=new_res,
                    next_attempt_at=datetime.now(timezone.utc),
                    group_label=entry.group_label,
                    ondemand_ineligibility=entry.ondemand_ineligibility,
                )
                log.info("%s", kv(
                    event="pod.requeued", ns=entry.pod_namespace, pod=entry.pod_name,
                    reason="reservation_cancelled",
                    **{"old.rid": entry.reservation.id, "new.rid": new_res.id},
                ))
            else:
                log.info("%s", kv(
                    event="pod.queue_dropped", ns=entry.pod_namespace,
                    pod=entry.pod_name, rid=entry.reservation.id,
                    reason="reservation_cancelled_no_replacement",
                ))

    # ------------------------------------------------------------------
    # On-demand candidate management
    # ------------------------------------------------------------------

    def add_ondemand_candidate(
        self,
        pod_uid: str,
        pod_name: str,
        pod_namespace: str,
        gpu_class_label: str,
        gpu_requested: int,
        min_runtime_seconds: int,
        pod_created_at: datetime,
        group_label: Optional[str] = None,
        usage_group: Optional[str] = None,
        best_effort: bool = False,
        usage_group_source: Optional[str] = None,
    ) -> None:
        """Register a pod as a JIT on-demand candidate (idempotent).

        If the pod is already registered with the same parameters this is a
        no-op.  A pod already tracked in ``occupancy`` (already placed) is also
        left untouched.  *usage_group* is the group name the ask will carry
        (see ``OnDemandCandidate.usage_group``), and *usage_group_source* where
        it came from (see ``OnDemandCandidate.usage_group_source``).

        *best_effort* marks a pod that declared ``galends/runtime-guarantee:
        none``: it is admitted under a zero-length, zero-SU stub rather than a
        guaranteed lease (see ``OnDemandCandidate.best_effort``).
        """
        # Already placed — don't re-add as a candidate.
        for occupants in self.occupancy.values():
            if pod_uid in occupants:
                return

        if pod_uid in self.ondemand_candidates:
            return  # already registered

        candidate = OnDemandCandidate(
            pod_uid=pod_uid,
            pod_name=pod_name,
            pod_namespace=pod_namespace,
            gpu_class_label=gpu_class_label,
            gpu_requested=gpu_requested,
            min_runtime_seconds=min_runtime_seconds,
            pod_created_at=pod_created_at,
            next_attempt_at=datetime.now(timezone.utc),
            group_label=group_label,
            usage_group=usage_group,
            usage_group_source=usage_group_source,
            best_effort=best_effort,
        )
        self.ondemand_candidates[pod_uid] = candidate
        log.info("%s", kv(
            event="ondemand.candidate_added", ns=pod_namespace, pod=pod_name,
            poduid=pod_uid, clabel=gpu_class_label, gpus=gpu_requested,
            # min_runtime_s is meaningless for a best-effort candidate (it sizes
            # nothing), so it is omitted rather than printed as a misleading 0.
            min_runtime_s=None if best_effort else min_runtime_seconds,
            group=usage_group,
            best_effort=best_effort or None,
        ))

    def remove_ondemand_candidate(self, pod_uid: str) -> None:
        """Remove a pod from the on-demand candidate list (e.g. pod deleted)."""
        if pod_uid in self.ondemand_candidates:
            c = self.ondemand_candidates.pop(pod_uid)
            log.debug("%s", kv(
                event="ondemand.candidate_removed", ns=c.pod_namespace,
                pod=c.pod_name, poduid=pod_uid,
            ))

    def plan_ondemand_gates(self, now: datetime) -> list[OnDemandGate]:
        """Every class-wide gate currently pausing JIT on-demand admission.

        Reads the same state ``main._preflight_ondemand_candidate`` consults —
        ``class_node_counts`` (guard 1b), ``stuck_holder_gpu_classes`` (guard 3)
        and ``overcommitted_gpu_classes`` (guard 4) — so the warning can never
        describe a gate the preflight is not actually applying, including guard
        1b's fail-open (only a *known* zero counts) and guard 4's best-effort
        exemption (a best-effort candidate is not counted as held by it).

        Side effect: maintains ``ondemand_gate_since`` — a gate seen for the
        first time is stamped *now*, and a gate no longer in force is pruned,
        so one that clears and later recurs is re-dated.  Sorted by (label,
        guard) so consecutive warnings line up in the log.
        """
        def waiting(label: str, *, count_best_effort: bool = True) -> int:
            return sum(
                1 for c in self.ondemand_candidates.values()
                if c.gpu_class_label == label
                and (count_best_effort or not c.best_effort)
            )

        gates: list[OnDemandGate] = []

        def add(label: str, guard: int, reason: str, **evidence) -> None:
            since = self.ondemand_gate_since.setdefault((label, guard), now)
            gates.append(OnDemandGate(
                label=label, guard=guard, reason=reason, since=since, **evidence,
            ))

        for label, nodes in self.class_node_counts.items():
            if nodes == 0:
                add(label, 1, "no_class_nodes", waiting=waiting(label))
        for label in self.stuck_holder_gpu_classes:
            add(
                label, 3, "stuck_holder_interlock", waiting=waiting(label),
                stuck_pods=tuple(sorted(self.stuck_holder_pods.get(label, ()))),
            )
        for label in self.overcommitted_gpu_classes:
            add(
                label, 4, "class_overcommitted",
                waiting=waiting(label, count_best_effort=False),
                app_gpus=self.gpu_class_capacity.get(label),
                phys_gpus=self.physical_gpu_capacity.get(label),
            )

        live = {(g.label, g.guard) for g in gates}
        for key in list(self.ondemand_gate_since):
            if key not in live:
                del self.ondemand_gate_since[key]
        return sorted(gates, key=lambda g: (g.label, g.guard))

    # ------------------------------------------------------------------
    # What a still-pending pod's owner has been told
    # ------------------------------------------------------------------

    def pending_status_due(
        self,
        pod_uid: str,
        topic: str,
        key: tuple[str, str],
        now: datetime,
        repeat: timedelta,
    ) -> bool:
        """Whether telling *pod_uid*'s owner *key* on *topic* would be news.

        True when nothing has been told on that topic yet, when the last thing
        told differs from *key* (a changed status is always reported), or when
        it was told at least *repeat* ago -- Events expire, so a pod still held
        for the same reason must say so again.  A zero *repeat* is always due.
        """
        told = self.pending_status.get(pod_uid, {}).get(topic)
        return told is None or told.key != key or now - told.at >= repeat

    def record_pending_status(
        self, pod_uid: str, topic: str, key: tuple[str, str], now: datetime
    ) -> None:
        """Remember that *key* was told to *pod_uid*'s owner on *topic* at *now*.

        Called only once the Event write has succeeded, so a failed write is
        retried on the next attempt instead of being suppressed as a repeat.
        """
        self.pending_status.setdefault(pod_uid, {})[topic] = PendingStatus(key, now)

    def forget_pending_status(self, pod_uid: str) -> None:
        """Drop everything told to *pod_uid*'s owner (the pod is admitted or gone)."""
        self.pending_status.pop(pod_uid, None)

    def prune_pending_status(self, cutoff: datetime) -> None:
        """Forget pods last told anything before *cutoff* that no path tracks.

        A pod deleted while the watch was down never produces a DELETED event
        (a re-LIST replays only the pods that still exist), so its entry would
        otherwise be kept forever.  A pod that *is* still pending re-reports on
        every repeat interval, which keeps its newest ``at`` recent; one on the
        candidate list or the reserved queue is kept regardless.  Pruning a live
        pod by mistake costs one early restatement, never a missed one.
        """
        for uid in list(self.pending_status):
            if uid in self.ondemand_candidates or uid in self.task_queue:
                continue
            if all(told.at < cutoff for told in self.pending_status[uid].values()):
                del self.pending_status[uid]

    # ------------------------------------------------------------------
    # Occupancy: availability, placement, release, reconciliation
    # ------------------------------------------------------------------

    def available(
        self, reservation: ReservationResponse, exclude_uid: Optional[str] = None
    ) -> int:
        """Return the GPUs still free on *reservation*, across all kinds.

        Counts every pod recorded against *reservation* in the unified occupancy
        map.  *exclude_uid* omits one pod (the one being evaluated) so a retry
        does not count itself.
        """
        used = sum(
            g
            for uid, g in self.occupancy.get(reservation.id, {}).items()
            if uid != exclude_uid
        )
        return max(0, reservation.gpu_count - used)

    def reservation_holders(
        self, reservation_id: int, namespace: str, exclude_uid: Optional[str] = None
    ) -> tuple[tuple[str, ...], int]:
        """The pods occupying *reservation_id*: names of those in *namespace*
        (sorted), and how many others there are.

        A pod waiting on a full reservation is told which of its owner's pods
        hold it, so only pods in the waiting pod's own namespace are named --
        normally every holder is, since a reservation matches only its owner's
        namespace, but a pod left behind by an ownership change is not, and
        its name is not the new owner's business.  A holder the directory has
        no name for yet (admitted since the last snapshot by a path that does
        not record one) is counted, not named.  *exclude_uid* omits the pod
        being evaluated, as in ``available``.  Sorted so the result, which ends
        up in a throttled Event message, does not flap between calls.
        """
        names: list[str] = []
        others = 0
        for uid in self.occupancy.get(reservation_id, {}):
            if uid == exclude_uid:
                continue
            holder = self.holder_names.get(uid)
            if holder is not None and holder[0] == namespace:
                names.append(holder[1])
            else:
                others += 1
        return tuple(sorted(names)), others

    def _effective_label(self, r: ReservationResponse) -> Optional[str]:
        """Return *r*'s GPU-class label: the cached value, else the label_value
        embedded in the reservation's ``gpu_class`` (populated for cancelled
        blocks whose class dropped out of the active-reservation cache)."""
        cached = self.gpu_class_labels.get(r.gpu_class_id)
        if cached is not None:
            return cached
        return r.gpu_class.label_value if r.gpu_class else None

    def _group_ok(
        self, r: ReservationResponse, group_label: Optional[str]
    ) -> bool:
        """Whether *r*'s usage group satisfies the REQUIRED_GROUP_LABEL gate.

        When the feature is disabled (``required_group_label is None``) this is
        always True — no constraint.  When enabled, *group_label* is the value
        of the pod's group label; it must equal ``r.group.name``.  A pod with no
        such label (``group_label is None`` while enabled) matches no grouped
        reservation, so it is never admitted on the reserved path (it falls
        through to the group-agnostic on-demand path)."""
        if self.required_group_label is None:
            return True
        return r.group is not None and r.group.name == group_label

    def record_placement(
        self, reservation_id: int, pod_uid: str, gpu_count: int
    ) -> None:
        """Record that *pod_uid* occupies *gpu_count* GPUs on *reservation_id*.

        Idempotent by pod uid.  Used by every admission path (reserved,
        on-demand, no-show) and by occupancy reconstruction.
        """
        if reservation_id not in self.occupancy:
            self.occupancy[reservation_id] = {}
        self.occupancy[reservation_id][pod_uid] = gpu_count
        log.debug("%s", kv(
            event="occupancy.placed", rid=reservation_id, poduid=pod_uid,
            gpus=gpu_count, free=self.available_by_id(reservation_id),
            reserved=self._reservation_gpu_count(reservation_id),
        ))

    def release_pod(self, pod_uid: str) -> Optional[int]:
        """Remove *pod_uid* from whatever reservation it occupies.

        Returns the reservation id it was released from, or ``None`` if the pod
        was not tracked (e.g. a candidate that was never placed).
        """
        for reservation_id, occupants in self.occupancy.items():
            if pod_uid in occupants:
                gpu_count = occupants.pop(pod_uid)
                log.info("%s", kv(
                    event="occupancy.released", rid=reservation_id, poduid=pod_uid,
                    gpus=gpu_count,
                ))
                # Clean up empty dicts to keep the map tidy.
                if not occupants:
                    del self.occupancy[reservation_id]
                return reservation_id
        return None

    def relink_occupancy(
        self, pod_uid: str, new_reservation_id: int, gpu_count: int
    ) -> None:
        """Move *pod_uid*'s occupancy to *new_reservation_id* (release + record).

        Used when an overstay pod is adopted into a reservation the same user
        has since booked: the pod is unlinked from whatever id it was recorded
        under and recorded against the new one, so budget accounting and
        ``guarantee_end`` immediately reflect the new binding.
        """
        self.release_pod(pod_uid)
        self.record_placement(new_reservation_id, pod_uid, gpu_count)

    def available_by_id(self, reservation_id: int) -> int:
        """GPU capacity remaining for *reservation_id* (0 if unknown)."""
        gpu_count = self._reservation_gpu_count(reservation_id)
        if gpu_count == 0:
            return 0
        used = sum(self.occupancy.get(reservation_id, {}).values())
        return max(0, gpu_count - used)

    def _reservation_gpu_count(self, reservation_id: int) -> int:
        """Return gpu_count for a reservation by id, or 0 if not found."""
        for r in self.reservations:
            if r.id == reservation_id:
                return r.gpu_count
        return 0

    # ------------------------------------------------------------------
    # Cancellation and owner-change detection
    # ------------------------------------------------------------------

    def detect_cancelled_in_window(
        self,
        all_reservations: list[ReservationResponse],
        now: datetime,
    ) -> list[ReservationResponse]:
        """Identify user reservations that were cancelled while their window was open.

        Scans *all_reservations* (the full ``status=all`` API response) for records
        with ``status == "cancelled"`` whose window has not yet closed.  A cancelled
        reservation is returned when:
        - its ``status`` is ``"cancelled"``, AND
        - its window has not yet ended (``slot_end > now``), AND
        - it was not already declared a no-show, AND
        - it was still in ``self.reservations`` (the active set as of the last
          reconcile).  Must be called *before* ``state.reservations`` is replaced
          with the new snapshot (mirrors ``detect_owner_changed_in_window``): this
          is what makes eviction idempotent across cycles — a handled cancellation
          drops out of the active set and is never re-added, so it fails this
          check on every subsequent fetch without needing a separate "already
          handled" record.

        Returns the list of ``ReservationResponse`` objects (with ``cancelled_by``
        already populated from the API) for every reservation that meets all criteria.
        """
        prior_active_ids = {r.id for r in self.reservations}
        return [
            r for r in all_reservations
            if r.status == "cancelled"
            and slot_end(r) > now
            and r.id not in self.noshow_reservation_ids
            and r.id in prior_active_ids
        ]

    def detect_owner_changed_in_window(
        self,
        incoming: list[ReservationResponse],
        now: datetime,
    ) -> list[tuple[ReservationResponse, str]]:
        """Identify in-progress reservations whose owner changed ("adoption").

        The reservation app can reassign a booking to a new owner (a teammate)
        while it stays ``status == "active"``.  Because ownership is coupled to
        the Kubernetes namespace (``pod.namespace == reservation.user.username``),
        the pod already admitted under the reservation lives in the *prior*
        owner's namespace and can no longer be legitimately matched to it.

        Compares each incoming active booking against the reservation of the same
        id **currently stored** in ``self.reservations`` (this must be called
        *before* the wholesale replace, while the old owner is still visible).  A
        reservation is returned as ``(new_reservation, prior_username)`` when:
        - incoming ``status == "active"``, ``kind == "booking"``, ``user`` set, AND
        - a prior reservation of the same id exists with ``user``/``user_id`` set, AND
        - the owner changed (``prior.user_id != r.user_id``), AND
        - it is in progress: ``slot_start(r) <= now < slot_end(r)``.

        *prior_username* (the old ``user.username``, i.e. the old namespace) is
        what lets the caller locate the pod "under the prior name/namespace".
        """
        prior_by_id = {r.id: r for r in self.reservations}
        changes: list[tuple[ReservationResponse, str]] = []
        for r in incoming:
            if r.status != "active" or r.kind != "booking" or r.user is None:
                continue
            prior = prior_by_id.get(r.id)
            if prior is None or prior.user is None or prior.user_id is None:
                continue
            if prior.user_id == r.user_id:
                continue
            if not (slot_start(r) <= now < slot_end(r)):
                continue
            changes.append((r, prior.user.username))
        return changes

    def preserve_local_ondemand_leases(
        self,
        fetched: list[ReservationResponse],
        now: datetime,
    ) -> list[ReservationResponse]:
        """Return locally-granted on-demand leases this fetch snapshot omits.

        The JIT lease path upserts a freshly-granted lease into
        ``self.reservations`` and admits its pod the same instant
        (``main._try_request_lease``).  But a periodic fetch issues its
        ``GET /api/reservations`` *before* taking ``reservation_lock``; if a lease
        is granted in the gap between that snapshot and the wholesale
        ``state.reservations = …`` replace, the snapshot predates the lease and the
        replace would drop it — even though its pod is live and well within its
        guarantee.  With the lease gone, ``guarantee_end`` resolves to ``None`` and
        the pod is wrongly treated as past-guarantee, making it eligible for
        boundary preemption until the next fetch re-includes it (a window bounded
        by ``RESERVATION_FETCH_INTERVAL``).

        This returns the leases the caller must re-add to the incoming active set.
        A lease is preserved only when **all** hold:
        - it is one of ours: ``kind == "on_demand"`` and still ``status ==
          "active"`` in local state, AND
        - its window has not yet closed (``slot_end > now``) — an ended lease is
          legitimately past guarantee, so there is nothing left to protect, AND
        - *fetched* does not mention its id at all.  This checks *fetched* (the full
          ``status=all`` response), not just its active subset: a lease the app
          reports as **cancelled** carries its id in *fetched* and is therefore
          **not** preserved, so a genuine server-side cancellation (including the
          controller's own compensating cancel) is respected and the pod released.
          Only a lease the app has simply not surfaced yet (replication lag between
          grant and snapshot) is bridged.

        A ``kind="best_effort"`` stub is deliberately **not** preserved, and fails
        on two counts (kind, and a window that closed the instant it opened).
        Nothing needs protecting: its pod has no guarantee to lose, so resolving
        to ``None`` after the fetch drops it reaches exactly the same conclusion
        the stub itself does — past guarantee, preemptible.  The app also omits
        these rows from the poll by default, so the common case is that they were
        never in *fetched* to begin with.

        Must be called *before* ``state.reservations`` is replaced (it reads the
        current local set), mirroring the other fetch-time detectors.  Pure — no
        I/O, no state mutation.
        """
        fetched_ids = {r.id for r in fetched}
        return [
            r
            for r in self.reservations
            if r.kind == "on_demand"
            and r.status == "active"
            and r.id not in fetched_ids
            and slot_end(r) > now
        ]

    def reconcile_occupancy(self, placements: list[tuple[int, str, int]]) -> None:
        """Rebuild the occupancy map from a live cluster snapshot.

        *placements* is ``(reservation_id, pod_uid, gpu_count)`` for every live
        (Running/Pending) tolerated pod, bucketed by the reservation id parsed
        from its booking-reference.  Rebuilding wholesale is self-healing: a pod
        deleted during a watch disconnect is dropped here even if its DELETE
        event was missed.

        Called each queue-processor tick.  An optimistic record made between
        ticks whose patch is not yet visible in the snapshot may be briefly
        dropped; the placing coroutine has already committed and the next tick
        re-captures it, so the window is bounded by the tick interval.
        """
        old_total = sum(g for pods in self.occupancy.values() for g in pods.values())
        rebuilt: dict[int, dict[str, int]] = {}
        for reservation_id, pod_uid, gpu_count in placements:
            rebuilt.setdefault(reservation_id, {})[pod_uid] = gpu_count
        self.occupancy = rebuilt
        new_total = sum(g for pods in rebuilt.values() for g in pods.values())
        n_res = len(rebuilt)
        if new_total != old_total:
            log.info("%s", kv(
                event="occupancy.reconciled", reservations=n_res,
                **{"new.used": new_total, "old.used": old_total},
            ))
        else:
            log.debug("%s", kv(
                event="occupancy.reconciled", reservations=n_res, used=new_total,
            ))

    # ------------------------------------------------------------------
    # Preemption planning (demand-driven capacity recovery)
    # ------------------------------------------------------------------

    def upcoming_boundaries(self, now: datetime, lead: timedelta) -> list[datetime]:
        """Return distinct booking start times within *lead* of *now*, ascending.

        A boundary ``S`` is in scope while ``now - lead < S <= now + lead``: it
        enters phase A (proactive) as soon as it is within the lead window
        before it opens, and stays in scope through phase B (at-boundary,
        ``S <= now``) for one more *lead*-sized window after it opens — long
        enough for a delayed sweep or a post-restart re-evaluation to still
        catch it.  Data-driven (distinct ``slot_start`` values of active
        bookings): hour alignment and DST are the reservation app's concern,
        not this arithmetic's.
        """
        boundaries = {
            slot_start(r)
            for r in self.reservations
            if r.kind == "booking" and now - lead < slot_start(r) <= now + lead
        }
        return sorted(boundaries)

    def _past_guarantee(self, view: PodRuntimeView, now: datetime) -> bool:
        """Return True if *view*'s runtime guarantee is over (or unresolvable).

        Shared by ``plan_boundary_preemption`` (victim eligibility) and
        ``plan_pod_adoptions`` (rescue eligibility) so both agree on what
        "overstay" means: ``guarantee_end`` is ``None`` (not ours / no longer
        active) or ``<= now``.
        """
        end = self.guarantee_end(view.reservation_id, now=now)
        return end is None or end <= now

    def plan_pod_adoptions(
        self,
        pods: list[PodRuntimeView],
        now: datetime,
    ) -> list[tuple[PodRuntimeView, ReservationResponse]]:
        """Plan (but do not execute) re-links of overstay pods to a user's open booking.

        A pod is rescued once **past its runtime guarantee** (``_past_guarantee``)
        — there is no reason to touch a pod already correctly tied to its own
        live booking. This rescues a pod whose user re-booked capacity the
        existing back-to-back chaining cannot reach (a non-abutting follow-on
        window or a different ``gpu_count``) before the preemption sweep would
        reclaim it.

        Requires the pod to be controller-admitted (``reservation_id`` set),
        physically present (``node_resident``, not ``terminating``,
        ``gpu_count > 0``), past its runtime guarantee, and a currently-open
        booking the same user holds with spare budget (``find_open_booking_for``)
        that differs from the pod's current reservation.

        Budget is respected across the pass: a running per-reservation tally is
        subtracted from ``available`` so several adopted pods cannot over-book
        one reservation. Pure — no I/O, no state mutation; the caller performs
        the annotation patch and occupancy re-home.
        """
        assignments: list[tuple[PodRuntimeView, ReservationResponse]] = []
        extra_used: dict[int, int] = {}
        for p in pods:
            if (
                p.reservation_id is None
                or not p.node_resident
                or p.terminating
                or p.gpu_count <= 0
            ):
                continue
            if not self._past_guarantee(p, now):
                continue
            res = self.find_open_booking_for(
                p.namespace,
                p.gpu_class,
                now,
                p.gpu_count,
                extra_used=extra_used,
                group_label=p.group_label,
            )
            if res is None or res.id == p.reservation_id:
                continue
            assignments.append((p, res))
            extra_used[res.id] = extra_used.get(res.id, 0) + p.gpu_count
        return assignments

    def plan_ondemand_merges(
        self,
        pods: list[PodRuntimeView],
        now: datetime,
    ) -> list[tuple[PodRuntimeView, ReservationResponse]]:
        """Plan (but do not execute) merges of JIT-lease pods into an open booking.

        The proactive counterpart to ``plan_pod_adoptions``: when a user starts a
        pod *before* their booked window opens, the controller admits it under a
        just-in-time on-demand **lease** (``kind="on_demand"``).  Once that user's
        matching **booking** window opens, the lease and the booking are two
        reservations covering the same job — double-holding capacity and
        double-charging SU on any overlap — until the pod happens to fall past its
        lease guarantee and ``plan_pod_adoptions`` re-links it.  This planner
        re-links such a pod as soon as the booking is open, **without** waiting
        for the lease guarantee to lapse (the one intended difference from
        ``plan_pod_adoptions``), so the caller can retire the lease immediately.

        A pod qualifies when it is controller-admitted, physically present
        (``node_resident``, not ``terminating``, ``gpu_count > 0``), its current
        reservation is a live ``kind="on_demand"`` lease, and the same user holds
        a currently-open booking with spare budget (``find_open_booking_for`` —
        which checks *budget*, not equal ``gpu_count``, so a pod using **fewer**
        GPUs than the booking reserved still merges).  A pod whose current
        reservation is already a booking, or that has no open booking to merge
        into, is left untouched (it stays on its lease — correctly — until the
        booking opens).

        A ``kind="best_effort"`` pod is likewise left to adoption rather than
        merged, and correctly so: merge exists to re-link a pod *before* its
        guarantee lapses, and a best-effort pod's already has — so
        ``plan_pod_adoptions`` picks it up on the same tick and re-links it onto
        the booking anyway (the free upgrade from unguaranteed to guaranteed).
        Merge's other half would also be a no-op: there is no lease to retire
        penalty-exempt, because a stub holds nothing and cost nothing.

        Budget is respected across the pass with a running per-reservation tally
        (mirrors ``plan_pod_adoptions``).  Pure — no I/O, no state mutation; the
        caller performs the annotation patch, occupancy re-home, and lease cancel.
        """
        assignments: list[tuple[PodRuntimeView, ReservationResponse]] = []
        extra_used: dict[int, int] = {}
        for p in pods:
            if (
                p.reservation_id is None
                or not p.node_resident
                or p.terminating
                or p.gpu_count <= 0
            ):
                continue
            current = next(
                (r for r in self.reservations if r.id == p.reservation_id), None
            )
            if current is None or current.kind != "on_demand":
                continue
            res = self.find_open_booking_for(
                p.namespace,
                p.gpu_class,
                now,
                p.gpu_count,
                extra_used=extra_used,
                group_label=p.group_label,
            )
            if res is None or res.id == p.reservation_id:
                continue
            assignments.append((p, res))
            extra_used[res.id] = extra_used.get(res.id, 0) + p.gpu_count
        return assignments

    def boundary_demand(
        self,
        boundary: datetime,
        pods: list[PodRuntimeView],
        now: datetime,
    ) -> dict[str, int]:
        """Return GPUs still needed per GPU-class label for bookings starting at *boundary*.

        For each active ``kind == "booking"`` reservation whose window starts
        exactly at *boundary*, the demand is its remaining budget
        (``available``) minus the GPUs already supplied by a live reserved-path
        holder whose back-to-back chain covers it (``reservations_claimed_by``)
        — a chained holder needs no new capacity for the window it already
        physically occupies.  Subtracting per-reservation rather than excluding
        a whole claimed reservation from demand avoids under-counting a
        partially-chained multi-GPU reservation (a 1-GPU holder chaining into a
        4-GPU booking still leaves 3 GPUs of genuine demand).  A no-show
        reservation contributes no demand — its capacity is already on-demand
        pool territory. Reservations whose GPU-class label cannot be resolved
        are skipped (logged at debug).
        """
        chained_gpus: dict[int, int] = {}
        for p in pods:
            if (
                not p.node_resident
                or p.terminating
                or p.reservation_id is None
            ):
                continue
            for rid in self.reservations_claimed_by(p.reservation_id, now):
                chained_gpus[rid] = chained_gpus.get(rid, 0) + p.gpu_count

        demand: dict[str, int] = {}
        for r in self.reservations:
            if r.kind != "booking" or r.user is None:
                continue
            if slot_start(r) != boundary:
                continue
            if r.id in self.noshow_reservation_ids:
                continue
            label = self._effective_label(r)
            if label is None:
                log.debug("%s", kv(
                    event="preempt.demand_skipped", rid=r.id,
                    reason="no_resolvable_class_label",
                ))
                continue
            need = max(0, self.available(r) - chained_gpus.get(r.id, 0))
            if need:
                demand[label] = demand.get(label, 0) + need
        return demand

    def plan_boundary_candidates(
        self,
        boundary: datetime,
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        now: datetime,
    ) -> BoundaryPreemptionNeed:
        """Compute *boundary*'s shortfall and eligible victim pool per GPU class.

        Per GPU class: ``kills_needed = max(0, demand - free)``.  Eligible
        candidates are pods of that class admitted by this controller
        (``reservation_id`` set), physically running or about to
        (``node_resident``), not already ``terminating``, requesting at least
        one GPU, and past their runtime guarantee (``guarantee_end`` is
        ``None`` — unresolvable, treated as already over — or ``<= now``).  A
        pod within its guarantee is never a candidate, regardless of shortfall.
        Only classes with a non-zero shortfall are included (their candidate
        list may still be empty → an unmet shortfall).  Pure — no I/O, no state
        mutation, and **no victim selection**: choosing which candidates to
        preempt is left to the caller (delegated to the reservation app, or the
        local random fallback ``select_victims_locally``).
        """
        demand = self.boundary_demand(boundary, pods, now)
        free = free_capacity_by_class(capacity_by_class, pods)

        kills_needed: dict[str, int] = {}
        candidates: dict[str, list[PodRuntimeView]] = {}
        for gpu_class, need in demand.items():
            shortfall = max(0, need - free.get(gpu_class, 0))
            if shortfall == 0:
                continue
            kills_needed[gpu_class] = shortfall
            candidates[gpu_class] = [
                p
                for p in pods
                if p.gpu_class == gpu_class
                and p.reservation_id is not None
                and p.node_resident
                and not p.terminating
                and p.gpu_count > 0
                and self._past_guarantee(p, now)
            ]

        return BoundaryPreemptionNeed(
            boundary=boundary,
            phase="B" if boundary <= now else "A",
            demand_by_class=demand,
            free_by_class=free,
            kills_needed_by_class=kills_needed,
            candidates_by_class=candidates,
        )

    def plan_boundary_preemption(
        self,
        boundary: datetime,
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        now: datetime,
        rng: Optional[random.Random] = None,
    ) -> PreemptionPlan:
        """Plan (but do not execute) the kills needed to clear *boundary*'s demand.

        The all-in-one local planner: computes the shortfall/candidate pool
        (``plan_boundary_candidates``), selects victims **locally** at random
        (``select_victims_locally``), and assembles the ``PreemptionPlan``
        (``build_preemption_plan``).  This is the fallback path — the sweep
        uses it verbatim whenever victim selection is not delegated to the app
        (delegation disabled, no client, or the app call failed).  Pure — no
        I/O, no state mutation; the caller executes the deletions.
        """
        need = self.plan_boundary_candidates(boundary, capacity_by_class, pods, now)
        selected = select_victims_locally(need, rng)
        return build_preemption_plan(need, selected)

    # ------------------------------------------------------------------
    # Anticipatory headroom (capacity held free for arriving on-demand jobs)
    # ------------------------------------------------------------------

    def _headroom_pool(
        self, gpu_class: str, pods: list[PodRuntimeView], now: datetime
    ) -> list[PodRuntimeView]:
        """Pods of *gpu_class* that headroom is allowed to consider at all.

        Exactly ``plan_boundary_candidates``' eligibility predicate — live,
        node-resident, admitted by this controller, and **past its runtime
        guarantee**.  Kept as one helper because the two headroom entry points
        below must agree on it: a pod warned by ``plan_headroom_warnings`` that
        ``plan_headroom_candidates`` would never consider is a warning that can
        never come true.
        """
        return [
            p
            for p in pods
            if p.gpu_class == gpu_class
            and p.reservation_id is not None
            and p.node_resident
            and not p.terminating
            and p.gpu_count > 0
            and self._past_guarantee(p, now)
        ]

    def headroom_shortfall_by_class(
        self,
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        target_percent: int,
    ) -> tuple[dict[str, int], dict[str, int], dict[str, int]]:
        """Return ``(target, free, shortfall)`` GPUs per class for a headroom goal.

        The target is ``ceil(capacity * target_percent / 100)`` — rounding *up*
        so a small class still reserves a whole GPU rather than rounding its
        headroom away entirely.  ``free`` reuses ``free_capacity_by_class``
        verbatim (and may be negative for an over-committed class, which simply
        makes the shortfall larger).  Only classes with a non-zero shortfall
        appear in the third map.  Pure — no I/O, no state mutation.
        """
        target: dict[str, int] = {}
        shortfall: dict[str, int] = {}
        free = free_capacity_by_class(capacity_by_class, pods)
        for gpu_class, capacity in capacity_by_class.items():
            want = math.ceil(capacity * target_percent / 100)
            target[gpu_class] = want
            short = max(0, want - free.get(gpu_class, 0))
            if short:
                shortfall[gpu_class] = short
        return target, free, shortfall

    def plan_headroom_candidates(
        self,
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        now: datetime,
        target_percent: int,
        require_elapsed_notice: bool = True,
    ) -> BoundaryPreemptionNeed:
        """Compute the per-class shortfall and victim pool for a headroom goal.

        The boundary-free sibling of ``plan_boundary_candidates``: instead of
        clearing demand at a reservation ``slot_start``, it keeps
        *target_percent* of every class's physical GPUs free so an arriving
        on-demand job finds capacity already open rather than waiting for it to
        be reclaimed reactively.

        Candidates are ``_headroom_pool`` members — so a pod inside its runtime
        guarantee is **never** a headroom victim, exactly as at a boundary — and,
        when *require_elapsed_notice*, additionally ones whose stamped
        ``termination_warning_at`` deadline has already passed.  That gate is the
        whole notice mechanism: a pod's first appearance in a short class only
        earns it a warning (see ``plan_headroom_warnings``); it becomes killable
        on a later evaluation, once the deadline it was given has elapsed.

        Returns a ``BoundaryPreemptionNeed`` so the entire selection pipeline
        downstream — app delegation, ``select_victims_locally``,
        ``build_preemption_plan`` — is reused unchanged.  ``boundary``/``phase``
        are inert on this path (there is no boundary): they carry ``now`` and
        ``"H"`` so the shape stays total, and the headroom log lines omit both.
        Pure — no I/O, no state mutation, and no victim selection.
        """
        target, free, shortfall = self.headroom_shortfall_by_class(
            capacity_by_class, pods, target_percent
        )
        candidates: dict[str, list[PodRuntimeView]] = {}
        for gpu_class in shortfall:
            pool = self._headroom_pool(gpu_class, pods, now)
            if require_elapsed_notice:
                pool = [
                    p
                    for p in pool
                    if p.termination_warning_at is not None
                    and p.termination_warning_at <= now
                ]
            candidates[gpu_class] = pool

        return BoundaryPreemptionNeed(
            boundary=now,
            phase="H",
            demand_by_class={c: target[c] for c in shortfall},
            free_by_class=free,
            kills_needed_by_class=dict(shortfall),
            candidates_by_class=candidates,
        )

    def plan_headroom_warnings(
        self,
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        now: datetime,
        target_percent: int,
        notice: timedelta,
    ) -> dict[str, "TerminationWarning"]:
        """Notice warnings for pods a headroom shortfall puts at risk.

        The headroom sibling of ``plan_termination_warnings``, returning the same
        ``{uid: TerminationWarning}`` shape so one annotation and one reconcile
        serve both sources.  The **full** eligible pool of a short class is
        warned rather than a ``kills_needed``-sized subset, for the same reason
        the boundary planner does it: the app's victim-selection policy is
        opaque, so any pool member could be the one chosen.

        ``terminate_at`` is **sticky** — a pod already carrying a warning keeps
        the deadline it has, and only a pod with none is given ``now + notice``.
        This is load-bearing twice over.  Recomputing ``now + notice`` every tick
        would re-patch the pod every tick (``_apply_termination_warnings`` diffs
        on the exact value) and, worse, the deadline would recede ahead of
        ``now`` forever, so ``plan_headroom_candidates``' notice gate could never
        open and no headroom kill would ever fire.

        Reusing a deadline the *boundary* warner wrote is deliberate rather than
        incidental: it is the earliest instant that pod has already been told it
        could be killed, which keeps ``galends/termination-warning-at`` meaning
        one thing — the earliest instant this pod could die, from any cause.

        Pure — no I/O, no state mutation; the caller writes the annotations.
        """
        _, _, shortfall = self.headroom_shortfall_by_class(
            capacity_by_class, pods, target_percent
        )
        warnings: dict[str, TerminationWarning] = {}
        for gpu_class, short in shortfall.items():
            pool = self._headroom_pool(gpu_class, pods, now)
            pool_gpus = sum(p.gpu_count for p in pool)
            if pool_gpus <= 0:
                continue  # shortfall with nothing eligible — nothing to warn
            risk = min(1.0, short / pool_gpus)
            for p in pool:
                deadline = p.termination_warning_at
                if deadline is None:
                    deadline = now + notice
                warnings[p.uid] = TerminationWarning(
                    view=p, terminate_at=deadline, risk=risk
                )
        return warnings

    def prune_preemption_marks(self, now: datetime, lead: timedelta) -> None:
        """Drop boundary bookkeeping once phase B's grace window has closed.

        Called at the start of every preemption sweep, mirroring
        ``reconcile_noshow``'s prune-then-act shape.
        """
        stale = [b for b in self.preemption_fired if b <= now - lead]
        for b in stale:
            del self.preemption_fired[b]

    # ------------------------------------------------------------------
    # Preemption-risk forecast
    # ------------------------------------------------------------------

    def forecast_boundaries(
        self, now: datetime, horizon_end: datetime
    ) -> list[datetime]:
        """Distinct future booking start times in ``(now, horizon_end]``, ascending.

        The forecast's own boundary enumeration — ``upcoming_boundaries`` only
        looks ``±lead`` around now.  Boundaries at or before *now* are
        excluded: their phase A already fired (or phase B fires within one
        sweep tick), and a fresh pod snapshot already reflects any victims as
        terminating, so there is nothing left to forecast for them.  No-show
        reservations are skipped up front (``boundary_demand`` would zero
        them anyway; this just avoids evaluating dead boundaries).
        """
        boundaries = {
            slot_start(r)
            for r in self.reservations
            if r.kind == "booking"
            and r.id not in self.noshow_reservation_ids
            and now < slot_start(r) <= horizon_end
        }
        return sorted(boundaries)

    def forecast_boundary_need(
        self,
        boundary: datetime,
        free_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        now: datetime,
        guarantee_end_by_uid: dict[str, Optional[datetime]],
    ) -> ForecastBoundaryNeed:
        """Project one future boundary's shortfall and at-risk pool.

        Demand is ``boundary_demand(boundary, pods, now)`` with the **real**
        *now* — never the boundary instant: ``_chain_for`` keeps chain members
        only while ``slot_end > now``, and boundaries are exactly the instants
        where chain members end, so evaluating "as of the boundary" would
        sever back-to-back chains (phantom demand, phantom overstay).  Pod
        eligibility instead compares each pod's live guarantee (computed once
        at the real now, chains intact, via *guarantee_end_by_uid*) against
        the boundary: eligible when ``guarantee_end is None or <= boundary``
        — the sweep's phase-B outcome for that instant.  Same physical gates
        as ``plan_boundary_candidates`` (admitted, node-resident, not
        terminating, at least one GPU).  Pure.
        """
        demand = self.boundary_demand(boundary, pods, now)
        kills_needed = {
            gpu_class: max(0, need - free_by_class.get(gpu_class, 0))
            for gpu_class, need in demand.items()
        }
        kills_needed = {c: k for c, k in kills_needed.items() if k > 0}

        eligible: dict[str, list[PodRuntimeView]] = {}
        for p in pods:
            if (
                p.reservation_id is None
                or not p.node_resident
                or p.terminating
                or p.gpu_count <= 0
            ):
                continue
            ge = guarantee_end_by_uid.get(p.uid)
            if ge is not None and ge > boundary:
                continue  # still within its guarantee at this boundary
            eligible.setdefault(p.gpu_class, []).append(p)

        return ForecastBoundaryNeed(
            boundary=boundary,
            demand_by_class=demand,
            kills_needed_by_class=kills_needed,
            eligible_by_class=eligible,
            pool_gpus_by_class={
                c: sum(p.gpu_count for p in ps) for c, ps in eligible.items()
            },
        )

    def forecast_preemption_risk(
        self,
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        pending: list[OnDemandCandidate],
        now: datetime,
        lead: timedelta,
    ) -> PreemptionForecast:
        """Forecast per-pod preemption risk for the current + next two hours.

        Pure and synchronous — the caller holds ``reservation_lock`` and
        passes fresh cluster snapshots (same contract as
        ``plan_boundary_candidates``).  The model:

        - Enumerate future booking boundaries in ``(now, horizon_end + lead]``
          — the tail extension mirrors the front lead-time attribution: a
          boundary just past the last bucket can still land its phase-A kill
          inside it.
        - Per boundary/class: ``shortfall = max(0, demand − free)``; every
          eligible pod's single-boundary risk is
          ``min(1, shortfall / pool_gpus)`` — the uniform-random local
          selection model (with delegation enabled the app's policy decides
          *which* pool member dies; pool membership is exact either way).
        - A boundary's kill can land anywhere in ``[boundary − lead,
          boundary]`` (phase A runs early), but never before the pod's own
          guarantee ends — each contribution is attributed to the buckets
          overlapping ``[max(boundary − lead, guarantee_end), boundary]``,
          which makes "guaranteed ⇒ zero risk" hold exactly per bucket.
        - Per bucket, contributions combine as ``1 − Π(1 − r)``
          (``combine_risks``; documented conservative).
        - Free capacity is projected constant (running pods are assumed to
          keep running — the pessimistic case a risk forecast should show).

        Pending JIT candidates are reported per class on bucket 0 as
        informational pressure only (see ``ForecastBucketSummary``).
        """
        self.require_reservation_lock("forecast_preemption_risk")
        buckets = forecast_buckets(now)
        horizon_end = buckets[-1][1]
        free = free_capacity_by_class(capacity_by_class, pods)

        admitted = [
            p
            for p in pods
            if p.reservation_id is not None and p.node_resident and not p.terminating
        ]
        # One chain walk per admitted pod, at the real now; reused by every
        # boundary and by the per-bucket state labels below.
        guarantee_end_by_uid: dict[str, Optional[datetime]] = {
            p.uid: self.guarantee_end(p.reservation_id, now=now)
            for p in pods
            if p.reservation_id is not None
        }

        needs = [
            self.forecast_boundary_need(b, free, pods, now, guarantee_end_by_uid)
            for b in self.forecast_boundaries(now, horizon_end + lead)
        ]

        pending_by_class: dict[str, int] = {}
        for c in pending:
            pending_by_class[c.gpu_class_label] = (
                pending_by_class.get(c.gpu_class_label, 0) + c.gpu_requested
            )

        # ----- bucket summaries -------------------------------------------------
        class_universe = (
            set(capacity_by_class)
            | {p.gpu_class for p in admitted}
            | set(pending_by_class)
        )
        for need in needs:
            class_universe |= set(need.demand_by_class)
        classes = sorted(class_universe)

        demand_sums: list[dict[str, int]] = [{} for _ in buckets]
        shortfall_sums: list[dict[str, int]] = [{} for _ in buckets]
        pool_maxes: list[dict[str, int]] = [{} for _ in buckets]
        for need in needs:
            attributed = overlapping_bucket_indices(
                need.boundary - lead, need.boundary, buckets
            )
            for i in attributed:
                for c, d in need.demand_by_class.items():
                    demand_sums[i][c] = demand_sums[i].get(c, 0) + d
                for c, k in need.kills_needed_by_class.items():
                    shortfall_sums[i][c] = shortfall_sums[i].get(c, 0) + k
                for c, g in need.pool_gpus_by_class.items():
                    pool_maxes[i][c] = max(pool_maxes[i].get(c, 0), g)

        summaries = [
            ForecastBucketSummary(
                start=s,
                end=e,
                capacity_by_class={c: capacity_by_class.get(c, 0) for c in classes},
                free_by_class={c: free.get(c, 0) for c in classes},
                demand_by_class={c: demand_sums[i].get(c, 0) for c in classes},
                shortfall_by_class={c: shortfall_sums[i].get(c, 0) for c in classes},
                eligible_pool_gpus_by_class={
                    c: pool_maxes[i].get(c, 0) for c in classes
                },
                pending_jit_gpus_by_class={
                    c: (pending_by_class.get(c, 0) if i == 0 else 0) for c in classes
                },
            )
            for i, (s, e) in enumerate(buckets)
        ]

        # ----- per-pod risk -----------------------------------------------------
        pod_forecasts: list[PodForecast] = []
        for p in admitted:
            ge = guarantee_end_by_uid.get(p.uid)
            risk_parts: list[list[float]] = [[] for _ in buckets]
            if p.gpu_count > 0:
                for need in needs:
                    kills = need.kills_needed_by_class.get(p.gpu_class, 0)
                    if kills <= 0:
                        continue
                    if ge is not None and ge > need.boundary:
                        continue  # within guarantee at this boundary — no risk from it
                    pool_gpus = need.pool_gpus_by_class.get(p.gpu_class, 0)
                    if pool_gpus <= 0:
                        continue
                    risk = min(1.0, kills / pool_gpus)
                    window_start = need.boundary - lead
                    if ge is not None and ge > window_start:
                        window_start = ge  # a kill can never precede the guarantee end
                    for i in overlapping_bucket_indices(
                        window_start, need.boundary, buckets
                    ):
                        risk_parts[i].append(risk)

            bucket_risks = []
            for i, (s, e) in enumerate(buckets):
                if ge is None or ge <= s:
                    state = "overstay"
                elif ge >= e:
                    state = "guaranteed"
                else:
                    state = "mixed"
                bucket_risks.append(
                    PodBucketRisk(risk=combine_risks(risk_parts[i]), state=state)
                )
            pod_forecasts.append(
                PodForecast(view=p, guarantee_end=ge, buckets=bucket_risks)
            )

        return PreemptionForecast(
            generated_at=now,
            lead_minutes=int(lead.total_seconds() // 60),
            buckets=summaries,
            pods=pod_forecasts,
        )

    def plan_termination_warnings(
        self,
        boundaries: list[datetime],
        capacity_by_class: dict[str, int],
        pods: list[PodRuntimeView],
        now: datetime,
        lead: timedelta,
    ) -> dict[str, "TerminationWarning"]:
        """Identify pods at risk of preemption at an upcoming boundary.

        The warning sibling of ``plan_boundary_candidates``, used by the
        preemption sweep after it has executed its kills to annotate the
        survivors that could still be preempted at a boundary in scope.  Two
        deliberate differences from the sweep's own candidate planning:

        - Eligibility is **boundary-relative** (``guarantee_end <= boundary``),
          reusing ``forecast_boundary_need`` — so a pod whose guarantee expires
          *between* now and the boundary is flagged, not only pods already past
          guarantee now (which the sweep is busy killing this tick).
        - The **full eligible pool** of a short class is warned, not a
          ``kills_needed``-sized subset — the app's victim-selection policy is
          opaque, so any pool member could be chosen.

        *boundaries* is the **warning look-ahead** set — the union of the sweep's
        own kill window (``upcoming_boundaries``) with a wider forward horizon
        (``forecast_boundaries(now, now + TERMINATION_WARNING_LEAD_MINUTES)``),
        decoupled from the kill *lead* so a pod that will be a **phase-A** victim
        (already past its guarantee, killed proactively at ``boundary − lead``)
        is warned *before* its boundary enters the kill window rather than on the
        same tick it is killed.  *pods* is the **post-kill** snapshot (the caller
        has removed pods it just preempted), so ``free`` here already reflects the
        recovered capacity and doomed pods can never be warned.  Iterating
        *boundaries* ascending means a pod at risk at several boundaries keeps its
        **soonest** one (both the ``terminate_at`` timestamp and the ``risk``
        come from that boundary).

        ``terminate_at`` is the projected **kill instant**
        ``max(boundary − lead, guarantee_end)`` — the start of the sweep's kill
        window and the earliest the pod could actually be deleted (phase A fires
        at ``boundary − lead``, but never before the pod's own guarantee ends).
        A fully-past-guarantee pod (``guarantee_end is None`` here, or already
        elapsed) projects to ``boundary − lead``; a phase-B victim whose
        guarantee ends at the boundary degrades to the boundary itself.

        Returns a map keyed by pod uid.  Pure — no I/O, no state mutation; the
        caller writes the annotations.
        """
        free = free_capacity_by_class(capacity_by_class, pods)
        guarantee_end_by_uid: dict[str, Optional[datetime]] = {
            p.uid: self.guarantee_end(p.reservation_id, now=now)
            for p in pods
            if p.reservation_id is not None
        }
        warnings: dict[str, TerminationWarning] = {}
        for boundary in sorted(boundaries):
            need = self.forecast_boundary_need(
                boundary, free, pods, now, guarantee_end_by_uid
            )
            for gpu_class, shortfall in need.kills_needed_by_class.items():
                pool_gpus = need.pool_gpus_by_class.get(gpu_class, 0)
                if pool_gpus <= 0:
                    continue  # unmet shortfall with no eligible pods — nothing to warn
                risk = min(1.0, shortfall / pool_gpus)
                for p in need.eligible_by_class.get(gpu_class, []):
                    if p.uid in warnings:
                        continue  # a sooner boundary already claimed this pod
                    # Project the kill-window start: phase A deletes an
                    # already-past-guarantee pod at ``boundary − lead``, but
                    # never before its own guarantee ends.
                    kill_start = boundary - lead
                    ge = guarantee_end_by_uid.get(p.uid)
                    if ge is not None and ge > kill_start:
                        kill_start = ge
                    warnings[p.uid] = TerminationWarning(
                        view=p, terminate_at=kill_start, risk=risk, boundary=boundary,
                    )
        return warnings

    def plan_reservation_facts(
        self,
        pods: list[PodRuntimeView],
    ) -> dict[int, ReservationResponse]:
        """Return the reservation each admitted pod is linked to, keyed by **reservation id**.

        Pure.  The reconcile in ``main`` digests each row into the annotation
        values the pod should carry and patches only where they differ from what
        it already has.

        Keyed by reservation id rather than pod uid because that is what the
        result *is* — several pods on one reservation share one answer, and
        keying by uid would fan the same row out per pod and invite a caller to
        believe the value was pod-specific.

        A pod whose ``reservation_id`` is ``None`` (not admitted by this
        controller, or an unparseable booking-reference) is skipped, as is one
        whose reservation is no longer in ``self.reservations`` — the set holds
        only ``status="active"`` rows, so an absent row means cancelled or
        expired.  Skipping leaves that pod's existing stamps frozen at their
        last-known values, the same discipline ``plan_guarantee_status`` applies
        to a lapsed pod's ``guaranteed-until``: these annotations describe what
        the pod was admitted under, and a reservation ending is not new
        information about that.
        """
        by_id = {r.id: r for r in self.reservations}
        out: dict[int, ReservationResponse] = {}
        for p in pods:
            if p.reservation_id is None or p.reservation_id in out:
                continue
            res = by_id.get(p.reservation_id)
            if res is not None:
                out[p.reservation_id] = res
        return out

    def plan_guarantee_status(
        self,
        pods: list[PodRuntimeView],
        now: datetime,
    ) -> dict[str, "GuaranteeStatus"]:
        """Return the desired live guarantee status per admitted pod, keyed by uid.

        Pure and now-relative.  For each pod carrying a resolvable
        ``reservation_id``, the live guarantee end (``guarantee_end``, chain-aware)
        decides the status: ``"guaranteed"`` with that end while it is still in the
        future, else ``"overstay"`` with ``None`` (the reconcile leaves the pod's
        existing, now-past ``guaranteed-until`` frozen and flips only the status).
        A pod with no ``reservation_id`` — not admitted by this controller, or
        whose booking-reference does not parse — is skipped: it is not ours to
        report a guarantee for.
        """
        out: dict[str, GuaranteeStatus] = {}
        # The "guaranteed" / "overstay" literals below must match the values
        # k8s_client.annotate_runtime_guarantee stamps at admission (see
        # GuaranteeStatus) — a drift would make _apply_guarantee_status re-patch
        # every pod on every tick.
        for p in pods:
            if p.reservation_id is None:
                continue
            ge = self.guarantee_end(p.reservation_id, now=now)
            if ge is not None and ge > now:
                out[p.uid] = GuaranteeStatus(
                    view=p, status="guaranteed", guaranteed_until=ge
                )
            else:
                out[p.uid] = GuaranteeStatus(
                    view=p, status="overstay", guaranteed_until=None
                )
        return out
