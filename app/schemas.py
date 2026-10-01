"""Pydantic models for the GPU Reservation API response types.

Mirrors the shapes documented in RESERVATION-API.md §6.
"""

from __future__ import annotations

from datetime import date, datetime
from typing import Any, Literal, Optional

from pydantic import BaseModel, Field


class UserBrief(BaseModel):
    id: int
    username: str


class GroupBrief(BaseModel):
    id: int
    name: str


class GpuClassBrief(BaseModel):
    id: int
    name: str
    label_value: Optional[str] = None


class ReservationResponse(BaseModel):
    id: int
    user_id: Optional[int] = None   # always set in practice (nullability is legacy)
    user: Optional[UserBrief] = None  # always set in practice (nullability is legacy)
    group_id: Optional[int] = None
    group: Optional[GroupBrief] = None
    gpu_class_id: int
    gpu_class: GpuClassBrief
    start_dt: Optional[datetime] = None  # site-local wall-clock (naive); display only
    end_dt: Optional[datetime] = None    # site-local wall-clock (naive); display only
    date: date               # calendar date of the reservation (local time, for display)
    start_utc: datetime      # UTC start; use this for all time comparisons
    end_utc: datetime        # UTC end; use this for guarantee arithmetic
    gpu_count: int           # number of GPUs reserved
    # SU accounting fields (RESERVATION-API.md §6); informational to the controller.
    su_cost_user: float = 0.0      # SU charged to the individual user
    su_cost_group: float = 0.0     # SU charged against the group pool
    su_cost_original: float = 0.0  # SU charged at creation; never rewritten
    status: str              # "active" | "cancelled"
    # "booking" | "on_demand" (JIT lease) | "best_effort" (zero-length, zero-SU
    # stub for a pod that asked for no runtime guarantee -- its window is already
    # over, which is what makes the pod preemptible from its first tick).
    kind: str
    notes: Optional[str] = None
    submitted_by_id: Optional[int] = None
    submitted_by: Optional[UserBrief] = None
    created_at: datetime
    updated_at: datetime
    cancelled_at: Optional[datetime] = None
    cancelled_by_id: Optional[int] = None
    # "no-show" | "controller-revoked" | "pod-terminated" | "superseded" for
    # controller- or continue-driven cancellations; null for human cancels.
    cancel_reason: Optional[str] = None
    # Set on a booking minted via POST /api/reservations/{id}/continue: the id
    # of the superseded source reservation whose pod it carries forward.
    continued_from_id: Optional[int] = None


class GpuClassDetail(GpuClassBrief):
    """Returned by GET /api/gpu-classes/{id} (and the bulk list).

    Extends ``GpuClassBrief`` (id, name, optional ``label_value`` — the
    Kubernetes node-label value used to match pod gpu-class labels, e.g.
    "h100") with the app-side GPU counts.  Subclassed rather than redeclared so
    the shared fields cannot drift (CODE-REVIEW H7).

    Two counts, and the hourly capacity audit wants the *second* one:

    ``total_gpus`` is the class's configured default.  ``effective_gpus_today``
    is that default after the app has applied any date-span capacity override
    covering today — which is the number the app actually admits against.  A
    maintenance window that halves a class for a week moves only the latter, so
    auditing ``total_gpus`` compares a figure nobody is enforcing against real
    physical capacity: it invents a mismatch when the override matches a genuine
    node drain (pausing JIT admission for a class that is not over-committed),
    and hides a real over-commit when the override raises the count.

    Both are ``Optional`` so a payload omitting them (an older app, or a bulk
    list that doesn't include them) degrades to "unknown" — such a class is left
    out of the app-side capacity map rather than defaulting to a misleading
    count.  ``effective_gpus_today`` absent falls back to ``total_gpus``, so an
    app predating the field still audits exactly as before.
    """

    total_gpus: Optional[int] = None
    effective_gpus_today: Optional[int] = None

    @property
    def audit_gpus(self) -> Optional[int]:
        """The app-side count the capacity audit should compare against.

        The override-resolved count when the app publishes one, else the
        configured default, else ``None`` (unknown — omit from the audit).
        """
        return self.total_gpus if self.effective_gpus_today is None else self.effective_gpus_today


class GroupMemberBrief(UserBrief):
    """One entry of a group's ``members`` (RESERVATION-API.md §6 GroupMemberBrief).

    Membership is any role, as the app's own membership check reads it, so
    ``role`` is carried but nothing here branches on it.
    """

    role: Optional[str] = None


class GroupDetail(GroupBrief):
    """Returned by GET /api/groups (RESERVATION-API.md §6 GroupResponse).

    Only what ``DEFAULT_USAGE_GROUP``'s per-user fallback reads: whether the
    group is live, whether it enrols anyone who asks (``on_demand_auto_join``),
    and who belongs to it.  The rest of the payload -- policy fields, the full
    GPU-class list -- is ignored.  The flags default the way an app predating
    them behaves: active, and no auto-join.
    """

    is_active: bool = True
    on_demand_auto_join: bool = False
    members: list[GroupMemberBrief] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Inbound push API (POST /api/reservations/push)
# ---------------------------------------------------------------------------


class ReservationPushRequest(BaseModel):
    """Body of a push from the reservation app.

    Carries one or more updated reservation entries (a partial delta, not a full
    snapshot — bulk synchronisation remains a controller-initiated pull).  Each
    entry is a full ``ReservationResponse`` so the same reconciliation code path
    that handles a fetched reservation applies unchanged.  The envelope object
    leaves room for future push kinds (e.g. standby assignments) without an API
    break.
    """

    reservations: list[ReservationResponse]


class ReservationPushResponse(BaseModel):
    """Summary returned after a push has been reconciled into controller state."""

    applied: int        # entries upserted into the active set
    cancelled: int      # in-window cancellations evicted / reclaimed
    adopted: int = 0    # in-window owner changes whose prior-owner pod was evicted
    total_active: int   # size of the active reservation set after the push


# ---------------------------------------------------------------------------
# JIT on-demand reservation request (POST /api/reservations)
# ---------------------------------------------------------------------------


class OnDemandReservationRequest(BaseModel):
    """Body of a JIT on-demand booking request, sent on behalf of a pending pod.

    The app anchors ``start_utc`` at its own "now" (avoids controller/app clock
    skew) and sets ``end_utc = start_utc + duration_seconds``.
    ``on_demand=True`` relaxes policy limits (SU/caps/min-duration) only —
    never physical calendar capacity.  ``idempotency_key`` is the admitting
    pod's UID: a retry with the same key returns the original reservation
    rather than creating a duplicate.

    ``username`` / ``group_name`` are both **required** natural keys on the
    app side (the user must be an active member of the named group) — the
    JIT-eligibility gate guarantees a group source (the REQUIRED_GROUP_LABEL
    value, or the pod's ``galends/usage-group`` annotation) before an ask is
    built, and requiring it here catches a regression at construction time
    rather than as a 422 on the wire.  ``notes`` is stored on the lease for
    admin traceability (which pod it covers).
    """

    username: str
    group_name: str
    gpu_class_id: int
    gpu_count: int
    duration_seconds: int
    on_demand: bool = True
    idempotency_key: str
    notes: Optional[str] = None


class BestEffortReservationRequest(BaseModel):
    """Body of a best-effort admission request, sent on behalf of a pending pod.

    For a pod that declared ``galends/runtime-guarantee: none``.  The app records
    a **stub** -- ``start_utc == end_utc == its own now``, ``su_cost = 0``,
    ``kind="best_effort"`` -- which exists to give the pod a reservation to be
    admitted under and its eventual overstay report a parent, not to hold
    capacity.  Read back, the stub's window is already over, which is how
    ``guarantee_end`` concludes the pod has no live guarantee and the preemption
    planner treats it as a candidate from its first tick.

    Deliberately carries **no** ``duration_seconds``: that is what separates this
    from ``OnDemandReservationRequest``, whose lease is guaranteed and charged.
    ``username`` / ``group_name`` are required natural keys app-side exactly as
    for a lease, and ``idempotency_key`` is likewise the admitting pod's UID.
    """

    username: str
    group_name: str
    gpu_class_id: int
    gpu_count: int
    best_effort: bool = True
    idempotency_key: str
    notes: Optional[str] = None


# ---------------------------------------------------------------------------
# Preemption victim selection (POST /api/reservations/preemption-victims)
# ---------------------------------------------------------------------------


class PreemptionCandidate(BaseModel):
    """One eligible overstay pod offered to the app for victim selection.

    The controller has already decided this pod is *preemptable* — live,
    past its runtime guarantee, admitted by this controller, of the class in
    question.  ``reservation_id`` is the pod's booking-reference id: the app's
    handle for looking the reservation (and thus its owner/group/kind) up when
    it prioritises.  ``pod_uid`` is opaque to the app — it is echoed back in the
    response so the controller can map the choice to a pod to delete.
    """

    pod_uid: str
    namespace: str
    pod_name: str
    gpu_class: str          # Kubernetes label value (e.g. "h100")
    gpu_count: int
    reservation_id: int     # booking-reference id (always set — eligibility requires it)


class PreemptionSelectionRequest(BaseModel):
    """Body of ``POST /api/reservations/preemption-victims``.

    ``needed_by_class`` is how many GPUs must be reclaimed per gpu-class label
    at one boundary; ``candidates`` is the full eligible pool the app chooses
    from.  The app returns the victims it selects; the controller kills only
    those (and only ones it offered).
    """

    needed_by_class: dict[str, int]
    candidates: list[PreemptionCandidate]


class PreemptionSelectionResponse(BaseModel):
    """Victims the app chose to preempt, as the ``pod_uid``s it was offered."""

    victim_pod_uids: list[str]


# ---------------------------------------------------------------------------
# On-demand admission selection (POST /api/reservations/ondemand-admission)
# ---------------------------------------------------------------------------


class OnDemandAdmissionCandidate(BaseModel):
    """One pending pod seeking JIT on-demand admission, offered to the app.

    Two blocks of fields, and the distinction matters to what the app may do
    with them.  ``username`` / ``group_name`` / ``gpu_class_id`` / ``gpu_count``
    / ``duration_seconds`` are the exact "ask" the controller would otherwise
    send to ``POST /api/reservations`` — so the app can weigh it against LAS
    priority and the same feasibility analysis a create would perform.
    ``pod_created_at`` and ``pod_annotations`` are **evidence about the waiting
    pod** rather than part of the ask: nothing in the create carries them, and
    they exist so admission policy can price a candidate on how long it has been
    waiting and on what its owner declared about the job.

    ``pod_uid`` is opaque to the app (it equals the create's
    ``idempotency_key``): it is echoed back in the response so the controller can
    map the choice to a pod to admit.

    ``pod_created_at`` is the pod's ``metadata.creationTimestamp`` as a UTC
    instant — the same value the controller FIFO-orders its own batch by, so the
    app sees the queue position the controller sees, and an age computed from it
    is real queueing delay rather than time since this batch was assembled.

    ``pod_annotations`` is every ``galends/``-prefixed annotation on the pod,
    bounded by ``k8s_client.get_pod_galends_annotations`` (32 keys, 1024 chars
    per value) because the values are whatever the pod's creator wrote.  Empty is
    legitimate: a pod admitted under ``DEFAULT_MINIMUM_RUNTIME_SECONDS`` and
    ``DEFAULT_USAGE_GROUP`` declares nothing at all.
    """

    pod_uid: str
    pod_created_at: datetime
    pod_annotations: dict[str, str] = Field(default_factory=dict)
    username: str
    group_name: Optional[str] = None
    gpu_class_id: int
    gpu_count: int
    duration_seconds: int
    # Which create follows a grant: a best-effort stub (duration_seconds 0)
    # rather than a lease, so the app judges the ask by that path's gates.
    best_effort: bool = False


class OnDemandAdmissionRequest(BaseModel):
    """Body of ``POST /api/reservations/ondemand-admission``.

    ``candidates`` is the full set of pending pods due for an admission attempt
    this round.  The app returns the subset it grants; the controller then
    creates a real lease (``POST /api/reservations``) for each granted pod, and
    only ones it offered.
    """

    candidates: list[OnDemandAdmissionCandidate]


class OnDemandAdmissionResponse(BaseModel):
    """Pods the app grants on-demand admission this round, and why it withheld the rest.

    ``granted_pod_uids`` is in the order to grant them.  An empty list is a
    deliberate "grant none" decision and is respected; the controller ignores
    any uid it did not offer.  ``withheld`` is read entry by entry
    (``reservation_client._withheld_entries``) rather than validated here, so
    one malformed entry cannot discard the app's grants with it; absent from an
    app predating it.
    """

    granted_pod_uids: list[str]
    withheld: list[Any] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Overstay report (POST /api/reservations/{id}/overstay)
# ---------------------------------------------------------------------------


class OverstayReportRequest(BaseModel):
    """Body of ``POST /api/reservations/{id}/overstay`` — analysis-only.

    Sent best-effort when a pod's overstay *ends* (deleted / terminated /
    preempted), so the full duration is known.  ``start_utc`` is the
    guarantee-end instant the pod crossed into overstay; ``end_utc`` is the
    termination instant.  The GPU class, owner, and group are resolved app-side
    from the parent reservation (the path id), so only the pod's ``gpu_count``,
    the window, and the ``end_reason`` are carried.  ``pod_uid`` is the app-side
    dedup key.
    """

    pod_uid: str
    gpu_count: int
    start_utc: datetime
    end_utc: datetime
    end_reason: Optional[str] = None


# ---------------------------------------------------------------------------
# Preemption-risk forecast (GET /api/forecast/preemption-risk)
# ---------------------------------------------------------------------------


class ForecastClassSummary(BaseModel):
    """One GPU class's capacity/demand outlook within one forecast bucket."""

    capacity: int            # physical GPUs (node snapshot); 0 = class unknown to nodes
    free: int                # capacity − node-resident use; may be NEGATIVE (overcommit)
    demand: int              # booking demand attributed to this bucket
    shortfall: int           # GPUs the sweep would have to reclaim (demand − free, ≥ 0)
    eligible_pool_gpus: int  # Σ gpu_count of past-guarantee pods the shortfall draws from
    pending_jit_gpus: int = 0  # pods awaiting a JIT lease; informational pressure only —
                               # a granted lease cannot trigger boundary preemption today,
                               # so this is never folded into ``shortfall``


class ForecastBucket(BaseModel):
    """One forecast bucket: a half-open ``[start, end)`` wall-clock interval."""

    start: datetime
    end: datetime
    classes: dict[str, ForecastClassSummary]  # keyed by gpu-class label (e.g. "h100")


class ForecastPodBucket(BaseModel):
    """One admitted pod's outlook within one bucket (index-aligned with buckets)."""

    risk: float  # 0.0–1.0 preemption likelihood; exactly 0 while guaranteed
    state: Literal["guaranteed", "overstay", "mixed"]


class ForecastPod(BaseModel):
    """Per-pod forecast entry.  Only controller-admitted pods appear — a pod
    without a booking-reference can never be preempted by this controller."""

    namespace: str
    name: str
    uid: str
    gpu_class: str
    gpu_count: int
    reservation_id: int
    guarantee_end: Optional[datetime]  # live chain-aware instant; None = already unresolvable
    buckets: list[ForecastPodBucket]


class PreemptionRiskForecastResponse(BaseModel):
    """Response of ``GET /api/forecast/preemption-risk``.

    Covers the remainder of the current wall-clock hour plus the next two
    full hours.  ``selection_delegated`` is the honesty flag for the numeric
    risk: when true (``PREEMPTION_DELEGATE_SELECTION``, the default), the
    reservation app chooses victims by its own policy and the number models
    the controller's uniform-random local fallback — pool *membership*
    (at-risk vs safe) is exact either way.
    """

    generated_at: datetime
    lead_minutes: int          # PREEMPTION_LEAD_MINUTES — how early a kill can land
    selection_delegated: bool
    buckets: list[ForecastBucket]
    pods: list[ForecastPod]
