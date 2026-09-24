"""GPU Reservation Kubernetes Controller — entry point.

Starts five background asyncio tasks inside a FastAPI lifespan:

1. reservation_fetch_loop  — periodically refreshes the reservation list
2. pod_watch_loop          — streams pod events and updates the work queue
3. queue_processor_loop    — applies tolerations when reservation windows open
4. preemption_loop         — recovers capacity from overstaying pods near a
                              reservation boundary (demand-driven preemption)
5. capacity_audit_loop     — reconciles app-side vs physical GPU capacity

Additionally, when a pod is detected arriving *inside* an already-open
reservation window (e.g. a JupyterHub notebook pod), the pod-watch loop
bypasses the queue-processor polling interval (QUEUE_PROCESSOR_INTERVAL,
default 300 s) and attempts to apply the toleration immediately, minimising
scheduler delay for the user.

A sixth task, lease_guard_loop, runs unless SINGLETON_LEASE_ENABLED is off:
it holds a coordination Lease so a *second* controller instance refuses to
run (two would issue duplicate toleration patches).  This is a
duplicate-instance guard, not leader election — there is no waiting to take
over — and it fails open if coordination.k8s.io is unreachable.

Every task is supervised (``_on_task_done``): an unhandled exception is
logged CRITICAL and recorded in ``_task_health``, and GET /health — the
liveness, readiness, and container health probe — turns 503 so Kubernetes
restarts the pod (in-memory state rebuilds on startup by design; there is
deliberately no in-process task restart).
"""

from __future__ import annotations

import asyncio
import logging
import os
import random
import secrets
import socket
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Awaitable, Callable, NamedTuple, Optional, Sequence

from fastapi import Depends, FastAPI, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse

from . import trace
from .config import Config, timezone_label
from .log_fields import kv, scrub
from .controller import (
    PENDING_TOPIC_ANNOTATIONS,
    PENDING_TOPIC_HOLD,
    TOLERATION_KEY,
    BoundaryPreemptionNeed,
    CapacityDiff,
    ControllerState,
    GuaranteeStatus,
    NearMiss,
    OnDemandCandidate,
    OnDemandGate,
    PodRuntimeView,
    PreemptionForecast,
    QueueEntry,
    TerminationWarning,
    apply_push_to_active,
    build_preemption_plan,
    canceller_description,
    free_gpus_by_node_class,
    gpu_capacity_by_class,
    largest_node_free_by_class,
    node_counts_by_class,
    reconcile_capacity,
    select_victims_locally,
    slot_end,
    slot_start,
)
from .k8s_client import (
    ANNOTATION_IGNORED_REASON,
    LEASE_NAME,
    LEASE_REJECTED_REASON,
    MIN_RUNTIME_ANNOTATION,
    NO_RESERVATION_REASON,
    RESERVATION_FULL_REASON,
    RESERVATION_TOO_SMALL_REASON,
    RUNTIME_GUARANTEE_ANNOTATION,
    RUNTIME_GUARANTEE_VALUES,
    TERMINAL_PHASES,
    UNKNOWN_GPU_CLASS_REASON,
    USAGE_GROUP_ANNOTATION,
    WAITING_FOR_RESERVATION_REASON,
    AnnotationProblem,
    PodWatcher,
    ReservationFacts,
    acquire_singleton_lease,
    annotate_guarantee_status,
    annotate_reservation_facts,
    annotate_runtime_guarantee,
    annotate_termination_warning,
    apply_toleration,
    clear_termination_warning,
    delete_pod,
    emit_admission_paused_event,
    emit_lease_denied_event,
    emit_overstay_relinked_event,
    emit_reservation_relinked_event,
    emit_pending_pod_event,
    emit_preempted_event,
    emit_reservation_cancelled_event,
    emit_reservation_reassigned_event,
    emit_best_effort_admitted_event,
    emit_runtime_guaranteed_event,
    get_pod_annotation_problems,
    get_pod_booking_reference,
    get_pod_creation_timestamp,
    get_pod_galends_annotations,
    get_pod_gpu_count,
    get_pod_guarantee_status,
    get_pod_min_runtime_seconds,
    get_pod_runtime_guarantee_request,
    get_pod_phase,
    get_pod_usage_group,
    get_unschedulable_message,
    init_k8s,
    is_gpu_gated_pending,
    is_terminal_phase,
    local_display,
    make_booking_reference,
    parse_booking_reference,
    pod_has_toleration,
    read_pod,
    remove_scheduling_gate,
    renew_singleton_lease,
    set_display_timezone,
    snapshot_node_gpu_capacity,
    snapshot_node_gpu_inventory,
    snapshot_tolerated_pods,
    utc_iso,
)
from .reservation_client import (
    LEASE_DENIED_STATUS,
    LEASE_NOT_FOUND_STATUS,
    ReservationClient,
)
from .schemas import (
    ForecastBucket,
    ForecastClassSummary,
    ForecastPod,
    ForecastPodBucket,
    BestEffortReservationRequest,
    OnDemandAdmissionCandidate,
    OnDemandAdmissionRequest,
    OnDemandReservationRequest,
    OverstayReportRequest,
    PreemptionCandidate,
    PreemptionRiskForecastResponse,
    PreemptionSelectionRequest,
    ReservationPushRequest,
    ReservationPushResponse,
    ReservationResponse,
)

class _TraceContextFilter(logging.Filter):
    """Stamp the in-scope trace id onto every LogRecord.

    Stamping happens at emit time, so a record captured mid-operation carries
    the trace that was in force then, not whatever is current when it is
    formatted.
    """

    def filter(self, record: logging.LogRecord) -> bool:
        record.req_trace = trace.current()  # type: ignore[attr-defined]
        return True


logging.basicConfig(
    # ``actor=`` is rendered here rather than at each call site because it is
    # constant: unlike the app, this daemon has exactly one principal. Emitting
    # it anyway means a line from either side of the pair parses under the one
    # ``key=value`` grammar (see docs/LOG-FIELDS.md).
    #
    # ``trace=`` is the unit-of-work id (see app/trace.py) — one per fetch cycle,
    # queue tick, sweep, admission batch or pod event, propagated to the app over
    # ``X-Client-Trace`` so both sides of an operation share it. "-" when no unit
    # of work is in scope.
    format=(
        "%(asctime)s %(levelname)-8s %(name)s: "
        "actor=controller trace=%(req_trace)s %(message)s"
    ),
)
_trace_filter = _TraceContextFilter()
for _handler in logging.root.handlers:
    _handler.addFilter(_trace_filter)
log = logging.getLogger(__name__)


# Third-party client libraries whose verbose output is wire-level tracing, not
# controller behaviour: ``kubernetes.client.rest`` dumps whole response bodies at
# DEBUG, ``urllib3`` / ``httpcore`` log every connection and request, and
# ``httpx`` logs one ``HTTP Request:`` line per call even at INFO.
_LIBRARY_LOGGERS = ("httpx", "httpcore", "urllib3", "kubernetes")


def _configure_logging(config: Config) -> None:
    """Apply the configured log levels (LOG_LEVEL via Config, CODE-REVIEW H1).

    Keeps all environment parsing in ``config.py`` — ``main.py`` no longer reads
    ``os.environ`` directly.

    The client libraries get ``LIBRARY_LOG_LEVEL`` instead of inheriting the
    root level, so ``LOG_LEVEL=DEBUG`` shows the controller's own DEBUG events
    without the raw API traces underneath them.  It can only make them quieter:
    the effective level is the stricter of the two, so ``LOG_LEVEL`` stays the
    ceiling on verbosity and ``LOG_LEVEL=ERROR`` still silences library warnings.
    """
    root = logging.getLogger()
    root.setLevel(config.log_level.upper())
    library_level = max(
        root.getEffectiveLevel(),
        logging.getLevelNamesMapping()[config.library_log_level],
    )
    for name in _LIBRARY_LOGGERS:
        logging.getLogger(name).setLevel(library_level)


# Retry backoff shared by both admission paths (CODE-REVIEW D1e).  The jittered
# range is used when a placement attempt fails for budget/transient reasons; the
# short retry is used when a pod's scheduling state is not yet knowable and we
# want to look again promptly (well within one QUEUE_PROCESSOR_INTERVAL tick).
RETRY_JITTER_RANGE = (120, 300)
SHORT_RETRY_SECONDS = 30

# Backoff for a lease failure that waiting cannot fix (a 4xx that is not 409).
# Doubles per consecutive failure from the ordinary denial floor up to the cap,
# so a deployment with a read-only service key settles at one attempt per half
# hour per pod instead of one every 2-5 min forever.
ERROR_RETRY_CAP_SECONDS = 1800


def _jittered_retry_at(now: datetime) -> datetime:
    """Return *now* pushed forward by a random 2–5 min backoff."""
    return now + timedelta(seconds=random.randint(*RETRY_JITTER_RANGE))


def _error_retry_at(now: datetime, failures: int) -> datetime:
    """Return *now* pushed forward by an exponential backoff for *failures*.

    Starts at the ordinary jittered denial delay and doubles per consecutive
    non-retryable failure, capped at ``ERROR_RETRY_CAP_SECONDS``.  The jitter is
    kept so a class-wide fault (every pod holding the same bad credential) does
    not resynchronise every candidate onto the same instant.
    """
    base = random.randint(*RETRY_JITTER_RANGE)
    delay = min(base * (2 ** max(0, failures - 1)), ERROR_RETRY_CAP_SECONDS)
    return now + timedelta(seconds=delay)


def _denial_retry_at(now: datetime, not_before: Optional[datetime]) -> datetime:
    """Return when to retry a contended lease denial.

    The ordinary 2–5 min jittered delay -- unless the app said when the denial
    clears (the envelope's ``not_before``: a budget window's end, a group's
    ``valid_from``), in which case polling toward that instant is wasted, so the
    retry waits for it.  Capped at ``ERROR_RETRY_CAP_SECONDS`` rather than
    sleeping days: the instant is advisory and can clear sooner (a cancellation
    frees budget), and a pod should not sit a week behind a stale estimate.
    """
    retry_at = _jittered_retry_at(now)
    if not_before is not None:
        capped = min(not_before, now + timedelta(seconds=ERROR_RETRY_CAP_SECONDS))
        retry_at = max(retry_at, capped)
    return retry_at


def _short_retry_at(now: datetime) -> datetime:
    """Return *now* pushed forward by the short (30 s) retry interval."""
    return now + timedelta(seconds=SHORT_RETRY_SECONDS)


# ---------------------------------------------------------------------------
# Background-task supervision
# ---------------------------------------------------------------------------


class TaskHealth:
    """Names of critical background tasks that have died, for GET /health.

    Module-level rather than ``app.state`` so ``health()`` stays a zero-arg
    handler directly callable in tests, matching the codebase's
    monkeypatch-a-module-global seam convention.  One process, one instance;
    ``lifespan`` resets it at startup so repeated ``TestClient`` uses of the
    module-level ``app`` stay isolated.
    """

    def __init__(self) -> None:
        self.dead: dict[str, str] = {}  # task name -> "ExceptionType: message"

    def mark_dead(self, name: str, exc: BaseException) -> None:
        self.dead[name] = f"{type(exc).__name__}: {exc}"

    def reset(self) -> None:
        self.dead.clear()

    @property
    def ok(self) -> bool:
        return not self.dead


_task_health = TaskHealth()

# Singleton-lease timings.  A 60 s duration renewed every 20 s tolerates two
# consecutive failed renewals before any observer can consider us expired.
LEASE_DURATION_SECONDS = 60
LEASE_RENEW_INTERVAL_SECONDS = 20


def _terminate_process() -> None:
    """Exit immediately, non-zero, so the kubelet restarts (or backs off).

    ``os._exit`` skips atexit handlers deliberately: the CRITICAL line
    explaining why is already flushed (logging's StreamHandler flushes per
    record), and a controller that has lost the singleton lease must stop
    patching pods *now*, not after a graceful unwind.  Single seam so tests
    can monkeypatch it.
    """
    os._exit(1)


def _lease_namespace(config: Config) -> str:
    """Namespace the singleton Lease lives in.

    POD_NAMESPACE (downward API) in-cluster; otherwise the service-account
    namespace file; otherwise "default" for out-of-cluster development.  The
    runtime fallbacks live here rather than in Config, which stays
    environment-only.
    """
    if config.pod_namespace:
        return config.pod_namespace
    try:
        with open(
            "/var/run/secrets/kubernetes.io/serviceaccount/namespace",
            encoding="utf-8",
        ) as handle:
            return handle.read().strip() or "default"
    except OSError:
        return "default"


def _lease_holder(config: Config) -> str:
    """Identity claimed in the Lease — the pod name, else the hostname."""
    return config.pod_name or socket.gethostname()


def _on_task_done(task: asyncio.Task) -> None:
    """Done-callback for every background task: make an unexpected death loud.

    Without this a crashed loop logs nothing at all — the ``tasks`` list holds
    a strong reference (so the "exception was never retrieved" warning never
    fires) and shutdown's ``gather(return_exceptions=True)`` discards it.  A
    clean return is *not* marked dead: the loops are ``while True`` and cannot
    return in production, and test harnesses stub them with no-op coroutines.
    """
    if task.cancelled():
        return  # normal shutdown path
    exc = task.exception()
    if exc is None:
        return
    _task_health.mark_dead(task.get_name(), exc)
    # exc_info takes the instance: a done-callback has no active exception
    # context for exc_info=True to pick up.
    log.critical("%s", kv(event="task.crashed", task=task.get_name(), err=exc), exc_info=exc)


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


class GpuClassMaps(NamedTuple):
    """The three per-class lookup maps a reconcile installs together.

    Resolved *outside* ``reservation_lock`` and handed in, so the HTTP round
    trips that build them do not serialise against the inbound push endpoint.
    """

    labels: dict[int, str]   # gpu_class_id → label_value
    ids: dict[str, int]      # label_value → gpu_class_id
    capacity: dict[str, int]  # label_value → app-side GPU count (audit input)
    # Whether ``ids`` is the app's whole class list: built from a successful
    # bulk GET /api/gpu-classes, now or on an earlier cycle.  A map resolved
    # only through the per-id fallback holds just the classes some reservation
    # happened to name, which cannot say a label is *unknown* to the app.
    complete: bool = False


async def _resolve_gpu_class_maps(
    client: ReservationClient,
    class_ids: set[int],
    prior: GpuClassMaps,
) -> GpuClassMaps:
    """Build the GPU-class lookup maps, doing every HTTP call here.

    Split out of ``_reconcile_after_reservation_change`` so it can run **before**
    the reservation lock is taken.  It reads no shared state — *prior* is a copy
    the caller passes for the bulk-fetch-failure path — which is what makes the
    hoist safe.

    That matters because in steady state the reconcile evicts nothing (both
    handlers are conditional), so this was the *only* thing the lock was held
    across: one 15 s-timeout bulk fetch plus a 10 s per-id fallback for each
    unresolved class, on every cycle, blocking any push that arrived meanwhile.
    """
    # A failed bulk fetch keeps the previous cycle's maps rather than losing all
    # label resolution.
    gpu_classes = await client.fetch_gpu_classes()
    if gpu_classes is not None:
        new_labels: dict[int, str] = {}
        new_ids: dict[str, int] = {}
        new_capacity: dict[str, int] = {}
        for gc in gpu_classes:
            if gc.label_value:
                new_labels[gc.id] = gc.label_value
                new_ids[gc.label_value] = gc.id
                # App-side GPU count for the hourly capacity audit: the
                # override-resolved count the app actually admits against, not
                # the configured default.  Recorded only when known (see
                # GpuClassDetail.audit_gpus).
                if gc.audit_gpus is not None:
                    new_capacity[gc.label_value] = gc.audit_gpus
    else:
        new_labels = dict(prior.labels)
        new_ids = dict(prior.ids)
        new_capacity = dict(prior.capacity)
    complete = gpu_classes is not None or prior.complete

    # Fallback: resolve any class the bulk list didn't cover (e.g. one created
    # since the last successful fetch, or referenced by a pushed reservation
    # whose id we have not seen).
    for cid in class_ids:
        if cid in new_labels:
            continue
        gpu_class = await client.fetch_gpu_class(cid)
        if gpu_class and gpu_class.label_value:
            new_labels[cid] = gpu_class.label_value
            new_ids[gpu_class.label_value] = cid
            if gpu_class.audit_gpus is not None:
                new_capacity[gpu_class.label_value] = gpu_class.audit_gpus
            log.info("%s", kv(event="class.resolved", cid=cid, class_=gpu_class.name, clabel=gpu_class.label_value))
        else:
            log.warning("%s", kv(
                event="class.unresolvable", cid=cid, reason="no_label_value",
            ))

    return GpuClassMaps(new_labels, new_ids, new_capacity, complete)


async def _reconcile_after_reservation_change(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
    active_reservations: list[ReservationResponse],
    cancelled_in_window: list[ReservationResponse],
    owner_changes: list[tuple[ReservationResponse, str]],
    now: datetime,
    gpu_class_maps: GpuClassMaps,
) -> list[_PodEviction]:
    """Apply a new active reservation set and run the reconciliation tail.

    Shared by the periodic fetch loop (which supplies a full snapshot) and the
    inbound push endpoint (which supplies a partial delta merged into the current
    set).  Installs the *gpu_class_maps* the caller resolved, assigns the new
    reservation list, reconciles the task queue, and evicts pods for any
    in-window cancellations or owner changes (adoption).

    The caller must already hold ``state.reservation_lock`` and must have computed
    *cancelled_in_window* and *owner_changes* against the OLD reservation set (both
    detectors compare the incoming entries with ``state.reservations`` before it is
    replaced here).  It must **also** have resolved *gpu_class_maps* before taking
    the lock (``_resolve_gpu_class_maps``) — that is the one part of this tail
    that needs the network, and holding the lock across it is what made the push
    endpoint wait out a whole fetch cycle.
    """
    state.require_reservation_lock("_reconcile_after_reservation_change")

    # Everything from here to reconcile_queue() is synchronous: on a cycle with
    # no evictions — the overwhelming majority — the whole critical section is
    # await-free.
    state.reservations = active_reservations
    state.gpu_class_labels = gpu_class_maps.labels
    state.gpu_class_ids = gpu_class_maps.ids
    state.gpu_class_capacity = gpu_class_maps.capacity
    state.gpu_classes_known = gpu_class_maps.complete

    # Drop / re-match queue entries whose reservation was cancelled.
    state.reconcile_queue()
    # Occupancy is rebuilt from a live cluster snapshot each queue-processor
    # tick (reconcile_occupancy), so no reservation-driven prune is needed here.

    if not (cancelled_in_window or owner_changes):
        # The common case: nothing to evict, so the whole critical section was
        # the synchronous block above.
        return []

    # One snapshot serves both planners, taken lazily — we only know whether it
    # is needed after detection, and detection happens under the lock.
    pod_snapshot = await _snapshot_pods_for_eviction(config)

    evictions: list[_PodEviction] = []
    # Mid-window cancellations: re-link each admitted pod onto another open
    # booking its user holds where possible (the Continue/supersede flow),
    # evicting only the pods with nowhere to go.
    if cancelled_in_window:
        evictions += await _plan_cancelled_reservations(
            state, config, cancelled_in_window, now, pod_snapshot
        )

    # Owner changes (adoption): evict the prior owner's admitted pod so the new
    # owner can claim the still-active reservation.
    if owner_changes:
        evictions += _plan_owner_changes(state, owner_changes, pod_snapshot)

    return evictions


async def _refresh_reservations(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Fetch the current reservation list and update shared state.

    Pulls the full ``status=all`` reservation set and resolves the GPU-class
    maps — both network round trips, both **outside** ``reservation_lock`` — then
    hands the results to ``_reconcile_after_reservation_change`` under the lock
    to apply them and run the reconciliation tail.
    """
    all_reservations = await client.fetch_reservations()
    active_reservations = [r for r in all_reservations if r.status == "active"]

    # Resolved before the lock: this is HTTP, and a push arriving mid-fetch used
    # to wait it out.  Reading the prior maps here is safe because they are only
    # a fallback for a failed bulk fetch, and the three are replaced together
    # under the lock below (last writer wins on the whole map — the same
    # granularity as before).
    gpu_class_maps = await _resolve_gpu_class_maps(
        client,
        {r.gpu_class_id for r in active_reservations},
        GpuClassMaps(
            dict(state.gpu_class_labels),
            dict(state.gpu_class_ids),
            dict(state.gpu_class_capacity),
            state.gpu_classes_known,
        ),
    )

    now = datetime.now(timezone.utc)
    async with state.reservation_lock:
        # Detect reservations cancelled mid-window or reassigned to a new owner
        # before overwriting the state (both compare against the old owner set).
        cancelled_in_window = state.detect_cancelled_in_window(all_reservations, now)
        owner_changes = state.detect_owner_changed_in_window(all_reservations, now)
        # Bridge the grant-vs-snapshot race: keep our own recently-granted
        # on-demand leases the app has not surfaced in this snapshot yet, so a
        # live lease's pod is not dropped to guarantee_end=None and wrongly
        # preempted (see ControllerState.preserve_local_ondemand_leases).  These
        # ids are absent from the fetch, so they never collide with the active
        # subset, the cancellation detector, or the owner-change detector.
        preserved = state.preserve_local_ondemand_leases(all_reservations, now)
        if preserved:
            log.info("%s", kv(
                event="lease.preserved", count=len(preserved),
                ids=[r.id for r in preserved], reason="absent_from_fetch_snapshot",
            ))
        evictions = await _reconcile_after_reservation_change(
            state,
            client,
            config,
            active_reservations + preserved,
            cancelled_in_window,
            owner_changes,
            now,
            gpu_class_maps,
        )
        # Only a full fetch can vouch for the whole reservation list: a push
        # is a delta, and before the first fetch lands the list is merely empty.
        state.reservations_known = True

    # Deleting pods is per-pod Kubernetes I/O over shared-state-free data, so it
    # runs with the lock released.
    await _execute_evictions(state, evictions)


# ---------------------------------------------------------------------------
# Cancellation and owner-change handlers
#
# Each is split "plan under the lock / execute outside it".  The planning half
# reads the merged reservation set and re-homes occupancy, so it must be atomic
# with the replace; the execution half is per-pod Kubernetes I/O that reads no
# shared state, and holding the lock across it made every eviction a serialising
# event for the push endpoint.
# ---------------------------------------------------------------------------


class _PodEviction(NamedTuple):
    """One pod to evict, and everything the execution half needs to do it."""

    pod: "ToleratedPodInfo"
    reason: str            # human-readable, rendered into the Kubernetes event
    event_reason: str      # the event's `reason` field, for failure logging
    emit: Callable         # emit_reservation_{cancelled,reassigned}_event


async def _snapshot_pods_for_eviction(config: Config) -> list:
    """One tolerated-pod snapshot, shared by both eviction planners.

    Taken lazily — only when a reconcile actually has something to evict.  A
    speculative LIST on every push would add latency to exactly the path this
    change exists to make fast.

    required_group_label must be passed: without it every ToleratedPodInfo
    carries group_label=None, `_group_ok` then rejects every reservation while
    the feature is enabled, and the adoption re-link can never find a booking to
    carry the pod onto.  default_usage_group rides along for the same reason: a
    pod admitted under the default group must read back carrying it.
    """
    try:
        return await snapshot_tolerated_pods(
            TOLERATION_KEY, config.required_group_label, config.default_usage_group
        )
    except Exception as exc:  # noqa: BLE001
        log.warning(
            "%s", kv(event="evict.snapshot_failed", target="pods", err=exc),
            exc_info=True,
        )
        return []


async def _plan_cancelled_reservations(
    state: ControllerState,
    config: Config,
    cancelled_in_window: list[ReservationResponse],
    now: datetime,
    pod_snapshot: list,
) -> list[_PodEviction]:
    """Re-link what can be rescued; return what still has to be evicted.

    For each in-window cancelled reservation (already carrying canceller info
    from the ``status=all`` fetch):
    1. Filter the snapshot to pods admitted under this reservation.
    2. **Adoption first** (``POD_ADOPTION_ENABLED``): a cancelled reservation's
       pod is by definition past its guarantee, so ``_adopt_pods`` re-links it
       onto another currently-open booking its user holds with spare budget —
       this is what carries a running job forward when the app supersedes its
       source via ``POST /api/reservations/{id}/continue`` (the new booking
       arrives in the same push/fetch as the ``superseded`` cancellation), and
       it matches the lazy re-link the next queue tick would do anyway, minus
       the eviction race.  The caller has already replaced
       ``state.reservations`` with the merged set, so the follow-on booking is
       visible to ``find_open_booking_for``.
    3. Everything not rescued is returned for the caller to evict once the lock
       is released.

    The caller must hold ``state.reservation_lock``: step 2 reads the merged
    ``state.reservations`` and mutates occupancy, so a concurrent fetch's
    wholesale replace landing mid-adoption would re-link a pod onto a booking
    that is no longer in the set.  This half is therefore *not* await-free —
    ``_adopt_pods`` patches each rescued pod — which is why lifting adoption's
    I/O is named as follow-up work rather than claimed as done.
    """
    state.require_reservation_lock("_plan_cancelled_reservations")
    evictions: list[_PodEviction] = []

    for cancelled_res in cancelled_in_window:
        cancelled_by_desc = canceller_description(cancelled_res)

        pods_for_res = [p for p in pod_snapshot if p.reservation_id == cancelled_res.id]

        if pods_for_res and config.pod_adoption_enabled:
            views = [_pod_view(p) for p in pods_for_res]
            await _adopt_pods(state, config, views, now, cause="replaced")
            adopted_uids = {
                v.uid for v in views if v.reservation_id != cancelled_res.id
            }
            if adopted_uids:
                log.info("%s", kv(
                    event="cancel.pods_relinked", rid=cancelled_res.id,
                    count=len(adopted_uids),
                ))
                pods_for_res = [p for p in pods_for_res if p.uid not in adopted_uids]

        if pods_for_res:
            log.info("%s", kv(
                event="cancel.evicting", rid=cancelled_res.id,
                count=len(pods_for_res), detail=cancelled_by_desc,
            ))
        evictions.extend(
            _PodEviction(
                pod=pod_info,
                reason=cancelled_by_desc,
                event_reason="ReservationCancelled",
                emit=emit_reservation_cancelled_event,
            )
            for pod_info in pods_for_res
        )

    return evictions


def _plan_owner_changes(
    state: ControllerState,
    owner_changes: list[tuple[ReservationResponse, str]],
    pod_snapshot: list,
) -> list[_PodEviction]:
    """Return the prior owner's admitted pods for each reassigned reservation.

    For each in-progress reservation whose owner changed (adoption), the pod
    already admitted under it lives in the *prior* owner's namespace and can no
    longer be legitimately matched to the reservation.  Pods are filtered by
    reservation id **and** the prior owner's namespace (the ``namespace ==
    prior_username`` guard ensures a pod the new owner may already have had
    admitted is never touched), then handed back for the caller to evict once
    the lock is released.

    Unlike cancellation, the reservation stays active; it simply changes hands.

    The caller must hold ``state.reservation_lock``.  Unlike its cancellation
    sibling this planner reads ``state.reservations`` not at all and is entirely
    pure — the requirement is a **consistency choice**, not a data dependency: it
    runs as one step of a reconcile that must appear atomic, and a caller that
    could reach it without the lock would be a caller in the wrong place.
    """
    state.require_reservation_lock("_plan_owner_changes")
    evictions: list[_PodEviction] = []

    for res, prior_username in owner_changes:
        new_owner = res.user.username if res.user else "another user"
        new_owner_desc = f"to {new_owner}"

        pods_for_res = [
            p
            for p in pod_snapshot
            if p.reservation_id == res.id and p.namespace == prior_username
        ]
        if pods_for_res:
            log.info("%s", kv(
                event="owner_change.evicting", rid=res.id, count=len(pods_for_res),
                detail=new_owner_desc, **{"old.user": prior_username},
            ))
        evictions.extend(
            _PodEviction(
                pod=pod_info,
                reason=new_owner_desc,
                event_reason="ReservationReassigned",
                emit=emit_reservation_reassigned_event,
            )
            for pod_info in pods_for_res
        )

    return evictions


async def _execute_evictions(
    state: ControllerState, evictions: list[_PodEviction]
) -> None:
    """Emit an event on each doomed pod and delete it — **outside** the lock.

    Reads no shared state.  ``release_pod`` is a synchronous occupancy edit
    keyed by pod uid, and occupancy is rebuilt wholesale from a live cluster
    snapshot every queue-processor tick, so a release lost to a crash here
    self-heals within one tick.

    Deliberately keeps ``release_pod`` *inside* the delete ``try``: a failed
    delete must leave occupancy alone, or the controller would believe capacity
    was freed while the pod is still holding GPUs.
    """
    for eviction in evictions:
        pod_info = eviction.pod
        # Emit event before deletion so the event record survives.
        try:
            pod_obj = await read_pod(pod_info.name, pod_info.namespace)
            await eviction.emit(
                pod_obj, pod_info.name, pod_info.namespace, eviction.reason
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="k8s.event_failed", ns=pod_info.namespace, pod=pod_info.name,
                reason=eviction.event_reason, err=exc,
            ))
        try:
            await delete_pod(pod_info.name, pod_info.namespace)
            state.release_pod(pod_info.uid)
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="pod.delete_failed", ns=pod_info.namespace,
                pod=pod_info.name, err=exc,
            ))


# ---------------------------------------------------------------------------
# Toleration applicator (shared by the queue processor and the fast path)
# ---------------------------------------------------------------------------


def _reservation_facts(reservation: ReservationResponse) -> ReservationFacts:
    """Digest *reservation* into the plain fields stamped onto an admitted pod.

    Keeps ``k8s_client`` free of the Pydantic response models, the mirror of the
    ``PodRuntimeView`` digest that keeps ``controller`` free of Kubernetes
    shapes.  The window goes through ``slot_start``/``slot_end`` so these stamps
    cannot disagree with the window arithmetic every other consumer uses.
    """
    return ReservationFacts(
        kind=reservation.kind,
        start_utc=slot_start(reservation),
        end_utc=slot_end(reservation),
        gpu_count=reservation.gpu_count,
        gpu_class_name=reservation.gpu_class.name,
    )


async def _record_guarantee(
    pod_name: str,
    namespace: str,
    fresh_pod,
    guaranteed_until: datetime,
    now: datetime,
    reservation: ReservationResponse,
    *,
    first_admission: bool = False,
) -> None:
    """Annotate the pod with its runtime guarantee and emit a RuntimeGuaranteed Event.

    Callers compute *guaranteed_until* by chaining back-to-back windows
    (``compute_guaranteed_until``).  This sets no Kubernetes enforcement — no
    ``spec.activeDeadlineSeconds`` is patched, so a pod may run past its
    guarantee freely.  Demand-driven preemption recovers capacity from an
    overstaying pod only when needed (see ``preemption_loop``), deciding by
    recomputing the guarantee live from reservation state
    (``ControllerState.guarantee_end``) — never by reading these annotations
    back.

    *reservation* is the one the pod is now linked to; its descriptive facts ride
    the same patch as the guarantee (no extra API call).  This runs again on every
    re-link (adoption, lease-to-booking merge), which is what keeps those facts
    describing the pod's *current* reservation rather than a retired one.
    *first_admission* distinguishes those re-links from a genuine admission: only
    the latter stamps ``galends/admitted-at``, so a re-link cannot restart a
    session-elapsed clock mid-job.

    Best-effort: logs a warning on failure but does not raise, so a failure to
    record the guarantee never rolls back an already-applied toleration.
    """
    try:
        # A best-effort stub's window is already over, so its guarantee is
        # genuinely zero -- the max(1, ...) floor exists to keep a real
        # guarantee's countdown from rendering as 0, and applying it here would
        # advertise a one-second guarantee nobody promised.
        best_effort = reservation.kind == "best_effort"
        seconds = 0 if best_effort else max(1, int((guaranteed_until - now).total_seconds()))
        await annotate_runtime_guarantee(
            pod_name,
            namespace,
            seconds,
            guaranteed_until,
            _reservation_facts(reservation),
            admitted_at=now if first_admission else None,
        )
        if best_effort:
            # Not RuntimeGuaranteed: that message would read "guaranteed for
            # 0m00s, until <now>", which is nonsense to the pod's owner.
            await emit_best_effort_admitted_event(fresh_pod, pod_name, namespace)
        else:
            await emit_runtime_guaranteed_event(
                fresh_pod, pod_name, namespace, seconds, guaranteed_until
            )
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="pod.guarantee_record_failed", ns=namespace, pod=pod_name, err=exc,
        ))


async def _enforce_scheduling_gate_removal(
    pod_name: str, namespace: str, fresh_pod, gate_name: str | None
) -> None:
    """Remove the configured scheduling gate from *fresh_pod* if present.

    Best-effort: logs a warning on failure; never revokes an applied toleration.
    """
    if not gate_name:
        return
    try:
        await remove_scheduling_gate(pod_name, namespace, fresh_pod, gate_name)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="pod.gate_remove_failed", ns=namespace, pod=pod_name,
            gate=gate_name, err=exc,
        ))


async def _try_apply_toleration(
    state: ControllerState, uid: str, entry: QueueEntry,
    scheduling_gate_name: str | None = None,
) -> bool:
    """Check GPU budget and patch the toleration onto the pod if eligible.

    Returns ``True``  — entry should be removed from the queue (toleration
                        applied, or the pod already carried it).
    Returns ``False`` — entry should remain; ``entry.next_attempt_at`` has
                        been pushed forward (budget full or transient error).

    The budget check reads the in-memory occupancy map (no API round-trip) and
    optimistically records the placement before any ``await`` so two concurrent
    attempts on the single event loop cannot both claim the same slot; the record
    is rolled back on failure.

    **Does not** evaluate timing (window open/closed, retry cooldown); callers
    are responsible for those guards before invoking this function.
    """
    booking_reference = make_booking_reference(entry.reservation.id)

    available = state.available(entry.reservation, exclude_uid=uid)
    if entry.gpu_requested > available:
        now = datetime.now(timezone.utc)
        log.debug("%s", kv(
            event="pod.budget_full", ns=entry.pod_namespace, pod=entry.pod_name,
            rid=entry.reservation.id, gpus=entry.gpu_requested,
            free=available, reserved=entry.reservation.gpu_count,
        ))
        entry.next_attempt_at = _jittered_retry_at(now)
        return False

    # Optimistically reserve capacity before any await (single-threaded loop).
    state.record_placement(entry.reservation.id, uid, entry.gpu_requested)
    # And say whose it is, for a pod left waiting on the same reservation
    # (inert if this is rolled back: holders are looked up through occupancy).
    state.holder_names[uid] = (entry.pod_namespace, entry.pod_name)
    try:
        # Re-fetch the pod immediately before patching so we include any
        # tolerations that arrived since we last saw it.
        fresh_pod = await read_pod(entry.pod_name, entry.pod_namespace)

        # Drop a pod that completed while queued — mirrors the on-demand path's
        # terminal-phase drop, so a finished pod is never tolerated / stamped with
        # a guarantee (which would only fail into the warning path) (CODE-REVIEW D1c).
        if is_terminal_phase(fresh_pod):
            log.info("%s", kv(
                event="pod.queue_dropped", ns=entry.pod_namespace, pod=entry.pod_name,
                phase=get_pod_phase(fresh_pod), reason="terminal_phase",
            ))
            state.release_pod(uid)
            return True

        if pod_has_toleration(
            fresh_pod, TOLERATION_KEY, entry.gpu_class_label, "NoSchedule"
        ):
            log.info("%s", kv(
                event="pod.dequeued", ns=entry.pod_namespace, pod=entry.pod_name,
                reason="toleration_already_present",
            ))
        else:
            await apply_toleration(
                entry.pod_name,
                entry.pod_namespace,
                fresh_pod,
                TOLERATION_KEY,
                entry.gpu_class_label,
                booking_reference,
            )
            now = datetime.now(timezone.utc)
            guaranteed_until = state.compute_guaranteed_until(now, entry.reservation)
            await _record_guarantee(
                entry.pod_name,
                entry.pod_namespace,
                fresh_pod,
                guaranteed_until,
                now,
                entry.reservation,
                first_admission=True,
            )
            await _enforce_scheduling_gate_removal(
                entry.pod_name, entry.pod_namespace, fresh_pod, scheduling_gate_name
            )
            log.info("%s", kv(
                event="pod.admitted", ns=entry.pod_namespace, pod=entry.pod_name,
                rid=entry.reservation.id, clabel=entry.gpu_class_label,
                gpus=entry.gpu_requested, free=state.available(entry.reservation),
                reserved=entry.reservation.gpu_count, until=guaranteed_until,
            ))
        return True

    except Exception as exc:  # noqa: BLE001
        # Roll back the optimistic reservation so capacity is not leaked.
        state.release_pod(uid)
        log.warning("%s", kv(
            event="pod.admission_error", ns=entry.pod_namespace, pod=entry.pod_name,
            rid=entry.reservation.id, err=exc,
        ))
        entry.next_attempt_at = _jittered_retry_at(datetime.now(timezone.utc))
        return False


async def _retry_waiters(
    state: ControllerState, config: Config, reservation_id: Optional[int]
) -> None:
    """A pod holding *reservation_id* has just gone: try the pods waiting on it.

    Without this a pod waiting on a full reservation -- told its owner's other
    pods hold it (``ReservationFull``) -- was retried only on the queue tick,
    and after a cooldown, so it could sit up to two ticks after the GPU it
    wanted came free: a queue of jobs run one after another under one booking
    lost minutes between each.  A release is new information, so the cooldown
    is not honoured here; the budget check in ``_try_apply_toleration`` is
    in-memory, so a waiter that still does not fit costs nothing.  Nothing is
    told from here: a waiter left waiting is re-told on the tick, which is what
    bounds how often a reservation's churn can restate it.  *reservation_id*
    ``None`` (the pod held nothing) is a no-op, so callers pass
    ``release_pod``'s result straight through.
    """
    if reservation_id is None:
        return
    now = datetime.now(timezone.utc)
    for uid, entry in list(state.task_queue.items()):
        if (
            entry.reservation.id != reservation_id
            # Changed by another event while an earlier waiter was patched.
            or state.task_queue.get(uid) is not entry
            or not slot_start(entry.reservation) <= now < slot_end(entry.reservation)
            # Already being admitted -- by the queue tick, mid-patch: an
            # attempt records its placement before its first await -- or
            # admitted and not yet dequeued.  A second attempt would only
            # patch the pod again and post a second RuntimeGuaranteed.
            or uid in state.occupancy.get(reservation_id, {})
        ):
            continue
        if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
            state.dequeue_pod(uid)


# ---------------------------------------------------------------------------
# JIT on-demand admission: preflight, grant, and the batch orchestrator
# ---------------------------------------------------------------------------

# Preflight outcomes (see _preflight_ondemand_candidate).
_PREFLIGHT_REMOVE = "remove"  # candidate is done: gone/terminal, rerouted, or ineligible
_PREFLIGHT_RETRY = "retry"    # keep it; candidate.next_attempt_at already pushed forward
_PREFLIGHT_READY = "ready"    # candidate is a valid on-demand ask (2nd tuple element set)


async def _preflight_ondemand_candidate(
    state: ControllerState,
    config: Config,
    uid: str,
    candidate: OnDemandCandidate,
    claimed_by_class: Optional[dict[str, int]] = None,
) -> tuple[str, Optional[OnDemandAdmissionCandidate]]:
    """Vet a JIT candidate before it is offered to the app for admission.

    Runs the controller-owned eligibility checks (formerly steps 1-5 of
    ``_try_request_lease``):

    1. Re-read the pod; drop if gone/terminal/Unknown.
    2. Re-run the reserved-path routing check: a matching reservation may have
       appeared since the candidate was queued (a new booking, or simply time
       passing into the horizon) — route there instead of requesting a lease.
    3. Resolve the pod's gpu-class label to the app's numeric class id; a label
       the app does not know holds the candidate, and its owner is told with an
       ``UnknownGpuClass`` Event (``_emit_unknown_class_event``).
    4. Guard 1 (Pending for a reason a lease can fix, and the class has nodes).
    5. Guard 3 (stuck reservation-holder safety interlock).
    6. Guard 4 (over-committed gpu-class admission pause) — not applied to a
       best-effort candidate, which consumes no app-side capacity to overcommit.
       A pod held by guard 1b, 3 or 4 is told so with an
       ``OnDemandAdmissionPaused`` Event (``_emit_admission_paused_event``).
    7. Guard 5 (per-node feasibility: no single node can host the ask) — applied
       to a multi-GPU ask, and to **every** best-effort ask regardless of count,
       since nothing app-side bounds how many of those are admitted.
    8. Size the ask.

    *claimed_by_class* is the GPUs already granted earlier in the current
    admission batch, so guard 5 measures each candidate against what is left
    rather than each against the same opening.

    Returns one of:
    - ``(_PREFLIGHT_REMOVE, None)`` — drop the candidate (gone/terminal, routed
      to the reserved queue, or blocked by something no lease can fix).
    - ``(_PREFLIGHT_RETRY, None)`` — keep it; ``candidate.next_attempt_at`` has
      been pushed forward (transient read error, conditions not yet set, a
      drained gpu-class, guard-3 interlock, or unknown gpu-class id).
    - ``(_PREFLIGHT_READY, ask)`` — the candidate is a valid on-demand ask.
    """
    now = datetime.now(timezone.utc)
    try:
        fresh_pod = await read_pod(candidate.pod_name, candidate.pod_namespace)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="ondemand.pod_read_failed", ns=candidate.pod_namespace,
            pod=candidate.pod_name, err=exc,
        ))
        candidate.next_attempt_at = _jittered_retry_at(now)
        return _PREFLIGHT_RETRY, None

    phase = get_pod_phase(fresh_pod)
    if phase in TERMINAL_PHASES or phase == "Unknown":
        log.info("%s", kv(
            event="ondemand.candidate_dropped", ns=candidate.pod_namespace,
            pod=candidate.pod_name, phase=phase, reason="terminal_phase",
        ))
        return _PREFLIGHT_REMOVE, None

    # Step 2: a matching reservation may have appeared since this candidate
    # was queued (or since its last attempt) — prefer it over requesting a
    # fresh lease.
    horizon = timedelta(minutes=config.ondemand_horizon_minutes)
    admittable = state.find_admittable_reservation(
        candidate.pod_namespace,
        candidate.gpu_class_label,
        candidate.gpu_requested,
        now,
        horizon,
        candidate.group_label,
    )
    if admittable is not None:
        state.enqueue_pod(
            uid,
            candidate.pod_name,
            candidate.pod_namespace,
            candidate.gpu_class_label,
            candidate.gpu_requested,
            candidate.group_label,
            reservation=admittable,
        )
        entry = state.task_queue.get(uid)
        if entry is not None:
            # Re-fetch now: enqueue_pod just stamped next_attempt_at with its
            # own datetime.now(), which can be a hair later than the *now*
            # captured at the top of this function.
            fast_path_now = datetime.now(timezone.utc)
            if (
                slot_start(entry.reservation) <= fast_path_now < slot_end(entry.reservation)
                and fast_path_now >= entry.next_attempt_at
            ):
                if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
                    state.dequeue_pod(uid)
            # Still queued -- its booking opens within the horizon -- so it now
            # waits for that, and the newest thing it was told (a denied lease,
            # a paused class) no longer applies: say so straight away rather
            # than at the next tick.
            if uid in state.task_queue:
                await _tell_queued_status(config, state, uid, entry, fast_path_now)
        return _PREFLIGHT_REMOVE, None

    # A gpu-class label the app does not know can never be granted anything,
    # whatever the scheduler or the guards below would say, so it is settled
    # first: that way the owner hears about a mistyped label straight away,
    # rather than only once the scheduler has ruled and every guard has passed
    # (and never, if guard 1a drops the pod first).
    gpu_class_id = state.gpu_class_ids.get(candidate.gpu_class_label)
    if gpu_class_id is None:
        log.warning("%s", kv(
            event="ondemand.candidate_held", ns=candidate.pod_namespace,
            pod=candidate.pod_name, clabel=candidate.gpu_class_label,
            reason="class_id_unknown",
        ))
        candidate.next_attempt_at = _jittered_retry_at(now)
        await _emit_unknown_class_event(
            config, state, uid, candidate.pod_name, candidate.pod_namespace,
            candidate.gpu_class_label, now,
        )
        return _PREFLIGHT_RETRY, None

    # Guard 1a: is the pod Pending for something a lease could fix?  The
    # scheduler's verdict is read for the blockers it can still name — it
    # cannot name a GPU shortage on this pod's own class, whose nodes it
    # rejected on our untolerated taint before ever weighing resources.
    gpu_gated = is_gpu_gated_pending(fresh_pod, TOLERATION_KEY)
    # Record *why* the candidate is (or isn't) waiting: True only when the
    # scheduler has not yet recorded a verdict (gpu_gated is None), so the
    # MODIFIED-driven fast re-attempt in pod_watch_loop fires solely for this
    # case and never short-circuits a denial or guard-3/4 backoff.
    candidate.awaiting_schedule_signal = gpu_gated is None
    if gpu_gated is False:
        log.info("%s", kv(
            event="ondemand.candidate_dropped", ns=candidate.pod_namespace,
            pod=candidate.pod_name, reason="blocked_not_by_gpu_gating",
            detail=get_unschedulable_message(fresh_pod),
        ))
        return _PREFLIGHT_REMOVE, None
    if gpu_gated is None:
        log.debug("%s", kv(
            event="ondemand.candidate_held", ns=candidate.pod_namespace,
            pod=candidate.pod_name, guard=1, reason="schedule_verdict_pending",
        ))
        candidate.next_attempt_at = _short_retry_at(now)
        return _PREFLIGHT_RETRY, None

    # Guard 1b: the physical half of the same question.  1a concluded only that
    # *something* we might tolerate is in the way; confirm the class actually
    # has a node to land on before minting an SU-charged lease.  The inventory
    # already excludes cordoned, terminating and NotReady nodes, so a known count
    # of zero means a fully drained or down class — hold, since nodes come back.
    # A class with no data is unknown and never blocks (fail-open, matching
    # guard 5), so a snapshot gap cannot wedge admission.  (Zero is "known" for every class
    # the app knows; see node_counts_by_class.)
    #
    # Guards 1b, 3 and 4 are class-wide pauses the pod's owner can neither see
    # nor fix, so each hold is also told to them as an Event (throttled; see
    # _emit_admission_paused_event).
    class_nodes = state.class_node_counts.get(candidate.gpu_class_label)
    if class_nodes == 0:
        log.info("%s", kv(
            event="ondemand.candidate_held", guard=1, reason="no_class_nodes",
            ns=candidate.pod_namespace, pod=candidate.pod_name,
            clabel=candidate.gpu_class_label, nodes=0,
        ))
        candidate.next_attempt_at = _short_retry_at(now)
        await _emit_admission_paused_event(config, state, uid, candidate, 1, now)
        return _PREFLIGHT_RETRY, None

    # Guard 3: safety interlock — hold JIT requests for any GPU class that has
    # a stuck reservation-holder pod.  Other classes are unaffected.
    if candidate.gpu_class_label in state.stuck_holder_gpu_classes:
        log.debug("%s", kv(
            event="ondemand.candidate_held", guard=3, reason="stuck_holder_interlock",
            ns=candidate.pod_namespace, pod=candidate.pod_name,
            clabel=candidate.gpu_class_label,
        ))
        candidate.next_attempt_at = _short_retry_at(now)
        await _emit_admission_paused_event(config, state, uid, candidate, 3, now)
        return _PREFLIGHT_RETRY, None

    # Guard 4: capacity overcommit — hold JIT requests for any GPU class whose
    # app-side count exceeds observed physical capacity (set by the hourly
    # capacity audit; recomputed each tick so it clears when the deficiency is
    # resolved).  Admitting on-demand jobs onto a class the app believes is
    # larger than it physically is would mint leases that can never schedule.
    # A best-effort candidate is exempt: overcommit means the *app's* per-class
    # count exceeds physical capacity, and a best-effort stub consumes no
    # app-side count at all, so the mismatch says nothing about whether one can
    # be admitted.  Guard 5 below checks the physical question directly, and for
    # a best-effort candidate it checks it at every GPU count.
    if (
        not candidate.best_effort
        and candidate.gpu_class_label in state.overcommitted_gpu_classes
    ):
        log.info("%s", kv(
            event="ondemand.candidate_held", guard=4, reason="class_overcommitted",
            ns=candidate.pod_namespace, pod=candidate.pod_name,
            clabel=candidate.gpu_class_label,
        ))
        candidate.next_attempt_at = _short_retry_at(now)
        await _emit_admission_paused_event(config, state, uid, candidate, 4, now)
        return _PREFLIGHT_RETRY, None

    # Guard 5: per-node feasibility — a multi-GPU (>=2) pod can only schedule if
    # some single node has enough free GPUs, and node-scoped extended resources
    # (nvidia.com/gpu) cannot be split across nodes.  Granting a lease when the
    # class has budget in aggregate but the free GPUs are fragmented one-per-node
    # would mint an SU-charged reservation the pod can never run under.  Hold
    # until a single-node opening appears.  Only a *known* per-class value blocks
    # (fail-open on unknown, so a stale/absent map never wedges admission); the
    # 1-GPU path is unaffected.  See ControllerState.node_free_by_class.
    #
    # A **best-effort** candidate is checked at every GPU count, not just >=2.
    # A guaranteed lease is bounded app-side -- it holds calendar capacity, so
    # the app refuses to sell the same GPU-hour twice.  A best-effort stub is
    # zero-length and therefore invisible to that arithmetic, so the app would
    # admit an unbounded number of them onto the same idle GPU and every one
    # after the first would sit Pending under a reservation it could never run
    # under.  Nothing else closes that; this is the only physical bound on
    # best-effort admission.
    #
    # *claimed_by_class* is the running tally of GPUs already granted earlier in
    # this same batch, so a burst of candidates cannot each be measured against
    # the same single-node opening.  It does not close the window between queue
    # ticks (the snapshot refreshes on QUEUE_PROCESSOR_INTERVAL), which is
    # acceptable precisely because a best-effort over-admission is cheap: no SU
    # is charged, no capacity is held, and the pod simply waits.
    if candidate.gpu_requested >= 2 or candidate.best_effort:
        largest_free = state.node_free_by_class.get(candidate.gpu_class_label)
        if largest_free is not None:
            claimed = (claimed_by_class or {}).get(candidate.gpu_class_label, 0)
            if largest_free - claimed < candidate.gpu_requested:
                log.info("%s", kv(
                    event="ondemand.candidate_held", guard=5,
                    reason="no_single_node_fit",
                    ns=candidate.pod_namespace, pod=candidate.pod_name,
                    clabel=candidate.gpu_class_label, gpus=candidate.gpu_requested,
                    node_free=largest_free, claimed=claimed or None,
                ))
                candidate.next_attempt_at = _short_retry_at(now)
                return _PREFLIGHT_RETRY, None

    # A best-effort admission reserves no window, so there is nothing to size.
    # The field is required by the delegation schema (shared with the lease
    # path), so it carries 0 -- which is also what the app would price it at.
    duration_seconds = (
        0 if candidate.best_effort
        else candidate.min_runtime_seconds + config.ondemand_lease_buffer_minutes * 60
    )
    ask = OnDemandAdmissionCandidate(
        pod_uid=uid,
        # Evidence about the pod, alongside the ask itself.  The creation time is
        # the candidate's own FIFO key, so the app orders by exactly what the
        # controller orders by; the annotations come off *fresh_pod* rather than
        # the candidate, so a pod re-annotated while it waited is offered as it
        # is now, not as it was when first seen.
        pod_created_at=candidate.pod_created_at,
        pod_annotations=get_pod_galends_annotations(fresh_pod),
        username=candidate.pod_namespace,
        group_name=candidate.usage_group,
        gpu_class_id=gpu_class_id,
        gpu_count=candidate.gpu_requested,
        duration_seconds=duration_seconds,
    )
    return _PREFLIGHT_READY, ask


async def _post_pending_status(
    config: Config,
    state: ControllerState,
    uid: str,
    pod_name: str,
    namespace: str,
    key: tuple[str, str],
    now: datetime,
    emit: Callable[[], Awaitable[None]],
    *,
    topic: str = PENDING_TOPIC_HOLD,
) -> None:
    """Put a status Event on a still-pending pod, unless it would only repeat
    the last one too soon.

    The one throttle behind every Event addressed to the owner of a pod that is
    not running: the app refusing its lease ask (``OnDemandLeaseDenied``,
    ``OnDemandLeaseRejected``), a class-wide pause (``OnDemandAdmissionPaused``),
    nothing able to admit the pod as written (``UnknownGpuClass``,
    ``NoReservation``), an annotation it had to ignore (``AnnotationIgnored``)
    and what its reservation is waiting on (``WaitingForReservation``,
    ``ReservationFull``, ``ReservationTooSmall``).  *key* is the Event's reason
    plus the text that varies with it; *emit* writes the Event.

    **Throttled by content, not only by clock.**  A key that differs from the
    last one told is new information and is emitted immediately; an unchanged
    one waits out ``ondemand_denial_event_repeat_minutes``, because the retry
    cadence is minutes and a pod blocked for an afternoon would otherwise
    accumulate a hundred identical Events.  The repeat is what keeps the signal
    alive rather than reporting once and going quiet: Events expire (an hour,
    by default), so a pod still waiting must restate its reason or ``kubectl
    describe`` goes blank on a pod that is still blocked.  ``0`` disables the
    throttle entirely.

    **One story per pod.**  What was told is kept in ``state.pending_status``
    under the pod's uid -- not on whichever object happens to track the pod --
    so a pod moving between the candidate list, the reserved queue and no path
    at all keeps one account, and one that goes paused, then denied, then
    paused again is told each time instead of its newest Event describing a
    block that no longer applies.  *topic* keeps apart the one thing that is
    not part of that story: an ignored annotation
    (``PENDING_TOPIC_ANNOTATIONS``) stays true whatever holds the pod, and
    sharing the hold's key would make the two alternate -- each a "change" --
    and restate both on every retry.

    Best-effort throughout, like every other emitter here: a failure to emit is
    logged and never disturbs the retry cadence, which is the thing that
    actually gets the pod running.
    """
    repeat = timedelta(minutes=config.ondemand_denial_event_repeat_minutes)
    if not state.pending_status_due(uid, topic, key, now, repeat):
        return
    try:
        await emit()
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="k8s.event_failed", ns=namespace, pod=pod_name,
            reason=key[0], err=exc,
        ))
        return
    # Stamped only on a successful emit, so a failed one is retried on the next
    # attempt rather than being suppressed for the whole repeat interval.
    state.record_pending_status(uid, topic, key, now)


# How long a pod can go untold before what it *was* told is forgotten, at the
# least -- the queue tick widens it to three repeat intervals when the repeat is
# set longer.  Only a pod nothing is evaluating any more goes this long: one
# still pending re-reports on every repeat interval.
_PENDING_STATUS_RETENTION = timedelta(hours=2)


def _support_phrase(config: Config) -> str:
    """The "contact support" ending shared by every Event addressed to a pod's owner.

    The contact ends the message with no full stop after it, so an address or
    URL copied out of ``kubectl describe`` does not pick one up.
    """
    return (
        f"contact support: {config.support_contact}"
        if config.support_contact else "contact support."
    )


# Text quoted back to a pod's owner is bounded and stripped of control
# characters even though most of it is their own: kubectl prints an Event
# message verbatim to a terminal, and an annotation value is free text.
_EVENT_QUOTE_MAX_CHARS = 64


def _plain(value: str) -> str:
    """*value* made fit to print in an Event message."""
    cleaned = scrub(value)
    if len(cleaned) > _EVENT_QUOTE_MAX_CHARS:
        cleaned = cleaned[:_EVENT_QUOTE_MAX_CHARS] + "…"
    return cleaned


def _quoted(value: str) -> str:
    """*value* made fit to print, in quotes -- for free text such as an
    annotation value, where the quotes show what was actually written."""
    return f"'{_plain(value)}'"


def _listed(names: tuple[str, ...], limit: int = 3) -> str:
    """Up to *limit* names, comma-joined, with an ellipsis standing for the rest."""
    shown = ", ".join(_plain(name) for name in names[:limit])
    return shown + (", …" if len(names) > limit else "")


async def _emit_lease_denial_event(
    config: Config,
    state: ControllerState,
    uid: str,
    candidate: OnDemandCandidate,
    detail: Optional[str],
    now: datetime,
    *,
    structural: bool = False,
    not_before: Optional[datetime] = None,
) -> None:
    """Tell the pod's owner why its JIT lease was refused, via a Kubernetes Event.

    This is the app's documented 409: capacity, an SU budget or a policy gate.
    A network failure or a 5xx says nothing about the ask, and the rest of the
    non-retryable 4xx (a read-only service key, a schema mismatch) is an
    operator fault the pod's owner can neither read usefully nor act on -- it
    already gets a WARNING log line.  The one exception, a 404 for a name the
    pod itself supplied, has an Event of its own (``_emit_lease_rejected_event``).
    Throttled by ``_post_pending_status``, keyed on the app's reason and on
    whether it is *structural* -- the same reason moving between "retrying" and
    "waiting will not help" changes what the owner should do, so it is news.
    """
    if not config.ondemand_denial_event_enabled or not detail:
        return
    key_text = f"structural: {detail}" if structural else detail
    await _post_pending_status(
        config, state, uid, candidate.pod_name, candidate.pod_namespace,
        ("OnDemandLeaseDenied", key_text), now,
        lambda: emit_lease_denied_event(
            uid,
            candidate.pod_name,
            candidate.pod_namespace,
            detail,
            gpu_class=candidate.gpu_class_label,
            gpu_count=candidate.gpu_requested,
            structural=structural,
            not_before=not_before,
            support=_support_phrase(config) if structural else None,
        ),
    )


def _usage_group_source_phrase(source: Optional[str], config: Config) -> Optional[str]:
    """Where a candidate's usage group came from, in terms its owner recognises.

    *source* is ``OnDemandCandidate.usage_group_source``.  The default is the
    case that matters most: the owner never wrote that group anywhere, so
    without this they would have no idea where it came from.
    """
    if source == "label" and config.required_group_label:
        return f"from the pod's {config.required_group_label} label"
    if source == "annotation":
        return f"from the pod's {USAGE_GROUP_ANNOTATION} annotation"
    if source == "default":
        return "the cluster's default usage group, as the pod names none"
    return None


def _near_miss_hint(
    miss: NearMiss, gpu_class: str, group: Optional[str], config: Config
) -> Optional[str]:
    """Name the reservations the user holds that this pod narrowly misses.

    "No reservation matches" is the wrong thing to tell someone looking at one
    in the booking calendar, so when they hold a live booking of this class
    under another usage group, or one of another class, say so and say which
    of the pod's labels decides it.  *group* is the pod's own usage group for
    matching (``QueueEntry.group_label``).  ``None`` when there is nothing to
    add.  Stable while the user's bookings are: it ends up in a throttle key.
    """
    sentences: list[str] = []
    if miss.other_groups and config.required_group_label:
        pod_group = (
            f"this pod's usage group is {_quoted(group)}" if group
            else "this pod names no usage group"
        )
        sentences.append(
            f"You do have a gpu-class {gpu_class} reservation under usage group "
            f"{_listed(miss.other_groups)}, but {pod_group}; set its "
            f"{config.required_group_label} label to that group to use it."
        )
    if miss.other_classes:
        sentences.append(
            f"You do have a reservation for gpu-class "
            f"{_listed(miss.other_classes)}, but this pod's gpu-class label is "
            f"{gpu_class}."
        )
    return " ".join(sentences) or None


def _lease_rejected_message(
    candidate: OnDemandCandidate,
    detail: Optional[str],
    hint: Optional[str],
    config: Config,
) -> str:
    """What the owner of a pod whose lease ask the app answered 404 reads.

    Every name a 404 can be about came off the pod, so beside the app's own
    reason the message says which names were sent and where the usage group
    came from -- which the app cannot know -- and that waiting will not fix it.
    It is the throttle key (``_post_pending_status``), so it carries nothing
    that changes between retries.
    """
    what = (
        f"On-demand GPU lease for {candidate.gpu_requested} x "
        f"{candidate.gpu_class_label} was rejected by the reservation service"
    )
    if detail:
        what += f": {scrub(detail).rstrip('.')}."
    else:
        what += (
            ", which does not recognise the user, usage group or GPU class it named."
        )
    source = _usage_group_source_phrase(candidate.usage_group_source, config)
    sent = (
        f"The request named user {candidate.pod_namespace} (this pod's namespace) "
        f"and usage group {_quoted(candidate.usage_group or '')}"
        + (f" ({source})" if source else "")
        + "."
    )
    todo = (
        "Waiting will not fix this: if the usage group is wrong, correct it and "
        f"recreate the pod; if these look right, {_support_phrase(config)}"
    )
    return " ".join(part for part in (what, sent, hint, todo) if part)


async def _emit_lease_rejected_event(
    config: Config,
    state: ControllerState,
    uid: str,
    candidate: OnDemandCandidate,
    detail: Optional[str],
    now: datetime,
) -> None:
    """Tell the pod's owner the app did not recognise a name its lease ask carried.

    The app answers 404 for an unknown or inactive user, usage group or GPU
    class, and all three come from the pod: its namespace, its group label or
    ``galends/usage-group`` annotation, its ``gpu-class`` label.  So unlike the
    rest of the non-retryable 4xx this is the pod owner's to fix -- a mistyped
    group label is the usual cause -- or at least theirs to report.  The retry
    keeps its exponential backoff; only the telling is new.  Emitted even
    without a ``detail``, because the names that were sent say enough on their
    own.  Rides the denial Event's switch: it is the app's refusal of the same
    ask.
    """
    if not config.ondemand_denial_event_enabled:
        return
    # Only a booking under another usage group is worth naming here: the usual
    # 404 is a mistyped group, and the user's bookings of other classes have
    # nothing to do with it.
    miss = state.near_miss_bookings(
        candidate.pod_namespace, candidate.gpu_class_label, candidate.group_label, now,
    )
    hint = _near_miss_hint(
        miss._replace(other_classes=()),
        candidate.gpu_class_label, candidate.group_label, config,
    )
    message = _lease_rejected_message(candidate, detail, hint, config)
    await _post_pending_status(
        config, state, uid, candidate.pod_name, candidate.pod_namespace,
        (LEASE_REJECTED_REASON, message), now,
        lambda: emit_pending_pod_event(
            uid, candidate.pod_name, candidate.pod_namespace, message,
            reason=LEASE_REJECTED_REASON,
            gpu_class=candidate.gpu_class_label, gpu_count=candidate.gpu_requested,
        ),
    )


# At most this many known class labels are listed in an UnknownGpuClass Event.
_KNOWN_CLASSES_SHOWN = 10


def _unknown_class_message(
    gpu_class: str, known: tuple[str, ...], config: Config
) -> str:
    """What the owner of a pod whose gpu-class label the app does not know reads.

    Lists the classes the app does know, because a typo is the likeliest cause
    and the list is how to spot one.  Changes only when that list does.
    """
    known_classes = (
        f"Known classes: {_listed(known, _KNOWN_CLASSES_SHOWN)}."
        if known else "The reservation service lists no GPU classes at all."
    )
    return (
        f"This pod's gpu-class label is {_plain(gpu_class)}, which is not a GPU "
        f"class the reservation service knows, so no reservation can match it and "
        f"it cannot be admitted on demand. {known_classes} Correct the label and "
        f"recreate the pod; if it is right, {_support_phrase(config)}"
    )


async def _emit_unknown_class_event(
    config: Config,
    state: ControllerState,
    uid: str,
    pod_name: str,
    namespace: str,
    gpu_class: str,
    now: datetime,
) -> None:
    """Tell the pod's owner its gpu-class label names no class the app knows.

    Only once the app has listed every class it *does* know
    (``state.gpu_classes_known``): before the first successful bulk class
    fetch a label can be missing from the map for want of data, and blaming the
    pod for the controller's own gap would be false.  A class created in the app
    since the last refresh can be reported for up to one fetch interval; the
    next attempt after the refresh proceeds normally.
    """
    if not state.gpu_classes_known or not config.pod_problem_event_enabled:
        return
    message = _unknown_class_message(gpu_class, tuple(sorted(state.gpu_class_ids)), config)
    await _post_pending_status(
        config, state, uid, pod_name, namespace,
        (UNKNOWN_GPU_CLASS_REASON, message), now,
        lambda: emit_pending_pod_event(
            uid, pod_name, namespace, message,
            reason=UNKNOWN_GPU_CLASS_REASON, gpu_class=gpu_class,
        ),
    )


def _annotation_problem_clause(problem: AnnotationProblem) -> str:
    """One ignored annotation, described for the pod's owner.

    Shared by the ``AnnotationIgnored`` notice and by ``NoReservation``, which
    names an ignored annotation in place of the missing input it explains.
    """
    value = _quoted(problem.value)
    if problem.reason == "not_enabled":
        return (
            f"its {problem.annotation} annotation ({value}) has no effect, because "
            f"best-effort admission is not enabled on this cluster"
        )
    if problem.annotation == MIN_RUNTIME_ANNOTATION:
        return (
            f"its {problem.annotation} annotation is {value}, which is not a whole "
            f"number of seconds greater than zero"
        )
    accepted = ", ".join(f"'{v}'" for v in sorted(RUNTIME_GUARANTEE_VALUES))
    return (
        f"its {problem.annotation} annotation is {value}, which is not a value it "
        f"accepts ({accepted})"
    )


def _annotation_notice_message(
    problems: list[AnnotationProblem], *, config: Config
) -> str:
    """What the owner of an on-demand candidate with ignored annotations reads.

    A candidate can only have an ignored runtime if the cluster default stood
    in for it, and an ignored runtime guarantee means it is charged a lease.
    """
    one = len(problems) == 1
    parts = [
        f"Ignored {'an annotation' if one else 'annotations'} on this pod: "
        + "; ".join(_annotation_problem_clause(p) for p in problems) + "."
    ]
    if any(p.annotation == MIN_RUNTIME_ANNOTATION for p in problems):
        parts.append(
            f"The cluster's default minimum runtime of "
            f"{config.default_min_runtime_seconds} seconds is used instead."
        )
    if any(p.annotation == RUNTIME_GUARANTEE_ANNOTATION for p in problems):
        parts.append(
            "It is admitted with a guaranteed runtime, charged like any "
            "on-demand lease, rather than on a best-effort basis."
        )
    parts.append(
        f"To change that, correct the {'annotation' if one else 'annotations'} "
        f"and recreate the pod."
    )
    return " ".join(parts)


async def _emit_annotation_notice(
    config: Config,
    state: ControllerState,
    uid: str,
    pod_name: str,
    namespace: str,
    problems: list[AnnotationProblem],
    now: datetime,
) -> None:
    """Tell the owner of a pod that went ahead which of its annotations were ignored.

    For an on-demand candidate -- running on the cluster's default runtime, or
    charged a lease it asked to be spared -- where the annotation changed the
    outcome but nothing else reports it.  A pod no path will admit is told the
    same thing inside ``NoReservation`` instead, and one queued for a booking
    inside its reservation-wait Event, where in both it is why the pod cannot
    start on demand.

    On its own topic (``PENDING_TOPIC_ANNOTATIONS``): an ignored annotation
    stays true whatever holds the pod, so sharing the hold's key would make the
    two alternate and restate each other on every retry.
    """
    if not config.pod_problem_event_enabled or not problems:
        return
    message = _annotation_notice_message(problems, config=config)
    await _post_pending_status(
        config, state, uid, pod_name, namespace,
        (ANNOTATION_IGNORED_REASON, message), now,
        lambda: emit_pending_pod_event(
            uid, pod_name, namespace, message, reason=ANNOTATION_IGNORED_REASON,
        ),
        topic=PENDING_TOPIC_ANNOTATIONS,
    )


def _ondemand_ineligibility(
    config: Config,
    *,
    min_runtime: Optional[int],
    best_effort: bool,
    usage_group: Optional[str],
    problems: list[AnnotationProblem],
    has_usage_group_annotation: bool,
) -> list[str]:
    """Why a Pending pod does not qualify for on-demand admission, one clause each.

    Mirrors the ``jit_eligible`` test in ``pod_watch_loop``: on-demand admission
    switched off, no usable minimum runtime (unless best-effort stands in for
    one), no usage group.  An ignored annotation is named in place of the
    "has none" clause it explains, so the owner reads what to correct rather
    than only what is missing.  Empty when the pod qualifies.
    """
    if not config.ondemand_lease_enabled:
        return ["on-demand admission is not enabled on this cluster"]
    reasons: list[str] = []
    if min_runtime is None and not best_effort:
        runtime_problems = [p for p in problems if p.annotation == MIN_RUNTIME_ANNOTATION]
        reasons.extend(_annotation_problem_clause(p) for p in runtime_problems)
        if not runtime_problems:
            reasons.append(
                f"it has no {MIN_RUNTIME_ANNOTATION} annotation saying how long it "
                f"needs to run, in seconds"
            )
        # A runtime-guarantee annotation that could have stood in for the runtime.
        reasons.extend(
            _annotation_problem_clause(p) for p in problems
            if p.annotation == RUNTIME_GUARANTEE_ANNOTATION
        )
    if usage_group is None:
        if config.required_group_label:
            clause = (
                f"it has no {config.required_group_label} label naming its usage group"
            )
            if has_usage_group_annotation:
                clause += (
                    f" (the {USAGE_GROUP_ANNOTATION} annotation is not used on this "
                    f"cluster)"
                )
        else:
            clause = f"it has no {USAGE_GROUP_ANNOTATION} annotation naming its usage group"
        reasons.append(clause)
    return reasons


def _joined(clauses: Sequence[str]) -> str:
    """*clauses* as one list in prose: ``a``, ``a; and b``, ``a; b; and c``.

    Semicolons rather than commas, because the clauses themselves have commas.
    """
    if len(clauses) > 1:
        return "; ".join(clauses[:-1]) + "; and " + clauses[-1]
    return clauses[0]


def _no_reservation_message(
    namespace: str,
    gpu_class: str,
    group: Optional[str],
    reasons: list[str],
    hint: Optional[str],
    config: Config,
) -> str:
    """What the owner of a pod that no path will admit reads.

    States what the pod was matched on, every reason it cannot be admitted on
    demand, any booking it narrowly misses, and what to do.  The throttle key,
    so nothing in it changes between evaluations of an unchanged pod.
    """
    matched_on = f"user {namespace}, gpu-class {gpu_class}"
    if config.required_group_label and group:
        matched_on += f", usage group {_plain(group)}"
    if config.ondemand_lease_enabled:
        todo = (
            f"correct the pod and recreate it, or book a gpu-class {gpu_class} "
            f"reservation."
        )
    else:
        todo = f"book a gpu-class {gpu_class} reservation to run it."
    todo = ("Otherwise, " + todo) if hint else (todo[0].upper() + todo[1:])
    return " ".join(part for part in (
        f"No GPU reservation matches this pod ({matched_on}), and it cannot be "
        f"admitted on demand: {_joined(reasons)}.",
        hint,
        todo,
    ) if part)


async def _emit_no_reservation_event(
    config: Config,
    state: ControllerState,
    uid: str,
    pod_name: str,
    namespace: str,
    gpu_class: str,
    group: Optional[str],
    reasons: list[str],
    now: datetime,
) -> None:
    """Tell the owner of a Pending pod that nothing will admit it as written.

    The pod matches no reservation, open or future, and does not qualify for
    on-demand admission, so no path will ever pick it up; before this it logged
    ``pod.left_pending`` at DEBUG and nothing else, and sat Pending indefinitely.
    *reasons* are ``_ondemand_ineligibility``'s clauses; a booking the user
    holds that the pod narrowly misses is named too.
    """
    if not config.pod_problem_event_enabled or not reasons:
        return
    hint = _near_miss_hint(
        state.near_miss_bookings(namespace, gpu_class, group, now),
        gpu_class, group, config,
    )
    message = _no_reservation_message(namespace, gpu_class, group, reasons, hint, config)
    await _post_pending_status(
        config, state, uid, pod_name, namespace,
        (NO_RESERVATION_REASON, message), now,
        lambda: emit_pending_pod_event(
            uid, pod_name, namespace, message,
            reason=NO_RESERVATION_REASON, gpu_class=gpu_class,
        ),
    )


def _gpus(n: int) -> str:
    """``1 GPU``, ``2 GPUs``."""
    return f"{n} GPU" if n == 1 else f"{n} GPUs"


def _reservation_phrase(entry: QueueEntry) -> str:
    """A queued pod's reservation, as its owner would find it in the calendar.

    Absolute instants only: the result ends up in a throttle key, so anything
    relative ("opens in 20 minutes") would make every evaluation a change.
    """
    r = entry.reservation
    return (
        f"GPU reservation #{r.id} ({r.gpu_count} x {entry.gpu_class_label}, "
        f"{local_display(slot_start(r))} to {local_display(slot_end(r))})"
    )


def _ondemand_meanwhile(entry: QueueEntry, when: str) -> Optional[str]:
    """Why the pod cannot start on demand *when* (``before then``, ...), if
    routing recorded why (``QueueEntry.ondemand_ineligibility``)."""
    if not entry.ondemand_ineligibility:
        return None
    return (
        f"It cannot be admitted on demand {when}: "
        f"{_joined(entry.ondemand_ineligibility)}."
    )


def _holders_phrase(names: tuple[str, ...], others: int) -> Optional[str]:
    """The pods holding a reservation: up to three of the owner's by name,
    the rest counted (``ControllerState.reservation_holders``)."""
    shown = names[:3]
    hidden = len(names) - len(shown) + others
    if not shown:
        return f"{hidden} other pod{'s' if hidden != 1 else ''}" if hidden else None
    named = (
        f"your pod{'s' if len(shown) > 1 else ''} "
        + ", ".join(_plain(name) for name in shown)
    )
    return named + (f" and {hidden} other{'s' if hidden != 1 else ''}" if hidden else "")


def _waiting_for_reservation_message(entry: QueueEntry) -> str:
    """What the owner of a pod queued for a reservation not yet open reads."""
    return " ".join(part for part in (
        f"Waiting for your {_reservation_phrase(entry)} to open; this pod will be "
        f"admitted shortly after it does.",
        _ondemand_meanwhile(entry, "before then"),
    ) if part)


def _reservation_full_message(
    entry: QueueEntry, free: int, names: tuple[str, ...], others: int
) -> str:
    """What the owner of a pod whose reservation is open but taken reads.

    Names the owner's pods holding it, since a forgotten notebook server is
    the usual cause and the name is how to find it.  Changes as those pods
    come and go -- each a real change of status, told straight away.
    """
    held = _holders_phrase(names, others)
    if free:
        what = (
            f"Your {_reservation_phrase(entry)} has only {free} of its "
            f"{entry.reservation.gpu_count} GPUs free, and this pod requests "
            f"{entry.gpu_requested}" + (f"; the rest are in use by {held}" if held else "")
            + "."
        )
    else:
        what = (
            f"Your {_reservation_phrase(entry)} is fully in use"
            + (f" by {held}" if held else "") + "."
        )
    enough = _gpus(entry.gpu_requested) + (" is" if entry.gpu_requested == 1 else " are")
    return " ".join(part for part in (
        what,
        f"This pod will be admitted as soon as {enough} free.",
        _ondemand_meanwhile(entry, "meanwhile"),
    ) if part)


def _reservation_too_small_message(entry: QueueEntry) -> str:
    """What the owner of a pod asking for more GPUs than its reservation holds reads.

    Routing queues a pod on a reservation too small for it only when its owner
    holds no larger one of the class, so booking one is the fix.
    """
    return " ".join(part for part in (
        f"This pod requests {_gpus(entry.gpu_requested)}, but your "
        f"{_reservation_phrase(entry)} holds only {entry.reservation.gpu_count}, "
        f"so the pod can never be admitted under it.",
        _ondemand_meanwhile(entry, "either"),
        f"Book a gpu-class {entry.gpu_class_label} reservation of at least "
        f"{_gpus(entry.gpu_requested)}, or lower the pod's nvidia.com/gpu request "
        f"and recreate it.",
    ) if part)


def _queued_status(
    state: ControllerState, entry: QueueEntry, now: datetime
) -> Optional[tuple[str, str]]:
    """What a pod on the reserved queue is waiting on: an Event reason and message.

    ``None`` when nothing holds it there but the queue's own cadence -- the
    window is open and has room, so the next attempt admits it, or an admission
    error is being retried, which is an operator's to see rather than the
    owner's.  Too small is checked first: waiting for such a reservation to
    open would be waiting for nothing.  It also implies the owner holds no
    larger one, so it waits, like ``NoReservation``, for a full reservation
    fetch (``state.reservations_known``).
    """
    r = entry.reservation
    if entry.gpu_requested > r.gpu_count:
        # Queued on a reservation too small only when its owner holds nothing
        # larger -- which only a full fetch can show (a push is a delta).
        if not state.reservations_known:
            return None
        return RESERVATION_TOO_SMALL_REASON, _reservation_too_small_message(entry)
    if now < slot_start(r):
        return WAITING_FOR_RESERVATION_REASON, _waiting_for_reservation_message(entry)
    free = state.available(r, exclude_uid=entry.pod_uid)
    if free < entry.gpu_requested:
        names, others = state.reservation_holders(
            r.id, entry.pod_namespace, exclude_uid=entry.pod_uid
        )
        return RESERVATION_FULL_REASON, _reservation_full_message(entry, free, names, others)
    return None


async def _tell_queued_status(
    config: Config,
    state: ControllerState,
    uid: str,
    entry: QueueEntry,
    now: datetime,
) -> None:
    """Tell the owner of a pod queued for one of their reservations what it waits on.

    A queued pod used to get no Event at all: kube-scheduler's
    ``FailedScheduling`` names only an untolerated taint, and the newest Event
    the controller had put on it could be a ``NoReservation`` from before its
    owner booked -- telling them to book what they already had.  Called where
    the pod is routed to the queue (first sight, each watch resync, and a JIT
    candidate rerouted to a booking) and on every queue-processor tick it
    stays queued, through the shared throttle, so a pod waiting all afternoon
    restates its status on the repeat interval and a change -- the window
    opening onto a full reservation, a holder ending -- is told at once.
    """
    if not config.reservation_wait_event_enabled:
        return
    status = _queued_status(state, entry, now)
    if status is None:
        return
    reason, message = status
    await _post_pending_status(
        config, state, uid, entry.pod_name, entry.pod_namespace,
        (reason, message), now,
        lambda: emit_pending_pod_event(
            uid, entry.pod_name, entry.pod_namespace, message, reason=reason,
            gpu_class=entry.gpu_class_label, gpu_count=entry.gpu_requested,
        ),
    )


def _admission_paused_message(guard: int, gpu_class: str, config: Config) -> str:
    """What the owner of a pod held by class-wide guard *guard* reads.

    *guard* is 1 (meaning 1b -- the only guard-1 outcome that holds a class; 1a
    drops a pod or waits on the scheduler), 3 or 4, as in ``ondemand.gated``.

    The pod-facing sibling of ``_ondemand_gate_detail``: the same gates, told to
    a user instead of an operator.  So it says what is happening, that nothing
    about their pod needs to change, and who to ask if it lasts -- and leaves
    out what only an operator can act on: the capacity counts and, above all,
    the stuck holders' names, which are other users' pods (a pod's namespace is
    its owner's username).

    It must not change while the gate stands.  It is the throttle key
    (``_post_pending_status``), so a count or a timestamp in it would turn every
    retry into a "changed" status and restate the Event each time.
    """
    if guard == 4:
        cause = (
            f"the reservation service expects more {gpu_class} GPUs than are "
            f"currently online in the cluster (for example, a GPU node is down or "
            f"under maintenance), so no new on-demand jobs are started on this "
            f"GPU class until that is resolved"
        )
    elif guard == 3:
        cause = (
            f"jobs that already hold a reservation for {gpu_class} GPUs are still "
            f"waiting for the cluster to place them, and reserved jobs go first, "
            f"so no new on-demand jobs are started on this GPU class until they "
            f"are running"
        )
    else:  # guard 1b
        cause = (
            f"there are no {gpu_class} GPU nodes available in the cluster right now "
            f"(for example, they are all down for maintenance), so no new on-demand "
            f"jobs are started on this GPU class until one is back"
        )
    return (
        f"On-demand GPU admission for gpu-class {gpu_class} is paused: {cause}. "
        f"Nothing about this pod needs to change; it stays Pending and the "
        f"controller keeps retrying on its own. If this persists, "
        f"{_support_phrase(config)}"
    )


async def _emit_admission_paused_event(
    config: Config,
    state: ControllerState,
    uid: str,
    candidate: OnDemandCandidate,
    guard: int,
    now: datetime,
) -> None:
    """Tell the pod's owner that on-demand admission for its GPU class is paused.

    Guards 1b, 3 and 4 hold every on-demand candidate of a class until something
    outside the pod changes -- a node of the class comes back, a stuck
    reservation holder schedules, or an operator reconciles app-side and
    physical capacity -- so the app is never asked and there is no denial to
    relay.  The operator gets ``ondemand.gated`` every minute; the owner, who
    cannot read the controller's log, otherwise gets nothing but a pod that
    stays Pending.  Guard 5 is not told: it is per-ask fragmentation that clears
    as other jobs finish, i.e. a full cluster rather than a fault.

    Throttled by ``_post_pending_status`` on the same cadence as a lease denial,
    keyed on the message itself.
    """
    if not config.ondemand_pause_event_enabled:
        return
    message = _admission_paused_message(guard, candidate.gpu_class_label, config)
    await _post_pending_status(
        config, state, uid, candidate.pod_name, candidate.pod_namespace,
        ("OnDemandAdmissionPaused", message), now,
        lambda: emit_admission_paused_event(
            uid,
            candidate.pod_name,
            candidate.pod_namespace,
            message,
            gpu_class=candidate.gpu_class_label,
            guard=guard,
        ),
    )


async def _grant_and_admit(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
    uid: str,
    candidate: OnDemandCandidate,
    ask: OnDemandAdmissionCandidate,
) -> bool:
    """Create the reservation for a preflight-approved candidate and admit its pod.

    (Formerly steps 6-7 of ``_try_request_lease``.)  Requests either a
    guaranteed JIT lease or — for a candidate that declared
    ``galends/runtime-guarantee: none`` — a zero-length, zero-SU best-effort
    stub.  Both are idempotent by pod UID; on grant, the pod is admitted under
    the reservation and, if admission does not land, a compensating cancel
    (``reason="controller-revoked"``) keeps it from dangling.

    The two differ **only** in the request built here.  Everything downstream —
    denial classification, the 2-5 min vs exponential backoff, the
    ``OnDemandLeaseDenied`` Event, the upsert, the admission, the compensating
    cancel — is shared, which is what keeps a best-effort 409 reaching the pod's
    owner by the same route as a lease's with no second implementation.

    Returns ``True`` if the candidate is done (admitted, or the pod went terminal
    while granting and the compensating cancel released the reservation);
    ``False`` to retry — ``candidate.next_attempt_at`` has been pushed forward.
    """
    now = datetime.now(timezone.utc)
    pod_ref = f"{candidate.pod_namespace}/{candidate.pod_name}"
    if candidate.best_effort:
        attempt = await client.create_best_effort_reservation(
            BestEffortReservationRequest(
                username=ask.username,
                group_name=ask.group_name,
                gpu_class_id=ask.gpu_class_id,
                gpu_count=ask.gpu_count,
                idempotency_key=uid,
                notes=f"best-effort admission for pod {pod_ref}",
            )
        )
    else:
        attempt = await client.create_ondemand_reservation(
            OnDemandReservationRequest(
                username=ask.username,
                group_name=ask.group_name,
                gpu_class_id=ask.gpu_class_id,
                gpu_count=ask.gpu_count,
                duration_seconds=ask.duration_seconds,
                idempotency_key=uid,
                notes=f"on-demand lease for pod {pod_ref}",
            )
        )
    if not attempt.granted:
        if attempt.status == LEASE_DENIED_STATUS:
            # The app refused the ask (409), and its envelope says whether waiting
            # can ever change that.  Contended: capacity or a budget window may
            # free up, so keep the ordinary cadence -- or wait for ``not_before``
            # when the app knows when it clears.  Structural (``retryable: false``,
            # e.g. more GPUs than the class allows): every retry is refused alike,
            # so back off like a fault rather than asking every 2-5 min forever,
            # but keep checking -- an administrator can change the answer.  Both
            # stay INFO: the pod's owner, not the operator, is the one to act.
            if attempt.structural:
                candidate.lease_error_count += 1
                candidate.next_attempt_at = _error_retry_at(
                    now, candidate.lease_error_count
                )
            else:
                candidate.lease_error_count = 0
                candidate.next_attempt_at = _denial_retry_at(now, attempt.not_before)
            log.info("%s", kv(
                event="lease.denied", ns=candidate.pod_namespace, pod=candidate.pod_name,
                clabel=candidate.gpu_class_label, gpus=candidate.gpu_requested,
                status=attempt.status, reason=attempt.code,
                retryable=attempt.app_retryable, detail=attempt.detail,
                retry_s=int((candidate.next_attempt_at - now).total_seconds()),
            ))
            # Surface the app's reason on the pod itself: the retry cadence is
            # invisible to its owner, who otherwise sees only a pod that stays
            # Pending with nothing saying why.
            await _emit_lease_denial_event(
                config, state, uid, candidate, attempt.detail, now,
                structural=attempt.structural, not_before=attempt.not_before,
            )
        elif attempt.retryable:
            # A transient network/5xx failure: it says nothing about the ask, so
            # keep the ordinary cadence and the ordinary INFO line, and tell the
            # pod nothing.
            candidate.lease_error_count = 0
            log.info("%s", kv(
                event="lease.denied", ns=candidate.pod_namespace, pod=candidate.pod_name,
                clabel=candidate.gpu_class_label, gpus=candidate.gpu_requested,
                status=attempt.status, detail=attempt.detail,
            ))
            candidate.next_attempt_at = _jittered_retry_at(now)
        else:
            # A fault waiting will not fix — a read-only service key, a schema
            # mismatch after an app upgrade, an unknown group name.  WARNING so
            # it clears an alerting threshold, and an exponential backoff so a
            # misconfigured deployment stops hammering the app per pod forever.
            candidate.lease_error_count += 1
            candidate.next_attempt_at = _error_retry_at(now, candidate.lease_error_count)
            log.warning("%s", kv(
                event="lease.error", ns=candidate.pod_namespace, pod=candidate.pod_name,
                clabel=candidate.gpu_class_label, gpus=candidate.gpu_requested,
                status=attempt.status, fails=candidate.lease_error_count,
                retry_s=int((candidate.next_attempt_at - now).total_seconds()),
            ))
            if attempt.status == LEASE_NOT_FOUND_STATUS:
                # Of these faults, a 404 is the one the pod's owner caused: the
                # user, group and class it can name all came off the pod.
                await _emit_lease_rejected_event(
                    config, state, uid, candidate, attempt.detail, now
                )
        return False

    candidate.lease_error_count = 0
    lease = attempt.reservation
    log.info("%s", kv(
        event="lease.granted", rid=lease.id, ns=candidate.pod_namespace,
        pod=candidate.pod_name, clabel=candidate.gpu_class_label,
        gpus=candidate.gpu_requested,
        # A best-effort stub has no duration to report; omitted rather than
        # printed as a 0 that would read like a zero-length *lease*.
        lease_dur_s=None if candidate.best_effort else ask.duration_seconds,
        best_effort=candidate.best_effort or None,
    ))

    async with state.reservation_lock:
        state.reservations = apply_push_to_active(state.reservations, [lease])
        entry = QueueEntry(
            pod_uid=uid,
            pod_name=candidate.pod_name,
            pod_namespace=candidate.pod_namespace,
            gpu_class_label=candidate.gpu_class_label,
            gpu_requested=candidate.gpu_requested,
            reservation=lease,
            next_attempt_at=now,
            group_label=candidate.group_label,
        )
        admitted_queue = await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name)
        admitted = uid in state.occupancy.get(lease.id, {})
        if not admitted:
            log.warning("%s", kv(
                event="lease.admission_failed", rid=lease.id,
                ns=candidate.pod_namespace, pod=candidate.pod_name,
                detail="issuing compensating cancel",
            ))
            await client.cancel_reservation(lease.id, "controller-revoked")
            state.reservations = [r for r in state.reservations if r.id != lease.id]

    if admitted_queue:
        # Either admitted successfully, or the pod went terminal while we were
        # granting the lease (in which case the compensating cancel above
        # already released it) — either way the candidate is done.
        return True
    # Budget-full or a transient patch error: keep the candidate, which will
    # request a fresh lease on its next attempt.
    candidate.next_attempt_at = _jittered_retry_at(now)
    return False


async def _try_request_lease(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
    uid: str,
    candidate: OnDemandCandidate,
) -> bool:
    """Single-pod JIT admission: preflight, then (if ready) grant + admit.

    Thin wrapper preserving the original bool contract — ``True`` = remove the
    candidate (rerouted, dropped, or admitted), ``False`` = keep it
    (``next_attempt_at`` pushed forward).  The batch orchestrator
    (``_run_ondemand_admission``) calls the two seams separately so it can insert
    the app's admission decision between preflight and grant.
    """
    status, ask = await _preflight_ondemand_candidate(state, config, uid, candidate)
    if status == _PREFLIGHT_READY:
        assert ask is not None
        return await _grant_and_admit(state, client, config, uid, candidate, ask)
    return status == _PREFLIGHT_REMOVE


def _build_admission_request(
    ready: list[tuple[str, OnDemandCandidate, OnDemandAdmissionCandidate]],
) -> OnDemandAdmissionRequest:
    """Flatten preflight-approved candidates into an app request body."""
    return OnDemandAdmissionRequest(candidates=[ask for _, _, ask in ready])


def _map_granted_uids(
    ready: list[tuple[str, OnDemandCandidate, OnDemandAdmissionCandidate]],
    granted: list[str],
) -> set[str]:
    """Resolve the app's granted uids to the offered set, dropping unknowns.

    Only pods the controller actually *offered* can be granted: an unknown uid
    is ignored (the app chooses among the candidates but can never introduce a
    new one), so a buggy or malicious response can never make the controller
    admit a pod it did not independently deem eligible this round.
    """
    offered = {uid for uid, _, _ in ready}
    result: set[str] = set()
    for uid in granted:
        if uid in offered:
            result.add(uid)
        else:
            log.warning("%s", kv(event="ondemand.unknown_grant", poduid=uid))
    return result


async def _run_ondemand_admission_once(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
) -> None:
    """Run one on-demand admission pass over all due candidates.

    Preflights every due candidate (FIFO by creation time), offers the survivors
    to the app for LAS prioritisation when delegation is enabled, and creates +
    admits a reservation for each granted candidate.  A non-granted survivor
    cools down for a normal-clock retry (same backoff as a denial).

    ``claimed`` tallies the GPUs each survivor would take, per class, so guard 5
    measures a candidate against what is left of the class's largest single-node
    opening rather than against the same snapshot every time.  Without it a
    burst of best-effort pods would all clear a one-GPU opening in the same
    batch — the app cannot catch that, since a best-effort stub holds no
    capacity for it to count.  It is deliberately optimistic: it accrues at
    preflight, before the app has granted anything, because a candidate the app
    later defers costs only a slightly conservative guard for the rest of *this*
    batch, whereas accruing after the grant would not bound the batch at all.
    """
    now = datetime.now(timezone.utc)
    ordered = sorted(
        # Not named `kv` — that is the log-field renderer imported module-wide.
        state.ondemand_candidates.items(), key=lambda item: item[1].pod_created_at
    )
    ready: list[tuple[str, OnDemandCandidate, OnDemandAdmissionCandidate]] = []
    claimed: dict[str, int] = {}
    for uid, candidate in ordered:
        if now < candidate.next_attempt_at:
            continue
        status, ask = await _preflight_ondemand_candidate(
            state, config, uid, candidate, claimed
        )
        if status == _PREFLIGHT_REMOVE:
            state.remove_ondemand_candidate(uid)
        elif status == _PREFLIGHT_READY:
            assert ask is not None
            ready.append((uid, candidate, ask))
            claimed[candidate.gpu_class_label] = (
                claimed.get(candidate.gpu_class_label, 0) + candidate.gpu_requested
            )
        # _PREFLIGHT_RETRY: leave in place; next_attempt_at already stamped.

    if not ready:
        return

    # Default (and fallback): grant every offered candidate — today's greedy
    # per-pod behaviour.  When delegation is enabled and the app answers, its
    # subset wins; an empty answer is respected (grant none this round).
    granted = {uid for uid, _, _ in ready}
    if config.ondemand_delegate_admission:
        result = await client.select_ondemand_admissions(_build_admission_request(ready))
        if result is not None:
            granted = _map_granted_uids(ready, result)
        else:
            log.warning("%s", kv(
                event="ondemand.selection_unavailable", fallback="grant_all",
                candidates=len(ready),
            ))

    deferred_at = datetime.now(timezone.utc)
    for uid, candidate, ask in ready:
        if uid in granted:
            if await _grant_and_admit(state, client, config, uid, candidate, ask):
                state.remove_ondemand_candidate(uid)
        else:
            # The app deferred this pod this round — cool it down like a denial
            # so it is re-offered on a later tick, not spun on every trigger.
            candidate.next_attempt_at = _jittered_retry_at(deferred_at)


async def _run_ondemand_admission(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
) -> None:
    """Drive a coalesced batch of JIT on-demand admissions.

    Serialised by ``state.ondemand_admission_lock``: only one batch runs at a
    time.  A trigger arriving while a batch is in flight sets
    ``ondemand_rerun_requested`` and returns immediately (non-blocking), so an
    ADDED burst collapses into at most one in-flight + one trailing batch rather
    than launching a full batch per event.  (The lock check precedes the first
    ``await``, so the check-then-acquire is race-free under asyncio's
    single-threaded model.)
    """
    if not config.ondemand_lease_enabled:
        return
    if state.ondemand_admission_lock.locked():
        state.ondemand_rerun_requested = True
        return
    async with state.ondemand_admission_lock:
        while True:
            state.ondemand_rerun_requested = False
            # Per pass, not per batch: a coalesced trailing re-run is a distinct
            # unit of work and gets its own id, so the two do not blur together.
            with trace.scope("jit"):
                await _run_ondemand_admission_once(state, client, config)
            if not state.ondemand_rerun_requested:
                break


# ---------------------------------------------------------------------------
# Background loop 1: reservation refresh
# ---------------------------------------------------------------------------


async def reservation_fetch_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Re-fetch reservations every ``config.reservation_fetch_interval`` seconds.

    The initial fetch is done synchronously in the lifespan before this loop
    starts, so we sleep first and then enter the refresh–sleep cycle.
    """
    while True:
        await asyncio.sleep(config.reservation_fetch_interval)
        # One trace per cycle: the fetch, the class resolutions it triggers, and
        # every app call it makes all correlate — on both sides of the boundary.
        with trace.scope("fetch"):
            log.debug("%s", kv(event="fetch.start"))
            try:
                await _refresh_reservations(state, client, config)
                now = datetime.now(timezone.utc)
                state.reconcile_noshow()
                state.update_noshow_tracking(
                    now,
                    config.noshow_timeout_minutes,
                    config.noshow_grace_minutes,
                )
                log.info("%s", kv(
                    event="fetch.complete", reservations=len(state.reservations),
                    classes=len(state.gpu_class_labels),
                ))
            except Exception as exc:  # noqa: BLE001
                # exc_info so an unexpected bug (e.g. a TypeError in merge
                # arithmetic) is distinguishable from a transient API error (H2).
                log.error("%s", kv(event="fetch.failed", err=exc), exc_info=True)


# ---------------------------------------------------------------------------
# Background loop 2: pod watch
# ---------------------------------------------------------------------------


async def _teardown_ondemand_lease(
    state: ControllerState, client: ReservationClient, pod
) -> None:
    """Cancel the JIT on-demand lease backing *pod* when the pod has gone away.

    A JIT lease exists solely to cover one pod (its ``idempotency_key`` is the
    pod's UID), so once that pod terminates (clean exit / crash), is deleted, or
    is preempted, the lease is no longer needed and should be released back to
    the app — otherwise it keeps holding capacity and accruing SU until it
    naturally expires.

    The on-demand-vs-booking distinction is read live off the reservation's
    ``kind`` field (the app returns leases as ``kind="on_demand"`` and the pull
    keeps them in ``state.reservations``), so nothing about which reservations
    are leases is tracked in memory — the pod's ``galends/booking-reference``
    annotation resolves to the lease id, and its ``kind`` is looked up there.

    Best-effort: only ``kind == "on_demand"`` rows are ever touched (a user
    booking's pod ending never cancels anything), the cancel is idempotent
    (already-cancelled / gone ids are a harmless no-op), and a failure just logs
    — the next app poll reconciles.  Occupancy is released separately by the
    caller, independent of this cancel succeeding.
    """
    booking_id = parse_booking_reference(get_pod_booking_reference(pod))
    if booking_id is None:
        return
    # Hold the lock across the cancel + list edit, mirroring the compensating
    # cancel in _try_request_lease, so a concurrent fetch can't replace
    # state.reservations mid-operation.
    async with state.reservation_lock:
        res = next((r for r in state.reservations if r.id == booking_id), None)
        if res is None or res.status != "active" or res.kind != "on_demand":
            return
        log.info("%s", kv(
            event="lease.teardown", rid=booking_id, class_=res.gpu_class.name,
            reason="pod_gone",
        ))
        if await client.cancel_reservation(booking_id, "pod-terminated"):
            state.reservations = [r for r in state.reservations if r.id != booking_id]


def _parse_utc_iso(value: Optional[str]) -> Optional[datetime]:
    """Parse a ``galends/*`` timestamp annotation — the inverse of ``utc_iso``.

    Returns a tz-aware UTC ``datetime`` or ``None`` if absent/unparseable.
    Used for ``galends/guaranteed-until`` (overstay reporting) and
    ``galends/termination-warning-at`` (the headroom notice gate); an
    unparseable value degrades to "absent" rather than raising, because these
    annotations are read back off pods the controller does not fully control.
    """
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
    except ValueError:
        return None


async def _report_overstay_if_any(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
    pod,
    now: datetime,
    end_reason: str,
) -> None:
    """Best-effort: record an ended overstay for *pod* to the reservation app.

    Called when a controller-admitted pod's life ends (deleted / terminated /
    preempted) — the moment the full overstay duration is known.  The overstay
    window is ``[guarantee-end, now)``: the start is the live chain-aware
    ``guarantee_end`` when the reservation is still resolvable, else the pod's
    frozen ``galends/guaranteed-until`` annotation (the reservation may already have
    left ``state.reservations`` by teardown).  A pod that finished **within** its
    guarantee (start unresolved, or ``now <= start``) is not an overstay and is
    skipped, so nothing is reported for the common case.

    Analysis-only and fully best-effort — gated behind ``OVERSTAY_REPORT_ENABLED``
    (ships dark), the client swallows every error, and the app dedups on the pod
    UID so a preempt + DELETED double-fire records a single row.
    """
    if not config.overstay_report_enabled or client is None:
        return
    reservation_id = parse_booking_reference(get_pod_booking_reference(pod))
    if reservation_id is None:
        return  # not a controller-admitted pod
    start = state.guarantee_end(reservation_id, now=now)
    if start is None:
        # Reservation no longer active — fall back to the frozen annotation.
        _, until_str = get_pod_guarantee_status(pod)
        start = _parse_utc_iso(until_str)
    if start is None or now <= start:
        return  # unresolvable, or the pod stayed within its guarantee
    await client.report_overstay(
        reservation_id,
        OverstayReportRequest(
            pod_uid=pod.metadata.uid,
            gpu_count=get_pod_gpu_count(pod),
            start_utc=start,
            end_utc=now,
            end_reason=end_reason,
        ),
    )


async def pod_watch_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Stream pod events and update the task queue / on-demand candidates.

    Reserved path (kind="booking"):
    - ADDED / MODIFIED, no toleration → enqueue for reservation matching
    - ADDED / MODIFIED, toleration present → dequeue (already admitted)
    - DELETED → dequeue
    - ADDED inside open window → fast-path immediate toleration attempt

    JIT on-demand path (when ``config.ondemand_lease_enabled``): a pod with
    no reservation admittable now or within ``ONDEMAND_HORIZON_MINUTES`` is
    routed here instead of waiting — see ``_try_request_lease``.
    - ADDED, Pending, has ``galends/minimum-runtime-seconds`` annotation (or
      ``DEFAULT_MINIMUM_RUNTIME_SECONDS`` standing in for it), and names its usage
      group (the group label when REQUIRED_GROUP_LABEL is set, else the
      ``galends/usage-group`` annotation, with ``DEFAULT_USAGE_GROUP`` standing in
      for either — the lease ask's required ``group_name``) → add as a candidate
      and attempt a lease request immediately
    - MODIFIED carrying the scheduler's verdict (``PodScheduled`` now set) for a
      tracked candidate that was parked on an indeterminate guard-1 result →
      re-attempt immediately, so a fresh pod does not wait a full periodic scan.
      Other MODIFIED events do NOT re-trigger a batch (denial and guard retries
      ride the queue-processor tick), so a reconcile-MODIFIED burst cannot
      hammer the reservation app.
    - DELETED or terminal (Succeeded/Failed) → release any held slot, and if the
      pod was admitted under a JIT on-demand lease, cancel that lease too
      (``_teardown_ondemand_lease`` — a lease covers only its one pod)
    - MODIFIED with toleration present → dequeue from candidates (already placed)

    A pod matching neither path (no admittable/future reservation, and not
    JIT-eligible) is left Pending.
    """
    watcher = PodWatcher(label_selector="gpu-class")
    horizon = timedelta(minutes=config.ondemand_horizon_minutes)
    async for event_type, pod in watcher.events():
        # One trace per pod event: routing, the fast-path admission it may
        # trigger, and any lease teardown all share an id. A watch stream is
        # interleaved, so without this the lines of two concurrent pods'
        # handling would be indistinguishable in the log.
        with trace.scope("pod"):
            try:
                uid: str = pod.metadata.uid
                name: str = pod.metadata.name
                namespace: str = pod.metadata.namespace
                labels: dict[str, str] = pod.metadata.labels or {}
                gpu_class_label: str | None = labels.get("gpu-class")

                if not gpu_class_label:
                    # Label key present but value is empty string — skip.
                    continue

                # Optional usage-group constraint (REQUIRED_GROUP_LABEL).  None both when
                # the feature is disabled and when the pod lacks a (non-empty) value; a
                # labelless pod (feature on) matches no booking and is never JIT-eligible
                # either — it is left Pending for future "born overstay" handling, unless
                # DEFAULT_USAGE_GROUP names a group to fall back to.  The default stands
                # in for the label wholesale: the pod matches that group's reservations
                # on the reserved path exactly as if it had carried the label itself.
                group_label: str | None = (
                    labels.get(config.required_group_label) or config.default_usage_group
                    if config.required_group_label
                    else None
                )

                if event_type == "DELETED":
                    # --- reserved path cleanup ---
                    state.dequeue_pod(uid)
                    # --- JIT candidate cleanup ---
                    unplaced = state.ondemand_candidates.get(uid)
                    if unplaced is not None:
                        deletion_time = datetime.now(timezone.utc)
                        waited = int((deletion_time - unplaced.pod_created_at).total_seconds())
                        log.info("%s", kv(
                            event="ondemand.unmet_demand", ns=unplaced.pod_namespace,
                            pod=unplaced.pod_name, clabel=unplaced.gpu_class_label,
                            gpus=unplaced.gpu_requested,
                            min_runtime_s=unplaced.min_runtime_seconds,
                            submitted=unplaced.pod_created_at, deleted=deletion_time,
                            waited_s=waited,
                        ))
                    state.remove_ondemand_candidate(uid)
                    state.forget_pending_status(uid)
                    # Occupancy is the unified budget map for every admission path, so a
                    # deleted pod must always be released, regardless of on-demand
                    # placement being enabled (otherwise reserved-path budget leaks until
                    # the next reconcile).
                    released = state.release_pod(uid)
                    # Record any overstay before teardown removes the lease from
                    # state.reservations (so guarantee_end can still resolve the window).
                    await _report_overstay_if_any(
                        state, client, config, pod, datetime.now(timezone.utc), "deleted"
                    )
                    # If this pod was admitted under a JIT on-demand lease, release the
                    # lease too — it exists only to cover this pod (no-op for bookings).
                    await _teardown_ondemand_lease(state, client, pod)
                    # Its GPUs are free: a pod waiting on the same reservation
                    # need not wait for the next tick to take them.
                    await _retry_waiters(state, config, released)

                elif event_type in ("ADDED", "MODIFIED"):
                    phase = get_pod_phase(pod)
                    has_tol = pod_has_toleration(pod, TOLERATION_KEY, gpu_class_label, "NoSchedule")

                    # --- terminal pod: free its slot ---
                    # Unconditional (not gated on the on-demand flag) for the same reason
                    # as the DELETED branch: occupancy covers all paths.  The continue also
                    # keeps a terminal pod out of the has_tol keep-warm below, which would
                    # otherwise re-add it to occupancy on every MODIFIED event.
                    if phase in TERMINAL_PHASES:
                        # Off the reserved queue too, or a pod that finished
                        # before its window opened would be told it is waiting
                        # for it until the window came round.
                        state.dequeue_pod(uid)
                        state.remove_ondemand_candidate(uid)
                        state.forget_pending_status(uid)
                        released = state.release_pod(uid)
                        # Record any overstay before teardown removes the lease from
                        # state.reservations (so guarantee_end can still resolve).
                        await _report_overstay_if_any(
                            state, client, config, pod, datetime.now(timezone.utc), "pod-terminated"
                        )
                        # A pod that finished on its own no longer needs its JIT lease;
                        # cancel it if that's what admitted this pod (no-op otherwise).
                        await _teardown_ondemand_lease(state, client, pod)
                        # The next job queued on the same reservation starts now,
                        # not on the next tick.
                        await _retry_waiters(state, config, released)
                        continue

                    if has_tol:
                        # Pod already admitted — remove from whichever queue it may be in.
                        state.dequeue_pod(uid)
                        state.remove_ondemand_candidate(uid)
                        # Nothing is holding it any more, so nothing is left to tell.
                        state.forget_pending_status(uid)
                        # A reserved-path holder vouches for every window its chained
                        # session spans; pass its booking id so all are cleared at once.
                        booking_id = parse_booking_reference(get_pod_booking_reference(pod))
                        state.mark_pod_seen_for_noshow(
                            namespace, gpu_class_label, booking_id, group_label
                        )
                        # Keep occupancy warm between ticks: record this admitted pod under
                        # its booking-reference id, so capacity accounting survives a restart.
                        if booking_id is not None:
                            state.record_placement(booking_id, uid, get_pod_gpu_count(pod))
                            state.holder_names[uid] = (namespace, name)
                        continue

                    gpu_count = get_pod_gpu_count(pod)
                    now = datetime.now(timezone.utc)
                    admittable = state.find_admittable_reservation(
                        namespace, gpu_class_label, gpu_count, now, horizon, group_label
                    )

                    if admittable is not None:
                        # ---- reserved path: a match is open now, or opens soon ----
                        state.remove_ondemand_candidate(uid)
                        state.enqueue_pod(
                            uid, name, namespace, gpu_class_label, gpu_count, group_label,
                            reservation=admittable,
                        )

                        # Fast path: ADDED pod inside an open window — don't wait for
                        # the queue processor's QUEUE_PROCESSOR_INTERVAL tick (default 300 s).
                        if event_type == "ADDED":
                            entry = state.task_queue.get(uid)
                            if entry is not None:
                                # Re-fetch now: enqueue_pod just stamped next_attempt_at
                                # with its own datetime.now(), which can be a hair later
                                # than the *now* captured above for the admittable check.
                                now = datetime.now(timezone.utc)
                                # Honor the retry cooldown: on a watch reconnect every
                                # pod is replayed as ADDED, and enqueue_pod is
                                # idempotent, so without this guard the fast path would
                                # retry an entry still in budget-full/error backoff,
                                # ignoring next_attempt_at as the queue processor does (B8).
                                if (
                                    slot_start(entry.reservation) <= now < slot_end(entry.reservation)
                                    and now >= entry.next_attempt_at
                                ):
                                    log.info("%s", kv(
                                        event="pod.fast_path", ns=namespace, pod=name,
                                        rid=entry.reservation.id,
                                    ))
                                    if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
                                        state.dequeue_pod(uid)
                                # Not admitted on the spot: say what it waits for
                                # -- its window, or (having lost a race for the
                                # last GPU) its owner's other pods.
                                if uid in state.task_queue:
                                    await _tell_queued_status(config, state, uid, entry, now)
                        continue

                    # A pod may declare that it wants no runtime guarantee at
                    # all.  It is then admitted under a zero-length, zero-SU
                    # best-effort stub rather than a guaranteed lease, so it
                    # needs no minimum runtime to size one -- which is the point:
                    # a job with no runtime to promise had no way to say so, and
                    # a 0 in the minimum-runtime annotation cannot mean it (that
                    # is the floor for "at least this long").
                    best_effort = (
                        config.best_effort_enabled
                        and get_pod_runtime_guarantee_request(pod) == "none"
                    )
                    # DEFAULT_MINIMUM_RUNTIME_SECONDS stands in for a pod that never
                    # declared one (or declared junk).  0 means "no default", which
                    # leaves the historical behaviour: no annotation, no JIT.
                    min_rt = get_pod_min_runtime_seconds(pod) or (
                        config.default_min_runtime_seconds or None
                    )
                    # Usage group a JIT lease ask would carry: group_name is a
                    # *required* natural key on the app's lease-create endpoint, so a
                    # pod must name its group to be JIT-eligible — via the group label
                    # when REQUIRED_GROUP_LABEL is on (the label doubles as the group
                    # source), else via the galends/usage-group annotation.  The group
                    # label already carries DEFAULT_USAGE_GROUP when it applies; the
                    # annotation branch falls back to the same default, so one setting
                    # covers a pod that named no group by either route.
                    own_group_annotation = get_pod_usage_group(pod)
                    if config.required_group_label:
                        usage_group: str | None = group_label
                        own_group = labels.get(config.required_group_label)
                        own_source = "label"
                    else:
                        usage_group = own_group_annotation or config.default_usage_group
                        own_group = own_group_annotation
                        own_source = "annotation"
                    # Where the group came from, so a group the app turns out not
                    # to know can be traced back to what set it (see
                    # _usage_group_source_phrase).
                    usage_group_source = (
                        own_source if own_group else ("default" if usage_group else None)
                    )
                    # A best-effort pod needs no minimum runtime -- it sizes
                    # nothing -- but still needs a usage group, because
                    # group_name is a required natural key on the app's create.
                    jit_eligible = (
                        config.ondemand_lease_enabled
                        and phase == "Pending"
                        and (min_rt is not None or best_effort)
                        and usage_group is not None
                    )
                    # Job-input annotations the controller is ignoring.  Read only
                    # where they are reported -- first sight of the pod, and each
                    # watch resync -- and without the minimum runtime when the pod
                    # is best-effort, which has no use for one.
                    problems: list[AnnotationProblem] = []
                    if event_type == "ADDED":
                        problems = [
                            problem
                            for problem in get_pod_annotation_problems(
                                pod, best_effort_enabled=config.best_effort_enabled
                            )
                            if not (best_effort and problem.annotation == MIN_RUNTIME_ANNOTATION)
                        ]

                    if jit_eligible:
                        # ---- JIT on-demand path ----
                        if event_type == "ADDED":
                            ts = get_pod_creation_timestamp(pod)
                            pod_created_at = ts if ts is not None else now
                            log.debug("%s", kv(
                                event="pod.routed_jit", ns=namespace, pod=name,
                                clabel=gpu_class_label, reason="no_admittable_reservation",
                            ))
                            # One path per pod: a pod queued for a reservation
                            # that can no longer take it (it filled up, or the
                            # pod lost a race for its last GPU) goes on demand
                            # instead of being held by both.  Only here, where
                            # the candidate is added -- a MODIFIED does not add
                            # one, so dequeuing on it would leave the pod on
                            # neither path until the next resync.
                            state.dequeue_pod(uid)
                            state.add_ondemand_candidate(
                                uid, name, namespace, gpu_class_label, gpu_count,
                                min_rt or 0,
                                pod_created_at, group_label, usage_group,
                                best_effort=best_effort,
                                usage_group_source=usage_group_source,
                            )
                            # Before the admission batch, so an ignored annotation is
                            # told ahead of whatever the batch does with the pod.
                            await _emit_annotation_notice(
                                config, state, uid, name, namespace, problems, now,
                            )
                            # Responsive path: a newly-discovered candidate kicks an
                            # immediate admission batch covering it plus every other due
                            # waiter (coalesced, so an ADDED burst does not launch a
                            # batch per event).  Most MODIFIED events deliberately do
                            # NOT re-trigger — denial and guard retries ride the
                            # queue-processor tick — so a burst of reconcile MODIFIEDs
                            # cannot hammer the reservation app.
                            await _run_ondemand_admission(state, client, config)
                        elif event_type == "MODIFIED":
                            # The one MODIFIED worth reacting to: the scheduler has just
                            # recorded a verdict for a candidate we parked on an
                            # indeterminate guard-1 result.  Without this, that candidate
                            # waits up to a full periodic scan (~270-300 s) even though
                            # it became admissible within ~1 s of ADDED.
                            #
                            # Tightly scoped so the anti-hammer property holds:
                            # is_gpu_gated_pending() is a pure in-memory check on the
                            # watch object (no API call), and only a tracked candidate
                            # still flagged awaiting_schedule_signal can trigger — so an
                            # ordinary reconcile MODIFIED costs one boolean and returns.
                            # The flag is cleared before the batch runs (fires at most
                            # once per park), and because only the guard-1-None branch
                            # sets it, resetting next_attempt_at here can never defeat a
                            # denial or guard-3/4 backoff.
                            candidate = state.ondemand_candidates.get(uid)
                            if (
                                candidate is not None
                                and candidate.awaiting_schedule_signal
                                and is_gpu_gated_pending(pod, TOLERATION_KEY) is not None
                            ):
                                candidate.awaiting_schedule_signal = False
                                candidate.next_attempt_at = datetime.now(timezone.utc)
                                log.debug("%s", kv(
                                    event="ondemand.schedule_verdict", ns=namespace, pod=name,
                                ))
                                await _run_ondemand_admission(state, client, config)
                        continue

                    # Not JIT-eligible (missing the min-runtime annotation, the
                    # required group label, or the galends/usage-group annotation):
                    # preserve the existing wait-for-window behaviour if some future
                    # reservation matches, however far off or over budget; otherwise
                    # leave the pod Pending.
                    any_match = state.find_best_reservation(
                        namespace, gpu_class_label, group_label, gpu_count
                    )
                    if any_match is not None:
                        state.remove_ondemand_candidate(uid)
                        state.enqueue_pod(
                            uid, name, namespace, gpu_class_label, gpu_count, group_label,
                            reservation=any_match,
                        )
                        entry = state.task_queue.get(uid)
                        if event_type == "ADDED" and entry is not None:
                            # This pod is queued only because it could not go on
                            # demand, so what stops it is what would let it start
                            # sooner: recorded for its wait Event, which also
                            # names any annotation ignored on the way (read only
                            # on ADDED).  Nothing to record where on-demand
                            # admission is off -- there is no sooner to offer.
                            entry.ondemand_ineligibility = tuple(
                                _ondemand_ineligibility(
                                    config,
                                    min_runtime=min_rt,
                                    best_effort=best_effort,
                                    usage_group=usage_group,
                                    problems=problems,
                                    has_usage_group_annotation=(
                                        own_group_annotation is not None
                                    ),
                                )
                                if config.ondemand_lease_enabled else ()
                            )
                            await _tell_queued_status(config, state, uid, entry, now)
                    elif event_type == "ADDED":
                        log.debug("%s", kv(
                            event="pod.left_pending", ns=namespace, pod=name,
                            reason="no_match_not_jit_eligible",
                        ))
                        # Nothing will ever pick this pod up as it stands, so its
                        # owner has to be told why -- a pod not waiting on the
                        # scheduler (not Pending) has nothing to be told.
                        # "No reservation matches" is only said once a full
                        # fetch has shown the reservation list: before that
                        # it is merely empty, and the claim would be false.
                        if phase == "Pending":
                            if (
                                state.gpu_classes_known
                                and gpu_class_label not in state.gpu_class_ids
                            ):
                                await _emit_unknown_class_event(
                                    config, state, uid, name, namespace,
                                    gpu_class_label, now,
                                )
                            elif state.reservations_known:
                                await _emit_no_reservation_event(
                                    config, state, uid, name, namespace,
                                    gpu_class_label, group_label,
                                    _ondemand_ineligibility(
                                        config,
                                        min_runtime=min_rt,
                                        best_effort=best_effort,
                                        usage_group=usage_group,
                                        problems=problems,
                                        has_usage_group_annotation=(
                                            own_group_annotation is not None
                                        ),
                                    ),
                                    now,
                                )
            except Exception as exc:  # noqa: BLE001
                # One bad event (malformed object, handler bug) must not kill
                # the consumer: before this guard the task died silently, and
                # the events() generator's finally tore the watch thread down
                # with it. Log and move to the next event; CancelledError is a
                # BaseException and still propagates for shutdown.
                log.error("%s", kv(
                    event="pod.event_failed", watch_event=event_type,
                    ns=getattr(getattr(pod, "metadata", None), "namespace", None),
                    pod=getattr(getattr(pod, "metadata", None), "name", None),
                    err=exc,
                ), exc_info=True)


async def _cancel_pending_noshows(
    state: ControllerState, client: ReservationClient, snapshot: list
) -> None:
    """Durably cancel each declared no-show still awaiting one (POST
    ``/api/reservations/{id}/cancel``, ``reason="no-show"``), so the app can
    re-book the window immediately and a restart never needs to re-arm it.

    Re-verified against *snapshot* (this tick's fresh pod snapshot) first — an
    id with a pod now admitted under it (a last-second arrival racing the
    declaration) is skipped rather than cancelled out from under it.  On
    success the id is dropped from ``state.reservations`` and
    ``state.pending_noshow_cancels``; on failure it is left pending and
    retried next tick.
    """
    if not state.pending_noshow_cancels:
        return
    occupied_ids = {
        p.reservation_id
        for p in snapshot
        if p.phase in ("Running", "Pending") and p.reservation_id is not None
    }
    async with state.reservation_lock:
        for rid in sorted(state.pending_noshow_cancels):
            if rid in occupied_ids:
                log.info("%s", kv(
                    event="noshow.cancel_skipped", rid=rid, reason="pod_now_admitted",
                ))
                continue
            if await client.cancel_reservation(rid, "no-show"):
                state.reservations = [r for r in state.reservations if r.id != rid]
                state.pending_noshow_cancels.discard(rid)
                log.info("%s", kv(event="noshow.cancelled", rid=rid))
            else:
                log.warning("%s", kv(event="noshow.cancel_failed", rid=rid))


# ---------------------------------------------------------------------------
# Background loop 3: queue processor
# ---------------------------------------------------------------------------


async def queue_processor_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Every ``config.queue_processor_interval`` s, scan the work queue and apply tolerations where eligible.

    Reserved-path logic per entry:
    1. If the reservation window has expired → remove from queue.
    2. If the window hasn't opened yet, or the entry is in retry cooldown → skip.
    3. Delegate budget check + patch to ``_try_apply_toleration``.

    JIT on-demand path (when ``config.ondemand_lease_enabled``):
    4. For each candidate whose ``next_attempt_at`` has passed, attempt a lease
       request (``_try_request_lease``) in FIFO order.

    Note: reserved pods that arrive inside an open window are handled immediately
    by the pod-watch loop fast path and typically won't reach this loop at all.
    This loop covers pods queued before their window opened and retries for pods
    that were ineligible on a previous attempt.
    """
    while True:
        await asyncio.sleep(config.queue_processor_interval)
        # One trace per tick, inherited by the merge/adopt/lease work it
        # fans out — including the app calls those make.
        with trace.scope("queue"):
            try:
                await _run_queue_tick(state, client, config)
            except Exception as exc:  # noqa: BLE001
                log.error("%s", kv(event="queue.tick_failed", err=exc), exc_info=True)


async def _run_queue_tick(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """One queue-processor tick, extracted so the loop can guard it whole
    (the same loop-body shape the fetch/preemption/audit loops already use).
    See ``queue_processor_loop`` for the tick's contract.
    """
    now = datetime.now(timezone.utc)

    # One cluster snapshot of tolerated pods drives occupancy, the claimed
    # set, and guard 3 — replacing the per-attempt namespaced counts and the
    # separate guard scans.  On failure, keep the previous state rather than
    # dropping budget / no-show protection.
    snapshot = None
    try:
        snapshot = await snapshot_tolerated_pods(
            TOLERATION_KEY, config.required_group_label, config.default_usage_group
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(event="queue.snapshot_failed", target="pods", err=exc), exc_info=True)

    if snapshot is not None:
        live = [p for p in snapshot if p.phase in ("Running", "Pending")]
        # Rebuild occupancy from live tolerated pods (self-healing).
        state.reconcile_occupancy(
            [
                (p.reservation_id, p.uid, p.gpu_count)
                for p in live
                if p.reservation_id is not None
            ]
        )
        # Rebuilt alongside, so a waiting pod can be told which pods hold its
        # reservation, and so the directory cannot outgrow the cluster.
        state.holder_names = {
            p.uid: (p.namespace, p.name) for p in live if p.reservation_id is not None
        }
        # Claim every window a live holder occupies (chain-aware) before
        # declaring no-shows.
        holder_ids = [
            p.reservation_id for p in live if p.reservation_id is not None
        ]
        state.refresh_claimed_reservations(holder_ids, now)

    state.check_noshow_deadlines(now)

    # No-show → cancel: durably free the window app-side.  Skipped
    # entirely when the snapshot failed this tick; pending ids simply
    # retry next tick.
    if snapshot is not None:
        await _cancel_pending_noshows(state, client, snapshot)

    # Merge JIT-lease pods into a now-open matching booking, then adopt
    # overstay pods whose user has re-booked capacity: both re-link pods to a
    # reservation the user holds (the merge additionally retires the lease),
    # so they stop surfacing as overstay even when no boundary is near for the
    # preemption sweep to act on.  Held under the reservation lock (unlike the
    # rest of this tick) so a concurrent fetch/push cannot swap the
    # reservation set across the patch awaits.  The same view list is threaded
    # through both so a just-merged pod is not re-processed by adoption.
    # Reconcile the live guarantee-status annotations from this snapshot,
    # *before* merge/adoption: a pod whose guarantee has lapsed since it was
    # stamped flips to "overstay" here, then a rescued pod is authoritatively
    # re-stamped "guaranteed" by the adoption/merge ``_record_guarantee``
    # below — so a just-re-linked pod is never left flickering on this tick's
    # stale (pre-re-link) reservation id.  The plan reads reservation state,
    # so it is computed under the lock; the best-effort I/O runs outside.
    if snapshot is not None:
        async with state.reservation_lock:
            views = [_pod_view(p) for p in snapshot]
            status_plan = state.plan_guarantee_status(views, now)
            # Same lock acquisition and the same view list: both planners read
            # the reservation set, and taking it twice would let a fetch replace
            # that set between them, so the two annotation families could
            # describe different reservations for the same pod.
            facts_plan = state.plan_reservation_facts(views)
        await _apply_guarantee_status(snapshot, status_plan)
        # After the status reconcile, so that on the tick an in-place extension
        # lands the pod's guaranteed-until moves forward first and the fact it
        # is derived from follows.  Both are best-effort and independent; the
        # order only decides which is briefly the staler of the two.
        await _apply_reservation_facts(snapshot, facts_plan)

    if snapshot is not None and (
        config.ondemand_merge_enabled or config.pod_adoption_enabled
    ):
        async with state.reservation_lock:
            views = [_pod_view(p) for p in snapshot]
            await _merge_ondemand_into_bookings(state, client, config, views, now)
            await _adopt_pods(state, config, views, now)
    # Retry any merged-lease cancels that did not land on an earlier tick so a
    # merged lease never lingers holding capacity / accruing SU.
    await _drain_pending_merge_cancels(state, client)

    # Guard 3: refresh safety interlock from the same snapshot.
    if config.ondemand_lease_enabled and snapshot is not None:
        stuck = [
            (p.namespace, p.name, p.gpu_class)
            for p in snapshot
            if p.phase == "Pending" and p.scheduled_false and p.gpu_class
        ]
        new_classes = {gpu_class for _, _, gpu_class in stuck}
        old_classes = state.stuck_holder_gpu_classes
        state.stuck_holder_gpu_classes = new_classes
        state.stuck_holder_pods = {
            gpu_class: [f"{ns}.{name}" for ns, name, gc in stuck if gc == gpu_class]
            for gpu_class in new_classes
        }
        for gpu_class in new_classes - old_classes:
            affected = [(ns, name) for ns, name, gc in stuck if gc == gpu_class]
            log.warning("%s", kv(
                event="interlock.activated", clabel=gpu_class, guard=3,
                count=len(affected),
                pods=[f"{ns}.{name}" for ns, name in affected],
            ))
        for gpu_class in old_classes - new_classes:
            log.info("%s", kv(event="interlock.cleared", clabel=gpu_class, guard=3))

    # Guards 1b and 5: refresh per-node feasibility (largest single-node free
    # GPUs per class, and the node count behind each class) from a node-inventory
    # snapshot joined with this tick's tolerated `snapshot`, deliberately reused
    # rather than re-fetched alongside `inventory` (avoids a second wide pod LIST
    # this tick); the two calls are not atomic, so a pod that finishes scheduling
    # in the gap is briefly invisible here, making the per-node free count
    # optimistic for the node it actually landed on.  Accepted: guard 3 and the
    # compensating cancel in _grant_and_admit backstop any grant this skew lets
    # through.  Fail-safe: if either snapshot is missing, leave both prior maps
    # intact — never open admission for a class based on unknown physical state.
    # Consulted synchronously by _preflight_ondemand_candidate.
    if config.ondemand_lease_enabled and snapshot is not None:
        try:
            inventory = await snapshot_node_gpu_inventory(TOLERATION_KEY)
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="queue.snapshot_failed", target="node_inventory", err=exc,
            ), exc_info=True)
        else:
            state.node_free_by_class = largest_node_free_by_class(
                free_gpus_by_node_class(
                    inventory, [_pod_view(p) for p in snapshot]
                )
            )
            # Guard 1b reads the node *count* from the same inventory: free
            # GPUs cannot distinguish "class is full" from "class has no nodes
            # left", and those want opposite treatment (grant and wait, versus
            # do not mint a lease at all).  The classes the app knows are passed
            # in because a drained class is absent from the inventory, not zero.
            state.class_node_counts = node_counts_by_class(inventory, state.gpu_class_ids)
            # Guard 4: re-check the overcommit pause from the same inventory, so
            # it lifts (or engages) on this tick's cadence instead of waiting
            # for the hourly audit.
            _refresh_overcommit_pause(state, gpu_capacity_by_class(inventory))
            for _cls, _free in sorted(state.node_free_by_class.items()):
                log.debug("%s", kv(
                    event="queue.node_feasibility", clabel=_cls, node_free=_free,
                    nodes=state.class_node_counts.get(_cls, 0),
                ))

    to_remove: list[str] = []

    # --- reserved path ---
    for uid, entry in list(state.task_queue.items()):
        # The watch runs during this loop's awaits: skip a pod it has since
        # dequeued (deleted, admitted) or re-queued, rather than re-queue a
        # deleted pod below or tell it anything.
        if state.task_queue.get(uid) is not entry:
            continue

        # --- a reservation that can take the pod now beats the one it waits on ---
        # Routing picks the best match when it sees the pod, but that is only
        # on first sight and each watch resync: a booking made since (say, in
        # answer to ReservationTooSmall) or one a holder has since left would
        # otherwise wait for the next resync to be noticed.  Only a reservation
        # open now with room qualifies, so this never trades a booking the pod
        # is admissible under for another.
        current = entry.reservation
        if not (
            slot_start(current) <= now < slot_end(current)
            and state.available(current, exclude_uid=uid) >= entry.gpu_requested
        ):
            better = state.find_admittable_reservation(
                entry.pod_namespace, entry.gpu_class_label, entry.gpu_requested,
                now, timedelta(0), entry.group_label,
            )
            if better is not None and better.id != current.id:
                log.info("%s", kv(
                    event="pod.requeued", ns=entry.pod_namespace, pod=entry.pod_name,
                    reason="open_reservation_with_room",
                    **{"old.rid": current.id, "new.rid": better.id},
                ))
                state.enqueue_pod(
                    uid, entry.pod_name, entry.pod_namespace, entry.gpu_class_label,
                    entry.gpu_requested, entry.group_label, reservation=better,
                )
                entry = state.task_queue[uid]
                # enqueue_pod stamps its own, later, now -- which would put the
                # attempt below off to the next tick.
                entry.next_attempt_at = now

        start = slot_start(entry.reservation)
        end = slot_end(entry.reservation)

        # --- window expired ---
        if now > end:
            log.info("%s", kv(
                event="pod.queue_dropped", ns=entry.pod_namespace,
                pod=entry.pod_name, rid=entry.reservation.id,
                reason="window_expired",
            ))
            to_remove.append(uid)
            continue

        # --- window not yet open, or still in retry cooldown ---
        if now < start or now < entry.next_attempt_at:
            await _tell_queued_status(config, state, uid, entry, now)
            continue

        # --- window is active: attempt to apply the toleration ---
        if await _try_apply_toleration(state, uid, entry, config.scheduling_gate_name):
            to_remove.append(uid)
        elif state.task_queue.get(uid) is entry:
            await _tell_queued_status(config, state, uid, entry, now)

    # Route removals through the logging helper so admissions produce a
    # "Dequeued" line, not just deletions (CODE-REVIEW D5).
    for uid in to_remove:
        state.dequeue_pod(uid)

    # --- JIT on-demand path: batch-admit all due candidates in one pass
    #     (app-delegated LAS selection when enabled; grant-all fallback
    #     otherwise).  Coalesces with any watch-triggered batch. ---
    await _run_ondemand_admission(state, client, config)

    # Forget what was told to pods nothing is evaluating any more -- in practice
    # a pod deleted while the watch was down, whose DELETED event never came.
    # A pod still pending re-reports every repeat interval, so three of them (or
    # the floor, whichever is longer) cannot catch one by mistake.
    repeat = timedelta(minutes=config.ondemand_denial_event_repeat_minutes)
    state.prune_pending_status(now - max(_PENDING_STATUS_RETENTION, 3 * repeat))

    log.debug("%s", kv(
        event="queue.tick", queued=len(state.task_queue),
        candidates=len(state.ondemand_candidates),
    ))


# ---------------------------------------------------------------------------
# Background loop 4: preemption sweep
# ---------------------------------------------------------------------------


def _pod_view(
    p, inventory: Optional[dict[str, dict[str, int]]] = None
) -> PodRuntimeView:
    """Digest a ``k8s_client.ToleratedPodInfo`` into the plain view the pure
    preemption-planning functions in controller.py operate on.

    Pass the node *inventory* the caller's capacity came from whenever that
    capacity feeds free-capacity or victim planning: it is what marks a pod
    bound to a node the snapshot left out (``node_excluded``).  Without it a pod
    on a cordoned or NotReady node is subtracted from capacity that never
    included its node.
    """
    return PodRuntimeView(
        uid=p.uid,
        namespace=p.namespace,
        name=p.name,
        gpu_class=p.gpu_class,
        gpu_count=p.gpu_count,
        reservation_id=p.reservation_id,
        node_resident=(p.phase == "Running" or (p.phase == "Pending" and not p.scheduled_false)),
        terminating=p.deletion_timestamp is not None,
        group_label=p.group_label,
        node_name=p.node_name,
        termination_warning_at=_parse_utc_iso(p.termination_warning_at),
        node_excluded=(
            inventory is not None
            and p.node_name is not None
            and p.node_name not in inventory.get(p.gpu_class, {})
        ),
    )


def _overstay_description(
    state: ControllerState, view: PodRuntimeView, now: datetime
) -> str:
    """Describe *view*'s standing against its runtime guarantee, for an Event.

    Shared by both preemption messages below: every victim — boundary-driven or
    headroom-driven — is by definition past its guarantee, so the *why this pod*
    half of the message is identical and only the *why now* half differs.
    """
    end = state.guarantee_end(view.reservation_id, now=now)
    if end is None:
        return "its runtime guarantee could no longer be resolved (reservation no longer active)"
    overstay = max(0, int((now - end).total_seconds()))
    minutes, secs = divmod(overstay, 60)
    hours, minutes = divmod(minutes, 60)
    human = f"{hours}h{minutes:02d}m{secs:02d}s" if hours else f"{minutes}m{secs:02d}s"
    return f"overstayed its runtime guarantee by {human}"


def _preemption_message(
    state: ControllerState, view: PodRuntimeView, boundary: datetime, now: datetime
) -> str:
    """Build the human-readable ``Preempted`` event message for *view*."""
    return (
        f"Pod preempted to free capacity for reservation(s) starting "
        f"{local_display(boundary)}: "
        f"{_overstay_description(state, view, now)}."
    )


def _headroom_preemption_message(
    state: ControllerState, view: PodRuntimeView, now: datetime, target_percent: int
) -> str:
    """Build the ``Preempted`` event message for a headroom victim.

    Names the standing goal rather than a boundary — there is none.  The pod is
    being reclaimed so an *arriving* on-demand job finds capacity already free,
    which is a different (and less obvious) reason than "your GPUs are booked in
    15 minutes", so the message says so explicitly.
    """
    return (
        f"Pod preempted to maintain {target_percent}% free on-demand capacity "
        f"headroom for gpu class {view.gpu_class}: "
        f"{_overstay_description(state, view, now)}."
    )


async def _preempt_pod(
    state: ControllerState,
    namespace: str,
    name: str,
    uid: str,
    message: str,
    *,
    client: Optional[ReservationClient] = None,
    config: Optional[Config] = None,
    now: Optional[datetime] = None,
) -> None:
    """Delete an overstaying pod to recover capacity.

    Mirrors ``_handle_cancelled_reservations``'s shape exactly: emit the event before
    deleting (best-effort — a failed emit does not block the delete), then
    delete and release occupancy together (best-effort — if the delete fails,
    occupancy is left as-is since the pod may still be there).

    When ``client``/``config`` are passed (and overstay reporting is enabled), an
    overstay record is filed **before** the delete — a preempted pod is by
    definition past its guarantee — so the ``"preempted"`` reason wins the
    app-side dedup over the ``"deleted"`` report the pod's own DELETED watch event
    files moments later.
    """
    pod_obj = None
    try:
        pod_obj = await read_pod(name, namespace)
        await emit_preempted_event(pod_obj, name, namespace, message)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="k8s.event_failed", ns=namespace, pod=name,
            reason="Preempted", err=exc,
        ))
    if client is not None and config is not None and pod_obj is not None:
        await _report_overstay_if_any(
            state, client, config, pod_obj, now or datetime.now(timezone.utc), "preempted"
        )
    try:
        await delete_pod(name, namespace)
        state.release_pod(uid)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(event="pod.delete_failed", ns=namespace, pod=name, err=exc))


def _termination_warning_message(
    terminate_at: datetime, risk_str: str, boundary: Optional[datetime] = None
) -> str:
    """Build the human-readable ``galends/termination-warning-message`` value.

    Rendered deterministically from the projected instant, risk and cause, so
    it only changes when they do (keeping the no-op-skip comparison stable).

    The cause is *boundary* -- the start of the booking whose demand puts the
    pod at risk -- or ``None`` for a headroom notice, where no booking is
    involved.  Neither is ``terminate_at`` in general: a proactive (phase-A)
    kill lands ``PREEMPTION_LEAD_MINUTES`` before the booking starts, and a
    headroom kill has no booking at all, so the old "a reservation starting
    then" was true only for a pod whose guarantee ends exactly at the boundary.

    The instants read in local time here, while the sibling
    ``galends/termination-warning-at`` annotation keeps the UTC wire format —
    this is prose for a person, that is a value a widget parses.  Taking the
    ``datetime`` rather than the caller's already-rendered UTC string is what
    keeps the two from being confused for one another.
    """
    if boundary is None:
        why = "to keep GPUs free for on-demand jobs"
    else:
        why = (
            f"to free GPUs for a reservation starting at "
            f"{local_display(boundary)}"
        )
    return (
        f"At risk of preemption: this pod is at or nearing the end of its GPU "
        f"runtime guarantee and may be terminated as early as "
        f"{local_display(terminate_at)} {why} (risk {risk_str}). Extend or "
        f"re-book the reservation to retain capacity."
    )


async def _apply_termination_warnings(
    snapshot: "list",
    warn_plan: dict[str, TerminationWarning],
    doomed: set[str],
) -> bool:
    """Reconcile termination-warning annotations against *warn_plan*.

    *snapshot* is the same ``snapshot_tolerated_pods`` list the sweep planned
    from (each entry carries the pod's current warning annotations); *warn_plan*
    is ``ControllerState.plan_termination_warnings`` output keyed by uid.  For
    every snapshot pod not being preempted this tick (``uid not in doomed``):

    - in *warn_plan* and its (at, risk, message) differ from what the pod
      carries → write
      (``annotate_termination_warning``);
    - not in *warn_plan* but currently carrying a warning → clear
      (``clear_termination_warning``) — the pod left the at-risk pool;
    - otherwise → no-op (skip the API round-trip; risk is compared at the
      2-decimal precision it is written with, so an unchanged warning is stable).

    Each pod is independently best-effort: a failure logs a warning and never
    affects preemption.  Mirrors ``_record_guarantee``'s failure handling.

    Returns whether any pod may still carry a warning afterwards -- one it was
    just given, one a failed patch left behind, or one on a pod being deleted
    this tick (whose delete could itself fail).  The sweep keeps running on
    otherwise-quiet ticks until this comes back ``False``
    (``ControllerState.termination_warnings_outstanding``).
    """
    outstanding = bool(warn_plan)
    for p in snapshot:
        if p.uid in doomed:
            if p.termination_warning_at is not None or p.termination_warning_risk is not None:
                outstanding = True
            continue  # being deleted this tick; the delete removes any annotation
        desired = warn_plan.get(p.uid)
        try:
            if desired is not None:
                risk_str = f"{desired.risk:.2f}"
                at_str = utc_iso(desired.terminate_at)
                message = _termination_warning_message(
                    desired.terminate_at, risk_str, desired.boundary
                )
                if (
                    p.termination_warning_at == at_str
                    and p.termination_warning_risk == risk_str
                    and p.termination_warning_message == message
                ):
                    continue  # unchanged — do not re-patch
                await annotate_termination_warning(
                    p.name,
                    p.namespace,
                    desired.terminate_at,
                    risk_str,
                    message,
                )
            elif (
                p.termination_warning_at is not None
                or p.termination_warning_risk is not None
            ):
                await clear_termination_warning(p.name, p.namespace)
        except Exception as exc:  # noqa: BLE001
            outstanding = True  # whatever the pod carried, it may still carry it
            log.warning("%s", kv(
                event="pod.termination_warning_failed", ns=p.namespace, pod=p.name, err=exc,
            ))
    return outstanding


async def _apply_guarantee_status(
    snapshot: "list",
    status_plan: dict[str, GuaranteeStatus],
) -> None:
    """Reconcile the live ``galends/guarantee-status`` annotations against *status_plan*.

    *snapshot* is a ``snapshot_tolerated_pods`` list (each entry carries the
    pod's current status annotations); *status_plan* is
    ``ControllerState.plan_guarantee_status`` output keyed by uid.  For every
    pod present in the plan:

    - ``"guaranteed"`` → write the status and the refreshed ``guaranteed-until``
      when either differs from what the pod carries;
    - ``"overstay"`` → write the status alone (the pod's now-past
      ``guaranteed-until`` is left frozen) when it differs;
    - otherwise → no-op (skip the API round-trip).

    Admission / adoption / merge already stamp ``"guaranteed"`` promptly via
    ``annotate_runtime_guarantee``; this per-tick reconcile is what flips a pod
    to ``"overstay"`` once its guarantee lapses.  There is no clear path: the
    status persists for the pod's admitted life and vanishes with the pod.  Each
    pod is independently best-effort, mirroring ``_apply_termination_warnings``.
    """
    for p in snapshot:
        desired = status_plan.get(p.uid)
        if desired is None:
            continue
        try:
            if desired.guaranteed_until is not None:
                until_str = utc_iso(desired.guaranteed_until)
                if (
                    p.guarantee_status == desired.status
                    and p.guaranteed_until == until_str
                ):
                    continue  # unchanged — do not re-patch
                await annotate_guarantee_status(
                    p.name, p.namespace, desired.status, desired.guaranteed_until
                )
            else:
                if p.guarantee_status == desired.status:
                    continue  # unchanged — do not re-patch
                await annotate_guarantee_status(
                    p.name, p.namespace, desired.status, None
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="pod.guarantee_status_failed", ns=p.namespace, pod=p.name, err=exc,
            ))


async def _apply_reservation_facts(
    snapshot: "list",
    facts_plan: dict[int, ReservationResponse],
) -> None:
    """Re-stamp reservation-fact annotations on pods whose reservation has changed.

    *snapshot* is a ``snapshot_tolerated_pods`` list (each entry carries the
    fact annotations the pod already has); *facts_plan* is
    ``ControllerState.plan_reservation_facts`` output keyed by reservation id.

    ``_record_guarantee`` already stamps these at admission and on every
    **re-link** — adoption and lease-to-booking merge both move a pod to a
    different reservation and re-stamp as part of that.  What neither covers is
    the same reservation being mutated underneath a pod: an on-demand lease
    whose window is extended in place keeps its id, so nothing re-links and
    nothing re-stamps, and the pod would go on advertising a
    ``galends/reservation-end`` that has since moved.

    Diff-and-skip, and it has to be exact: the desired values are rendered with
    the same ``utc_iso`` / ``str`` the writers use, so an unchanged pod compares
    equal and costs no API call.  A format that drifted between writer and
    comparison here would re-patch every admitted pod on every tick, which is
    the failure mode this shape exists to avoid.

    Best-effort per pod, mirroring ``_apply_guarantee_status``: a failure logs
    and is retried on the next tick.
    """
    for p in snapshot:
        if p.reservation_id is None:
            continue
        res = facts_plan.get(p.reservation_id)
        if res is None:
            continue
        facts = _reservation_facts(res)
        current = (
            p.reservation_kind,
            p.reservation_start,
            p.reservation_end,
            p.reservation_gpu_count,
            p.gpu_class_name,
        )
        desired = (
            facts.kind,
            utc_iso(facts.start_utc),
            utc_iso(facts.end_utc),
            str(facts.gpu_count),
            facts.gpu_class_name,
        )
        if current == desired:
            continue  # unchanged — do not re-patch
        try:
            await annotate_reservation_facts(p.name, p.namespace, facts)
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="pod.facts_refresh_failed", ns=p.namespace, pod=p.name,
                rid=p.reservation_id, err=exc,
            ))


async def _adopt_pods(
    state: ControllerState,
    config: Config,
    pods: list[PodRuntimeView],
    now: datetime,
    *,
    cause: str = "overstay",
) -> None:
    """Re-link overstay pods to a reservation their user has since booked.

    Caller must hold ``state.reservation_lock`` and pass the ``pods`` list it
    derived from a fresh ``snapshot_tolerated_pods``.  For each rescue planned
    by ``plan_pod_adoptions`` (a pod already past its runtime guarantee whose
    user has since booked a non-abutting or differently-sized follow-on
    window), re-annotate the pod's booking-reference to the new reservation
    (the toleration is already present, so this patch just rewrites the
    annotation) and, **only on patch success**, move its occupancy and update
    the in-memory view so subsequent planning in the same tick sees the new
    binding.  Each pod is independently best-effort: a failure logs a warning
    and never deletes the pod.  *pods* is mutated in place — an adopted entry
    is replaced with a view carrying the new reservation id.

    *cause* is what the pod's owner is told.  ``"overstay"`` (the sweep and
    queue-tick tidy-up) means the pod really had run past its guarantee, and
    emits ``OverstayRelinked``.  ``"replaced"`` is the cancellation path: the
    pod counts as past guarantee there only because its reservation was just
    cancelled mid-window (typically superseded by Extend), so it emits
    ``ReservationRelinked`` rather than calling a running job an overstay.
    """
    state.require_reservation_lock("_adopt_pods")
    if not config.pod_adoption_enabled:
        return
    for view, res_new in state.plan_pod_adoptions(pods, now):
        previous_reservation_id = view.reservation_id
        booking_reference = make_booking_reference(res_new.id)
        try:
            fresh_pod = await read_pod(view.name, view.namespace)
            if is_terminal_phase(fresh_pod):
                continue
            await apply_toleration(
                view.name,
                view.namespace,
                fresh_pod,
                TOLERATION_KEY,
                view.gpu_class,
                booking_reference,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="pod.relink_failed", ns=view.namespace, pod=view.name,
                rid=res_new.id, err=exc,
            ))
            continue

        # Patch landed: re-home occupancy and refresh the in-memory view so the
        # adopted pod contributes zero demand and is no longer past-guarantee.
        state.relink_occupancy(view.uid, res_new.id, view.gpu_count)
        idx = pods.index(view)
        pods[idx] = replace(view, reservation_id=res_new.id)

        guaranteed_until = state.compute_guaranteed_until(now, res_new)
        log.info("%s", kv(
            event="pod.relinked", ns=view.namespace, pod=view.name,
            rid=res_new.id, until=guaranteed_until, reason="adoption",
        ))
        await _record_guarantee(
            view.name, view.namespace, fresh_pod, guaranteed_until, now, res_new
        )
        event_reason = "OverstayRelinked" if cause == "overstay" else "ReservationRelinked"
        try:
            if cause == "overstay":
                await emit_overstay_relinked_event(
                    fresh_pod, view.name, view.namespace, res_new.id, guaranteed_until
                )
            else:
                await emit_reservation_relinked_event(
                    fresh_pod, view.name, view.namespace, res_new.id, guaranteed_until,
                    previous_reservation_id=previous_reservation_id, cause=cause,
                )
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="k8s.event_failed", ns=view.namespace, pod=view.name,
                reason=event_reason, err=exc,
            ))


async def _merge_ondemand_into_bookings(
    state: ControllerState,
    client: ReservationClient,
    config: Config,
    pods: list[PodRuntimeView],
    now: datetime,
) -> None:
    """Merge a JIT on-demand lease's pod into a now-open matching booking.

    Caller must hold ``state.reservation_lock`` and pass the ``pods`` list it
    derived from a fresh ``snapshot_tolerated_pods``.  For each merge planned by
    ``plan_ondemand_merges`` (a pod running under a ``kind="on_demand"`` lease
    whose user now holds an open matching booking), re-link the pod's
    booking-reference to the booking — the same annotation-only patch
    ``_adopt_pods`` performs — and, **only on patch success**, retire the lease:
    cancel it penalty-exempt (``reason="superseded"``, so the app charges only
    already-consumed time, never a penalty on the unused tail — the pod's future
    time is re-covered by the booking) and drop it from ``state.reservations``.

    Ordering is deliberate: the pod is re-linked to the booking **first** (so it
    is never left stranded without a reservation), then the lease is cancelled.
    If the cancel does not land, the lease id is parked in
    ``state.pending_ondemand_merge_cancels`` for the queue processor to retry —
    the pod is already safely on its booking, and the lease's short natural
    expiry is the backstop.  Each pod is independently best-effort: a patch
    failure logs a warning and never deletes the pod.  *pods* is mutated in place
    so subsequent adoption planning in the same tick sees the new binding.
    """
    state.require_reservation_lock("_merge_ondemand_into_bookings")
    if not config.ondemand_merge_enabled:
        return
    for view, res_new in state.plan_ondemand_merges(pods, now):
        lease_id = view.reservation_id
        booking_reference = make_booking_reference(res_new.id)
        try:
            fresh_pod = await read_pod(view.name, view.namespace)
            if is_terminal_phase(fresh_pod):
                continue
            await apply_toleration(
                view.name,
                view.namespace,
                fresh_pod,
                TOLERATION_KEY,
                view.gpu_class,
                booking_reference,
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="pod.merge_failed", ns=view.namespace, pod=view.name,
                rid=res_new.id, err=exc,
            ))
            continue

        # Patch landed: re-home occupancy and refresh the in-memory view so the
        # merged pod contributes zero demand and is guaranteed by the booking.
        state.relink_occupancy(view.uid, res_new.id, view.gpu_count)
        idx = pods.index(view)
        pods[idx] = replace(view, reservation_id=res_new.id)

        guaranteed_until = state.compute_guaranteed_until(now, res_new)
        log.info("%s", kv(
            event="pod.relinked", ns=view.namespace, pod=view.name,
            rid=res_new.id, until=guaranteed_until, reason="ondemand_merge",
            **{"old.rid": lease_id},
        ))
        await _record_guarantee(
            view.name, view.namespace, fresh_pod, guaranteed_until, now, res_new
        )
        try:
            # Not OverstayRelinked: a merge does not wait for the lease guarantee
            # to lapse, so the pod was never overstaying.
            await emit_reservation_relinked_event(
                fresh_pod, view.name, view.namespace, res_new.id, guaranteed_until,
                previous_reservation_id=lease_id, cause="merge",
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(
                event="k8s.event_failed", ns=view.namespace, pod=view.name,
                reason="ReservationRelinked", err=exc,
            ))

        # Retire the now-superfluous lease (penalty-exempt: its future time is
        # re-covered by the booking).  Best-effort; parks for retry on failure.
        if lease_id is not None:
            await _cancel_merged_lease(state, client, lease_id)


async def _cancel_merged_lease(
    state: ControllerState, client: ReservationClient, lease_id: int
) -> None:
    """Cancel a lease whose pod merged into a booking; park for retry on failure.

    Caller holds ``state.reservation_lock``.  Cancels the lease penalty-exempt
    (``reason="superseded"``) and, on success, drops it from
    ``state.reservations`` so it stops holding capacity; on failure the id is
    added to ``state.pending_ondemand_merge_cancels`` for
    ``_drain_pending_merge_cancels`` to retry.  A row that is already gone or no
    longer an active lease is simply discarded from the retry set.  Idempotent
    and best-effort, mirroring ``_teardown_ondemand_lease``.
    """
    state.require_reservation_lock("_cancel_merged_lease")
    res = next((r for r in state.reservations if r.id == lease_id), None)
    if res is None or res.status != "active" or res.kind != "on_demand":
        state.pending_ondemand_merge_cancels.discard(lease_id)
        return
    if await client.cancel_reservation(lease_id, "superseded"):
        state.reservations = [r for r in state.reservations if r.id != lease_id]
        state.pending_ondemand_merge_cancels.discard(lease_id)
    else:
        state.pending_ondemand_merge_cancels.add(lease_id)


async def _drain_pending_merge_cancels(
    state: ControllerState, client: ReservationClient
) -> None:
    """Retry penalty-exempt cancels for merged leases that did not land earlier.

    A merge re-links the pod to its booking immediately, but the lease cancel is
    best-effort; a failed cancel parks the lease id in
    ``state.pending_ondemand_merge_cancels``.  This drains that set (mirroring
    ``_cancel_pending_noshows``) so a merged lease never lingers holding capacity
    / accruing SU.  Held under the reservation lock so a concurrent fetch/push
    cannot swap the reservation set across the cancel awaits.
    """
    if not state.pending_ondemand_merge_cancels:
        return
    async with state.reservation_lock:
        for lease_id in sorted(state.pending_ondemand_merge_cancels):
            await _cancel_merged_lease(state, client, lease_id)


def _build_selection_request(need: BoundaryPreemptionNeed) -> PreemptionSelectionRequest:
    """Flatten a boundary's per-class candidate pool into an app request body."""
    candidates = [
        PreemptionCandidate(
            pod_uid=p.uid,
            namespace=p.namespace,
            pod_name=p.name,
            gpu_class=gpu_class,
            gpu_count=p.gpu_count,
            reservation_id=p.reservation_id,
        )
        for gpu_class, cands in need.candidates_by_class.items()
        for p in cands
    ]
    return PreemptionSelectionRequest(
        needed_by_class=dict(need.kills_needed_by_class),
        candidates=candidates,
    )


def _map_selected_victims(
    need: BoundaryPreemptionNeed, uids: list[str]
) -> dict[str, list[PodRuntimeView]]:
    """Resolve the app's chosen ``pod_uid``s back to candidate views, by class.

    Only pods the controller actually *offered* can be selected: an unknown or
    duplicate uid is dropped (the app can choose among the candidates but can
    never introduce a new victim), so a buggy or malicious response can never
    make the controller kill a pod it did not independently deem preemptable.
    """
    by_uid = {
        p.uid: (gpu_class, p)
        for gpu_class, cands in need.candidates_by_class.items()
        for p in cands
    }
    selected: dict[str, list[PodRuntimeView]] = {}
    seen: set[str] = set()
    for uid in uids:
        entry = by_uid.get(uid)
        if entry is None:
            log.warning("%s", kv(
                event="preempt.unknown_victim", poduid=uid,
            ))
            continue
        if uid in seen:
            continue
        seen.add(uid)
        gpu_class, view = entry
        selected.setdefault(gpu_class, []).append(view)
    return selected


async def _select_victims(
    config: Config,
    client: Optional[ReservationClient],
    need: BoundaryPreemptionNeed,
) -> dict[str, list[PodRuntimeView]]:
    """Choose victims for one reclaim need, delegating to the app when possible.

    Serves both reclaim passes — a boundary's demand and the anticipatory
    headroom goal — because ``BoundaryPreemptionNeed`` is all either produces and
    ``PreemptionSelectionRequest`` carries no boundary, so the app sees the same
    "here is a pool and a number" question either way.

    When delegation is enabled and a client is available, the eligible pool is
    sent to the app (``POST /api/reservations/preemption-victims``) so it can
    prioritise; the returned uids are mapped back to candidate views.  A
    ``None`` return (endpoint absent / network / parse failure) falls back to
    local uniform-random selection so preemption still works when the app is
    unreachable — an empty list, by contrast, is a deliberate app decision and
    is respected as-is.
    """
    if config.preemption_delegate_selection and client is not None:
        uids = await client.select_preemption_victims(_build_selection_request(need))
        if uids is not None:
            return _map_selected_victims(need, uids)
        log.warning("%s", kv(event="preempt.selection_unavailable", fallback="local_random"))
    return select_victims_locally(need)


async def _run_preemption_sweep(
    state: ControllerState,
    config: Config,
    client: Optional[ReservationClient] = None,
    now: Optional[datetime] = None,
) -> None:
    """One preemption-sweep evaluation: clear boundary demand, then hold headroom.

    Two reclaim passes share this sweep's snapshots and lock.  The **boundary**
    pass (below) frees capacity a booking needs at an imminent ``slot_start``.
    The **headroom** pass then holds ``HEADROOM_TARGET_PERCENT`` of each class
    free for on-demand jobs that have not arrived yet — anticipatory rather than
    demand-driven, throttled to ``HEADROOM_CHECK_INTERVAL``, and gated on a
    notice period so a doomed job is warned before it is killed.

    For each slot boundary within ``PREEMPTION_LEAD_MINUTES`` of *now* whose
    phase ("A" = lead-time, "B" = at-boundary) has not already been evaluated,
    plan the kills needed to cover its demand and execute them.  The two
    snapshots (pods, node capacity) are taken outside the lock; either
    failing skips the whole sweep with a WARNING — the controller never kills
    a pod based on unknown physical state.  Planning and the resulting
    deletions run under ``reservation_lock`` (mirrors the
    cancellation/owner-change eviction paths); boundaries are
    processed in ascending order with a running ``doomed`` set so one sweep
    never double-selects a pod's GPUs across two boundaries.
    """
    now = now or datetime.now(timezone.utc)
    lead = timedelta(minutes=config.preemption_lead_minutes)
    state.prune_preemption_marks(now, lead)
    boundaries = state.upcoming_boundaries(now, lead)
    # Warnings look further ahead than the kill window (see the warn block under
    # the lock): a phase-A victim's boundary can sit beyond the kill window yet
    # still warrant advance notice, so the sweep must run — and take its
    # snapshots — even when there are no kill-window boundaries.  forecast_boundaries
    # is forward-only (never widens the already-open side) and drops no-shows;
    # unioning with the kill window floors the horizon there.
    warn_boundaries: list[datetime] = []
    if config.termination_warning_enabled:
        warning_lead = timedelta(minutes=config.termination_warning_lead_minutes)
        warn_boundaries = sorted(
            set(boundaries) | set(state.forecast_boundaries(now, now + warning_lead))
        )
    # Anticipatory headroom rides this sweep rather than a loop of its own: one
    # pair of cluster snapshots, and — decisively — one writer for the
    # termination-warning annotations, which a second loop would race.  It is
    # throttled to its own (much slower) interval so an otherwise-idle cluster is
    # not LISTed on the sweep's 60 s cadence just to re-check headroom.
    headroom_on = config.headroom_target_percent > 0
    headroom_due = headroom_on and (
        state.headroom_last_eval is None
        or now - state.headroom_last_eval
        >= timedelta(seconds=config.headroom_check_interval)
    )
    # A tick with nothing to kill, warn about or re-check skips its snapshots --
    # unless a warning may still be standing on some pod, in which case it runs
    # so the reconcile below can clear it.  Without that, a warning whose
    # booking was cancelled, or whose boundary passed without killing the pod,
    # outlived its cause until another boundary happened to come into range.
    warnings_to_reconcile = (
        config.termination_warning_enabled and state.termination_warnings_outstanding
    )
    if (
        not boundaries and not warn_boundaries and not headroom_due
        and not warnings_to_reconcile
    ):
        return

    try:
        snapshot = await snapshot_tolerated_pods(
            TOLERATION_KEY, config.required_group_label, config.default_usage_group
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(event="preempt.snapshot_failed", target="pods", err=exc), exc_info=True)
        return
    try:
        inventory = await snapshot_node_gpu_inventory(TOLERATION_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="preempt.snapshot_failed", target="node_capacity", err=exc,
        ), exc_info=True)
        return
    # Per node rather than per class, so the views can be marked against the
    # same node set the capacity counts: a pod on a cordoned or NotReady node
    # must neither be subtracted from capacity that excludes its node nor be
    # offered as a victim whose death frees nothing placeable.
    capacity = gpu_capacity_by_class(inventory)

    async with state.reservation_lock:
        pods = [_pod_view(p, inventory) for p in snapshot]
        # Merge a JIT lease's pod into a now-open matching booking, then rescue
        # overstay pods whose user has re-booked capacity — both before planning
        # any kills.  A merged/adopted pod's occupancy re-homes to its booking
        # (zeroing that boundary's demand) and its refreshed view is no longer
        # past-guarantee, so it can never be selected as a victim.
        await _merge_ondemand_into_bookings(state, client, config, pods, now)
        await _adopt_pods(state, config, pods, now)
        doomed: set[str] = set()
        to_kill: list[tuple[PodRuntimeView, str]] = []
        for boundary in boundaries:
            phase = "B" if boundary <= now else "A"
            fired = state.preemption_fired.setdefault(boundary, set())
            if phase in fired:
                continue
            fired.add(phase)
            available_pods = [p for p in pods if p.uid not in doomed]
            need = state.plan_boundary_candidates(boundary, capacity, available_pods, now)
            # Delegate the *choice* of victims to the app (it can prioritise);
            # the controller still owns which pods are eligible at all.  Skip the
            # round-trip entirely when nothing needs reclaiming here.
            if need.kills_needed_by_class:
                selected = await _select_victims(config, client, need)
            else:
                selected = {}
            plan = build_preemption_plan(need, selected)
            # demand/free/kills are per-class maps, so they are fanned out to one
            # line per class rather than emitted as dicts inside single fields —
            # each class's shortfall is then independently greppable and alertable.
            kills_by_class: dict[str, int] = {}
            for victim in plan.victims:
                kills_by_class[victim.gpu_class] = kills_by_class.get(victim.gpu_class, 0) + 1
            for gpu_class, demand in sorted(plan.demand_by_class.items()):
                log.info("%s", kv(
                    event="preempt.boundary", boundary=boundary, sweep=phase,
                    clabel=gpu_class, demand=demand,
                    free=plan.free_by_class.get(gpu_class, 0),
                    kills=kills_by_class.get(gpu_class, 0),
                ))
            for gpu_class, shortfall in sorted(plan.unmet_by_class.items()):
                log.warning("%s", kv(
                    event="preempt.unmet", boundary=boundary, sweep=phase,
                    clabel=gpu_class, short=shortfall,
                ))
            for victim in plan.victims:
                doomed.add(victim.uid)
                to_kill.append((victim, _preemption_message(state, victim, boundary, now)))

        # Anticipatory headroom: reclaim from overstayers to hold a fixed
        # fraction of each class free for on-demand jobs that have not arrived
        # yet.  Runs after the boundary loop and is handed the post-kill pod set,
        # so GPUs already freed above count towards the headroom goal and one
        # pod is never selected twice — the same running-``doomed`` discipline
        # the boundary loop uses across boundaries.  No preemption_fired mark:
        # headroom is a standing goal, not a one-shot per boundary/phase.
        if headroom_due:
            state.headroom_last_eval = now
            available_pods = [p for p in pods if p.uid not in doomed]
            need = state.plan_headroom_candidates(
                capacity, available_pods, now, config.headroom_target_percent,
                require_elapsed_notice=config.termination_warning_enabled,
            )
            if need.kills_needed_by_class:
                selected = await _select_victims(config, client, need)
            else:
                selected = {}
            plan = build_preemption_plan(need, selected)
            kills_by_class: dict[str, int] = {}
            for victim in plan.victims:
                kills_by_class[victim.gpu_class] = kills_by_class.get(victim.gpu_class, 0) + 1
            for gpu_class, target in sorted(plan.demand_by_class.items()):
                log.info("%s", kv(
                    event="preempt.headroom", clabel=gpu_class, demand=target,
                    free=plan.free_by_class.get(gpu_class, 0),
                    kills=kills_by_class.get(gpu_class, 0),
                ))
            for gpu_class, shortfall in sorted(plan.unmet_by_class.items()):
                log.warning("%s", kv(
                    event="preempt.headroom_unmet", clabel=gpu_class, short=shortfall,
                ))
            for victim in plan.victims:
                doomed.add(victim.uid)
                to_kill.append((victim, _headroom_preemption_message(
                    state, victim, now, config.headroom_target_percent,
                )))

        # After all kills are chosen, flag the post-kill survivors that are still
        # at risk of preemption at an in-scope boundary (see
        # plan_termination_warnings).  Computed under the lock because it reads
        # reservation state; the annotation I/O runs after the lock is released.
        warn_plan: dict[str, TerminationWarning] = {}
        if config.termination_warning_enabled:
            pods_after = [p for p in pods if p.uid not in doomed]
            warn_plan = state.plan_termination_warnings(
                warn_boundaries, capacity, pods_after, now, lead
            )
            # Headroom warnings are recomputed on *every* sweep, not only when
            # the throttled kill evaluation above ran.  They are pure arithmetic
            # over snapshots already in hand, so they are free — and omitting
            # them on a boundary-only tick would leave a headroom-warned pod out
            # of warn_plan, whereupon _apply_termination_warnings clears its
            # annotation and the notice can never ripen into a kill.
            if headroom_on:
                for uid, warning in state.plan_headroom_warnings(
                    capacity, pods_after, now, config.headroom_target_percent,
                    timedelta(minutes=config.headroom_notice_minutes),
                ).items():
                    existing = warn_plan.get(uid)
                    # Keep whichever deadline is sooner: the pod should be told
                    # the earliest instant it could die, from any cause.
                    if existing is None or warning.terminate_at < existing.terminate_at:
                        warn_plan[uid] = warning

        for victim, message in to_kill:
            log.info("%s", kv(
                event="pod.preempting", ns=victim.namespace, pod=victim.name,
                clabel=victim.gpu_class, gpus=victim.gpu_count, detail=message,
            ))
            await _preempt_pod(
                state, victim.namespace, victim.name, victim.uid, message,
                client=client, config=config, now=now,
            )

    # Reconcile warning annotations outside the lock — it is best-effort pod
    # patching that mutates no controller state.  Pods preempted this tick
    # (``doomed``) are skipped; the delete removes any annotation they carried.
    if config.termination_warning_enabled:
        state.termination_warnings_outstanding = await _apply_termination_warnings(
            snapshot, warn_plan, doomed
        )


async def preemption_loop(
    state: ControllerState, client: ReservationClient, config: Config
) -> None:
    """Every ``config.preemption_check_interval`` s, run a preemption sweep.

    Passes the reservation *client* through so the sweep can delegate victim
    selection to the app (``PREEMPTION_DELEGATE_SELECTION``); the sweep falls
    back to local random selection if that call fails.
    """
    while True:
        await asyncio.sleep(config.preemption_check_interval)
        with trace.scope("sweep"):
            try:
                await _run_preemption_sweep(state, config, client)
            except Exception as exc:  # noqa: BLE001
                log.error("%s", kv(event="preempt.sweep_failed", err=exc), exc_info=True)


# ---------------------------------------------------------------------------
# Background loop 5: capacity audit
# ---------------------------------------------------------------------------


async def _run_capacity_audit(
    state: ControllerState,
    config: Config,
) -> None:
    """One capacity audit: compare app-side vs physical per-class GPU capacity.

    The app-side counts come from ``state.gpu_class_capacity`` (the reservation
    app's ``effective_gpus_today`` per class — its ``total_gpus`` after any date-
    span capacity override covering today, refreshed each reservation fetch; see
    ``GpuClassDetail.audit_gpus``).  Physical capacity is snapshotted live from
    Kubernetes node taints.  Any difference is logged at WARNING; any class the
    app believes is larger than it physically is (``app_side > physical``) is
    added to ``state.overcommitted_gpu_classes``, which pauses new on-demand
    admissions for that class only.  The pause set itself is also recomputed
    every queue-processor tick (``_refresh_overcommit_pause``); this audit is
    what reports the mismatches.

    Fail-safe: if the node snapshot fails, the audit is skipped and the current
    pause set is left unchanged — a transient LIST failure must never silently
    lift a pause (the same "never act on unknown physical state" rule the
    preemption sweep follows).
    """
    try:
        physical = await snapshot_node_gpu_capacity(TOLERATION_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="capacity_audit.snapshot_failed", target="node_capacity", err=exc,
        ), exc_info=True)
        return

    for diff in _refresh_overcommit_pause(state, physical):
        log.warning("%s", kv(
            event="capacity_audit.mismatch", clabel=diff.label,
            app_gpus=diff.app_side, phys_gpus=diff.physical,
            overcommitted=diff.overcommitted,
        ))


def _refresh_overcommit_pause(
    state: ControllerState, physical: dict[str, int]
) -> list[CapacityDiff]:
    """Recompute the guard-4 pause set from a fresh *physical* snapshot.

    Shared by the hourly capacity audit and every queue-processor tick (which
    already takes a node inventory for guards 1b and 5), so a pause is set or
    lifted within one ``QUEUE_PROCESSOR_INTERVAL`` of the counts changing rather
    than up to a full ``CAPACITY_CHECK_INTERVAL`` later.  Logs only the pause
    set's transitions; the per-class ``capacity_audit.mismatch`` WARNING stays
    the audit's, on its hourly cadence, via the returned diffs.  Callers must
    pass a snapshot that succeeded — a failed one never reaches here, so the
    pause set is left as it was (fail-safe).
    """
    diffs, overcommitted = reconcile_capacity(dict(state.gpu_class_capacity), physical)
    previous = state.overcommitted_gpu_classes
    newly_paused = overcommitted - previous
    resumed = previous - overcommitted
    if newly_paused:
        log.info("%s", kv(
            event="capacity_audit.paused", clabels=sorted(newly_paused),
        ))
    if resumed:
        log.info("%s", kv(
            event="capacity_audit.resumed", clabels=sorted(resumed),
        ))
    state.overcommitted_gpu_classes = overcommitted
    state.physical_gpu_capacity = dict(physical)
    return diffs


async def capacity_audit_loop(
    state: ControllerState, config: Config
) -> None:
    """Every ``config.capacity_check_interval`` s (default hourly), audit
    app-side vs physical GPU capacity and update the on-demand pause set."""
    while True:
        await asyncio.sleep(config.capacity_check_interval)
        with trace.scope("audit"):
            try:
                await _run_capacity_audit(state, config)
            except Exception as exc:  # noqa: BLE001
                log.error("%s", kv(event="capacity_audit.failed", err=exc), exc_info=True)


# ---------------------------------------------------------------------------
# Background loop 6: on-demand gate warning
# ---------------------------------------------------------------------------

# Fixed rather than configurable: the point is a line that keeps showing up in
# whatever log view an operator has open until the cause is fixed, and a knob to
# quieten it would be the wrong fix for that.
ONDEMAND_GATE_WARNING_INTERVAL_S = 60


def _ondemand_gate_detail(gate: OnDemandGate, config: Config) -> str:
    """Plain-English account of *gate*: what is paused, why, what to check,
    and what lifts it — written for an operator who has never seen this
    service.  Rendered into the ``detail=`` field of ``ondemand.gated``."""
    c = gate.label
    paused = (
        f"On-demand GPU jobs for GPU class '{c}' are paused: the controller is "
        f"not granting new on-demand reservations for pods labelled gpu-class={c}, "
        f"so those pods stay Pending."
    )
    if gate.guard == 4:
        app = "an unknown number of" if gate.app_gpus is None else str(gate.app_gpus)
        phys = "fewer" if gate.phys_gpus is None else f"only {gate.phys_gpus}"
        best_effort = (
            " Best-effort pods (galends/runtime-guarantee: none) are not affected."
            if config.best_effort_enabled else ""
        )
        return (
            f"{paused} Cause: capacity mismatch. The reservation app believes this "
            f"class has {app} GPUs today, but schedulable Kubernetes nodes tainted "
            f"{TOLERATION_KEY}={c} provide {phys} (allocatable nvidia.com/gpu, or "
            f"the node's galends/force-node-capacity annotation). Leases sold "
            f"against GPUs that do not exist could never run. To fix: check for "
            f"cordoned, NotReady or missing GPU nodes and a failing NVIDIA device "
            f"plugin (kubectl get nodes; kubectl describe node <node>), or, if the "
            f"hardware is really gone, lower the class's GPU count or add a capacity "
            f"override for today in the reservation app.{best_effort} Clears "
            f"automatically within one queue tick (every "
            f"{config.queue_processor_interval}s, QUEUE_PROCESSOR_INTERVAL) once the "
            f"counts agree."
        )
    if gate.guard == 3:
        return (
            f"{paused} Cause: safety interlock. {len(gate.stuck_pods)} pod(s) already "
            f"admitted under a reservation for this class are stuck Pending — the "
            f"scheduler cannot place them even with the reservation toleration — so "
            f"the controller assumes the class is out of room and will not hand out "
            f"more. To fix: run kubectl describe pod on the pods listed in pods= and "
            f"read their scheduling events; common causes are GPUs taken by pods this "
            f"controller does not manage, a down or cordoned GPU node, insufficient "
            f"CPU/memory on the GPU nodes, or a volume that cannot attach. Clears "
            f"automatically within one queue tick (every "
            f"{config.queue_processor_interval}s, QUEUE_PROCESSOR_INTERVAL) after "
            f"those pods schedule or are deleted."
        )
    # guard 1b
    return (
        f"{paused} Cause: no schedulable node carries the taint {TOLERATION_KEY}={c}, "
        f"so a lease would be charged for a job with nowhere to run. To fix: check "
        f"whether this class's GPU nodes are cordoned, draining or NotReady (kubectl "
        f"get nodes; kubectl describe node <node>) and uncordon or repair them. "
        f"Clears automatically within one queue tick (every "
        f"{config.queue_processor_interval}s, QUEUE_PROCESSOR_INTERVAL) once a node "
        f"is back."
    )


def _warn_ondemand_gates(state: ControllerState, config: Config) -> None:
    """Emit one ``ondemand.gated`` WARNING per class-wide gate in force."""
    now = datetime.now(timezone.utc)
    for gate in state.plan_ondemand_gates(now):
        log.warning("%s", kv(
            event="ondemand.gated", clabel=gate.label, guard=gate.guard,
            reason=gate.reason, dur_s=int((now - gate.since).total_seconds()),
            candidates=gate.waiting, app_gpus=gate.app_gpus,
            phys_gpus=gate.phys_gpus, pods=list(gate.stuck_pods) or None,
            detail=_ondemand_gate_detail(gate, config),
        ))


async def ondemand_gate_warning_loop(
    state: ControllerState, config: Config
) -> None:
    """Every ``ONDEMAND_GATE_WARNING_INTERVAL_S`` s, restate at WARNING every
    class-wide gate pausing JIT on-demand admission (guards 1b, 3, 4).

    The transition lines (``capacity_audit.paused``, ``interlock.activated``)
    fire once and are easy to scroll past, and the per-candidate
    ``ondemand.candidate_held`` lines are per pod, partly DEBUG, and silent
    when nobody is waiting.  This repeats for as long as the gate stands —
    whether or not a pod is currently held by it — and says what to do.
    Reads in-memory state only; no API calls.
    """
    while True:
        await asyncio.sleep(ONDEMAND_GATE_WARNING_INTERVAL_S)
        with trace.scope("gate"):
            try:
                _warn_ondemand_gates(state, config)
            except Exception as exc:  # noqa: BLE001
                log.error("%s", kv(event="ondemand.gate_warning_failed", err=exc), exc_info=True)


# ---------------------------------------------------------------------------
# Background loop 7: singleton lease renewal
# ---------------------------------------------------------------------------


async def lease_guard_loop(config: Config, *, held: bool) -> None:
    """Hold the singleton Lease for this instance's lifetime.

    Not leader election: there is no waiting to take over.  The one job is to
    notice that a *second* controller is running and get this one out of the
    way, since two instances would issue duplicate toleration patches.

    - Renewal lost to another live holder → CRITICAL and terminate.
    - Renewal failed (API error) → keep running and retry.  A blip is not
      evidence of a duplicate, and a controller that stops admitting pods
      because coordination.k8s.io was briefly unreachable is a worse outcome
      than a small risk of overlap (the same fail-open reasoning as startup).
    - Not held at startup because acquisition *errored* → keep retrying; an
      affirmative ``held_by_other`` at any point is the duplicate signal.
    """
    namespace = _lease_namespace(config)
    holder = _lease_holder(config)
    fails = 0
    while True:
        await asyncio.sleep(LEASE_RENEW_INTERVAL_SECONDS)
        if held:
            outcome = await renew_singleton_lease(
                namespace, holder, LEASE_DURATION_SECONDS
            )
        else:
            outcome = await acquire_singleton_lease(
                namespace, holder, LEASE_DURATION_SECONDS
            )

        if outcome.status == "lost" or outcome.status == "held_by_other":
            log.critical("%s", kv(
                event="singleton.lost", name=LEASE_NAME, ns=namespace,
                holder=outcome.holder, age_s=outcome.age_s,
            ))
            _terminate_process()
            return
        if outcome.status == "error":
            fails += 1
            if fails == 1 or fails % 30 == 0:  # ~every 10 min at 20 s ticks
                log.warning("%s", kv(
                    event="singleton.renew_failed", fails=fails, err=outcome.err,
                ))
            continue
        if not held:
            log.info("%s", kv(
                event="singleton.acquired", name=LEASE_NAME, ns=namespace,
                holder=holder, mode=outcome.mode, age_s=outcome.age_s,
            ))
            held = True
        else:
            log.debug("%s", kv(event="singleton.renewed", name=LEASE_NAME))
        fails = 0


async def _claim_singleton_lease(config: Config) -> bool:
    """Claim the Lease at startup; raise if another instance holds it.

    Returns whether the lease is held (False = acquisition errored and we are
    running unguarded, fail-open).  Raising on ``held_by_other`` aborts the
    FastAPI lifespan, which exits the process non-zero so the kubelet's
    crash-backoff paces retries until the other instance's lease expires.
    """
    namespace = _lease_namespace(config)
    holder = _lease_holder(config)
    outcome = await acquire_singleton_lease(namespace, holder, LEASE_DURATION_SECONDS)
    if outcome.status == "held_by_other":
        log.critical("%s", kv(
            event="singleton.held_by_other", name=LEASE_NAME, ns=namespace,
            holder=outcome.holder, age_s=outcome.age_s,
        ))
        raise RuntimeError(
            f"another controller instance holds the {LEASE_NAME} lease "
            f"in namespace {namespace}"
        )
    if outcome.status == "error":
        # Fail open: an image-only upgrade whose ClusterRole lacks the new
        # leases rule (403) must not brick the controller.
        log.warning("%s", kv(event="singleton.acquire_failed", err=outcome.err))
        return False
    log.info("%s", kv(
        event="singleton.acquired", name=LEASE_NAME, ns=namespace,
        holder=holder, mode=outcome.mode, age_s=outcome.age_s,
    ))
    return True


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    config: Config = app.state.config  # injected in create_app()
    _task_health.reset()  # fresh lifespan, fresh supervision slate
    client = ReservationClient(config)
    state = ControllerState()
    # Enable the optional usage-group match constraint (REQUIRED_GROUP_LABEL).
    # None keeps the group gate off, preserving prior behaviour.
    state.required_group_label = config.required_group_label

    # Expose the shared state and client so request handlers (e.g. the inbound
    # push endpoint) can reach them; the background loops receive them as task
    # arguments.  Only ``config`` is available on ``app.state`` before this.
    app.state.controller_state = state
    app.state.reservation_client = client

    # Localise the human-readable Event/annotation prose (display only: stored
    # instants, galends/* timestamp annotations and log fields all stay UTC).
    set_display_timezone(config.display_timezone)
    log.info("%s", kv(
        event="config.timezone",
        name="EVENT_DISPLAY_TIMEZONE" if config.display_timezone is not None else "TZ",
        value=timezone_label(config.display_timezone),
    ))

    # Initialise Kubernetes client.
    init_k8s(
        config.kubeconfig_path, strict_tls_verify=config.k8s_tls_strict_verify
    )

    # Claim the singleton lease before doing any work: two controllers would
    # issue duplicate toleration patches.  Raises (aborting startup, non-zero
    # exit) if another instance holds it; fail-open on any API error.
    lease_held = False
    if config.singleton_lease_enabled:
        lease_held = await _claim_singleton_lease(config)
    else:
        log.info("%s", kv(event="singleton.disabled"))

    # Perform the first reservation fetch synchronously so that the pod-watch
    # loop has data to match against from the moment it starts.
    # The synchronous startup work is one unit: the first fetch, the
    # no-show arming it feeds, and the first capacity audit.
    with trace.scope("startup"):
        log.info("%s", kv(event="startup.initial_fetch"))
        try:
            await _refresh_reservations(state, client, config)
            log.info("%s", kv(
                event="startup.initial_fetch_complete",
                reservations=len(state.reservations), classes=len(state.gpu_class_labels),
            ))
            now = datetime.now(timezone.utc)
            state.update_noshow_tracking(
                now,
                config.noshow_timeout_minutes,
                config.noshow_grace_minutes,
                reason="init",
            )
            log.info("%s", kv(
                event="startup.noshow_armed", watched=len(state.noshow_deadlines),
            ))
        except Exception as exc:  # noqa: BLE001
            log.error("%s", kv(
                event="startup.initial_fetch_failed", err=exc,
                retry_s=config.reservation_fetch_interval,
            ))

        # Run one capacity audit synchronously so an app-side overcommit pauses
        # on-demand admission from the start rather than up to an interval later.
        # Best-effort: a snapshot failure here just logs and leaves the pause set
        # empty (the loop re-checks on its normal cadence).
        try:
            await _run_capacity_audit(state, config)
        except Exception as exc:  # noqa: BLE001
            log.warning("%s", kv(event="startup.capacity_audit_failed", err=exc), exc_info=True)

    # Launch the background loops as asyncio tasks, each supervised by
    # _on_task_done so an unhandled crash is logged and flips /health to 503.
    tasks = [
        asyncio.create_task(
            reservation_fetch_loop(state, client, config),
            name="reservation-fetch",
        ),
        asyncio.create_task(pod_watch_loop(state, client, config), name="pod-watch"),
        asyncio.create_task(
            queue_processor_loop(state, client, config), name="queue-processor"
        ),
        asyncio.create_task(preemption_loop(state, client, config), name="preemption"),
        asyncio.create_task(
            capacity_audit_loop(state, config), name="capacity-audit"
        ),
    ]
    if config.ondemand_lease_enabled:
        tasks.append(
            asyncio.create_task(
                ondemand_gate_warning_loop(state, config), name="ondemand-gate-warning"
            )
        )
    if config.singleton_lease_enabled:
        tasks.append(
            asyncio.create_task(
                lease_guard_loop(config, held=lease_held), name="lease-renew"
            )
        )
    for task in tasks:
        task.add_done_callback(_on_task_done)
    log.info("%s", kv(event="startup.ready", loops=[t.get_name() for t in tasks]))

    try:
        yield
    finally:
        log.info("%s", kv(event="shutdown.start"))
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await client.aclose()
        log.info("%s", kv(event="shutdown.complete"))


def create_app() -> FastAPI:
    config = Config.from_env()
    _configure_logging(config)
    app = FastAPI(
        title="GPU Reservation Controller",
        description="Applies GPU reservation tolerations to Kubernetes pods",
        lifespan=lifespan,
    )
    app.state.config = config
    return app


app = create_app()


# ---------------------------------------------------------------------------
# Health endpoint
# ---------------------------------------------------------------------------


@app.get("/health", tags=["ops"])
async def health() -> JSONResponse:
    """Liveness / readiness probe reflecting background-task health.

    200 while every supervised background loop is alive; 503 once any has
    died with an unhandled exception (``_on_task_done``).  The same endpoint
    backs the liveness probe, the readiness probe, and the container
    HEALTHCHECK, so a dead loop unpublishes the inbound API and then gets the
    pod restarted — the intended recovery, since all state rebuilds from the
    cluster and the reservation API on startup.  There is deliberately no
    in-process task restart.
    """
    if _task_health.ok:
        return JSONResponse({"status": "ok"})
    return JSONResponse(
        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
        content={
            "status": "degraded",
            "dead_tasks": sorted(_task_health.dead),
            "detail": dict(_task_health.dead),
        },
    )


# ---------------------------------------------------------------------------
# Inbound reservation-push API
# ---------------------------------------------------------------------------


def _require_inbound_auth(
    request: Request,
    authorization: str | None = Header(default=None),
) -> None:
    """Authenticate an inbound API call (push, forecast) via a static bearer token.

    - 503 if ``INBOUND_API_TOKEN`` is unset — the inbound API is opt-in and
      disabled by default, so existing deployments are unaffected.
    - 401 if the ``Authorization: Bearer <token>`` header is missing or does not
      match (constant-time compare).
    """
    config: Config = request.app.state.config
    expected = config.inbound_api_token
    if not expected:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Inbound API is disabled (INBOUND_API_TOKEN not set)",
        )
    scheme, _, token = (authorization or "").partition(" ")
    if scheme.lower() != "bearer" or not secrets.compare_digest(token, expected):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or missing bearer token",
            headers={"WWW-Authenticate": "Bearer"},
        )


def _bind_trace(prefix: str):
    """Dependency factory binding a trace id for one inbound request.

    Adopts the caller's ``X-Client-Trace`` when present, so a push the app makes
    while handling a user's cancel is logged here under *the user's* trace — the
    same id that request carries in the app's own log. Falls back to a locally
    minted id for callers that send none (curl, a probe).

    A ``yield`` dependency rather than a middleware so the scope is entered and
    exited around the endpoint within one task, which is what keeps the context
    var from leaking between concurrent requests.
    """

    async def dependency(request: Request):
        inbound = request.headers.get(trace.TRACE_HEADER)
        with trace.scope(prefix, inbound=inbound):
            yield

    return dependency


@app.post(
    "/api/reservations/push",
    tags=["sync"],
    response_model=ReservationPushResponse,
    dependencies=[Depends(_require_inbound_auth), Depends(_bind_trace("push"))],
)
async def push_reservations(
    body: ReservationPushRequest, request: Request
) -> ReservationPushResponse:
    """Apply one or more pushed reservation entries into controller state.

    A fast, partial delta from the reservation app (bulk sync remains a
    controller-initiated pull).  Entries are upserted by id; an entry whose
    ``status`` is not ``"active"`` (e.g. a cancellation) drops the reservation
    from the active set, and any in-window cancellation evicts its admitted pod
    and reclaims the freed capacity — after first re-linking the pod onto
    another open booking its user holds when adoption is enabled (the
    Continue/supersede flow pushes the ``superseded`` source together with its
    replacement booking) — the same path a mid-window cancellation
    takes on a normal fetch.  An entry that keeps the same id but changes owner
    (adoption) evicts the prior owner's admitted pod from its namespace so the
    new owner can claim the still-active reservation.  The next full pull remains
    the source of truth.
    """
    state: ControllerState = request.app.state.controller_state
    client: ReservationClient = request.app.state.reservation_client
    config: Config = request.app.state.config

    pushed = body.reservations
    now = datetime.now(timezone.utc)

    # Resolved before the lock — the whole point of this endpoint is to beat the
    # poll interval, and it cannot do that from behind a fetch cycle's HTTP.
    #
    # Only the *pushed* ids are resolved, not the merged set: the merge reads
    # state.reservations, which is only safe under the lock.  That is sufficient
    # because merged ⊆ state.reservations ∪ pushed, and every id already in
    # state.reservations was resolved by whichever reconcile admitted it.  The
    # residual gap is a class that failed to resolve on an earlier cycle and is
    # not named by this push; the fetch loop re-probes it every cycle.
    gpu_class_maps = await _resolve_gpu_class_maps(
        client,
        {r.gpu_class_id for r in pushed},
        GpuClassMaps(
            dict(state.gpu_class_labels),
            dict(state.gpu_class_ids),
            dict(state.gpu_class_capacity),
            state.gpu_classes_known,
        ),
    )

    async with state.reservation_lock:
        # Evictable in-window cancellations carried by this push (idempotent:
        # detect_cancelled_in_window skips ids already recorded / declared no-show).
        cancelled_in_window = state.detect_cancelled_in_window(pushed, now)
        # Owner changes (adoption) must be detected before apply_push_to_active
        # upserts the new owner over the old one in state.reservations.
        owner_changes = state.detect_owner_changed_in_window(pushed, now)
        merged_active = apply_push_to_active(state.reservations, pushed)

        evictions = await _reconcile_after_reservation_change(
            state,
            client,
            config,
            merged_active,
            cancelled_in_window,
            owner_changes,
            now,
            gpu_class_maps,
        )

        # Re-arm / prune no-show tracking for the new set, mirroring what the
        # fetch loop does after _refresh_reservations.  Synchronous, so it stays
        # inside the hold.
        state.reconcile_noshow()
        state.update_noshow_tracking(
            now,
            config.noshow_timeout_minutes,
            config.noshow_grace_minutes,
            reason="push",
        )

    # Per-pod Kubernetes I/O, with the lock released.
    await _execute_evictions(state, evictions)

    applied = sum(1 for r in pushed if r.status == "active")
    log.info("%s", kv(
        event="push.applied", upserts=applied,
        cancellations=len(cancelled_in_window), owner_changes=len(owner_changes),
        reservations=len(state.reservations),
    ))
    return ReservationPushResponse(
        applied=applied,
        cancelled=len(cancelled_in_window),
        adopted=len(owner_changes),
        total_active=len(state.reservations),
    )


# ---------------------------------------------------------------------------
# Preemption-risk forecast API
# ---------------------------------------------------------------------------


def _forecast_response(
    forecast: PreemptionForecast,
    config: Config,
    namespace: Optional[str],
) -> PreemptionRiskForecastResponse:
    """Convert a pure ``PreemptionForecast`` into the API response shape.

    *namespace* filters ``pods`` only — the bucket/class summaries stay
    cluster-global, because every displayed risk's denominator
    (``eligible_pool_gpus``) and driver (demand from *other* users' bookings)
    are global; a scoped summary could not explain the pod numbers beside it.
    Pure, so it is unit-testable without a TestClient.
    """
    buckets = [
        ForecastBucket(
            start=b.start,
            end=b.end,
            classes={
                c: ForecastClassSummary(
                    capacity=b.capacity_by_class.get(c, 0),
                    free=b.free_by_class.get(c, 0),
                    demand=b.demand_by_class.get(c, 0),
                    shortfall=b.shortfall_by_class.get(c, 0),
                    eligible_pool_gpus=b.eligible_pool_gpus_by_class.get(c, 0),
                    pending_jit_gpus=b.pending_jit_gpus_by_class.get(c, 0),
                )
                # All six summary maps share one key set (see
                # ForecastBucketSummary), so iterating any one of them is safe.
                for c in b.capacity_by_class
            },
        )
        for b in forecast.buckets
    ]
    pods = [
        ForecastPod(
            namespace=pf.view.namespace,
            name=pf.view.name,
            uid=pf.view.uid,
            gpu_class=pf.view.gpu_class,
            gpu_count=pf.view.gpu_count,
            reservation_id=pf.view.reservation_id,
            guarantee_end=pf.guarantee_end,
            buckets=[
                ForecastPodBucket(risk=pb.risk, state=pb.state) for pb in pf.buckets
            ],
        )
        for pf in forecast.pods
        if namespace is None or pf.view.namespace == namespace
    ]
    return PreemptionRiskForecastResponse(
        generated_at=forecast.generated_at,
        lead_minutes=forecast.lead_minutes,
        selection_delegated=config.preemption_delegate_selection,
        buckets=buckets,
        pods=pods,
    )


@app.get(
    "/api/forecast/preemption-risk",
    tags=["forecast"],
    response_model=PreemptionRiskForecastResponse,
    dependencies=[Depends(_require_inbound_auth), Depends(_bind_trace("forecast"))],
)
async def preemption_risk_forecast(
    request: Request, namespace: Optional[str] = None
) -> PreemptionRiskForecastResponse:
    """Per-pod preemption-risk forecast for the current + next two hours.

    Read-only: projects the same demand/free/eligibility arithmetic the
    preemption sweep runs, across every booking boundary in the horizon.  A
    pod inside its runtime guarantee has zero risk; an overstayer's risk is
    the projected GPU shortfall over the eligible overstay pool for each
    boundary whose kill window (``[boundary − lead, boundary]``) touches the
    bucket.  ``?namespace=`` filters the ``pods`` list only (unknown
    namespace ⇒ empty list, 200); summaries stay cluster-global.

    503 when either cluster snapshot fails — the forecast never reports risk
    based on unknown physical state (same fail-safe rule as the sweep).
    """
    state: ControllerState = request.app.state.controller_state
    config: Config = request.app.state.config
    now = datetime.now(timezone.utc)

    # Snapshots are awaited OUTSIDE the lock, mirroring the preemption sweep.
    try:
        snapshot = await snapshot_tolerated_pods(
            TOLERATION_KEY, config.required_group_label, config.default_usage_group
        )
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(event="forecast.snapshot_failed", target="pods", err=exc), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Cluster pod snapshot unavailable; forecast cannot be computed",
        )
    try:
        inventory = await snapshot_node_gpu_inventory(TOLERATION_KEY)
    except Exception as exc:  # noqa: BLE001
        log.warning("%s", kv(
            event="forecast.snapshot_failed", target="node_capacity", err=exc,
        ), exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Node capacity snapshot unavailable; forecast cannot be computed",
        )
    # Per node for the same reason as the sweep: the views are marked against
    # the node set the capacity counts.
    capacity = gpu_capacity_by_class(inventory)

    async with state.reservation_lock:
        pods = [_pod_view(p, inventory) for p in snapshot]
        pending = list(state.ondemand_candidates.values())
        forecast = state.forecast_preemption_risk(
            capacity,
            pods,
            pending,
            now,
            lead=timedelta(minutes=config.preemption_lead_minutes),
        )

    return _forecast_response(forecast, config, namespace)


def main() -> None:
    """Run the controller, binding uvicorn to the configured HTTP_PORT.

    Launching programmatically (rather than via a hardcoded ``uvicorn`` CLI port)
    is what makes ``HTTP_PORT`` actually take effect, so Helm's ``httpPort``
    and both probes stay consistent with the listening port (CODE-REVIEW P2).
    """
    import uvicorn

    config: Config = app.state.config
    uvicorn.run(app, host="0.0.0.0", port=config.http_port)


if __name__ == "__main__":
    main()
