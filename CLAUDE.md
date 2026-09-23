## Initial Setup — IMPORTANT

The first time you interact with this repository, review and follow the
initial setup instructions in AGENTS.md.

---

## What this project is

A **Kubernetes controller daemon** — not a web application.  It has no
database and no user-facing frontend.  It authenticates *outbound* to the GPU
Reservation API using a long-lived service key, and *inbound* to Kubernetes
using either a kubeconfig file or an in-cluster service account.  It also
exposes a small optional **inbound push API**
(`POST /api/reservations/push`) that the reservation app calls to propagate
reservation updates faster than the poll interval; it is guarded by a single
static bearer token and disabled unless that token is configured.

The app schedules only real reservations (`kind="booking"`) — there is no
ad-hoc "reclaim" capacity type.  A pod with no reservation open now or soon
gets one **just-in-time (JIT)**: the controller requests a short on-demand
booking on the pod's behalf (`POST /api/reservations`), so on-demand jobs are
ordinary reservations — charged SU and protected by runtime guarantees.  (A
granted lease does **not** itself trigger boundary preemption of overstayers —
see the caveat in **Just-in-time (JIT) on-demand leases**.)

---

## Technology choices

| Choice | Rationale |
|--------|-----------|
| **FastAPI** | Provides the `GET /health` liveness endpoint, the `POST /api/reservations/push` inbound API, the `GET /api/forecast/preemption-risk` forecast, and clean lifespan management for background tasks; no routers or static files needed |
| **asyncio** | Single event loop drives all four background loops concurrently without threads for the application logic |
| **httpx** | Async HTTP client for the reservation management API; supports connection pooling and clean timeout handling |
| **kubernetes** (official Python client) | LIST + WATCH pod streams; strategic-merge-patch for toleration injection; supports both in-cluster and kubeconfig auth |
| **Pydantic v2** | Validates and deserialises reservation API responses into typed models |

> **Do not add** SQLAlchemy, SQLite, Argon2id, python-jose, or a frontend.
> This daemon has no persistent state and no human login flow.

---

## Architecture

```
app/
├── __init__.py
├── main.py               Entry point — FastAPI app, lifespan, four background tasks
├── config.py             Config dataclass populated from environment variables
├── schemas.py            Pydantic models mirroring RESERVATION-API.md §6
├── reservation_client.py httpx async client — fetches reservations + GPU classes; creates/cancels JIT on-demand reservations
├── log_fields.py         kv() — renders log message bodies as key=value fields (see docs/LOG-FIELDS.md)
├── trace.py              Per-unit-of-work trace ids + X-Client-Trace propagation (see **Trace ids**)
├── k8s_client.py         Kubernetes wrapper — PodWatcher, apply_toleration, annotate_runtime_guarantee, emit_preempted_event, snapshot_tolerated_pods / snapshot_node_gpu_inventory (per-node, honouring the galends/force-node-capacity node annotation) / snapshot_node_gpu_capacity (per-class collapse of it)
└── controller.py         ControllerState, QueueEntry, matching, window arithmetic, preemption planning, preemption-risk forecast
```

### Background tasks (started in `lifespan`, cancelled on shutdown)

| Task | Cadence | Responsibility |
|------|---------|----------------|
| `reservation_fetch_loop` | every `RESERVATION_FETCH_INTERVAL` s (default 300) | Re-fetches active reservations; refreshes `gpu_class_id ↔ label_value` maps; reconciles stale queue entries |
| `pod_watch_loop` | continuous (WATCH resumed by `resourceVersion`; LIST at start and every ~10 min resync) | Routes a pod with the `gpu-class` label and no toleration to the reserved queue (a match is open or opens soon) or to a JIT on-demand lease request; dequeues deleted pods and, when a deleted/terminated pod was admitted under a JIT lease, cancels that lease; **fast-path**: applies toleration immediately when a new pod arrives inside an open window.  Each event is handled under its own try/except, so one bad event cannot kill the consumer |
| `queue_processor_loop` | every `QUEUE_PROCESSOR_INTERVAL` s (default 300) | Handles pods queued before their window opened; retries pods that were over-budget; requests/retries JIT leases; cancels declared no-shows; schedules retries with 2–5 min jitter |
| `preemption_loop` | every `PREEMPTION_CHECK_INTERVAL` s (default 60) | Recovers capacity from pods running past their runtime guarantee: reactively, when an upcoming reservation boundary needs it (see **Runtime guarantees and demand-driven preemption**), and — throttled to `HEADROOM_CHECK_INTERVAL` — anticipatorily, to hold a fixed fraction of each class free for on-demand jobs that have not arrived yet (see **Anticipatory headroom preemption**) |
| `capacity_audit_loop` | every `CAPACITY_CHECK_INTERVAL` s (default 3600) | Compares app-side per-class GPU capacity (`effective_gpus_today`) against physical cluster capacity; logs any difference as a WARNING and pauses on-demand admission for over-committed classes (see **App-side vs physical capacity reconciliation**) |
| `ondemand_gate_warning_loop` | every 60 s (only when `ONDEMAND_LEASE_ENABLED`) | Restates at WARNING (`ondemand.gated`) every GPU class whose JIT admission is paused by a class-wide gate — guard 1b, 3 or 4 — with a plain-English cause and remedy for an operator (see **Operator warning for paused on-demand admission**) |
| `lease_guard_loop` | every 20 s (only when `SINGLETON_LEASE_ENABLED`) | Renews the singleton `coordination.k8s.io` Lease; terminates the process if another live instance takes it (see **Singleton lease guard**) |

Every task is **supervised**: `_on_task_done` records an unhandled exception in
`_task_health` and logs it CRITICAL (`task.crashed`), and `GET /health` — which
backs the liveness probe, the readiness probe, *and* the container HEALTHCHECK —
returns 503 while any task is dead, so Kubernetes restarts the pod.  There is
deliberately no in-process restart: all state rebuilds from the cluster and the
reservation API on startup, and a half-dead controller silently not admitting
pods is the failure this replaces.

---

## Key design decisions

### Matching pods to reservations

A pod matches a reservation when **both** of the following hold:

```
pod.metadata.namespace  ==  reservation.user.username
pod.labels["gpu-class"] ==  gpu_class.label_value   # from GET /api/gpu-classes/{id}
```

`label_value` is cached in `ControllerState.gpu_class_labels`, refreshed every
reconcile (fetch or push) from `GET /api/gpu-classes` (the full list), with a
per-id `GET /api/gpu-classes/{id}` fallback for a class referenced by a
reservation but missing from that list (e.g. one created since the last
successful bulk fetch).  A failed bulk fetch keeps the previous cycle's map
rather than losing all label resolution.  If a GPU class has no `label_value`,
its reservations are skipped with a warning.  The reverse map,
`ControllerState.gpu_class_ids` (label → id), is refreshed the same way and is
what the JIT lease path resolves a pod's `gpu-class` label to the numeric
`gpu_class_id` a booking request needs (see **Just-in-time (JIT) on-demand
leases**).

**Optional usage-group constraint** (`REQUIRED_GROUP_LABEL`, default off).  When
set to a pod-label name (e.g. `dsmlp/course`), a **third** equality is required:

```
pod.labels[REQUIRED_GROUP_LABEL]  ==  reservation.group.name
```

It behaves like `gpu-class` — an additional match axis threaded alongside
`gpu_class_label` (`ControllerState._group_ok`, gated on
`ControllerState.required_group_label`, set once from config at startup).  The
gate applies to the **reserved path**: `find_best_reservation`,
`find_admittable_reservation` (the JIT routing gate), `find_open_booking_for`
(adoption), `mark_pod_seen_for_noshow`, and reserved back-to-back chaining
(`_chain_for` additionally requires equal `group_id`, so a guarantee never
chains across a different group's window).  Matching is against the group
**name** (the reservation carries only `GroupBrief{id, name}`; there is no
per-group `label_value`).  A pod with no such label (feature on) matches **no**
grouped reservation on the reserved path; it is also never JIT-eligible (a
JIT request carries the group name from the pod's label, so a labelless pod
has none to carry) and is left Pending.

### Toleration applied

```
key:      gpu-class-reservation
operator: Equal
value:    <pod's gpu-class label value>   # mirrors the pod's own label
effect:   NoSchedule
```

When patching, all existing tolerations are preserved (Kubernetes rejects
patches that remove tolerations from running pods).  The pod is re-fetched
immediately before patching to avoid working with a stale toleration list.

The patch also stamps the pod with the `galends/booking-reference` annotation:
`res-<id>`, where `<id>` is the reservation the pod was admitted under —
whether it pre-existed or was requested just-in-time on the pod's behalf, every
admitted pod is a reserved-path holder under a real reservation, so there is
only the one prefix.  This is the controller's single record of which
reservation a pod was admitted under.  The GPU **budget check**
(`ControllerState.available`) counts every pod recorded against a reservation
id in the **unified occupancy map** (keyed by reservation id), so each
reservation has an independent budget; the id parsed from this annotation
(`parse_booking_reference`) is also how occupancy is rebuilt from the cluster
after a restart.

`annotate_runtime_guarantee` additionally writes `galends/pod-runtime-limit-seconds`
and `galends/guaranteed-until` — see **Runtime guarantees and demand-driven
preemption** below — plus the **reservation facts** described next.  None of
them is enforced by Kubernetes; all are informational only.

### Reservation-fact annotations

`booking-reference` names *which* reservation a pod runs under, but an in-pod
consumer cannot resolve an id — it has no API access.  So the same
`annotate_runtime_guarantee` patch also stamps the reservation's own facts, for
free (one patch, no extra API call):

| Annotation | Value |
|------------|-------|
| `galends/reservation-kind` | `booking` \| `on_demand` — whether the user reserved this window or the controller minted a JIT lease for them |
| `galends/reservation-start` / `-end` | The reservation's **own** window (UTC ISO-8601), which is *not* `guaranteed-until` — that is the end of the back-to-back chain |
| `galends/reservation-gpu-count` | GPUs the reservation holds; against the pod's own request this yields "using 1 of the 4 you booked" |
| `galends/gpu-class-name` | The class's display name (`gpu_class.name`), as opposed to the `label_value` used for matching |
| `galends/admitted-at` | When this pod was first admitted |

`main._reservation_facts` digests a `ReservationResponse` into the plain-field
`k8s_client.ReservationFacts` — the mirror of the `PodRuntimeView` digest that
keeps `controller.py` free of Kubernetes shapes, here keeping `k8s_client.py`
free of the Pydantic response models.

Because these describe the reservation a pod is **currently** linked to, every
re-link path (adoption, JIT-lease-to-booking merge) re-stamps them by calling
`_record_guarantee` with the new reservation — which those paths already did for
the guarantee, so nothing new had to be wired.  `galends/admitted-at` is the one
exception: `_record_guarantee(..., first_admission=True)`, passed only from
`_try_apply_toleration`, so a re-link cannot restart a session-elapsed clock
mid-job.

Re-linking covers every way the pod's reservation can change **identity**, but
not the way it can change **content**: the same reservation mutated underneath
the pod — a lease whose window is extended in place — keeps its id, so no
re-link happens and none of those callers run.  The pod would keep advertising
its pre-extension `galends/reservation-end` while `galends/guaranteed-until`,
recomputed live every tick, moved forward; a consumer reading both would see
them contradict.  `ControllerState.plan_reservation_facts` (pure, keyed by
reservation id) plus `main._apply_reservation_facts` close that, once per
**queue-processor tick**, reusing that tick's `snapshot_tolerated_pods` and the
same `reservation_lock` acquisition as `plan_guarantee_status` — one lock, one
view list, so the two annotation families can never describe different
reservations for the same pod.  The write (`annotate_reservation_facts`) touches
only the five fact keys, never `guaranteed-until`/`guarantee-status` (owned by
the status reconcile) or `admitted-at` (write-once).  Like the status reconcile
there is **no clear path** and it is diff-and-skip, so an unchanged pod costs no
API call.

`k8s_client.utc_iso` is the single definition of the `YYYY-MM-DDTHH:MM:SSZ` wire
format `docs/POD-ANNOTATIONS.md` promises consumers.  It matters that writers and
the diff-and-skip comparisons in `main` share it: those rebuild the string to
decide whether a re-patch is needed, so a format that drifted between the two
sides would re-patch every pod on every tick.

### Runtime guarantees and demand-driven preemption

When a pod is admitted (toleration successfully applied), the controller
records how long its GPU access is **guaranteed** — but does **not** enforce
that guarantee with `spec.activeDeadlineSeconds`.  A pod may keep running past
its guarantee freely; the controller does not hard-kill it at a fixed runtime
estimate and reclaims capacity from an overstaying pod only when a new
reservation actually needs it.

**Guarantee calculation** — the guaranteed instant is:

1. `slot_end` of the current reservation window, **plus**
2. The full duration of any directly **back-to-back** future reservations
   sharing the same `user.username`, GPU class, and `gpu_count`, where
   `slot_start(next) == slot_end(previous)` with no gap.

The back-to-back chaining rule is `ControllerState.compute_guaranteed_until`.
The result is an **absolute UTC instant recomputed live on every call**, not a
duration frozen at admission — so a pod's guarantee can *grow* after admission
(the user books an abutting follow-on reservation), something
`spec.activeDeadlineSeconds` cannot express (Kubernetes forbids raising an
existing deadline).  `ControllerState.guarantee_end` is the general-purpose
entry point: given a booking-reference id, it returns the live guarantee
instant, or `None` if the reservation is no longer active (its window is
unconditionally over) — every admitted pod resolves through this one path
now that on-demand jobs are ordinary reservations too.

**Recording the guarantee** — after calling `apply_toleration`,
`_record_guarantee` in `main.py`:

- Annotates the pod (`annotate_runtime_guarantee`) with informational-only
  `galends/pod-runtime-limit-seconds` (guaranteed duration in seconds),
  `galends/guaranteed-until` (the same instant as an absolute UTC ISO-8601
  timestamp), and `galends/guarantee-status` (`guaranteed` at admission — see
  **Live guarantee-status annotations** below).  A guarantee can technically
  shrink after the annotation is written (a window shortened server-side, or a
  merge component vanishing) — nothing re-reads these
  annotations to make a decision, so a widget should treat them as
  best-effort, not authoritative.
- Emits a `Normal` Kubernetes Event with `reason: RuntimeGuaranteed`,
  `action: GuaranteeRuntime`, stating when the guarantee ends and that the
  pod may be preempted afterward if capacity is needed.
- Both steps are best-effort.  A failure logs a warning and does **not**
  revoke the toleration.

**Recovering capacity** is the job of `preemption_loop` (`_run_preemption_sweep`
in `main.py`), which on every tick:

1. Finds every distinct booking `slot_start` ("boundary") within
   `PREEMPTION_LEAD_MINUTES` (default 15) of now.
2. Snapshots **physical capacity** per GPU class (`snapshot_node_gpu_capacity`
   — one node LIST summing allocatable `nvidia.com/gpu` grouped by the
   `gpu-class-reservation` taint value on each node, or a node's
   `galends/force-node-capacity` override — see **Forcing a node's GPU
   capacity**).  This is the controller's only notion of how many GPUs
   physically exist; nothing else in the codebase tracks it.
3. For each boundary/phase not yet evaluated, computes **demand**
   (`ControllerState.boundary_demand`): per class, the sum over bookings
   starting exactly there of their remaining budget (`available`) minus GPUs
   already supplied by a live reserved-path holder whose back-to-back chain
   covers that reservation — subtracted per-reservation rather than excluding
   the whole claimed reservation from demand, so a partially-chained
   multi-GPU booking is not under-counted.
4. Computes **free** capacity (`free_capacity_by_class`): physical capacity
   minus GPUs used by node-resident, non-terminating pods.
5. If `demand > free`, the controller identifies the **eligible candidate
   pool** per class (`ControllerState.plan_boundary_candidates`) — pods of the
   same GPU class, admitted by this controller, live, not already terminating,
   and **past their runtime guarantee** (`guarantee_end` is `None` or `<= now`)
   — and the per-class GPU shortfall (`kills_needed`).  A pod within its
   guarantee is **never** a candidate, however severe the shortfall.  **Which**
   candidates become victims is then **delegated to the reservation app**
   (`POST /api/reservations/preemption-victims`, write-scope key; see
   `ReservationClient.select_preemption_victims`), so prioritisation policy
   (by reservation owner/group/kind) can live in the app rather than the
   controller.  The controller kills only pods it offered — an unknown uid in
   the response is ignored, and an **empty** response ("spare everyone") is
   respected.  When delegation is disabled (`PREEMPTION_DELEGATE_SELECTION=false`)
   or the app call **fails** (network/non-2xx/absent endpoint), the sweep falls
   back to local **uniform-random** selection (`select_victims_locally`) so
   preemption still works — this is the same greedy random pick the controller
   did before delegation existed.  A shortfall the returned victims do not cover
   is logged as an "unmet" warning (priority ranking within the app's random
   policy is deferred future design; the controller's own fallback is uniform).
6. Selected victims are deleted (`_preempt_pod`: emit a `Normal` Event with
   `reason: Preempted`, `action: PreemptPod`, then delete and release
   occupancy — all best-effort, mirroring the cancellation-eviction shape in
   `_handle_cancelled_reservations`).

**Two-phase, boundary-anchored trigger**: phase **A** (lead-time,
`boundary > now`) runs at `T − PREEMPTION_LEAD_MINUTES` and proactively frees
capacity from overstayers regardless of whether the incoming holder ever
shows up (this can preempt jobs on behalf of a reservation that turns out to
be a no-show — an accepted trade-off, not a bug).  Phase **B** (at-boundary,
`boundary <= now`) runs at the boundary itself and additionally makes
eligible any pod whose own guarantee ends exactly there.  Each boundary/phase
combination is evaluated at most once (`ControllerState.preemption_fired`,
pruned once the boundary leaves scope) — a restart loses the marks, but
re-evaluation is safe because a killed pod's capacity already shows as freed
(`deletionTimestamp` set, or gone) by the time anything re-checks.  Either
the pod snapshot or the node-capacity snapshot failing skips the whole sweep
with a warning — the controller never kills a pod based on unknown physical
state.

**RBAC**: the controller's ServiceAccount must have `create` on `events` and
`get`/`list` on `nodes`, in addition to the existing pod permissions.

### Anticipatory headroom preemption

Boundary preemption is **reactive**: it frees GPUs only once a booking actually
needs them at a `slot_start`.  A JIT on-demand job has no boundary of its own
(see the caveat in **Just-in-time (JIT) on-demand leases**), so one landing on
squatted GPUs simply waits.  **Headroom** (`HEADROOM_TARGET_PERCENT`, default
`0` = off) closes that gap from the other side: it holds a fixed fraction of
every GPU class free *before* the demand arrives.

- **The goal** is per class, `ceil(capacity × HEADROOM_TARGET_PERCENT / 100)`
  GPUs free — rounding **up**, so a small class still reserves a whole GPU
  rather than rounding its headroom away.  Capacity is the physical node
  snapshot (`snapshot_node_gpu_capacity`); free reuses `free_capacity_by_class`.
  `ControllerState.headroom_shortfall_by_class` is the single arithmetic.
- **Eligibility is unchanged from a boundary kill**
  (`ControllerState._headroom_pool` applies exactly
  `plan_boundary_candidates`' predicate): live, node-resident, admitted by this
  controller, and **past its runtime guarantee**.  A pod inside its guarantee is
  never a headroom victim, however large the shortfall.
- **Notice period.**  Because no calendar event announces a headroom kill, a pod
  is **warned before it is killable**.  `ControllerState.plan_headroom_warnings`
  stamps the at-risk pool with the same `galends/termination-warning-*`
  annotations the boundary warner uses, and
  `plan_headroom_candidates` only considers pods whose stamped deadline has
  already elapsed.  The deadline is **sticky** — a pod that already carries one
  keeps it — which is load-bearing twice over: recomputing `now + notice` every
  tick would re-patch every tick *and* the deadline would recede ahead of `now`
  forever, so the gate could never open.  The pod's own annotation is the state,
  so the notice survives a controller restart.  With
  `TERMINATION_WARNING_ENABLED=false` nothing writes notices, so the gate is
  **bypassed** (headroom kills immediately) rather than silently wedging.
- **It rides the preemption sweep**, throttled by `HEADROOM_CHECK_INTERVAL`
  (default 600 s) via `ControllerState.headroom_last_eval` — one pair of cluster
  snapshots, and decisively **one writer** for the termination-warning
  annotations, which a second loop would race.  The sweep still ticks on
  `PREEMPTION_CHECK_INTERVAL`; headroom only *forces* it (and its two LISTs)
  once per interval, so an idle cluster costs 2 LISTs / 10 min, not 2 / min.
  A failed snapshot skips the sweep **without** consuming the interval.
- **Ordering**: the headroom pass runs **after** the boundary loop and is handed
  the post-kill pod set, so GPUs freed there count towards the goal and no pod is
  selected twice — the same running-`doomed` discipline the boundary loop uses
  across boundaries.  No `preemption_fired` mark: headroom is a standing goal,
  not a one-shot per boundary/phase.
- **Headroom *warnings* are recomputed on every sweep**, not only when the
  throttled kill evaluation runs.  They are pure arithmetic over snapshots
  already in hand, so they are free — and skipping them on a boundary-only tick
  would leave a headroom-warned pod out of `warn_plan`, whereupon
  `_apply_termination_warnings` clears its annotation as stale and the notice
  could never ripen into a kill.  Where both sources warn the same pod, the
  **sooner** deadline wins, so `galends/termination-warning-at` keeps one
  meaning: the earliest instant this pod could die, from any cause.
- **Victim selection reuses the boundary pipeline verbatim** — the planner
  returns a `BoundaryPreemptionNeed` (with `boundary=now`, `phase="H"` inert on
  this path), so app delegation, `select_victims_locally` and
  `build_preemption_plan` all apply unchanged.  `PreemptionSelectionRequest`
  carries no boundary, so **no app-side change was needed**.

**Extending a reservation aborts a pending headroom kill**, through machinery
that already existed — eligibility is recomputed from **live** reservation state
every tick, not from the pod snapshot:

| Extension shape | Mechanism |
|---|---|
| Abutting (`slot_start(new) == slot_end(old)`, same user/class/`gpu_count`) | `compute_guaranteed_until` chains it; `guarantee_end` grows, `_past_guarantee` goes false, the pod leaves the pool |
| Non-abutting, or a different `gpu_count` | `plan_pod_adoptions` → `_adopt_pods` re-links the pod, deliberately **above** victim planning in the sweep |
| App-side `POST /api/reservations/{id}/continue` | Pushed via the inbound API, which takes the same `reservation_lock` the sweep holds |

Plan → select → delete is one uninterrupted `reservation_lock` acquisition, so an
extension cannot land between selection and deletion — including across the
delegation round-trip.  The stamped warning then self-clears (the pod drops out
of `warn_plan`), and if the pod later lapses again a **fresh** full notice period
is minted rather than the old deadline being resurrected.

**Known gap**: `GET /api/forecast/preemption-risk` models booking boundaries
only, so a pod at risk purely from headroom reports near-zero risk while
carrying a real warning annotation.  Headroom is also **count-based per class**,
inheriting the fragmentation blind spot documented under **Per-node capacity
accounting**.  **RBAC**: none new — it reuses the sweep's existing node LIST and
pod delete/patch permissions.

### Termination-warning annotations

After each preemption sweep executes its kills, it stamps the **survivors that
are still at risk** of being preempted at an upcoming boundary with a set of
informational `galends/termination-warning-*` annotations (`TERMINATION_WARNING_ENABLED`,
default on).  Like the runtime-guarantee annotations these enforce nothing and
are never read back to make a decision — a widget should treat them as a
best-effort heads-up so a job can checkpoint, extend, or re-book.

- **Annotations** (written by `k8s_client.annotate_termination_warning`):
  `galends/termination-warning-at` (the projected **kill instant** —
  `max(boundary − PREEMPTION_LEAD_MINUTES, guarantee_end)`, the start of the
  sweep's kill window and the earliest the pod could actually be deleted,
  absolute UTC ISO-8601), `galends/termination-warning-risk`
  (`min(1, shortfall/pool_gpus)` at that boundary, rounded to 2 decimals), and
  `galends/termination-warning-message` (human-readable, rendered deterministically
  from the two).  A pod killed proactively at a boundary (a **phase-A** victim,
  already past its guarantee) therefore reports the earlier `boundary − lead`
  kill time, not the boundary; a **phase-B** victim whose guarantee ends at the
  boundary degrades to the boundary itself.
- **Identification** (`ControllerState.plan_termination_warnings`, pure): reusing
  the forecast primitive `forecast_boundary_need`, for each in-scope boundary it
  flags the **full eligible pool** of any class with a residual shortfall.  The
  warning look-ahead is **decoupled from the kill lead** (`TERMINATION_WARNING_LEAD_MINUTES`,
  default 30, wider than `PREEMPTION_LEAD_MINUTES`): the boundary set is the
  **union** of the sweep's own kill window (`upcoming_boundaries(now, lead)`) with
  a wider forward horizon (`forecast_boundaries(now, now + warning_lead)` —
  forward-only, so it never widens the already-open side, and it drops no-shows).
  This gives a **phase-A** victim advance notice: an overstayer killed
  proactively at `boundary − lead` is flagged *before* its boundary enters the
  kill window, rather than on the very tick the sweep kills it (which is all the
  old kill-window-only horizon could manage — phase-B victims already got notice,
  phase-A victims got none).  Eligibility is **boundary-relative**
  (`guarantee_end <= boundary`, chains intact — computed once at the real now), so
  a pod whose guarantee expires *between* now and the boundary is flagged even
  though the sweep's own now-relative candidate planning would not yet consider
  it.  The set is computed from the **post-kill** pod set (pods preempted this
  tick are excluded), so a warning never targets a pod already being terminated.
  Iterating boundaries ascending gives each pod its **soonest** at-risk boundary
  (both the kill-instant timestamp and the risk come from it).  Because the wider
  horizon can put an at-risk boundary beyond the kill window, the sweep now runs
  (and takes its snapshots) whenever *either* a kill-window or a warning boundary
  is in scope, not only when there is a boundary to kill at.
- **Reconciliation** (`main._apply_termination_warnings`, best-effort, outside
  `reservation_lock`): diffs the desired warning against what each pod in the
  snapshot already carries (surfaced on `ToleratedPodInfo`) — writes a new/changed
  warning, **clears** (`clear_termination_warning`, annotation values set to
  `None`) a stale one when a pod leaves the at-risk pool (its user re-booked,
  demand evaporated, or it was adopted), and skips an unchanged one to avoid
  per-tick API churn.  This is restart-safe: the pod's own annotations are the
  state, so nothing leaks across a restart.
- **RBAC / config**: none new — the write/clear reuses the existing `pods: patch`
  permission.  `TERMINATION_WARNING_ENABLED=false` disables the feature;
  `TERMINATION_WARNING_LEAD_MINUTES` (default 30) sets how far ahead warnings
  look, independent of the kill lead — larger gives more advance notice at the
  cost of more speculative warnings on future demand that may change (bounded by
  the best-effort framing, the diff-and-skip reconcile, and live self-clearing
  when a user re-books or the booking no-shows).

### Live guarantee-status annotations

Distinct from the at-risk termination warning, every controller-admitted pod
carries a **general, always-present status** so a widget can show a job's
guarantee standing regardless of whether preemption is imminent.  Two keys,
both informational-only and never read back to make a decision:

- `galends/guarantee-status` — `guaranteed` while the pod is inside its live
  (chain-aware) runtime guarantee, `overstay` once it is running past it.
- `galends/guaranteed-until` — the guarantee-end instant (the *same* key
  `annotate_runtime_guarantee` writes at admission), kept **live**: future while
  in-guarantee, and left at its now-past value once overstay.

**Stamping and lifecycle.**  The status is written at the moments the guarantee
itself changes, plus a periodic reconcile for the one transition no event
covers:

- **Admission / adoption / merge** — `annotate_runtime_guarantee` (called by
  `_record_guarantee`) now also stamps `galends/guarantee-status: guaranteed`
  alongside the runtime-limit and `guaranteed-until` keys, so a freshly admitted
  or re-linked pod is immediately `guaranteed` with a forward end.
- **Expiration** — `main._apply_guarantee_status` runs once per
  **queue-processor tick** (reusing that tick's `snapshot_tolerated_pods`, no
  extra API call): `ControllerState.plan_guarantee_status` (pure) computes each
  admitted pod's live status from `guarantee_end`, and the reconcile flips a
  lapsed pod to `overstay` (leaving its now-past `guaranteed-until` frozen).  A
  pod whose guarantee **grew** (an abutting follow-on booking) gets its
  `guaranteed-until` refreshed forward on the same path.

**Diff-and-skip / restart-safe.**  The reconcile compares against the status the
pod already carries (surfaced on `ToleratedPodInfo.guarantee_status /
guaranteed_until`) and skips an unchanged pod, so it patches only on a real
transition.  There is **no clear path** — unlike the termination warning, the
status persists for the pod's admitted life and vanishes with the pod.  The
pod's own annotations are the state, so nothing leaks across a restart.  Each
pod is best-effort (a failure logs and is retried next tick).

**RBAC / config**: none new — the writes reuse the existing `pods: patch`
permission, and the feature is always on (like the runtime-guarantee
annotations it extends).

### Overstay reporting

For **offline analysis** of how far jobs run past their guarantee, the controller
reports each ended overstay to the app (`OVERSTAY_REPORT_ENABLED`, default off —
ships dark).  The report is filed when the overstay *ends* (the moment the full
duration is known), never during the run.

`main._report_overstay_if_any` is the single shared helper.  Given a pod at
teardown it: resolves the reservation id from `galends/booking-reference`; takes the
overstay **start** as the live chain-aware `guarantee_end` when the reservation is
still resolvable, else the pod's frozen `galends/guaranteed-until` annotation (the
reservation has often already left `state.reservations` by teardown); takes the
**end** as now; and **skips** the pod unless that is a genuine overstay
(`start` resolved and `now > start`) — a pod that finished within its guarantee
files nothing.  It then calls `ReservationClient.report_overstay`
(`POST /api/reservations/{id}/overstay`, write-scope key, modelled on
`cancel_reservation`: best-effort, swallows every error, never raises).  The app
copies GPU class / owner / group from the parent reservation, so the request
carries only the pod's `gpu_count`, the UTC window, and an `end_reason`.

Wired at the three sites a pod's life ends, adjacent to `release_pod`:
`pod_watch_loop`'s **DELETED** branch (`end_reason="deleted"`, before
`_teardown_ondemand_lease` removes the lease so `guarantee_end` can still resolve)
and its **terminal-phase** branch (`"pod-terminated"`), and `_preempt_pod`
(`"preempted"`, filed **before** the delete so it wins the app-side dedup over the
`"deleted"` report the pod's own DELETED event files moments later).  The app is
idempotent on `pod_uid`, so the preempt + DELETED double-fire records one row.

**RBAC / config**: none new (reuses the existing outbound service key); the
feature is off unless `OVERSTAY_REPORT_ENABLED` is set.

### Adopting overstay pods into a re-booked reservation

Because pods may overrun, a user can book a **fresh** reservation (a new,
distinct id) while their pod from the previous window is still running.  The
running pod should continue under the new reservation rather than be preempted.

When the new window **abuts** the old one (`slot_start(new) == slot_end(old)`),
same user, GPU class, and `gpu_count`, the existing back-to-back **chaining**
(`_chain_for` / `compute_guaranteed_until`) already covers this: the old
reservation stays in the fetched set (status is only `active`/`cancelled`, and
`fetch_reservations` widens `date_start` to `today − 1`), so `guarantee_end`
recomputes live and grows to the new window's end, and `boundary_demand`
credits the pod via `reservations_claimed_by` — no re-link needed.

**Adoption** (`POD_ADOPTION_ENABLED`, default on) covers what chaining cannot,
via `ControllerState.plan_pod_adoptions` (pure), which pairs a pod already
**past its runtime guarantee** (`_past_guarantee`, shared with the preemption
planner) — a **non-abutting** follow-on window (a gap, or one that starts at
"now") or a **different `gpu_count`** that chaining cannot reach — with a
currently-open booking the same user holds that has spare budget
(`find_open_booking_for`).  There is no proactive-upgrade trigger: every
admitted pod is already tied to a real reservation (JIT or otherwise), so
there is nothing to "upgrade" — only overstay pods are ever rescued.

`_adopt_pods` in `main.py` then re-annotates the pod's `galends/booking-reference`
to `res-<new id>` (the toleration is already present, so this patch only
rewrites the annotation) and, **only on patch success**, re-homes occupancy
(`relink_occupancy`) and refreshes the in-memory `PodRuntimeView`.  It emits an
`OverstayRelinked` event and re-records the runtime guarantee.

Adoption runs in two places, both under `reservation_lock`: inside
`_run_preemption_sweep` **before** victims are planned — so a pod the user has
just re-booked is re-homed (zeroing that boundary's demand) and can never be
selected as a victim — and once per **queue-processor tick** as a lazy tidy-up
that re-links pods even when no boundary is near.  A pod with no open booking to
adopt is left untouched (legitimately overstay).

Separately, a still-*pending* JIT candidate's own routing is re-checked on
every attempt (`main._try_request_lease`, step 2): if a reservation has since
become admittable for it, it is routed to the reserved queue instead of
requesting a lease — the pre-admission analogue of adoption, before there is
even a pod to preempt.

### Merging a JIT lease into a matching booking

A user who starts a pod **before** their booked window opens is admitted under a
just-in-time on-demand **lease** (`kind="on_demand"`; see **Just-in-time (JIT)
on-demand leases**).  Once that user's matching **booking** window opens, the
lease and the booking cover the same job — double-holding capacity and
double-charging SU on any overlap — until the pod happens to fall past its lease
guarantee and adoption re-links it.  **Merge** (`ONDEMAND_MERGE_ENABLED`, default
on) closes that gap proactively: as soon as the booking is open, the pod is
re-linked onto it and the lease is retired — **without** waiting for the lease
guarantee to lapse (the one intended difference from adoption).

`ControllerState.plan_ondemand_merges` (pure) is the planner: it pairs a pod
whose **current** reservation is a live `kind="on_demand"` lease with a
currently-open booking the same user holds that has spare budget
(`find_open_booking_for` — budget-based, **not** equal-`gpu_count`, so a pod using
**fewer** GPUs than the booking reserved still merges, which chaining never
could).  It is the proactive sibling of `plan_pod_adoptions`, sharing the same
occupancy-budget tally, but drops the `_past_guarantee` gate.  A pod whose
current reservation is already a booking, or that has no open booking, is left
untouched (a pod still in its **pre-booking** window stays on its lease —
correctly — until the booking opens).

`_merge_ondemand_into_bookings` in `main.py` executes it, modelled on
`_adopt_pods`: re-annotate the pod's `galends/booking-reference` to the booking
(annotation-only patch), and **only on patch success** re-home occupancy
(`relink_occupancy`), refresh the in-memory view, re-record the guarantee
(now the booking's — up to its chain end), and emit an `OverstayRelinked` event.
**Then** it retires the now-superfluous lease: cancels it **penalty-exempt**
(`POST /api/reservations/{id}/cancel`, `reason="superseded"` — the app charges
only already-consumed time, never a penalty on the unused tail, since the booking
re-covers the lease's remaining time) and drops it from `state.reservations`.
Ordering is deliberate — re-link **first** (the pod is never stranded without a
reservation), cancel **second**.  A cancel that does not land parks the lease id
in `ControllerState.pending_ondemand_merge_cancels`, retried every
queue-processor tick by `_drain_pending_merge_cancels` (mirroring
`pending_noshow_cancels`); the lease's short natural expiry is the backstop.

Merge runs in the same two places adoption does — inside `_run_preemption_sweep`
**before** victims are planned (a merged pod is on its booking and can never be a
victim) and once per **queue-processor tick** — always **before** `_adopt_pods`,
threading one shared view list so a just-merged pod is not re-processed by
adoption.  **RBAC / config**: no new Kubernetes permissions (merge reuses the
existing pod-patch + cancel paths).

### Log field grammar

Log lines are moving to a single `key=value` grammar — an `event=` noun followed
by flat fields, one concept per field, with no parentheses, slashes (`pod
ns/name` becomes `ns=… pod=…`), arrows or `a..b` ranges inside a value.
`docs/LOG-FIELDS.md` is the canonical field dictionary and `app/log_fields.py`
(`kv()`) is the only thing that renders it.

Both are **duplicated verbatim in the reservation app**, the same arrangement
`docs/RESERVATION-API.md` / the app's `docs/contracts/RESERVATION-API.md` already
use — update the copies together.  Sharing the dictionary is the point: a controller line and an app line
join on the same key (`rid`, `poduid`, `cid`), which the old vocabularies could
not do (`uid=` meant the pod UID here and nothing there; a reservation was `#42`
here and `id=42` there).

`kv()` is also the **sanitisation chokepoint** — pod names, namespaces and
annotation values come from whoever created the pod, and it scrubs every value of
non-printable characters so no call site can forget.  Never pre-format such a
value into the message string.

**Every call site in this repo now emits it** — no prose log lines remain, and the
`actor=controller` envelope field is constant here (unlike the app, this daemon has
one principal).  `tests/test_log_grammar.py` fails the build if a new call site
skips `kv()`, emits a field `docs/LOG-FIELDS.md` does not document, or adds an
`event=` that `OBSERVABILITY.md` does not list — which is what stops that file
drifting the way it did before.  `OBSERVABILITY.md` is the per-event inventory.

Two renames the grammar forced, worth knowing when reading older lines:
`phase=` now means the **pod lifecycle phase** (`Pending`/`Running`/`Succeeded`/…)
and the preemption sweep's A/B is `sweep=`; and the per-class `demand=`/`free=`
maps the sweep used to print as dicts are **fanned out to one line per GPU class**,
so each class's shortfall is independently greppable.

### Trace ids

Every log line carries two **envelope** fields the formatter writes, not the call
site: `actor=controller` (constant — this daemon has one principal) and `trace=`,
a correlation id for the **unit of work** that produced the line.  `app/trace.py`
owns it; it is deliberately *not* part of `log_fields.py`, which must stay
byte-identical to the app's copy.

`rid` and `poduid` already join an app line to a controller line *about the same
object*.  A trace joins the lines of one **operation**, including the ones with no
object in common — "what did that JIT admission batch do, on both sides" becomes
one grep rather than a reconstruction from timestamps across concurrent loops.

- **Minted per unit of work**, via `with trace.scope(prefix)`: one fetch cycle
  (`fetch-`), one queue-processor tick (`queue-`), one preemption sweep (`sweep-`),
  one capacity audit (`audit-`), one JIT admission batch (`jit-`), one pod watch
  event (`pod-`), and lifespan startup (`startup-`).  The id is
  `<prefix>-<10 hex chars>`; `-` when nothing is in scope.  Each background loop is
  its own asyncio task and a task gets a *copy* of the context, so one loop's trace
  is invisible to the others; work fanned out *within* a scope inherits it.
- **Propagated outbound** by an httpx `event_hooks={"request": ...}` hook on the
  `ReservationClient` — the one chokepoint, so no call site passes the header and an
  endpoint added later cannot forget it.  The header is `X-Client-Trace`, exactly the
  one the app's `_RequestLoggingMiddleware` already reads, so a controller-initiated
  request lands in the app's log under the controller's trace **with no app-side
  change**.  Nothing is sent when no scope is in force (the literal `-` must never be
  echoed as if it were an id).
- **Adopted inbound** on both inbound endpoints via the `_bind_trace(prefix)` yield
  dependency (`push-` / `forecast-`), so a pushed reservation update is logged under
  the *app's* trace and the two sides of that call join.
- **The inbound value is untrusted** and is whitelist-matched against `_TRACE_RE`
  (`[A-Za-z0-9_-]{1,36}`, kept identical to the app's), never escaped — the formatter
  interpolates it and cannot decide to quote it, so a crafted newline would otherwise
  forge whole log lines.  A value that fails is dropped for a locally-minted one.
  Minted ids are truncated to satisfy the same regex, because the app **silently
  discards** a trace it cannot match — propagation would look fine from here while the
  far end logged `trace=-`.

### Timezone

All reservation window arithmetic uses **UTC-aware `datetime` objects** (`timezone.utc`).
`slot_start` and `slot_end` return `r.start_utc` / `r.end_utc` directly from the API
response; no local-time conversion is performed in the controller.  Every
`datetime.now()` call in the codebase uses `datetime.now(timezone.utc)`.

**Human-readable prose is the one exception, and only ever at the last step.**
A Kubernetes Event message and the `galends/termination-warning-message`
annotation are read by a person — a user running `kubectl describe pod` — so
they render their instants in a **local** zone (`2026-08-21 10:30:16 PDT`)
rather than UTC.  Nothing else moves: every stored `datetime`, every
`galends/*` timestamp annotation, and every log field stays UTC.  That split is
load-bearing in both directions.  `docs/POD-ANNOTATIONS.md` promises consumers
the `YYYY-MM-DDTHH:MM:SSZ` wire format, `main`'s diff-and-skip reconciles
*rebuild* those strings to decide whether to re-patch (so a localised `utc_iso`
would re-patch every pod on every tick), and the `until=` / `at=` / `boundary=`
log fields join a controller line to an app line — which only works while both
sides are UTC.

`k8s_client.format_local` is the renderer and is deliberately a **sibling** of
`utc_iso`, never a replacement: the two formats differ visibly (`2026-08-21
10:30:16 PDT` vs `2026-08-21T17:30:16Z`) so neither is mistaken for the other in
a bug report.  The zone is always named, because a bare local timestamp is
ambiguous and one labelled `Z` would be wrong.  Four messages carry an absolute
instant and all four are localised: the `RuntimeGuaranteed` and
`OverstayRelinked` Events, the boundary `Preempted` Event, and
`galends/termination-warning-message`.  The `OnDemandLeaseDenied` Event is
**not** — it carries the app's 409 `detail` verbatim (which may embed a
timestamp of the app's own), and rewriting a timestamp out of someone else's
prose is how "carried, not re-derived" gets broken.

The zone comes from `EVENT_DISPLAY_TIMEZONE` (an IANA name) when set, and
otherwise from the process's local zone — i.e. `TZ`, which the chart already
wires.  So `TZ` alone localises both the logs and the prose; the explicit
variable exists for keeping logs on UTC while events read local.  It is
resolved to a `ZoneInfo`, **not** to a fixed offset captured at startup: this
daemon runs across DST transitions, and a frozen offset would render every
later instant at the offset it booted with.  An unknown zone is not fatal — the
same tolerant posture as every other setting: `config.invalid` at WARNING with
`reason=unknown_timezone`, then fall back.  `config.timezone` at INFO on startup
records what actually took effect, because the default is *derived* from the
environment rather than read off a variable.

### Fast path for mid-window pods

When a pod ADDED event arrives while its reservation window is already open
(the common case for JupyterHub notebook servers launched during a session),
`pod_watch_loop` calls `_try_apply_toleration` immediately rather than waiting
up to a full `QUEUE_PROCESSOR_INTERVAL` (default 300 s) for the next
queue-processor tick.

Only ADDED events trigger the fast path.  MODIFIED events — which can arrive in
rapid bursts as Kubernetes reconciles pod state — go through the normal queue so
the Kubernetes API is not hammered.  If the immediate attempt fails (budget full
or transient error), the entry remains in the queue and the processor retries it
on its normal schedule.

`_try_apply_toleration` is the single shared coroutine that performs the budget
check and patch; both the fast path and the queue processor call it.

### Just-in-time (JIT) on-demand leases

A pod with the `gpu-class` label but no reservation open now or opening soon
does not wait indefinitely and is not opportunistically placed onto ad-hoc
spare capacity — instead the controller requests a **real reservation** on
its behalf, just-in-time.  On-demand jobs are therefore ordinary
reservations: charged SU and protected by the same runtime guarantee as any
booking — most of that machinery needed no separate on-demand code path.
One asymmetry remains: the preemption sweep enumerates boundaries and demand
only for `kind == "booking"` reservations (`upcoming_boundaries`,
`boundary_demand`), so a granted lease (`kind="on_demand"`) never *triggers*
boundary preemption of overstayers on its own behalf — a JIT pod that lands on
squatted GPUs waits (guard 3) rather than displacing them.  Extending the
sweep to on-demand boundaries is possible future work; until then the
preemption-risk forecast reports pending JIT pressure as informational only
for the same reason (see **Preemption-risk forecast API**).

**Routing** (`pod_watch_loop`, re-evaluated on every attempt): for a pod
without the toleration,

1. `ControllerState.find_admittable_reservation` — the budget/horizon-aware
   sibling of `find_best_reservation` — looks for a match whose window is
   open now or opens within `ONDEMAND_HORIZON_MINUTES` (default 30) **and**
   has spare budget (`available(r) >= gpu_requested`).  If found, the pod is
   queued for it (`enqueue_pod`), with the same fast-path immediate-apply
   when the window is already open.
2. Otherwise, if the pod is **JIT-eligible** — `ONDEMAND_LEASE_ENABLED`,
   `Pending`, carries `galends/minimum-runtime-seconds`, and names its usage
   group (the group label when `REQUIRED_GROUP_LABEL` is set, else the
   `galends/usage-group` annotation — the lease request's `group_name` is a
   **required** natural key app-side) — it becomes an
   `OnDemandCandidate` and, on the **ADDED** event, kicks an immediate
   admission batch (`main._run_ondemand_admission`) covering it plus every
   other due waiter.  Most `MODIFIED` events do **not** re-trigger a batch —
   denial and guard short-retries ride the queue-processor tick — so a burst of
   reconcile `MODIFIED`s cannot hammer the app.  The **one** exception is the
   `MODIFIED` that carries the scheduler's verdict: when a fresh pod's
   `PodScheduled` condition is not yet set at ADDED time, guard 1 is
   indeterminate and the candidate is parked with `awaiting_schedule_signal`;
   the `MODIFIED` that finally sets the condition (`is_gpu_gated_pending` now
   returns non-`None`) clears the flag, resets the candidate's cooldown, and
   re-attempts immediately — resolving in ~1 s + a lease round-trip instead of
   waiting up to a full periodic scan (~270–300 s).  The flag is set **only** by
   the indeterminate guard-1 branch, so this fast-path reset can never
   short-circuit a 409-denial or guard-3/4 backoff, and it fires at most once
   per park (the flag is cleared before the batch runs), preserving the
   anti-hammer property.
3. Otherwise, if `find_best_reservation` finds *any* future match (beyond the
   horizon, or over budget), the pod is queued for it anyway — this preserves
   the plain wait-for-window behaviour for a pod that isn't JIT-eligible.
4. Otherwise the pod is left **Pending** (a pod missing the group label or the
   minimum-runtime annotation is deliberately not guessed at; that is left
   for a future "born overstay" design), and its owner is told why with a
   `NoReservation` Event — or `UnknownGpuClass`, when its `gpu-class` label names
   no class the app knows (see **Telling the pod's owner the pod itself is the
   problem**).

**Batch admission** (`main._run_ondemand_admission`, the single entry point for
both the ADDED trigger and the queue-processor tick): gathers every **due**
candidate (`now >= next_attempt_at`) in FIFO order and runs each through a
two-step **preflight → delegate → grant** pipeline.

- **Preflight** (`_preflight_ondemand_candidate`): re-reads the pod (drops it
  if gone/terminal/Unknown), re-runs step 1 above (a matching reservation may
  have appeared since the candidate was queued), resolves the pod's `gpu-class`
  label to a numeric id via `ControllerState.gpu_class_ids` — first, because a
  label the app does not know can never be granted anything, and settling it
  ahead of the guards is what lets its owner be told (`UnknownGpuClass`) rather
  than the pod being dropped by guard 1a before the class is ever looked at —
  then applies guard 1 (`is_gpu_gated_pending` + `class_node_counts` — see
  **Guard 1: what the scheduler can and cannot tell us** below), guard 3
  (`stuck_holder_gpu_classes`), guard 4 (`overcommitted_gpu_classes`), and guard 5
  (per-node feasibility — see **Per-node capacity accounting** below).  Survivors become an
  `OnDemandAdmissionCandidate` — the exact "ask" (username, group, class id, gpu
  count, and `duration_seconds = minimum-runtime + ONDEMAND_LEASE_BUFFER_MINUTES
  * 60`, default buffer 10 min), plus the **pod evidence** described next.
- **Pod evidence on the ask** (`pod_created_at`, `pod_annotations`).  The ask
  alone says what a lease would cost, not which waiting pod most deserves one, so
  the candidate also carries two things the create never sees.  `pod_created_at`
  is the pod's `metadata.creationTimestamp` — deliberately the candidate's *own*
  FIFO key rather than a freshly read one, so the app orders by exactly what the
  controller orders by and an age computed from it is real queueing delay rather
  than time since the batch was assembled.  `pod_annotations` is every
  `galends/`-prefixed annotation, read from the **freshly re-read** pod (so a pod
  re-annotated while it waited is offered as it is now) via
  `k8s_client.get_pod_galends_annotations`.  No judgement is made about which
  keys matter — the whole point is that app-side policy can weigh a signal the
  controller does not itself understand — so the controller's own keys
  (`booking-reference`, the guarantee and warning keys) go too when a re-queued
  pod carries them.  Both are advisory: neither reaches
  `OnDemandReservationRequest`.

  The values are whatever the pod's creator wrote and Kubernetes allows 256 KiB
  of annotations per object, so they are **bounded before sending**: 32 keys
  (sorted, so the subset does not flap between attempts) and 1024 chars per
  value.  The bound lives on the *sending* side because the app's schema is
  lenient by design — one candidate it refuses 422s the whole batch, admitting
  none of them that round — and for the same reason both fields are optional
  app-side, so a controller predating them is not rejected.  A dropped key logs
  `pod.annotations_truncated` at DEBUG (it repeats per attempt for as long as the
  pod waits); a truncated *value* is silent, being still recognisably itself.
- **Delegate** (only when `ONDEMAND_DELEGATE_ADMISSION` is on): the whole
  survivor set is offered to the app in one call
  (`POST /api/reservations/ondemand-admission`,
  `ReservationClient.select_ondemand_admissions`), which returns the subset of
  `pod_uid`s to admit this round — the delegation point for **future LAS
  prioritization**.  Mirrors the preemption-victims pattern: only offered uids
  are honoured (`_map_granted_uids` drops unknowns), an **empty** answer is
  respected (grant none), and a **call failure or the flag being off** falls
  back to granting *every* survivor — the prior greedy per-pod behaviour, so the
  change ships safely dark.
- **Grant** (`_grant_and_admit`, per granted pod): calls `POST /api/reservations`
  with `on_demand=True` (the app relaxes policy limits — SU, caps, minimum
  duration — never physical calendar capacity), **idempotent by the pod's UID**
  (`idempotency_key`).  The client returns a `LeaseAttempt` carrying the HTTP
  status, because the two non-grant cases need different handling: a **409**
  (the app's documented "infeasible right now") or a network/5xx failure is
  routine, logs `lease.denied` at INFO, and cools the candidate down 2–5 min;
  any **other 4xx** — a `read_only` service key, a schema mismatch after an app
  upgrade, an unknown `group_name` — is a fault waiting cannot fix, so it logs
  `lease.error` at **WARNING** with the response body and backs off
  exponentially to `ERROR_RETRY_CAP_SECONDS` (30 min) via
  `OnDemandCandidate.lease_error_count`, reset on any grant or routine denial.
  A **404** among them is also told to the pod (`OnDemandLeaseRejected`): every
  user, group and class it can name came off the pod.
  Collapsing the two was how a misconfigured deployment retried every 2–5 min
  per pending pod indefinitely while logging only below WARNING.
  On **grant** the lease is upserted into `state.reservations`
  (`apply_push_to_active`) and the pod is admitted immediately
  (`_try_apply_toleration`) — the existing admission path, so it stamps
  `res-<id>`, records the guarantee, and emits `RuntimeGuaranteed`.  **If
  admission does not succeed** (budget race, transient patch error, or the pod
  having gone terminal), the controller issues a compensating cancel
  (`POST /api/reservations/{id}/cancel`, `reason="controller-revoked"`) so the
  grant is never left dangling.  A **non-granted** survivor cools down like a
  denial and is re-offered on a later tick.

The batch is coalesced by `ControllerState.ondemand_admission_lock`: only one
runs at a time, and a trigger arriving mid-batch sets `ondemand_rerun_requested`
so exactly one trailing pass follows — an ADDED burst collapses into at most one
in-flight + one trailing batch.  The single-pod `_try_request_lease` remains as a
thin `preflight → grant` wrapper (the non-delegated path and the unit-test seam).

**Surviving the fetch that hasn't seen the grant yet.**  The grant upserts the
lease locally, but the periodic `reservation_fetch_loop` takes its
`GET /api/reservations` snapshot **before** acquiring `reservation_lock` and then
**wholesale-replaces** `state.reservations`.  A lease granted in the gap between
that snapshot and the replace is absent from the snapshot, and a naive replace
would drop it even though its pod is live inside its guarantee — leaving
`guarantee_end` unresolvable (`None`), which the preemption planner treats as
*past guarantee* and hence a valid victim.  To close that race, the fetch loop
re-adds any locally-granted on-demand lease the snapshot omits
(`ControllerState.preserve_local_ondemand_leases`): a `kind="on_demand"`,
locally-`active` lease whose window is still open (`slot_end > now`) and whose id
does **not** appear anywhere in the full `status=all` response.  Checking the full
response (not just the active subset) means a lease the app reports **cancelled**
carries its id and is therefore **not** preserved — a genuine server-side cancel,
including the controller's own compensating cancel, still wins and its pod is
released.  Only replication lag (grant not yet surfaced) is bridged; the push path
is unaffected because it merges deltas (`apply_push_to_active`) instead of
replacing.

**Lease teardown when the pod goes away** (`main._teardown_ondemand_lease`): a
JIT lease exists solely to cover one pod (its `idempotency_key` is the pod's
UID), so once that pod terminates on its own (Succeeded/Failed), is deleted, or
is preempted, the lease is no longer needed and the controller cancels it
(`POST /api/reservations/{id}/cancel`, `reason="pod-terminated"`), releasing the
capacity and stopping SU accrual instead of letting the lease linger until it
expires.  `pod_watch_loop` calls this from both its DELETED and terminal-phase
branches, right after `release_pod`.  **No on-demand-vs-booking state is tracked
in memory**: the pod's `galends/booking-reference` annotation resolves to the
reservation id, and the reservation's own `kind` field — the app returns leases
as `kind="on_demand"` and the pull keeps them in `state.reservations` — is read
live to decide.  Only `kind == "on_demand"` rows are touched (a booking's pod
ending never cancels anything); the cancel is idempotent and best-effort (a
failure just logs — the next poll reconciles), and occupancy is released
independently.

**RBAC / config**: no new Kubernetes permissions (admission reuses the
existing pod-patch path); `ONDEMAND_HORIZON_MINUTES` and
`ONDEMAND_LEASE_BUFFER_MINUTES` tune the routing horizon and lease sizing.

### Best-effort admission

A pod annotated `galends/runtime-guarantee: none` is asking for the one thing the
system could not previously express: **no runtime guarantee at all** — idle GPUs
now, yielded the moment anything else wants them, charged nothing.  Before this,
a job with no runtime to promise either bought a guarantee it did not want or sat
Pending; a `0` in `galends/minimum-runtime-seconds` looked like the way to say so
and silently disqualified the pod instead (that `0` now logs
`pod.annotation_invalid reason=not_positive` naming this annotation, and tells
the pod's owner — see **Telling the pod's owner the pod itself is the problem**).

Such a pod is admitted under a **zero-length, zero-SU `kind="best_effort"`
reservation** (`POST /api/reservations` with `best_effort: true`;
`ReservationClient.create_best_effort_reservation`, sharing
`_post_reservation_create` with the lease path so denial classification, backoff
and the `OnDemandLeaseDenied` Event are identical).

**Almost nothing downstream needed a branch**, and that is the whole reason the
representation is a stub rather than a "preemptible" flag on the pod.  The stub's
window is already over, so `guarantee_end` resolves to an instant in the past and
`_past_guarantee` — the single predicate victim eligibility, headroom and adoption
all share — is true from the pod's first tick.  Boundary preemption, headroom and
adoption therefore handle it correctly with no best-effort code in any of them.
A flag would have had to be threaded through all four plus the status annotations.

Three consequences worth knowing, each pinned by a test:

- **The stub is not preserved across a fetch** (`preserve_local_ondemand_leases`
  covers `kind="on_demand"` inside its window), and the app omits `best_effort`
  rows from the poll by default, so it simply vanishes.  `guarantee_end` then
  returns `None`, which reaches the *same* conclusion — past guarantee — so
  nothing flaps.  Nothing is torn down when the pod ends either: a stub is
  already over, holds nothing, and cost nothing.
- **Merge skips it** (`plan_ondemand_merges` requires `kind="on_demand"`).  Merge
  exists to re-link a pod *before* its guarantee lapses; a best-effort pod's
  already has, so `plan_pod_adoptions` picks it up on the same tick and re-links
  it onto a booking its user has since made — the free upgrade from unguaranteed
  to guaranteed.
- **`guarantee-status` reads `overstay` for the pod's whole life**, which is
  literally true and deliberately not given a third literal (that would risk the
  diff-and-skip drift `plan_guarantee_status` warns about).
  `galends/reservation-kind: best_effort` is what disambiguates it for a consumer.
  Admission emits **`BestEffortAdmitted`**, not `RuntimeGuaranteed` — whose
  message would read "guaranteed for 0m00s, until *the instant the pod started*".

**Admission is where the real work is**, because the app cannot bound this path:

- **Guard 4 does not apply.**  Over-commit means the app's per-class count
  exceeds physical capacity, and a best-effort stub consumes no app-side count,
  so the mismatch says nothing about whether one can be admitted.
- **Guard 5 applies at *every* GPU count**, not just ≥2, and is the **only**
  physical bound on best-effort admission.  A guaranteed lease is bounded
  app-side — it holds calendar capacity, so the app will not sell the same
  GPU-hour twice — but a zero-length stub is invisible to that arithmetic, so the
  app would admit an unbounded number onto the same idle GPU and every one after
  the first would sit Pending.  The app's own 10-minute capacity probe is an
  anti-thrash heuristic ("is this class about to be booked solid?"), **not**
  admission control, for exactly this reason.
- **An in-batch tally** (`claimed_by_class`, accrued at preflight in
  `_run_ondemand_admission`) nets GPUs already taken by earlier candidates off
  `node_free_by_class`, so a burst is not all measured against the same opening.
  It does not close the window between queue ticks, which is acceptable because
  an over-admitted best-effort pod costs nothing: no SU, no capacity, it just
  waits.

**App side**: `_prioritize_candidates` now sacrifices by what the reservation
promised — `best_effort`, then `on_demand`, then `booking` — replacing the
uniform-random placeholder.  **RBAC / config**: none new;
`BEST_EFFORT_ENABLED=false` (the default) ignores the annotation — telling the
pod's owner so, as `AnnotationIgnored` or inside `NoReservation`, since a pod
that asked for free best-effort time is otherwise charged a guaranteed lease or
left Pending without a word.
User-facing documentation is `docs/POD-ANNOTATIONS.md` §3.1.

### Surfacing a lease denial to the pod's owner

Every other reason a JIT candidate does not get a lease is the controller's own
(the guards above), and is logged as such.  A **409** is the one that is not: it
is the *app* refusing the ask, and its `detail` states why in terms the pod's
owner can act on — `Only 1 GPU(s) available for this group at 2026-08-21 14:00
(group ceiling: 4)`, an exhausted SU budget, a group they are not a member of.
That reason used to reach the controller's log and stop there.  The owner has
`kubectl` on their own namespace and no access to those logs, so what they saw
was a pod sitting Pending with nothing explaining it.

`main._emit_lease_denial_event` mirrors it onto the pod as a **`Warning`
Kubernetes Event** (`reason=OnDemandLeaseDenied`, via
`k8s_client.emit_lease_denied_event`), which is what makes it visible where the
owner already looks — `kubectl describe pod` — **without** the pod needing any
Kubernetes API access of its own.  `Warning` because the pod is not running and
will not until something changes; it is the same shape kube-scheduler's
`FailedScheduling` takes, immediately above it in the same Event list.

Four properties are load-bearing:

- **Only the app's own answers about the ask are surfaced.**  A network failure
  or a 5xx says nothing about the ask (the app never answered), and most
  non-retryable 4xx — a `read_only` service key, a schema mismatch — is an
  operator fault the pod's owner can neither read usefully nor fix.  Those keep
  the `lease.error` WARNING and reach no pod.  The exception is the **404**: the
  app does not recognise the user, usage group or GPU class the ask named, and
  every one of those came off the pod (its namespace, its group label or
  annotation, its `gpu-class` label).  Filing it with the operator faults was how
  a pod carrying a mistyped `dsmlp/course` label sat Pending with nothing but a
  404 in the controller's log.  It keeps the exponential backoff and the
  `lease.error` line, and is also told to the pod as `OnDemandLeaseRejected`
  (`main._emit_lease_rejected_event`) — see **Telling the pod's owner the pod
  itself is the problem**.
- **The detail is carried, not re-derived.**  `LeaseAttempt.detail` (already
  truncated to 200 chars by `_response_detail`, so a proxy's HTML error page
  cannot become a 5 KB Event message) is the single value both the `lease.denied`
  log line and the Event render.
- **Throttled by content first, clock second.**  A *changed* reason is new
  information and emits immediately; an *unchanged* one waits out
  `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` (default 30).  The retry cadence is
  2–5 min, so emitting every denial would bury the pod's other Events — but
  reporting once and going quiet is equally wrong, because Events expire and a
  pod still blocked an hour later would `kubectl describe` clean.  The repeat is
  what keeps the two failure modes apart.  The throttle is
  `main._post_pending_status`, shared by every Event addressed to the owner of
  a pod that is not running (see **Telling the pod's owner that admission is
  paused** and **Telling the pod's owner the pod itself is the problem**).
- **Best-effort, and the stamp follows the emit.**  What was told is recorded in
  `ControllerState.pending_status` (keyed by pod uid) **only** after a
  successful emit, so a failed one is retried on the next attempt rather than
  suppressed for the whole interval.  A failure logs `k8s.event_failed` and never
  disturbs the retry cadence, which is the thing that actually gets the pod
  running.  The state is in-memory: after a restart the pod is re-warned once,
  which is the right side to err on.

**RBAC / config**: none new — Event creation reuses the `events: create`
permission the guarantee and preemption emitters already need.
`ONDEMAND_DENIAL_EVENT_ENABLED=false` disables the feature.  User-facing
documentation is `docs/POD-ANNOTATIONS.md` §5.1.

### Guard 1: what the scheduler can and cannot tell us

Guard 1 vets a JIT candidate's `PodScheduled` condition before a lease is
minted.  Its original form asked "is this pod Pending *solely* for want of
GPUs?" and answered by looking for `Insufficient nvidia.com/gpu` in the
scheduler's message.  **That string can never appear for a candidate on a
correctly taint-gated cluster**, and the guard therefore never granted anything:
GPU nodes carry `gpu-class-reservation=<class>:NoSchedule`, an unadmitted
candidate by definition does not tolerate it, and kube-scheduler evaluates
`TaintToleration` *before* `NodeResourcesFit` — recording only the first filter
that rejects each node.  So the pod's own class's nodes are attributed to the
taint bucket and never report a resource verdict at all.  Every candidate parked
at `guard=1 reason=schedule_verdict_pending`, at DEBUG, indefinitely.

The rewrite splits the question along the line of what evidence actually exists.

**1a — `k8s_client.is_gpu_gated_pending(pod, taint_key)`** (pure) reads the
message only for blockers it can still name reliably, and keeps the same
tri-state the call site already branched on:

| | Condition | Outcome |
|---|---|---|
| `False` | `Insufficient <non-GPU-resource>`; **or** no node was rejected on *any* taint (a toleration changes nothing) and no GPU shortage named; **or** every taint the message *names* is somebody else's | **drop** — `ondemand.candidate_dropped reason=blocked_not_by_gpu_gating` |
| `None` | no verdict at all: not Pending, no `PodScheduled`, not `Unschedulable`, or an empty message | hold, `awaiting_schedule_signal` |
| `True` | otherwise — `Insufficient nvidia.com/gpu` outright, or nodes rejected on a taint that may be ours | proceed |

Both scheduler message formats are handled: older versions name the offending
taint inline (`untolerated taint {key=value: NoSchedule}`), current ones emit an
anonymous count (`25 node(s) had untolerated taint(s)`).  Named taints are
checked against `TOLERATION_KEY`; an anonymous bucket gets the benefit of the
doubt.

**1b — the physical half**, since 1a establishes only that *something* we might
tolerate is in the way.  `ControllerState.class_node_counts` (class → schedulable
nodes carrying its reservation taint, from `controller.node_counts_by_class` over
the same inventory snapshot guard 5 reads, refreshed each queue-processor tick)
must not be a **known zero**.  A fully drained class is **held**, not dropped —
nodes come back — and a class with no data yet never blocks (fail-open, matching
guard 5), so a snapshot gap cannot wedge admission.  Free GPUs cannot answer this:
`node_free_by_class` reports `0` both for a full class and for a class with no
nodes, and those want opposite treatment.

**What makes a zero "known".**  `snapshot_node_gpu_inventory` lists a class only
while one of its nodes is schedulable, so a fully cordoned class is *absent* from
the inventory, never `0` — and absent is exactly the "no data" the guard fails
open on.  Until that was fixed, guard 1b could not hold anything: every test
injected a hand-built `{"x": {}}` or a `0` count, shapes the real snapshot never
produces, and a drained class fell through to a lease request (or, when the app
still counted its GPUs, to guard 4).  `node_counts_by_class` therefore
takes the labels the app knows (`gpu_class_ids`) and records each one
explicitly, `0` when the inventory has no node for it.  Unknown stays unknown: a
label the app does not list, and everything before the first successful
snapshot.  `TestCordonedClassEndToEnd` runs the real snapshot over a cordoned
node so this cannot regress silently again.  Two side effects follow from the
guard now working: a drained class that the app still counts GPUs for is
reported under **both** guard 1 and guard 4 in `ondemand.gated` (it is held at
1b, which runs first), and an app-known class with no schedulable node is
reported under guard 1 every minute even when nobody is waiting — which is the
documented contract of that warning, but is new for a class that never had
hardware.

**Known limitation**, inherent to the same filter ordering: a candidate blocked
by a *second* constraint on its own class's nodes — cpu/memory there, an
unrelated taint, an unbindable volume — is invisible, because those nodes never
get past the taint filter to report it.  Such a pod classifies `True`, is granted
a lease, and stays Pending under it.  Backstops: guard 3 (a stuck holder freezes
the class), the lease's short natural expiry, and `_teardown_ondemand_lease` when
the pod goes away.  **RBAC / config**: none new — 1b reuses the node LIST the
queue tick already issues for guard 5.

### Defaults for pods that declare neither

JIT eligibility asks a pod for two things it must declare itself — a minimum
runtime and a usage group — and a pod declaring neither is left Pending
(routing step 4).  That is right where users are expected to annotate their
workloads and wrong where the operator would rather every unannotated pod fall
into a house default.  Two settings supply that, both **shipping disabled** so
an unconfigured deployment behaves exactly as before:

- **`DEFAULT_MINIMUM_RUNTIME_SECONDS`** (default `0` = off) stands in when
  `get_pod_min_runtime_seconds` yields nothing — annotation absent, or present
  but junk/non-positive, which that function already rejects.  A zero default is
  indistinguishable from no default (the same rejection floor), which is why
  `0` is the "off" value rather than a sentinel.
- **`DEFAULT_USAGE_GROUP`** (unset = off) stands in for whichever group source
  is in force.  With `REQUIRED_GROUP_LABEL` on it substitutes for the missing
  **label**, and it does so *wholesale*: the pod matches that group's
  reservations on the **reserved path** exactly as if it carried the label, not
  merely on the JIT lease ask.  So this is a statement about which group
  unlabelled workloads belong to.  With the label feature off it substitutes for
  a missing `galends/usage-group` annotation, which only ever fed the lease ask,
  so there it changes nothing else.  It is not a wildcard — it names one group,
  and another group's booking still does not match.

The pod's own annotation/label always wins; the default only fills a gap.

`snapshot_tolerated_pods` takes the group default alongside the label key and
applies the same fallback, so a pod **admitted** under the default group reads
back carrying it.  Without that, `ToleratedPodInfo.group_label` would be `None`
for exactly the pods routing had treated as grouped, `_group_ok` would reject
every reservation for them, and adoption / JIT-lease merge would evict a job
they were meant to carry forward.  Every call site passes
`config.default_usage_group` next to `config.required_group_label` for that
reason — the two travel together.

**RBAC / observability**: none new.  There is deliberately no "a default was
applied" log event: `ondemand.candidate_added` already prints the effective
`min_runtime_s` and `group`, which is the value that matters.  The one case the
pod's owner is told about is a default standing in for a value they *did* write
but which was invalid (`AnnotationIgnored`), since they would otherwise believe
their own runtime was in force; a default filling a gap they left says nothing,
and one supplying the usage group of a lease the app 404s is named as such in
`OnDemandLeaseRejected`.

### App-side vs physical capacity reconciliation

The controller derives physical GPU capacity solely from Kubernetes node taints
(`snapshot_node_gpu_capacity` — total allocatable `nvidia.com/gpu` per
`gpu-class-reservation` taint value, or whatever a node's
`galends/force-node-capacity` annotation forces it to; see **Forcing a node's
GPU capacity**).  The reservation app has its **own**
per-class GPU count, modelled on `schemas.GpuClassDetail` and cached per label in
`ControllerState.gpu_class_capacity` — refreshed on every reconcile from the
same `GET /api/gpu-classes` fetch that builds the label maps.

**Which of the app's two counts to audit against.**  The app publishes both
`total_gpus` (the class's configured default) and `effective_gpus_today` (that
default after any date-span capacity override covering today) —
RESERVATION-API.md §4.  The audit reads **`effective_gpus_today`**, because that
is the number the app actually admits against; `total_gpus` is a figure nobody
is enforcing while an override is in force.  Auditing the default instead was
wrong in both directions: an override that lowers a class to match a genuine
node drain read as a mismatch and **paused JIT admission via guard 4** for a
class that was not over-committed, while an override *raising* a class above its
default hid a real over-commit entirely.  `GpuClassDetail.audit_gpus` is the
single accessor, falling back to `total_gpus` when the app publishes no
effective count (an older app audits exactly as before).  Only classes whose
count is known are recorded; a payload omitting both degrades to "unknown".

`capacity_audit_loop` (`_run_capacity_audit` in `main.py`) runs every
`CAPACITY_CHECK_INTERVAL` s (default hourly, plus once synchronously at
startup):

1. Snapshots physical capacity (`snapshot_node_gpu_capacity`).  **Fail-safe**:
   if the node LIST fails, the audit is skipped and the current pause set is
   left unchanged — a transient failure must never silently lift a pause (the
   same "never act on unknown physical state" rule the preemption sweep
   follows).
2. Calls the pure `controller.reconcile_capacity(app_side, physical)`, which
   walks the union of class labels and returns (a) a `CapacityDiff` for every
   class whose two counts disagree in either direction — a class present in
   only one map treats the other side as `0` — and (b) the set of
   **over-committed** labels (`app_side > physical`).  A class whose app-side
   count is unknown is never flagged over-committed (overcommit cannot be
   concluded from missing data).
3. Logs every diff at **WARNING**; sets `ControllerState.overcommitted_gpu_classes`
   to the over-committed set (logging INFO as classes enter/leave it).

**Per-class on-demand pause.**  `_preflight_ondemand_candidate` gates JIT
admission on `overcommitted_gpu_classes` as **guard 4** (mirroring the guard-3
`stuck_holder_gpu_classes` interlock): a candidate whose `gpu-class` is
over-committed is short-retried rather than granted, so it stays queued and
resumes automatically once the class leaves the set.  Only
the JIT/on-demand path is gated; reserved-path admission under a real user
booking is untouched (a booking already implies the app granted real calendar
capacity).  **RBAC**: none new — the audit reuses the existing `nodes: list`
permission.

**The pause set is re-checked every queue-processor tick, not only hourly.**
The tick already takes a node inventory for guards 1b and 5; it collapses that
to per-class totals (`controller.gpu_capacity_by_class`, identical to
`snapshot_node_gpu_capacity`) and calls the same `main._refresh_overcommit_pause`
the audit does.  So a pause engages or lifts within one `QUEUE_PROCESSOR_INTERVAL`
of the counts changing — an operator who fixes a node no longer waits up to an
hour for admission to resume.  The split is deliberate: the tick only moves the
pause set (logging `capacity_audit.paused` / `.resumed` on transitions, under a
`queue-` trace), while the per-class `capacity_audit.mismatch` WARNING stays on
the audit's hourly cadence rather than repeating every tick.  The fail-safe is
unchanged: a failed pod or node snapshot skips the refresh, leaving the pause
set as it was.

### Operator warning for paused on-demand admission

Three JIT guards pause admission for a **whole GPU class** until something
changes: guard 1b (no schedulable node), guard 3 (stuck holder interlock) and
guard 4 (app-side overcommit).  Each announces itself once on transition
(`interlock.activated`, `capacity_audit.paused`), and the per-candidate
`ondemand.candidate_held` lines are per pod, partly DEBUG, and silent when nobody
is waiting — so a pause could stand for days with nothing in the log saying so.

`ondemand_gate_warning_loop` closes that: every 60 s
(`ONDEMAND_GATE_WARNING_INTERVAL_S`, fixed — a knob to quieten it would be the
wrong fix) it logs one `ondemand.gated` WARNING per gated class, whether or not a
pod is currently waiting.  `ControllerState.plan_ondemand_gates` (pure bar its
`ondemand_gate_since` bookkeeping) reads exactly the state the preflight reads,
including guard 1b's fail-open and guard 4's best-effort exemption, so the warning
cannot describe a gate the preflight is not applying.  The `detail` field is
prose written for a sysadmin unfamiliar with the service (`main._ondemand_gate_detail`):
what is paused, the cause, the `kubectl` checks, and when it clears.  To make it
concrete, the audit now keeps its physical snapshot (`physical_gpu_capacity`, so a
guard-4 line states both counts) and the queue tick keeps the stuck holders' names
(`stuck_holder_pods`, so a guard-3 line names the pods to inspect).  Guard 5 is
deliberately not reported (per-ask fragmentation, not a fault).  **RBAC / config**:
none new; in-memory reads only; runs only when `ONDEMAND_LEASE_ENABLED`.
The pod owner's side of guards 1b, 3 and 4 is the next section.

### Telling the pod's owner that admission is paused

The operator warning reaches the controller's log; the owner of a pod the pause
is holding cannot read that log, and — unlike a 409 — there is no denial to
mirror, because a class-wide gate means the app is never asked.  What they saw
was a pod Pending with nothing explaining it, for as long as the pause stood.

So each **guard 1b**, **guard 3** or **guard 4** hold in
`_preflight_ondemand_candidate` also calls `main._emit_admission_paused_event`,
which puts a **`Warning` Event**
(`reason=OnDemandAdmissionPaused`, via `k8s_client.emit_admission_paused_event`)
on the pod: the class is paused, why in plain terms, nothing about the pod needs
to change, and — the point of it — **if this persists, contact support**.
`SUPPORT_CONTACT` (an address or URL) is named at the end of that sentence when
set, with no full stop after it so it copies cleanly out of `kubectl describe`.

- **Emitted where the hold is decided, not from the gate-warning loop.**  The
  preflight knows this pod was held and by which guard; `plan_ondemand_gates`
  counts every candidate of a class, including ones held earlier (guard 1a/1b)
  or about to be rerouted to a reservation.  It also keeps
  `ondemand_gate_warning_loop` what it is documented as — in-memory reads, no API
  calls.  The guards run 1b, 3, 4, so a class under several reports the first —
  and a drained class, which also reads overcommitted whenever the app still
  counts its GPUs (physical `0` < app-side `N`), reports the drain.
- **One throttle, one cadence, one story.**  `main._post_pending_status` is the
  throttle for every pending-pod Event, keyed per pod uid on
  `(reason, varying text)` — the app's `detail` for a denial, the rendered
  message for a pause.  So the pause runs on exactly the denial's cadence
  (changed status at once, unchanged once per
  `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES`; the name predates the pause and was
  kept rather than breaking deployed values), and moving between the two is a
  change that is reported immediately.  A throttle per Event reason would have
  let a pod go paused → denied → paused with the third notice suppressed, so the
  newest Event on it described a block that no longer applied.  A pod flapping
  between the two is told on every flip; the gates themselves only move on the
  queue tick (and, for guard 4, the hourly audit), which bounds that.
- **The message is the throttle key, so it must not drift.**
  `_admission_paused_message` carries no count and no timestamp — either would
  make every retry a "changed" status and restate the Event each time.  It also
  deliberately leaves out what only an operator can act on: the capacity counts,
  and the stuck holders' names, which are *other users'* pods (a namespace is a
  username).  An operator correlates a quoted Event with `ondemand.gated` by
  `clabel`.
- **Scope is the class-wide gates: 1b, 3 and 4** — the same three
  `ondemand.gated` reports.  Guard 5 is fragmentation that clears as jobs finish,
  a full cluster rather than a fault.  A best-effort candidate is exempt from
  guard 4, so it is never told about one; guard 1b has no such exemption (a pod
  that wants no guarantee still needs a node).
- **Best-effort like every emitter.**  The hold's `next_attempt_at` is set before
  the emit; a failure logs `k8s.event_failed reason=OnDemandAdmissionPaused`,
  stamps nothing (so the next hold retries it), and changes nothing else.

**RBAC / config**: none new — `events: create` is already required.
`ONDEMAND_PAUSE_EVENT_ENABLED=false` disables it; `SUPPORT_CONTACT` is optional.
User-facing documentation is `docs/POD-ANNOTATIONS.md` §5.2.

### Telling the pod's owner the pod itself is the problem

The two sections above cover the app refusing a lease and a class being paused.
Four more ways a `gpu-class` pod could be held or ignored reached only the
controller's log — or nothing at all — although in each the thing to fix is the
pod itself, which only its owner can change:

| Event (`Warning`) | When | Was |
|---|---|---|
| `OnDemandLeaseRejected` | The app answered the lease ask **404**: it does not recognise the user, usage group or GPU class the ask named — all of which came off the pod | `lease.error` WARNING, filed with the operator faults |
| `UnknownGpuClass` | The pod's `gpu-class` label names no class the app knows | `ondemand.candidate_held reason=class_id_unknown` WARNING, every 2–5 min |
| `NoReservation` | No reservation matches the pod and it does not qualify for on-demand admission, so no path will ever pick it up | `pod.left_pending` at **DEBUG** |
| `AnnotationIgnored` | A `galends/*` job-input annotation was invalid, or asks for something the deployment does not offer, and ignoring it changed what happens | `pod.annotation_invalid` WARNING, or nothing (`galends/runtime-guarantee` while `BEST_EFFORT_ENABLED` is off) |

Each message says what is wrong, what the controller did instead, and what to
do; the renderers are the `_…_message` helpers in `main`, and the one write is
`k8s_client.emit_pod_problem_event` (capped at the 1024 characters
`events.k8s.io/v1` allows a note).

- **`OnDemandLeaseRejected`** (`main._emit_lease_rejected_event`, from
  `_grant_and_admit`) carries the app's `detail` when it sent one, and states
  what the controller *sent* — the user (the pod's namespace) and the usage
  group, and where that group came from: the `REQUIRED_GROUP_LABEL` label, the
  `galends/usage-group` annotation, or `DEFAULT_USAGE_GROUP`
  (`OnDemandCandidate.usage_group_source`, recorded by routing).  The app cannot
  know the last, and it is the difference between "fix your label" and "you
  never named a group; contact support".  A booking the user holds under another
  usage group is named too.  Emitted even without a `detail`, unlike the 409.
  Rides `ONDEMAND_DENIAL_EVENT_ENABLED`, being the app's refusal of the same ask.
- **`UnknownGpuClass`** is told from two places: preflight, which now resolves
  the class id **before** guard 1a (a label the app does not know can never be
  granted anything, and a guard-1a drop used to hide it for good), and the
  left-Pending branch of `pod_watch_loop`.  It lists the classes the app does
  know, since a typo is the usual cause.
- **`NoReservation`** (`main._emit_no_reservation_event`) replaces the silent
  `pod.left_pending` outcome.  `_ondemand_ineligibility` mirrors the
  `jit_eligible` test and yields one clause per cause — on-demand admission off
  (and nothing else, since then nothing else matters); no usable minimum
  runtime, unless best-effort stands in for one; no usage group — naming an
  ignored annotation in place of the "has none" clause it explains.
  `ControllerState.near_miss_bookings` adds what "no reservation matches" hides:
  a live booking this user holds for the same class under another usage group
  (only while `REQUIRED_GROUP_LABEL` is on), or one of another class.
- **`AnnotationIgnored`** (`main._emit_annotation_notice`) is for a pod that went
  ahead: an on-demand candidate running on `DEFAULT_MINIMUM_RUNTIME_SECONDS`
  because its own runtime was junk, one charged a guaranteed lease because it
  asked for best-effort on a cluster without it, or one left waiting for its
  booking because the runtime was all that kept it off the on-demand path.  A
  pod no path will admit gets the same facts inside `NoReservation` instead.
  The verdicts come from `k8s_client.get_pod_annotation_problems`, which shares
  its parse with the `pod.annotation_invalid` getters, so the log line and the
  Event cannot disagree about a value.  A best-effort pod's minimum runtime is
  never reported: it has no use for one.

Four properties are load-bearing:

- **One ledger per pod, two topics.**  `ControllerState.pending_status` (pod uid
  → topic → the last Event and when) replaced `status_event_key/at` on
  `OnDemandCandidate`, so a pod tells one story whichever path holds it — a
  candidate, the reserved queue, or none — and moving between them is not
  mistaken for news.  The `annotations` topic is separate from the `hold` topic
  because an ignored annotation stays true whatever holds the pod: on one key
  the two would alternate, each a "change", and both would be restated on every
  retry.
- **Never blame the pod for the controller's gap.**  `UnknownGpuClass` needs
  `state.gpu_classes_known` — a *bulk* class fetch has succeeded, now or on an
  earlier cycle (`GpuClassMaps.complete`); the per-id fallback alone holds only
  the classes some reservation named.  `NoReservation` needs
  `state.reservations_known` — a full fetch has installed its result; a push is
  a delta, and before the first fetch the list is merely empty.  With the app
  unreachable at startup, neither is said.
- **Told at first sight and on each resync, not on every MODIFIED.**  A
  left-Pending pod is tracked by no path, so it is re-evaluated only when the
  watch replays it (ADDED, every ~10 min) — which, with the repeat interval, is
  the restatement cadence.  A burst of scheduler-condition MODIFIEDs drives
  nothing.
- **Forgotten when the pod is no longer pending.**  DELETED, a terminal phase
  and admission (the toleration seen) drop the pod's entry;
  `prune_pending_status` runs each queue tick for a pod whose DELETED event was
  never seen (a re-LIST replays only pods that still exist), dropping untracked
  entries not told anything for `max(2 h, 3 × ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES)`
  — a pod still pending is re-told every repeat interval, so it cannot be
  caught.

**Known gap**: a pod waiting on the reserved path — queued for a booking that
has not opened, or over its booking's budget — still gets no status Event, so a
`NoReservation` told before the user booked stays the newest Event on the pod
until it expires.  **RBAC / config**: none new.  `POD_PROBLEM_EVENT_ENABLED=false`
disables `UnknownGpuClass`, `NoReservation` and `AnnotationIgnored`;
`OnDemandLeaseRejected` rides `ONDEMAND_DENIAL_EVENT_ENABLED`.  User-facing
documentation is `docs/POD-ANNOTATIONS.md` §5.3.

### Per-node capacity accounting

Extended resources (`nvidia.com/gpu`) are **node-scoped and atomic**: the
Kubernetes scheduler only places an N-GPU pod on a *single* node with N free, or
leaves it Pending — it never splits a job across nodes.  The controller therefore
cannot (and need not) enforce single-node placement.  But its own budget/capacity
accounting is otherwise **per-class and count-based** (`available`,
`free_capacity_by_class`, `snapshot_node_gpu_capacity`), which is exactly right
for 1-GPU jobs (any free GPU is interchangeable) yet **blind to fragmentation**
for multi-GPU jobs: a class can show enough free GPUs in aggregate while they are
scattered one-per-node so no single node can host a 2+ GPU pod.

Per-node accounting closes that blind spot for the **JIT on-demand path**, using
data already fetched (no new API calls, no new RBAC):

- `k8s_client.snapshot_node_gpu_inventory` returns allocatable GPUs **per class,
  per node** (`{gpu_class: {node_name: allocatable}}`); `snapshot_node_gpu_capacity`
  is now a per-class collapse of it.  `ToleratedPodInfo` / `PodRuntimeView` carry
  `node_name` (`spec.nodeName`), captured through `_pod_view`.
- Two pure helpers in `controller.py`: `free_gpus_by_node_class` (per-node free =
  allocatable minus the GPUs of bound, node-resident, non-terminating pods on that
  node — an unscheduled pod belongs to no node and counts against none) and
  `largest_node_free_by_class` (the largest single-node opening per class).  Because
  GPU nodes are tainted `gpu-class-reservation=<class>:NoSchedule`, controller-tolerated
  pods are the only GPU consumers on them, so a tolerated-pods-only occupancy count is
  accurate.
- `ControllerState.node_free_by_class` (largest single-node free GPUs per class) is
  refreshed every `queue_processor_loop` tick from a node-inventory + tolerated-pod
  snapshot, alongside `ControllerState.class_node_counts` (nodes per class, which
  guard 1b reads — see **Guard 1: what the scheduler can and cannot tell us**).
  **Fail-safe**: if either snapshot fails, both prior maps are left unchanged
  (never open admission on unknown physical state).

**Guard 5** (`_preflight_ondemand_candidate`): a JIT candidate requesting **≥2
GPUs** whose `gpu-class` has a *known* largest-single-node-free below the ask is
short-retried rather than granted — the controller does not mint an SU-charged
lease the pod could never schedule under.  **Fail-open on unknown**: a class absent
from `node_free_by_class` (no data yet, or a snapshot gap) never blocks, so a stale
map cannot wedge admission; the reactive guard-3 interlock and the compensating
cancel in `_grant_and_admit` remain the backstop.  The 1-GPU path is unaffected.

Two node-aware consumers are **deliberately deferred** to a follow-up: preemption
victim-targeting (concentrating kills on one node so a reserved multi-GPU booking
can land — today the sweep still frees GPUs per class, count-based) and the
preemption-risk forecast's shortfall (still per-class).  Guard 5 is a per-candidate
feasibility check against a snapshot, not batch-level bin-packing: two ≥2-GPU
candidates can both pass against the same single-node opening in one batch, with
guard 3 backstopping the loser.

### Forcing a node's GPU capacity

`status.allocatable["nvidia.com/gpu"]` is the controller's only evidence of how
many GPUs physically exist, and it is not always the number the reservation
system should account against — some of a node's GPUs are failing, or are held
back for non-reservation work, or the device plugin reports nonsense.  The
**node** annotation `galends/force-node-capacity` (`k8s_client.FORCE_NODE_CAPACITY`)
replaces that reading for one node.

- **Read by `k8s_client.get_node_forced_gpu_capacity`** (pure) and applied in
  `snapshot_node_gpu_inventory` — the *single* place allocatable is read, which
  is what makes the override total without touching anything downstream.  Every
  notion of physical capacity in the controller (per-class totals, headroom,
  `free_capacity_by_class`, the capacity audit, guards 1b and 5) derives from
  that one map, so all of them inherit it for free.
- **`0` is a valid override**, deliberately unlike the `> 0` floor
  `get_pod_min_runtime_seconds` applies: masking a node's GPUs *from the
  reservation system* while leaving its pods (and every non-GPU pod) running is
  the main thing the annotation is for, and it is precisely what cordoning is
  not — a cordoned node leaves the snapshot entirely.  The node stays in the
  inventory at `0`, the same shape a node with no GPUs already produces, which
  matters because `node_counts_by_class` counts nodes: dropping the entry would
  read to guard 1b as "this class has no nodes at all".
- **Negative or unparseable is rejected** with a `k8s.node_capacity_forced_invalid`
  WARNING naming the node and the value, and the node keeps its allocatable
  count — the same tolerant-parse-and-say-so posture `config.py` takes, for the
  same reason (an operator who set something and saw no effect can find out why).
  A negative value is not merely useless: summed into its class's total it would
  silently eat another node's real GPUs.
- **It is a replacement, not a cap.**  A value above allocatable is honoured; the
  controller then plans against GPUs kube-scheduler cannot place, so leases it
  grants can leave their pods Pending.  That is the operator's call, and the
  reason the applied override is logged (`k8s.node_capacity_forced`, DEBUG, per
  node per snapshot — INFO would be per-60 s spam, and the *invalid* case is
  already WARNING).
- **The annotation does not enrol a node.**  The taint is still what puts a node
  in a class, and cordoned/terminating nodes are still excluded, so the override
  only ever sets a number for a node already in the snapshot.  A node tainted for
  several classes contributes the forced count to each — the annotation is per
  node, not per class.

There is deliberately **no config flag**: the annotation is itself the opt-in, an
unannotated cluster behaves exactly as before, and anyone able to annotate a node
can already retaint it.  **RBAC**: none new — annotations arrive with the node
LIST the sweep and queue tick already issue.

The interaction worth knowing is with **App-side vs physical capacity
reconciliation**: forcing a class below the app's `effective_gpus_today` makes it
read over-committed, which pauses JIT admission for it (guard 4) and logs the
hourly mismatch WARNING — usually the point of forcing capacity down.  Forcing it
*up* to silence that audit conceals a real shortage instead of fixing it.

### Inbound push API

`POST /api/reservations/push` lets the reservation app push **one or more
updated reservation entries** (today: cancellations and owner changes) so they
propagate within seconds instead of waiting up to a full
`RESERVATION_FETCH_INTERVAL`.  Bulk synchronisation stays a controller-initiated
**pull** — the push is a partial delta, and the next full fetch remains the
source of truth.

- **Auth**: a single static bearer token in `INBOUND_API_TOKEN` (mount from a
  Secret).  Unset ⇒ endpoint disabled (503); wrong/missing bearer ⇒ 401
  (constant-time compare).  It rides the existing FastAPI app / `HTTP_PORT`,
  so no extra container port or Service is needed.
- **Body**: `{"reservations": [ReservationResponse, …]}` — the same entry shape
  the pull returns (`schemas.ReservationPushRequest`).
- **Semantics**: entries are **upserted by id** (`apply_push_to_active` in
  `controller.py`); an entry whose `status` is not `"active"` drops that id from
  the active set, and an in-window cancellation evicts its admitted pod and
  releases its capacity — after first attempting an adoption re-link onto
  another currently-open booking the same user holds (`POD_ADOPTION_ENABLED`;
  this is what carries a pod forward when the app supersedes its reservation
  via `POST /api/reservations/{id}/continue` and pushes the `superseded`
  source together with its replacement) — the *same* path a mid-window
  cancellation takes on a fetch.  An entry that keeps its id but changes owner (**adoption** — a
  reservation reassigned to a teammate) evicts the prior owner's admitted pod
  from its namespace and releases the capacity, so the new owner can claim the
  still-active window (`detect_owner_changed_in_window` + `_handle_owner_changes`).
- **Shared reconciliation**: both the fetch loop and the push run
  `_reconcile_after_reservation_change` in `main.py` (GPU class map refresh,
  queue reconcile, cancellation eviction, owner-change eviction).
- **Concurrency**: the endpoint and the fetch loop both mutate `reservations`
  across `await` points, so each reconcile is serialised by
  `ControllerState.reservation_lock` (the one place the "no locking" rule is
  relaxed). **The lock covers the state transition, not the I/O that feeds it.**
  Both callers resolve the GPU-class maps first (`_resolve_gpu_class_maps`,
  which touches no shared state) and take the lock only for
  `detect → detect → preserve → replace`, which is synchronous — so a push no
  longer queues behind a fetch cycle's HTTP, which was the endpoint's entire
  reason to exist. Evictions are likewise **planned** under the lock
  (`_plan_cancelled_reservations` / `_plan_owner_changes`, sharing one pod
  snapshot) and **executed** outside it (`_execute_evictions`), so per-pod
  Kubernetes I/O never serialises against a push.

  Three ordering invariants make that split safe, and all three are load-bearing:
  **detect before replace** (the detectors compare against the old set, which is
  what makes eviction idempotent across cycles), **replace before adopt**
  (`find_open_booking_for` must see a replacement booking pushed alongside its
  `superseded` source, or the Continue flow kills the job it was carrying
  forward), and **adopt before evict**. The critical section must also stay one
  uninterrupted acquisition — two would re-open the
  `preserve_local_ondemand_leases` clobber it exists to prevent.

  **Not yet await-free on the eviction path.** Planning still awaits the pod
  snapshot and `_adopt_pods`' per-pod patches. The no-eviction path — the
  overwhelming majority of cycles, since both handlers are conditional — is
  fully synchronous, which is where the latency win comes from. Lifting
  adoption's I/O out is open follow-up work, as is the six remaining lock sites
  that hold across their own cancel HTTP (`_grant_and_admit`,
  `_teardown_ondemand_lease`, `_cancel_pending_noshows`,
  `_drain_pending_merge_cancels`, the queue tick's merge/adopt block, and
  `_run_preemption_sweep`).
- **RBAC**: unchanged — eviction reuses the existing `pods: delete` /
  `events: create` permissions.

### Preemption-risk forecast API

`GET /api/forecast/preemption-risk` (optional `?namespace=`) answers, per
controller-admitted pod, "how likely is this job to be preempted during the
remainder of the current hour and the next two full hours?" — computed
entirely from in-memory state (reservation map, occupancy, pending JIT
candidates) plus the same two cluster snapshots the preemption sweep takes
(pods, node capacity; either failing ⇒ 503 — the sweep's fail-safe rule,
never report risk from unknown physical state).

- **Auth / wiring**: guarded by the same `INBOUND_API_TOKEN` bearer check as
  the push API (`_require_inbound_auth`; unset ⇒ 503, bad bearer ⇒ 401) and
  rides the same FastAPI app / `HTTP_PORT` — no new port, Service, or RBAC.
- **Model** (`ControllerState.forecast_preemption_risk`, pure and
  synchronous, computed under `reservation_lock` with snapshots awaited
  outside it): three calendar-aligned buckets (`[now, top of next hour)` plus
  two full hours).  For every future booking boundary in `(now, horizon_end +
  lead]` — the tail extension mirrors the front, a boundary just past the
  last bucket still lands its phase-A kill inside it — demand reuses
  `boundary_demand` and free capacity `free_capacity_by_class`; an eligible
  pod's single-boundary risk is `min(1, shortfall / eligible-pool GPUs)`,
  attributed to the buckets overlapping the kill window
  `[max(boundary − PREEMPTION_LEAD_MINUTES, guarantee_end), boundary]` and
  combined per bucket as `1 − Π(1 − r)`.  A pod inside its runtime guarantee
  has exactly zero risk (`state: "guaranteed"`); past it, `"overstay"`
  (`"mixed"` when the guarantee ends mid-bucket).
- **Chain-safe eligibility**: guarantees are computed once at the real "now"
  (`guarantee_end`) and compared against each boundary — never by simulating
  `now = boundary`, which would sever back-to-back chains exactly at the
  boundary instants (`_chain_for` keeps members only while `slot_end > now`)
  and fabricate phantom overstay and phantom demand.
- **Documented approximations** (the response carries `selection_delegated`
  so consumers can label the number): with delegation enabled the app picks
  victims by its own policy — the numeric risk models the local
  uniform-random fallback, while pool *membership* (at-risk vs safe) is exact
  either way; boundaries combine as independent chances (conservative — an
  earlier kill actually frees capacity for later boundaries); running pods
  are assumed to keep running (free capacity constant across buckets);
  pending JIT candidates surface per class as `pending_jit_gpus` on the
  current bucket, informational only, never folded into shortfall (see the
  JIT caveat above).
- **`?namespace=`** filters the `pods` list only; bucket/class summaries stay
  cluster-global (every displayed risk's denominator and demand driver is
  global).  Unknown namespace ⇒ empty `pods`, 200.

### Startup behaviour

On startup, the controller performs an initial reservation fetch and then issues
a LIST of all current pods before entering the WATCH stream.  This ensures that
pods created before the controller was running (e.g. during a restart) are not
missed.

The watch itself maintains **resourceVersion continuity** (bookmarks enabled):
a clean stream close — the server honouring the ~4.5 min `timeout_seconds` —
*resumes* from the last seen resourceVersion with no LIST and no replay.  A
full re-LIST (replaying every matching pod as ADDED — safe, replays are
idempotent and the fast path honors retry cooldowns) happens only at start,
after a stream error, on HTTP 410 (resourceVersion expired — immediate, no
backoff), and every ~10 min as a deliberate **resync**, which is the self-heal
for a pod whose ADDED event was never seen (nothing else discovers unrouted
pods).  The watch-to-consumer queue is **bounded** (drop-oldest with a
warning; the next resync heals any gap), so a stalled consumer can no longer
grow memory without limit.

### Singleton lease guard

The controller must run as exactly one instance — two would issue duplicate
toleration patches against the same pods — which the chart states as
`replicas: 1`.  That alone is not enough: the Deployment's default
RollingUpdate strategy surges the new pod to Ready *before* terminating the
old one, so every upgrade transiently ran two controllers.  Two defences,
both added together:

- **`strategy: Recreate`** in the chart (and the README's manual manifest), so
  a rollout stops the old pod first.
- **A `coordination.k8s.io` Lease** (`SINGLETON_LEASE_ENABLED`, default on),
  named `gpu-reservation-controller` in the controller's own namespace,
  claimed in `lifespan` before any work starts and renewed every 20 s by
  `lease_guard_loop` (duration 60 s — two renewals may fail before anyone
  considers it expired).

This is a **duplicate-instance guard, not leader election**: there is no
waiting to take over, no standby.  If another live instance holds the lease,
startup raises, which aborts the FastAPI lifespan and exits non-zero — the
kubelet's crash-backoff then paces retries until the other lease expires.  An
**expired** lease is taken over (the previous holder crashed) and our **own**
lease is simply refreshed, so a container restart of the same pod recovers at
once rather than waiting out the duration.  Losing the lease mid-life
(`singleton.lost`) terminates the process immediately via `os._exit`.

**Fail-open on API errors.**  If `coordination.k8s.io` is unreachable — most
plausibly a 403 on an image-only upgrade whose ClusterRole predates the
`leases` rule — the controller logs a warning and runs unguarded rather than
refusing to start.  Only an affirmative "another live holder" answer stops it;
a blip is not evidence of a duplicate, and a controller that stops admitting
pods over a coordination hiccup is the worse outcome.  **RBAC**: `get`,
`create`, `update` on `coordination.k8s.io/leases` (optional, per the
fail-open behaviour above).

### Strict TLS verification against the API server

`init_k8s` builds the `CoreV1Api` / `CoordinationV1Api` clients, and each owns
its own `RESTClientObject` and therefore its own urllib3 pool manager.  Python
3.13's `ssl.create_default_context()` turns on `VERIFY_X509_STRICT`, and
urllib3 2.x mirrors that flag in the context it builds for every HTTPS pool
(`urllib3/util/ssl_.py`, gated on `sys.version_info >= (3, 13)`).  Under it
OpenSSL requires an **Authority Key Identifier** extension on the certificates
it chains through — which a cluster PKI generated by older tooling may not
carry.  Every API call then fails with `CERTIFICATE_VERIFY_FAILED ... Missing
Authority Key Identifier` even though the service account's `ca.crt` is mounted
and correct; the *same image* on Python 3.12 connected fine.  This is a
`python:3.13-slim` regression against an unchanged cluster, not a credentials
problem, and the mounted CA being present is what rules the obvious reading out.

`K8S_TLS_STRICT_VERIFY=false` (`_relax_strict_tls_verify`) is the escape hatch:
for each API client it builds **urllib3's own default context** for that
client's `cert_reqs`, clears just `VERIFY_X509_STRICT`, and injects it as
`ssl_context` into the pool manager's `connection_pool_kw`.  Three properties
are load-bearing:

- **It is not `verify_ssl: false`.**  The chain is still built and verified
  against the mounted CA, validity dates still apply, and the hostname is still
  matched (`create_urllib3_context` sets `check_hostname` from `cert_reqs`,
  which is why the existing `cert_reqs` is read back rather than hardcoded — a
  kubeconfig with `insecure-skip-tls-verify` sets `CERT_NONE`, and a context
  with `check_hostname=True` cannot accept it).  Only the AKID-presence
  requirement is dropped.
- **It runs before any request.**  Pools are built lazily from
  `connection_pool_kw` on first use, so a pool that already existed would keep
  the strict context it was created with.
- **The context is otherwise urllib3's**, not a hand-rolled one, so protocol
  options, ciphers and post-handshake auth stay exactly as they were.

The real fix is regenerating the apiserver certificate with an AKID; the flag
logs `k8s.tls_relaxed` at WARNING on every startup so a deployment running on
the escape hatch says so in its own logs rather than looking normal.

### In-memory state only

No database.  If the controller restarts, it rebuilds all state from the
Kubernetes API and the reservation API within one fetch cycle.  Queue entries
that were waiting for a window that has already opened will be re-evaluated
immediately on the next processor tick.

Occupancy is reconstructed from the `galends/booking-reference` annotation: the
reservation id parsed from each admitted pod's booking-reference is summed into
the unified occupancy map.  The startup pod LIST seeds this, and every
queue-processor tick rebuilds the map wholesale from a live cluster snapshot
(`snapshot_tolerated_pods` → `reconcile_occupancy`), so a missed watch event
self-heals within one tick.  An optimistic placement recorded between ticks whose
patch is not yet visible in the snapshot may be briefly dropped and re-captured
on the next tick — a window bounded by the `QUEUE_PROCESSOR_INTERVAL` tick
(default 300 s).

**No-show declaration is in-memory, but the resulting cancel is durable.**
`noshow_reservation_ids` (and `pending_noshow_cancels`, the queue of ids still
awaiting their cancel) are never written back and don't survive a restart.
After a restart, every mid-window user reservation — including ones the prior
controller lifetime already declared no-show — receives a fresh
`NOSHOW_GRACE_MINUTES` deadline, so a late-arriving holder can reclaim their
window across a restart.  But once a no-show's cancel actually **lands**
(`POST /api/reservations/{id}/cancel`, `reason="no-show"`), that id is
durably gone from the app's active set — the very next fetch simply no longer
reports it, so there is nothing to re-arm and no window for a restart to race.
Related: when a holder's pod is deleted mid-window (e.g. idle-culled), the
reservation's deadline was already cleared, so the next refresh's
`update_noshow_tracking` re-arms it with the grace timeout — this is what
eventually converts a vacated window into a no-show cancel.

**Chained holders are protected from no-show conversion.**  A pod admitted
under `res-X` whose runtime guarantee is chained across back-to-back windows
(`X`, `X+1`, …) physically occupies those later windows even though no pod is
booked directly under them.  Each queue-processor tick scans live reserved-path holder
pods and marks every window they occupy (`reservations_claimed_by`) as
**claimed**; claimed reservations have their no-show deadline cleared and are
skipped by `check_noshow_deadlines` and `update_noshow_tracking`.  This stops a
reservation a holder is still using from being declared a no-show and
cancelled out from under it.  When the holder vacates, the reservation leaves
the claimed set and the grace re-arm path above applies.

---

## Environment variables

| Variable | Default | Purpose |
|----------|---------|---------|
| `RESERVATION_API_URL` | *(required)* | Base URL of the reservation management app |
| `RESERVATION_API_KEY` | *(required)* | `gpures_…` service key; mount from a Kubernetes Secret |
| `RESERVATION_FETCH_INTERVAL` | `300` | Seconds between reservation refresh cycles |
| `RESERVATION_LOOKAHEAD_DAYS` | `7` | Calendar days ahead to fetch reservations |
| `KUBECONFIG` | *(absent = in-cluster)* | Path to kubeconfig file for out-of-cluster use |
| `HTTP_PORT` | `8000` | Bind port for the **whole** HTTP listener — `GET /health` plus `POST /api/reservations/push` and `GET /api/forecast/preemption-risk` |
| `INBOUND_API_TOKEN` | *(absent = inbound API disabled)* | Bearer token guarding the inbound APIs (`POST /api/reservations/push` and `GET /api/forecast/preemption-risk`); mount from a Kubernetes Secret. Unset ⇒ both endpoints return 503 |
| `TZ` | system default | Log timestamp display **and** the default zone for human-readable Event/annotation prose (see **Timezone**); window arithmetic is UTC-based and does not depend on it |
| `EVENT_DISPLAY_TIMEZONE` | *(absent = the process zone, i.e. `TZ`)* | IANA zone the human-readable Event messages and `galends/termination-warning-message` render in, overriding `TZ` — for keeping logs on UTC while events read local. An unknown zone logs `config.invalid` and falls back rather than failing startup. Display only: stored instants, `galends/*` timestamp annotations and log fields stay UTC |
| `ONDEMAND_LEASE_ENABLED` | `true` | Set to `false` to disable the JIT on-demand lease path entirely (a non-JIT-eligible pod still waits for a matching reservation; an ineligible one is left Pending) |
| `ONDEMAND_HORIZON_MINUTES` | `30` | JIT routing horizon: a pod is queued for a reservation that opens within this many minutes (with budget) instead of requesting a lease |
| `ONDEMAND_LEASE_BUFFER_MINUTES` | `10` | Minutes added to a pod's `galends/minimum-runtime-seconds` when sizing a requested JIT lease's duration |
| `BEST_EFFORT_ENABLED` | `false` | Honour a pod's `galends/runtime-guarantee: none` by admitting it under a zero-length, zero-SU `kind="best_effort"` reservation instead of a guaranteed lease (see **Best-effort admission**). Ships dark: the app must serve the best-effort create shape, and against one that does not, every such candidate takes a non-retryable 4xx into `lease.error` backoff. The pod annotation alone is deliberately *not* the opt-in — unlike `galends/force-node-capacity`, any pod author can set it |
| `ONDEMAND_DELEGATE_ADMISSION` | `false` | Ask the app which pending pods to admit on-demand from the eligible batch (`POST /api/reservations/ondemand-admission`) for LAS prioritization; `false` (or any app-call failure) grants every eligible candidate — the prior greedy per-pod behaviour. The app endpoint **is shipped**, but its selection is currently grant-all, so turning this on changes nothing yet; enable it once the app carries real admission policy |
| `ONDEMAND_DENIAL_EVENT_ENABLED` | `true` | Mirror the app's refusal of a JIT lease onto the waiting pod as a `Warning` Event — its **409** denial reason (`reason=OnDemandLeaseDenied`), or a **404** for a user, usage group or GPU class it does not recognise (`reason=OnDemandLeaseRejected`) — so its owner can see why it is still Pending without the controller's logs (see **Surfacing a lease denial to the pod's owner**). Informational only; `false` disables |
| `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` | `30` | How long an **unchanged** status is suppressed before being restated on a pending pod — a lease denial or rejection, an admission pause, or a pod-problem Event, which all share one throttle (the name predates all but the first) — the retry cadence is 2–5 min, and Events expire, so neither "every attempt" nor "once only" is right. A **changed** status, including a switch between any two, emits immediately regardless; `0` emits on every attempt |
| `ONDEMAND_PAUSE_EVENT_ENABLED` | `true` | Put a `Warning` Event (`reason=OnDemandAdmissionPaused`) on every pod held by guard 1b (no schedulable node in the class), guard 3 (stuck reservation holder) or guard 4 (app-side overcommit), saying on-demand admission for its class is paused and to contact support if it persists (see **Telling the pod's owner that admission is paused**). Throttled with the denial Event, on its cadence; `false` disables |
| `SUPPORT_CONTACT` | *(absent)* | How a pod's owner reaches support — an email address or URL — named at the end of the "contact support" suggestion in that Event and the pod-problem Events. Unset = the suggestion names no one |
| `POD_PROBLEM_EVENT_ENABLED` | `true` | Put a `Warning` Event on a pod the controller cannot act on as written: its `gpu-class` label names no class the app knows (`UnknownGpuClass`), no reservation matches it and it does not qualify for on-demand admission (`NoReservation`), or one of its `galends/*` annotations was ignored (`AnnotationIgnored`) (see **Telling the pod's owner the pod itself is the problem**). Throttled with the denial Event, on its cadence; `false` disables |
| `NOSHOW_TIMEOUT_MINUTES` | `15` | Minutes after window opens before a reservation is declared a no-show |
| `NOSHOW_GRACE_MINUTES` | `30` | Grace period after controller startup before mid-window no-shows are declared |
| `QUEUE_PROCESSOR_INTERVAL` | `300` | Seconds between queue-processor ticks — the whole work-queue loop (pod LIST, JIT lease retries, no-show cancels, overstay adoption), not just a pod LIST |
| `POD_SCHEDULING_GATE_NAME` | *(absent)* | Name of the SchedulingGate to remove after admitting a pod; unset = disabled |
| `REQUIRED_GROUP_LABEL` | *(absent)* | Pod label naming the usage group (e.g. `dsmlp/course`); when set, the pod's value must equal the reservation's `group.name` — an extra match axis alongside `gpu-class` (see **Matching pods to reservations**), and a pod without the label is never JIT-eligible either. Unset = disabled |
| `DEFAULT_MINIMUM_RUNTIME_SECONDS` | `0` | Minimum runtime assumed for a pod with no usable `galends/minimum-runtime-seconds` annotation, so it is still JIT-eligible; `0` = disabled (see **Defaults for pods that declare neither**) |
| `DEFAULT_USAGE_GROUP` | *(absent)* | Usage group assumed for a pod that names none — standing in for the `REQUIRED_GROUP_LABEL` label when that feature is on (and therefore for the reserved-path match too), else for the `galends/usage-group` annotation. Unset = disabled |
| `PREEMPTION_LEAD_MINUTES` | `15` | Minutes before a reservation slot boundary that phase-A preemption runs |
| `PREEMPTION_CHECK_INTERVAL` | `60` | Seconds between preemption sweeps |
| `CAPACITY_CHECK_INTERVAL` | `3600` | Seconds between app-side vs physical GPU capacity audits; each audit logs per-class differences as WARNING and pauses on-demand admission for classes the app over-counts (the pause itself is also re-checked every queue-processor tick) (see **App-side vs physical capacity reconciliation**) |
| `HEADROOM_TARGET_PERCENT` | `0` | Percentage of each GPU class's physical capacity to hold free for on-demand jobs that have not arrived yet, reclaimed from pods past their runtime guarantee (see **Anticipatory headroom preemption**). `0` disables the feature; a pod inside its guarantee is never a headroom victim |
| `HEADROOM_NOTICE_MINUTES` | `15` | Notice a headroom victim gets before it becomes killable — it is stamped with a `galends/termination-warning-at` deadline first and only becomes eligible once that deadline elapses. `0` = no notice. Requires `TERMINATION_WARNING_ENABLED`; with warnings off the gate is bypassed |
| `HEADROOM_CHECK_INTERVAL` | `600` | Seconds between headroom evaluations. Headroom rides the preemption sweep but is throttled to this slower cadence so an idle cluster is not LISTed on `PREEMPTION_CHECK_INTERVAL`. Kill latency is therefore `HEADROOM_NOTICE_MINUTES` to `HEADROOM_NOTICE_MINUTES + this` after a pod is warned |
| `PREEMPTION_DELEGATE_SELECTION` | `true` | Ask the app to choose preemption victims from the eligible pool (`POST /api/reservations/preemption-victims`); `false` (or any app-call failure) falls back to local uniform-random selection |
| `POD_ADOPTION_ENABLED` | `true` | Re-link an overstay pod to a reservation its user has since booked (see **Adopting overstay pods into a re-booked reservation**); `false` disables |
| `ONDEMAND_MERGE_ENABLED` | `true` | Merge a JIT on-demand lease's pod into the user's matching booking the moment that booking's window opens — re-link the pod and retire the lease penalty-exempt, without waiting for the lease guarantee to lapse (see **Merging a JIT lease into a matching booking**); `false` disables (the pod then converges lazily via adoption once past its lease guarantee) |
| `TERMINATION_WARNING_ENABLED` | `true` | After each preemption sweep, stamp pods still at risk of preemption at an upcoming boundary with informational `galends/termination-warning-*` annotations — projected termination time, risk score, and a message (see **Termination-warning annotations**); `false` disables |
| `TERMINATION_WARNING_LEAD_MINUTES` | `30` | How far ahead (minutes) the termination-warning look-ahead scans, decoupled from `PREEMPTION_LEAD_MINUTES` so a pod killed proactively at `boundary − lead` (a phase-A victim) is warned before its boundary enters the kill window; larger = more advance notice but more speculative warnings |
| `OVERSTAY_REPORT_ENABLED` | `false` | When on, report each ended overstay's duration to the app for offline analysis (`POST /api/reservations/{id}/overstay`) — see **Overstay reporting**. Best-effort and analysis-only; ships dark (default off) |
| `SINGLETON_LEASE_ENABLED` | `true` | Hold a `coordination.k8s.io` Lease so a second controller instance refuses to run (see **Singleton lease guard**); `false` disables |
| `K8S_TLS_STRICT_VERIFY` | `true` | OpenSSL strict X.509 verification on the Kubernetes API connection. `false` clears `VERIFY_X509_STRICT`, which Python 3.13 enables by default, for clusters whose certificates lack an Authority Key Identifier (see **Strict TLS verification against the API server**). Not `verify_ssl: false` — chain, validity and hostname are still checked |
| `POD_NAME` | *(hostname)* | This pod's name (downward API) — the Lease holder identity; falls back to `HOSTNAME`, then the system hostname |
| `POD_NAMESPACE` | *(SA namespace)* | Namespace the Lease lives in (downward API); falls back to the service-account namespace file, then `default` |
| `LOG_LEVEL` | `INFO` | Root Python logging level (parsed by `config.py`). Unknown level ⇒ `config.invalid`, default used |
| `LIBRARY_LOG_LEVEL` | `WARNING` | Level for the HTTP/Kubernetes client libraries (`httpx`, `httpcore`, `urllib3`, `kubernetes`), whose verbose output is raw API traces, so `LOG_LEVEL=DEBUG` shows the controller's own DEBUG events without them. The effective level is the stricter of this and `LOG_LEVEL`; set both to `DEBUG` for the raw traces. Unknown level ⇒ `config.invalid`, default used |

---

## Configuration

Runtime configuration is through environment variables only — no config files,
no database, no secrets embedded in the image.

**Parsing is tolerant, in both vocabularies.**  `_env_bool` takes a recognised
truthy/falsy word and falls back to the default on anything else; `_env_int` does
the same for numbers, rejecting junk *and* out-of-range values rather than
raising or accepting them.  A rejected value logs `config.invalid` at WARNING
naming the variable, so an operator who set something and saw no effect can find
out why.  Both mirror the reservation app's `config_utils` (`env_bool` /
`_env_positive_float`) — keep the two repos in step.

Numeric **floors are per-setting, not uniform**: an interval of `0` is a busy
loop against the Kubernetes API and is rejected, while a lead/grace/horizon of
`0` legitimately means "disabled" and is honoured.  `HTTP_PORT` additionally
caps at 65535.  `tests/test_config_env.py` asserts the whole matrix and fails the
build if a new numeric setting is added with a bare `int()`.

---

## Deployment

The Dockerfile builds a minimal image:

- Base: `python:3.13-slim`
- **Dependencies installed from compiled lockfiles** (`requirements.lock` /
  `requirements-dev.lock`), never the lower-bound-only `requirements*.txt`.  The
  `final` stage reinstalls on a clean base rather than copying site-packages from
  `deps` — that is what keeps dev-only packages out of the runtime image, but it
  also means the two stages resolve independently, so without a lock the suite
  could pass against one dependency set while the image shipped another.  The dev
  lock is compiled with the prod lock as a constraint, making it a superset at
  identical versions; `tests/test_dependency_lock.py` asserts both properties and
  fails the build on drift.  Regeneration commands are in `AGENTS.md`.
- Non-root user `appuser` (UID 1000)
- Health check: `GET http://localhost:8000/health`
- Entrypoint: `python -m app.main` (starts uvicorn programmatically so `HTTP_PORT` controls the bind port)

A Helm chart at `helm/gpu-reservation-controller/` renders the
ServiceAccount, ClusterRole/Binding, Deployment, and `/health` Service; keep
its `values.yaml`/`deployment.yaml` env wiring in sync when adding settings
to `config.py`.  See README.md for RBAC requirements and a sample manual
Deployment manifest.

**The chart's default image reference must stay registry-qualified and must
name a tag the workflow actually publishes.**  An unqualified repository
resolves to `docker.io/library/…`, and `docker/metadata-action`'s
`flavor.latest=auto` emits `latest` only for a semver git-tag push — which this
repository never makes — so the workflow requests it explicitly via
`type=raw,value=latest`.  The floating default tag is paired with
`pullPolicy: Always`, without which a node holding an old `latest` layer never
re-pulls and `helm upgrade` rolls nothing; pin an immutable tag and switch back
to `IfNotPresent` for production.  The previous defaults were wrong on every one
of those points, and nothing read the chart to notice.

Two layers now do, and they are complementary rather than redundant:

- **`tests/test_chart_image.py`** asserts the four properties above by *parsing*
  the chart.  It cannot render — the suite also runs inside the Dockerfile's
  `test` stage, which has no helm — but the defects it covers render perfectly
  well while being wrong, so parsing is the right tool for them.
- **The `chart` job in `.github/workflows/docker.yml`** installs the latest
  stable helm and renders for real: `helm lint`, a default render asserting the
  full object set, a render with the optional blocks (`imagePullSecrets`,
  `INBOUND_API_TOKEN`, `REQUIRED_GROUP_LABEL`) populated — they are empty by
  default, so the default render proves nothing about them — and a negative case
  asserting the `reservationApiUrl` `required` guard still fires.  This is what
  catches template syntax, indentation and undefined values.  `build-and-push-image`
  needs it: an image whose chart cannot render is not deployable.
