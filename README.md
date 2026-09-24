# GPU Reservation Controller

A Kubernetes controller daemon that enforces time-bound GPU reservations by
patching pods with Kubernetes tolerations.  

Pods that belong to an active reservation — and fit within its GPU budget — 
are permitted to schedule on the additional nodes during the reservation window.

---

## How it works

A configurable fraction of our GPU nodes will carry a taint:

```
gpu-class-reservation=<gpu-class-label>:NoSchedule
```

This blocks all ordinary pods from scheduling there.  (Note that this is 
distinct from and in addition to "gpu-class=&lt;gpu-class-label&gt;:NoSchedule" 
which ensures that only jobs intended for that GPU type end up there.)

The controller's job is to add the matching **toleration** to pods
that have a valid, active reservation, subject to the GPU budget 
for that reservation.  The budget is the reservation's `gpu_count` — how
many GPUs it holds — and every pod admitted under it counts its
`nvidia.com/gpu` request against that, so several pods can share one
multi-GPU reservation.

### Control loop

```
┌──────────────────────────────────────────────────────┐
│ 1. Reservation fetch (every RESERVATION_FETCH_INTERVAL s)
│    GET /api/reservations?status=all                   │
│        &date_start=today&date_end=today+LOOKAHEAD     │
│        (paginated, 200/page)                          │
│    GET /api/gpu-classes  → refresh label_value maps   │
└──────────────────────────────────────────────────────┘
           │ updates in-memory reservation list
           ▼
┌──────────────────────────────────────────────────────┐
│ 2. Pod watch  (LIST at startup + periodic resync,    │
│                then WATCH resumed by resourceVersion)│
│    Pods with label gpu-class=<X> are routed:         │
│      a. A reservation is open now, or opens within   │
│         ONDEMAND_HORIZON_MINUTES, with budget         │
│           → enter the reserved work queue             │
│      b. Else, if JIT-eligible (min-runtime annotation,│
│         group label if required)                     │
│           → become an on-demand candidate; attempt a  │
│             lease request immediately                 │
│      c. Else, if some future reservation matches      │
│         (beyond horizon / no budget)                  │
│           → enter the reserved work queue anyway       │
│      d. Else → left Pending, and the pod's owner told │
│         why (NoReservation / UnknownGpuClass Event)   │
│                                                       │
│    Fast path: if a new pod (ADDED) arrives while its │
│    window is already open, the toleration is applied │
│    immediately — no wait for the queue processor.    │
└──────────────────────────────────────────────────────┘
           │ task queue / on-demand candidates (in memory)
           ▼
┌──────────────────────────────────────────────────────┐
│ 3. Queue processor  (every 300 s)                    │
│    Reserved path — pods queued before their window   │
│    opened, and retries for pods over-budget:          │
│      a. Count nvidia.com/gpu already in use by other │
│         sibling pods that hold the toleration for    │
│         the same booking (matched via the            │
│         galends/booking-reference annotation)          │
│      b. If pod_gpus + sibling_gpus ≤ reserved_gpus:  │
│           PATCH pod → add toleration + annotations   │
│           PATCH pod → annotate runtime guarantee      │
│           Create RuntimeGuaranteed Event on pod      │
│      c. Otherwise: retry in 2–5 min                  │
│                                                      │
│    No-show → cancel: for each reservation declared   │
│      no-show, re-verify no pod raced in, then         │
│      POST /api/reservations/{id}/cancel (no-show)     │
│                                                      │
│    JIT on-demand path (ONDEMAND_LEASE_ENABLED):      │
│      d. Safety interlock (guard 3): if a reservation │
│         holder is stuck Pending for a GPU class,     │
│         hold lease requests for that class            │
│      e. Vet every due candidate: guard 1 (Pending    │
│         for a reason a lease can fix, the class has  │
│         nodes, and the pod's node selector allows    │
│         one), resolve gpu-class → gpu_class_id.      │
│         If ONDEMAND_DELEGATE_ADMISSION, offer the     │
│         whole batch to the app (LAS prioritization):  │
│           POST /api/reservations/ondemand-admission   │
│         → app returns which to admit (else grant all).│
│         For each granted:                             │
│           POST /api/reservations (on_demand=true,     │
│             idempotency_key=pod UID)                  │
│           Granted → admit under it (same as 3a/3b)    │
│           Admission failed → compensating cancel      │
│             (reason=controller-revoked)                │
│           Denied → retry in 2–5 min                    │
└──────────────────────────────────────────────────────┘
           │ (independent of the task queue)
           ▼
┌──────────────────────────────────────────────────────┐
│ 4. Preemption sweep  (every PREEMPTION_CHECK_INTERVAL)│
│    No activeDeadlineSeconds is ever set — a pod may   │
│    run past its guarantee until capacity is needed:   │
│      a. For each upcoming reservation start ("boundary│
│         ") within PREEMPTION_LEAD_MINUTES of now:     │
│           demand = incoming bookings' unclaimed GPUs  │
│           free   = node capacity − live pod usage     │
│      b. If demand > free, delete past-guarantee pods  │
│         of that GPU class until covered (the app      │
│         picks which; random if it cannot be asked):   │
│           Create Preempted Event, then delete pod     │
│      c. Phase A runs before the boundary (proactive); │
│         phase B runs at the boundary itself and also  │
│         makes pods whose own window just ended eligible│
└──────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────┐
│ 5. Capacity audit   (every CAPACITY_CHECK_INTERVAL)  │
│    Compare app-side effective_gpus_today against the │
│    GPUs physically present in the cluster:           │
│      a. Log every per-class difference as a WARNING  │
│      b. If app-side > physical for a class, hold new │
│         on-demand admissions for THAT class that do  │
│         not fit in its physical GPUs (all of them if │
│         ONDEMAND_OVERCOMMIT_FIT=false) until the     │
│         deficiency is resolved                       │
└──────────────────────────────────────────────────────┘
```

### Just-in-time (JIT) on-demand leases

The reservation app schedules only real bookings — there is no ad-hoc
capacity-hold type.  A pod with no reservation open now or opening soon does
not wait indefinitely and is not placed onto ad-hoc spare capacity: the
controller requests a **real reservation** on its behalf, just-in-time.
On-demand jobs are therefore ordinary reservations — charged SU and protected
by the same runtime guarantee as any booking.  (One asymmetry: the preemption
sweep plans only for `kind="booking"` boundaries, so a granted lease does not
itself displace overstayers — a JIT pod landing on squatted GPUs waits rather
than preempting them.)

**Routing** (re-evaluated on every attempt, in `pod_watch_loop`):

1. A reservation matching the pod's namespace/GPU-class (and usage group, if
   `REQUIRED_GROUP_LABEL` is set) that is open now or opens within
   `ONDEMAND_HORIZON_MINUTES` **and** has spare budget → the pod is queued for
   it, same as any reserved-path pod.
2. Otherwise, if the pod is **JIT-eligible** — `Pending`, carries
   `galends/minimum-runtime-seconds`, and names its usage group (the group label
   when `REQUIRED_GROUP_LABEL` is set, else the `galends/usage-group`
   annotation; the lease request's `group_name` is a required natural key
   app-side) — it becomes an on-demand candidate and, when
   first discovered, kicks an immediate admission batch covering it plus every
   other waiting candidate.  Later retries ride the queue-processor tick.
3. Otherwise, if some future reservation matches at all (beyond the horizon,
   or currently over budget), the pod is queued for it anyway — the plain
   wait-for-window behaviour, preserved for a pod that isn't JIT-eligible.
4. Otherwise the pod is left **Pending** (a pod missing its usage-group source
   or the minimum-runtime annotation is not guessed at), and its owner is told
   why with a `Warning` Event (`reason=NoReservation`, or `UnknownGpuClass` when
   its `gpu-class` label names no class the app knows) — see **Telling the pod's
   owner the pod itself is the problem** below.

Steps 1–2 read a pod's minimum runtime and usage group through
`DEFAULT_MINIMUM_RUNTIME_SECONDS` / `DEFAULT_USAGE_GROUP`, which stand in when
the pod declares neither.  Both ship disabled, so by default step 4 still
catches an undeclared pod; configure them for a cluster where every unannotated
pod should fall into a house default instead.  `DEFAULT_USAGE_GROUP` substitutes
for the `REQUIRED_GROUP_LABEL` label *wholesale* — the pod also matches that
group's reservations in step 1 — so it is a statement about which group
unlabelled workloads belong to, not merely a JIT lease detail.

**Batch admission** — each attempt gathers all due candidates, vets each
(re-routing, the guards below, and resolving its `gpu-class` label to a numeric
`gpu_class_id`), and — when `ONDEMAND_DELEGATE_ADMISSION` is enabled — offers the
whole eligible set to the app in one call
(`POST /api/reservations/ondemand-admission`), which returns the subset to admit
this round.  This is where **LAS (least-attained-service) prioritization** will
live.  When the flag is off, or the app call fails, the controller grants every
eligible candidate (its prior greedy behaviour), so the feature ships safely
dark.  For each granted pod it then calls `POST /api/reservations` with
`duration_seconds = minimum-runtime + ONDEMAND_LEASE_BUFFER_MINUTES * 60` and
`on_demand=true` (the app relaxes policy limits — SU, caps, minimum duration
— never physical calendar capacity).  The request is **idempotent by the
pod's Kubernetes UID**: a retry after a prior grant returns the same
reservation rather than creating a duplicate.

- **Denied** (409/error): the candidate cools down 2–5 min and retries.  A
  **409** carries the app's reason, such as `Only 1 GPU(s) available for this
  group at 2026-08-21 14:00 (group ceiling: 4)`, and its verdict on whether
  waiting can help: a **structural** denial (`retryable: false` — over the
  class's per-reservation GPU cap, not a member, the group's term over) backs off
  to at most one retry per 30 min instead, and one that names when it clears
  (`not_before`) waits for that.  The reason is also mirrored onto the pod as a `Warning`
  Kubernetes Event (`reason=OnDemandLeaseDenied`), so the pod's **owner** can see
  why their job is still Pending with `kubectl describe pod` rather than needing
  the controller's logs.  An unchanged reason is restated at most once per
  `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` (a changed one is reported at once), and
  only the app's own answers are surfaced this way — a network failure or a
  controller misconfiguration (a read-only service key, a schema mismatch) is an
  operator's problem and stays in the log.
  `ONDEMAND_DENIAL_EVENT_ENABLED=false` disables it.  See
  [`docs/POD-ANNOTATIONS.md` §5.1](docs/POD-ANNOTATIONS.md).
- **Rejected** (404): the app does not recognise the user, usage group or GPU
  class the request named — and every one of those came off the pod (its
  namespace, its group label or `galends/usage-group` annotation, its
  `gpu-class` label), so unlike the other non-retryable faults this one *is*
  the owner's to fix.  It keeps the exponential backoff of any non-retryable
  fault, and is also told to the pod as a `Warning` Event
  (`reason=OnDemandLeaseRejected`) carrying the app's reason, the user and
  usage group that were sent, where the group came from (label, annotation or
  `DEFAULT_USAGE_GROUP`), and any booking the user holds under another group.
  Rides `ONDEMAND_DENIAL_EVENT_ENABLED`.
- **Granted**: the pod is admitted under the new reservation immediately,
  through the same admission path as any reserved-path pod (stamps
  `res-<id>`, records the guarantee, emits `RuntimeGuaranteed`).  If
  admission does **not** succeed — a transient patch error, or the pod
  vanishing in the interim — the controller issues a compensating cancel
  (`POST /api/reservations/{id}/cancel`, `reason=controller-revoked`) so the
  grant is never left dangling.

**Scheduling verdict (guard 1)** — before requesting a lease the controller
reads the pod's `PodScheduled` condition and drops any candidate the scheduler
rules out for a reason a lease cannot fix: an `Insufficient <non-GPU-resource>`,
a failure where *no* node was rejected on a taint at all (so adding a toleration
changes nothing), or one where every taint the message names belongs to somebody
else.  A pod with no verdict yet is held briefly and re-attempted the moment the
scheduler records one.

What it deliberately does **not** require is the string `Insufficient
nvidia.com/gpu`.  GPU nodes carry `gpu-class-reservation=<class>:NoSchedule`, a
candidate does not tolerate that yet, and kube-scheduler's `TaintToleration`
filter runs ahead of `NodeResourcesFit` — so the pod's own class's nodes are
rejected on the taint and never report a GPU verdict at all.  Requiring it held
every candidate at "indeterminate" indefinitely on exactly the clusters this
controller is built for.  The corresponding *physical* check is that the class
has at least one schedulable, Ready node carrying its taint (same per-node
inventory guard 5 uses); a fully drained class is held, not dropped, since nodes come
back, and a class with no data yet does not block (fail-open).  "Fully drained"
is judged against the classes the reservation app knows: the node inventory
simply omits a class with no schedulable node, so each known class is recorded
explicitly, as zero when none of its nodes are schedulable.  A label the app
does not know, or no successful snapshot yet, still counts as no data.

The residual gap is inherent to the same filter ordering: a candidate blocked by
a *second* constraint on its own class's nodes — cpu/memory there, an unrelated
taint, an unbindable volume — cannot be seen, because those nodes never get past
the taint filter to report it.  Such a pod is granted a lease and stays Pending
under it; guard 3, the lease's short expiry and the pod-teardown cancel are the
backstops.

The one second constraint the controller *does* check itself is the pod's own
`nodeSelector` and required node affinity — a host name, a hardware label —
against the labels of the class's nodes, taken from the node LIST it already
does for guard 5.  A pod allowing none of the class's nodes is held and told
(`NoMatchingNode`); one whose allowed nodes are all short of its GPUs is held
until one has room, at any GPU count, and told (`WaitingForNode`).  Either way
no lease is requested for a pod that could not start under it.  A constraint
that allows every node of the class narrows nothing and changes nothing, and
one the controller cannot evaluate (or cannot yet, before the first inventory)
never holds a pod.

**Safety interlock (guard 3)** — if any reservation-holder pod for a given
GPU class is stuck in Pending (admitted but the scheduler cannot place it),
lease requests are suspended for that class until the stuck pod is resolved.
Other GPU classes are unaffected.  A holder whose own node placement explains
it — it allows none of the class's nodes, or only full ones while another node
has room — is left out: it says nothing about the class, and counting it would
let one pod pinned to a busy host stall on-demand admission for everyone.

**Capacity overcommit (guard 4)** — when the reservation app counts more GPUs
for a class than the cluster physically has (a node down, say; see
`CAPACITY_CHECK_INTERVAL`), the app would sell leases against GPUs that do not
exist.  By default (`ONDEMAND_OVERCOMMIT_FIT=true`) the controller then grants a
lease only if it fits in the physical GPUs for its whole duration, alongside
every reservation already booked in that time — so a lightly loaded class keeps
starting on-demand jobs through an outage, while a job that would collide with a
booking starting mid-lease waits.  `ONDEMAND_OVERCOMMIT_FIT=false` instead pauses
every on-demand admission for the class until the counts agree.  Best-effort
pods are exempt either way.

**Telling the pod's owner about a paused class** — a drained class (guard 1b),
a stuck reservation holder (guard 3) and the capacity audit's overcommit
(guard 4) hold on-demand pods of a GPU class until something outside the
pod changes, and the app is never asked, so there is no denial to relay.  Each
pod they hold gets a `Warning` Kubernetes Event (`reason=OnDemandAdmissionPaused`)
saying that on-demand admission for its class is paused (for a guard-4 hold
under `ONDEMAND_OVERCOMMIT_FIT`, *limited*, adding that a smaller or shorter job
may start sooner), why in plain terms,
that nothing about the pod needs to change, and to contact support if it
persists — naming `SUPPORT_CONTACT` when that is set.
It runs on the denial Event's throttle and cadence: a changed status is reported
at once, an unchanged one at most once per `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES`,
and because the two Events share that throttle, the newest one on a pod always
describes what is holding it now.  It never names the stuck holders (other
users' pods) or the capacity counts; an operator has those in the
`ondemand.gated` log line.  A fragmented class (guard 5) is not reported this
way — that is the cluster being full, not a fault.
`ONDEMAND_PAUSE_EVENT_ENABLED=false` disables it.  See
[`docs/POD-ANNOTATIONS.md` §5.2](docs/POD-ANNOTATIONS.md).

**Telling the pod's owner the pod itself is the problem** — four more
`Warning` Events cover a pod the controller cannot act on *as written*, which
used to reach only the controller's log (or not even that):

- `UnknownGpuClass` — the pod's `gpu-class` label names no class the
  reservation app knows, so nothing can admit it; the Event lists the classes
  that do exist, since a typo is the usual cause.
- `NoReservation` — no reservation matches the pod and it does not qualify for
  on-demand admission either (on-demand admission is off, or the pod has no
  usable minimum runtime, or names no usage group), so no path will ever pick it
  up.  The Event gives every reason, and names any booking the user holds that
  the pod narrowly misses — the right class under another usage group, or
  another class.
- `AnnotationIgnored` — a `galends/*` annotation was invalid or asks for
  something this cluster does not offer (`galends/runtime-guarantee` with
  best-effort admission off) and was ignored, where that changed what happens:
  the cluster's default runtime is used instead, or the pod is charged a
  guaranteed lease instead of running best-effort.  (A pod that waits for its
  booking instead of being admitted now is told inside the Event below.)
- `NoMatchingNode` — the pod's `nodeSelector` or required node affinity allows
  none of its class's schedulable nodes, so no lease is requested; the Event
  quotes the constraint and names any other class whose nodes it does match.
  Its `Normal` sibling `WaitingForNode` says the nodes it allows are all short
  of its GPUs right now, and that widening the constraint would let it use the
  rest of the class.

The first two are said only once the controller holds the data that makes them
true — a full reservation fetch, and the app's complete class list — never while
the reservation app is unreachable at startup.  All of these Events and the two
above share one throttle, which now keeps what was told **per pod** (not per
on-demand candidate), so a pod keeps one story whichever path holds it; an
ignored annotation runs on a separate track so it never alternates with the
hold.  `POD_PROBLEM_EVENT_ENABLED=false` disables these four and
`WaitingForNode`.  See
[`docs/POD-ANNOTATIONS.md` §5.3](docs/POD-ANNOTATIONS.md).

**Telling the pod's owner what its reservation is waiting on** — a pod queued
for one of its owner's reservations gets an Event saying why it is not running
yet, on the same throttle, rather than only kube-scheduler's "untolerated taint"
(or a `NoReservation` from before they booked):

- `WaitingForReservation` (`Normal`) — the reservation has not opened; the
  Event gives its id and window.
- `ReservationFull` (`Warning`) — it is open, but the owner's other pods hold its
  GPUs; the Event names them, since a forgotten notebook server is the usual
  cause.
- `ReservationTooSmall` (`Warning`) — it holds fewer GPUs than the pod requests,
  so it can never admit it.

When the pod waits only because it cannot be admitted on demand (a booking far
off, full or too small), the Event says why not — including an ignored
annotation.  The queue also now keeps each pod on a reservation that can take
it: the booking routing chose (not a sooner, full one), one big enough over a
sooner smaller one, and, every queue-processor tick, any of the owner's bookings
open now with room.  When a holder is deleted or finishes, the pods waiting on
its reservation are retried at once rather than on the next tick.
`RESERVATION_WAIT_EVENT_ENABLED=false` disables the Events.  See
[`docs/POD-ANNOTATIONS.md` §5.4](docs/POD-ANNOTATIONS.md).

**Per-node feasibility (guard 5)** — GPUs are node-scoped: a pod requesting N
`nvidia.com/gpu` only schedules if a *single* node has N free (Kubernetes never
splits a job across nodes).  Before requesting a lease for a **multi-GPU (≥2)**
pod, the controller checks the largest single-node free block for its class
(computed each queue-processor tick from the same per-node inventory + pod
snapshot guard 1 reads — `nodes: list`, no new RBAC).  If no single node can host it, the request is held
and retried, rather than minting an SU-charged lease that could never schedule
onto fragmented capacity.  A class with no per-node data yet does not block
(fail-open); 1-GPU pods are unaffected.  (Node-aware *preemption* — freeing a
whole node for a reserved multi-GPU booking — is planned follow-up work; the
preemption sweep is still per-class.)

**No-show → cancel** — if a reservation holder fails to launch a pod within
`NOSHOW_TIMEOUT_MINUTES` of the window opening, the controller durably
cancels the reservation (`POST /api/reservations/{id}/cancel`,
`reason="no-show"`) so the app can re-book the window immediately.  A
booking that is vacated mid-window — its last pod finished, was deleted or
was idle-culled — is re-armed with `NOSHOW_GRACE_MINUTES` from the next
reservation refresh, and cancelled the same way if nothing starts under it
by then (the same grace covers windows already open when the controller
starts).  The
cancel is re-verified against a fresh pod snapshot first (a pod that raced in
at the last second is never cancelled out from under it) and retried next
tick if it fails.  A reservation a live holder is still occupying — directly
or via a chained runtime guarantee — is **claimed** and is never declared a
no-show.  The declaration itself is in-memory and does not survive a
restart, but once the cancel actually lands, the reservation is durably gone
from the app's active set, so there is nothing to re-arm.

**Occupancy tracking across restarts** — capacity for every admitted pod is
tracked in one occupancy map keyed by reservation id.  The map is rebuilt
from the cluster — the reservation id parsed from each pod's
`galends/booking-reference` — by the startup pod LIST and, on every
queue-processor tick, from a live snapshot, so a missed event or a restart
self-heals within one tick.

### Toleration applied

```yaml
key:      gpu-class-reservation
operator: Equal
value:    <pod's gpu-class label value>   # e.g. "h100"
effect:   NoSchedule
```

### Annotations stamped on managed pods

| Annotation | Written when | Purpose |
|------------|--------------|---------|
| `galends/booking-reference` | toleration applied | Identifies the reservation the pod was admitted under (`res-<id>` — the only prefix, since every admitted pod is tied to a real reservation, JIT or otherwise); the id is the key for the per-reservation GPU budget and for rebuilding occupancy from the cluster |
| `galends/pod-runtime-limit-seconds` | guarantee recorded, rewritten on re-link | The runtime guarantee's duration in seconds when it was recorded (`0` for a best-effort pod), for operator visibility and in-pod notification widgets; not refreshed as the guarantee grows. See *Runtime guarantees and demand-driven preemption* |
| `galends/guaranteed-until` | guarantee recorded, kept live | The same guarantee as an absolute UTC ISO-8601 instant; refreshed while the pod is in guarantee (it can move later when an abutting window is booked), frozen at its now-past value once the pod overstays |
| `galends/guarantee-status` | guarantee recorded, kept live | `guaranteed` while the pod is inside its runtime guarantee, `overstay` once it is running past it; see *Live guarantee-status annotations* |
| `galends/reservation-kind` | guarantee recorded, re-stamped on re-link | `booking` (a window the user reserved), `on_demand` (a just-in-time lease the controller minted on the pod's behalf) or `best_effort` (a zero-length, zero-SU stub for a pod that asked for no runtime guarantee — `BEST_EFFORT_ENABLED`) |
| `galends/reservation-start` / `-end` | guarantee recorded, re-stamped on re-link | The reservation's **own** window as absolute UTC ISO-8601 instants — distinct from `guaranteed-until`, which is the end of the back-to-back guarantee *chain* |
| `galends/reservation-gpu-count` | guarantee recorded, re-stamped on re-link | GPUs the reservation reserves; against the pod's own request this shows how much of a booking the pod is using |
| `galends/gpu-class-name` | guarantee recorded, re-stamped on re-link | The GPU class's human display name (e.g. `H100`), as opposed to the `gpu-class` label value used for matching |
| `galends/admitted-at` | first admission only | When the controller admitted this pod, absolute UTC ISO-8601. Deliberately **not** rewritten on a re-link — a re-link is not a new admission |
| `galends/termination-warning-at` | at risk of preemption | Projected kill instant `max(boundary − lead, guarantee_end)` (the start of the sweep's kill window at the soonest boundary the pod is an eligible victim at, absolute UTC ISO-8601); cleared when the pod is no longer at risk. See *Termination-warning annotations* |
| `galends/termination-warning-risk` | at risk of preemption | Preemption risk in (0, 1] at that boundary (`min(1, shortfall/pool_gpus)`, 2 decimals) |
| `galends/termination-warning-message` | at risk of preemption | Human-readable warning text. Renders its instants in **local** time (see `EVENT_DISPLAY_TIMEZONE`; UTC when neither it nor `TZ` is set) — it is prose for a person, unlike the UTC `-at` above that a widget parses |

(`galends/minimum-runtime-seconds` and `galends/usage-group` — the usage-group
name a JIT lease is created under when `REQUIRED_GROUP_LABEL` is not in use —
are the two annotations **consumed** rather
than written — see *Just-in-time (JIT) on-demand leases* above, and
`DEFAULT_MINIMUM_RUNTIME_SECONDS` / `DEFAULT_USAGE_GROUP` for the deployment-wide
stand-ins when a pod carries neither.  Every
annotation the controller writes is **informational only**: nothing in the
controller reads them back to make a decision, and a guarantee can technically
shrink after being written — e.g. a window shortened server-side — so treat
them as best-effort, not authoritative.)

The guarantee and reservation-fact annotations are written in a **single patch**
(`annotate_runtime_guarantee`), so the descriptive set costs no extra API call.
Because the facts describe the reservation the pod is *currently* linked to,
every path that re-links a pod — adoption, and the JIT-lease-to-booking merge —
re-stamps them; `galends/admitted-at` is the one exception, written only on the
pod's first admission.

`docs/POD-ANNOTATIONS.md` is the consumer-facing reference for these: value
formats, lifecycle, propagation latency, and how to project them into a
container with a downward-API volume — written for in-pod widgets (JupyterLab,
VS Code, MOTD) that surface guarantee and preemption status to the user.  Its
§6 goes further, for the workload rather than the widget: how a PyTorch training
job should turn a termination warning into a checkpoint, per fine-tuning and
from-scratch scenario.

### Runtime guarantees and demand-driven preemption

When a pod is admitted, the controller records how long its GPU access is
**guaranteed** — but does **not** enforce that with `spec.activeDeadlineSeconds`.
A pod may run past its guarantee freely; the controller reclaims capacity
from an overstaying pod only when a booking starting on its GPU class
actually needs it — or, if `HEADROOM_TARGET_PERCENT` is set, to keep that
share of each class free for on-demand jobs.

**Guarantee calculation** — the guaranteed instant is:

- The **end** of the pod's current reservation window, plus
- The **full duration** of any directly back-to-back future bookings with
  the same owner, GPU class, and GPU count — and usage group, when
  `REQUIRED_GROUP_LABEL` is set — with no gap between consecutive windows.

This is an absolute instant **recomputed live** on every check rather than
frozen at admission — so a pod's guarantee can *grow* after admission (an
abutting follow-on booking), something a Kubernetes deadline cannot do.

**Recording the guarantee** — after applying the toleration, the controller
annotates the pod (see table above) and creates a Kubernetes **Event** with
reason `RuntimeGuaranteed` explaining when the guarantee ends and that the
pod may later be preempted — the same Event is emitted again, with the new
guarantee, whenever the pod is later re-linked to another reservation.  Event
messages state their times in local time (`EVENT_DISPLAY_TIMEZONE`, defaulting
to `TZ`), since a user reads them through `kubectl describe pod`; with neither
set — the chart's default — they read in UTC.  The annotations alongside stay
UTC either way.  Recording is best-effort: if the PATCH or Event
creation fails, a warning is logged but the toleration that was already
applied is not revoked.

**Recovering capacity** happens in a separate, periodic sweep
(`PREEMPTION_CHECK_INTERVAL`, default 60s) — see step 4 of the control-loop
diagram above. In short: for each upcoming reservation start ("boundary")
within `PREEMPTION_LEAD_MINUTES` (default 15) of now, the controller computes
demand (incoming bookings' unclaimed GPUs) against free physical capacity
(from a node LIST — see [step 5](#5--overriding-a-nodes-gpu-capacity-optional)
to override what a node contributes); if demand exceeds free capacity, it deletes
past-guarantee pods of the same GPU class until the shortfall is covered. A
pod still within its guarantee is never selected, however severe the
shortfall — that's logged as an unmet-demand warning instead. **Which**
eligible pods are deleted is delegated to the reservation app by default
(`PREEMPTION_DELEGATE_SELECTION`, `POST /api/reservations/preemption-victims`),
whose current policy sacrifices best-effort pods first, then on-demand leases,
then bookings, at random within each; with delegation off, or when the app
call fails, the controller picks uniformly at random among them. Only booking
starts create boundaries — a granted on-demand lease never triggers
preemption on its own behalf. Each boundary is evaluated at most once per phase; a
pod snapshot or node-capacity-snapshot failure skips the sweep entirely
rather than risk a kill based on unknown physical state. Preempted pods get a
Kubernetes Event with reason `Preempted` before deletion.

**Termination-warning annotations** (`TERMINATION_WARNING_ENABLED`, default on).
After the sweep executes its kills, it stamps the **survivors still at risk** of
preemption at an upcoming boundary with informational `galends/termination-warning-*`
annotations (see the annotations table above): the projected kill instant
`max(boundary − lead, guarantee_end)` — the start of the kill window, so a pod
killed proactively `lead` minutes before its boundary reports that earlier time
— a risk score in (0, 1], and a human-readable message. The warning look-ahead is
**decoupled from the kill lead** via `TERMINATION_WARNING_LEAD_MINUTES` (default
30, wider than `PREEMPTION_LEAD_MINUTES`): the boundary set unions the sweep's own
kill window with a wider forward horizon, so a **phase-A** victim (an overstayer
killed proactively at `boundary − lead`) is warned *before* its boundary enters
the kill window instead of on the same tick it is killed. Eligibility is evaluated
*as of the boundary*, so a pod whose guarantee expires between now and the boundary
is flagged. The full eligible pool of a short class is warned (the app's victim
choice is opaque, so any pool member could be picked), the set is computed after
the kills so a pod being preempted is never also warned, and a warning is
**cleared** once the pod is no longer at risk (its user re-booked, demand
evaporated, or it was adopted). Purely informational — nothing is enforced
Kubernetes-side; a widget can surface it so a job checkpoints or re-books in time.

**Adopting overstay pods into a re-booked reservation**
(`POD_ADOPTION_ENABLED`, default on). Because pods overrun, a user may book a
*fresh* reservation (a new, distinct id) while their pod from the previous
window is still running. If the new window abuts the old one (same owner,
GPU class, and GPU count), the guarantee chaining above already extends the
pod's guarantee onto it — no action needed. For the cases chaining cannot
reach (a non-abutting follow-on window, or a different GPU count), and only
once the pod is **past its runtime guarantee**, the controller instead
*re-links* the pod: it re-annotates the pod's `galends/booking-reference` to
the new reservation and moves the pod's occupancy accordingly, so it is
credited against the new reservation. This runs before each preemption sweep
plans any kills (so a just-re-booked pod is never a victim) and once per
queue-processor tick. Re-linked pods get a Kubernetes Event with reason
`OverstayRelinked` — or `ReservationRelinked` when the pod was never
overstaying (a JIT lease merged into a booking as it opened, or a reservation
replaced mid-window by Extend).

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Kubernetes cluster | 1.24+ recommended |
| GPU nodes tainted | `kubectl taint node <node> gpu-class-reservation=<label>:NoSchedule` |
| Reservation management API | Running instance; service key pre-created |
| Python 3.11+ | For local / out-of-cluster use |

---

## Installation

### Docker (recommended)

```bash
docker build -t ghcr.io/agt/gpu-reservation-controller:latest .
```

Or pull the published image, which `.github/workflows/docker.yml` pushes to
`ghcr.io/<github-owner>/<repo-name>` — `ghcr.io/agt/gpu-reservation-controller`
for this repository, tagged `latest` on the default branch:

```bash
docker pull ghcr.io/agt/gpu-reservation-controller:latest
```

Keep the registry prefix. An unqualified `gpu-reservation-controller:latest`
resolves to `docker.io/library/gpu-reservation-controller`, which is a
different image that does not exist.

### Local (development / out-of-cluster)

```bash
uv venv .venv
source .venv/bin/activate
uv pip install -r requirements.txt
```

---

## Configuration

All settings are supplied via environment variables.

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `RESERVATION_API_URL` | **yes** | — | Base URL of the reservation management app, e.g. `https://gpures.example.edu` |
| `RESERVATION_API_KEY` | **yes** | — | Service key (`gpures_…`) — mount from a Kubernetes Secret |
| `RESERVATION_FETCH_INTERVAL` | no | `300` | Seconds between reservation refresh cycles |
| `RESERVATION_LOOKAHEAD_DAYS` | no | `7` | How many calendar days ahead to fetch reservations |
| `KUBECONFIG` | no | *(absent)* | Path to a kubeconfig file; if unset, in-cluster service-account credentials are used |
| `HTTP_PORT` | no | `8000` | Bind port for the whole HTTP listener — `GET /health` liveness plus the inbound push (`POST /api/reservations/push`) and preemption-risk forecast (`GET /api/forecast/preemption-risk`) APIs |
| `TZ` | no | system default | Log timestamp display **and** the default zone for the human-readable Event/annotation prose (see `EVENT_DISPLAY_TIMEZONE`); reservation window arithmetic is UTC-based and does not depend on it |
| `EVENT_DISPLAY_TIMEZONE` | no | *(absent = the process zone, i.e. `TZ`)* | IANA zone (e.g. `America/Los_Angeles`) the human-readable Kubernetes Event messages and `galends/termination-warning-message` render in. Set it to keep logs on UTC while events read local; an unknown zone logs a warning and falls back rather than failing startup. **Display only** — stored instants, every `galends/*` timestamp annotation and every log field stay UTC |
| `ONDEMAND_LEASE_ENABLED` | no | `true` | Set to `false` to disable the JIT on-demand lease path entirely |
| `ONDEMAND_HORIZON_MINUTES` | no | `30` | JIT routing horizon: a pod is queued for a reservation opening within this many minutes (with budget) instead of requesting a lease |
| `ONDEMAND_LEASE_BUFFER_MINUTES` | no | `10` | Minutes added to a pod's `galends/minimum-runtime-seconds` when sizing a requested JIT lease's duration |
| `ONDEMAND_DENIAL_EVENT_ENABLED` | no | `true` | Mirror the reservation app's refusal of a JIT lease onto the waiting pod as a `Warning` Kubernetes Event, so its owner can see why it is still Pending without access to the controller's logs: a **409** denial (`reason=OnDemandLeaseDenied`), or a **404** for a user, usage group or GPU class the app does not recognise (`reason=OnDemandLeaseRejected`) — every one of which came off the pod. Informational only — the controller retries either way (rarely, for a denial the app marks `retryable: false`, which the Event says waiting will not fix). A network failure or any other non-409 fault (a read-only service key, a schema mismatch) stays in the log. Set to `false` to disable |
| `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` | no | `30` | How long an **unchanged** status is suppressed before being restated on a pending pod — a lease denial or rejection, an admission pause (`ONDEMAND_PAUSE_EVENT_ENABLED`), a pod-problem Event (`POD_PROBLEM_EVENT_ENABLED`) or a reservation-wait Event (`RESERVATION_WAIT_EVENT_ENABLED`); the name predates all but the first. The retry cadence is 2–5 min, so without this a blocked pod would accumulate a new Event every few minutes; the repeat still fires because Events expire, and a pod that is still stuck should still say so. A status that **changes** — including a switch between any two of them — is emitted immediately regardless. `0` emits on every attempt |
| `ONDEMAND_PAUSE_EVENT_ENABLED` | no | `true` | Put a `Warning` Kubernetes Event (`reason=OnDemandAdmissionPaused`) on every pod held by a class-level on-demand gate — no schedulable node in the class (guard 1b), a stuck reservation holder (guard 3) or an app-side capacity overcommit (guard 4) — saying why it is still Pending and suggesting its owner contact support if it persists. Throttled with the denial Event above, on the same cadence. Informational only. Set to `false` to disable |
| `SUPPORT_CONTACT` | no | *(absent)* | How a pod's owner reaches support — an email address or URL — named at the end of that suggestion (`If this persists, contact support: <value>`), and of the pod-problem Events below. Keep it short: it is part of every such Event. Unset, the suggestion names no one |
| `POD_PROBLEM_EVENT_ENABLED` | no | `true` | Put a `Warning` Kubernetes Event on a pod the controller cannot act on as written: its `gpu-class` label names no class the reservation app knows (`reason=UnknownGpuClass`), no reservation matches it and it does not qualify for on-demand admission (`reason=NoReservation`), one of its `galends/*` annotations was ignored (`reason=AnnotationIgnored`), or its node selector / required node affinity allows none of its class's nodes (`reason=NoMatchingNode`); plus the `Normal` `reason=WaitingForNode` while the nodes it allows are all full. Throttled with the denial Event above, on the same cadence. Informational only. Set to `false` to disable |
| `RESERVATION_WAIT_EVENT_ENABLED` | no | `true` | Put a Kubernetes Event on a pod queued for one of its owner's reservations, saying what it waits on: the window has not opened (`reason=WaitingForReservation`, a `Normal` Event), the owner's other pods hold its GPUs, which are named (`reason=ReservationFull`), or it holds fewer GPUs than the pod requests (`reason=ReservationTooSmall`). Throttled with the denial Event above, on the same cadence. Informational only. Set to `false` to disable |
| `BEST_EFFORT_ENABLED` | no | `false` | Honour a pod's `galends/runtime-guarantee: none` annotation by admitting it under a zero-length, zero-SU `kind="best_effort"` reservation rather than a guaranteed lease — no runtime guarantee, no Service Units, preemptible from the first second. Requires an app build serving the best-effort create shape; `false` ignores the annotation entirely |
| `ONDEMAND_DELEGATE_ADMISSION` | no | `false` | Delegate on-demand admission selection to the app for LAS prioritization (`POST /api/reservations/ondemand-admission`); `false` (or any app-call failure) grants every eligible candidate. The app endpoint is shipped but selects grant-all today, so enabling this changes no behaviour until the app carries real admission policy |
| `NOSHOW_TIMEOUT_MINUTES` | no | `15` | Minutes after a reservation window opens before declaring a no-show and cancelling it app-side |
| `NOSHOW_GRACE_MINUTES` | no | `30` | Grace period (minutes) before a booking whose window is already open is declared a no-show: one already in progress when the controller starts, or one vacated mid-window after its last pod ends (finished, deleted or idle-culled) |
| `QUEUE_PROCESSOR_INTERVAL` | no | `300` | Seconds between queue-processor ticks — the whole work-queue loop (pod LIST, JIT lease retries, no-show cancels, overstay adoption), not just a pod LIST |
| `POD_SCHEDULING_GATE_NAME` | no | *(absent)* | Name of a SchedulingGate to remove from a pod after admitting it; unset disables scheduling-gate removal |
| `INBOUND_API_TOKEN` | no | *(absent)* | Bearer token for the inbound APIs (`POST /api/reservations/push` and `GET /api/forecast/preemption-risk`); mount from a Kubernetes Secret. Unset leaves both endpoints **disabled** (returns 503) |
| `PREEMPTION_LEAD_MINUTES` | no | `15` | Minutes before a reservation slot boundary that phase-A preemption runs, proactively freeing capacity from overstaying pods |
| `PREEMPTION_CHECK_INTERVAL` | no | `60` | Seconds between preemption sweeps |
| `PREEMPTION_DELEGATE_SELECTION` | no | `true` | Delegate preemption victim selection to the app (`POST /api/reservations/preemption-victims`) so prioritisation policy lives there; `false` (or any app-call failure) falls back to local uniform-random selection |
| `CAPACITY_CHECK_INTERVAL` | no | `3600` | Seconds between app-side vs physical GPU capacity audits (default hourly). Each audit compares the reservation app's per-class `effective_gpus_today` (its `total_gpus` after any date-span capacity override covering today — the count the app actually admits against) with the GPUs physically present in the cluster (per-node allocatable, or a node's `galends/force-node-capacity` override), logs any difference as a **WARNING**, and gates new on-demand admissions for any class the app over-counts until the deficiency clears (see `ONDEMAND_OVERCOMMIT_FIT`) |
| `ONDEMAND_OVERCOMMIT_FIT` | no | `true` | How a class the app over-counts is gated (guard 4): grant an on-demand lease only if it fits in the physically present GPUs for its whole duration, alongside every reservation already booked in that time, so a node outage on a lightly loaded class does not stop on-demand jobs. Set to `false` to pause every on-demand admission for the class instead, until the counts agree |
| `POD_ADOPTION_ENABLED` | no | `true` | Re-link an overstay pod to a reservation its user has since booked. Set to `false` to disable |
| `ONDEMAND_MERGE_ENABLED` | no | `true` | Merge a JIT on-demand lease's pod into the user's matching booking the moment that booking's window opens — re-link the pod and retire the lease penalty-exempt (`reason="superseded"`), without waiting for the lease guarantee to lapse. Set to `false` to disable (the pod converges lazily via adoption once past its lease guarantee) |
| `TERMINATION_WARNING_ENABLED` | no | `true` | After each preemption sweep, stamp pods still at risk of preemption at an upcoming boundary with informational `galends/termination-warning-*` annotations (projected kill instant, risk score, message). Purely informational — nothing is enforced. Set to `false` to disable |
| `TERMINATION_WARNING_LEAD_MINUTES` | no | `30` | How far ahead (minutes) the termination-warning look-ahead scans, decoupled from `PREEMPTION_LEAD_MINUTES` so a pod killed proactively at `boundary − lead` (a phase-A victim) is warned before its boundary enters the kill window; larger = more advance notice but more speculative warnings |
| `HEADROOM_TARGET_PERCENT` | no | `0` | Percentage of each GPU class's **physical** capacity to hold free for on-demand jobs that have not arrived yet, reclaimed from pods running past their runtime guarantee. Anticipatory, unlike boundary preemption, which only frees GPUs a booking already needs. `0` disables it; a pod inside its runtime guarantee is **never** a headroom victim |
| `HEADROOM_NOTICE_MINUTES` | no | `15` | Notice a headroom victim gets before it becomes killable: it is stamped with a `galends/termination-warning-at` deadline first and only becomes eligible once that deadline elapses, so a job can checkpoint, extend, or re-book. `0` means no notice. Requires `TERMINATION_WARNING_ENABLED` — with warnings off the gate is bypassed |
| `HEADROOM_CHECK_INTERVAL` | no | `600` | Seconds between headroom evaluations. Headroom rides the preemption sweep but is throttled to this slower cadence, so an otherwise-idle cluster is not LISTed on `PREEMPTION_CHECK_INTERVAL` just to re-check headroom. Kill latency is therefore between `HEADROOM_NOTICE_MINUTES` and `HEADROOM_NOTICE_MINUTES` + this interval after a pod is warned |
| `REQUIRED_GROUP_LABEL` | no | *(absent)* | Pod label naming the usage group a pod belongs to (e.g. `dsmlp/course`). When set, the pod's value for this label must equal the reservation's group name — an additional match constraint alongside `gpu-class` — before the controller admits it, adopts it, or chain-extends its guarantee; a pod without the label is also never JIT-eligible. Unset disables the group constraint |
| `DEFAULT_MINIMUM_RUNTIME_SECONDS` | no | `0` | Minimum runtime to assume for a pod that carries no usable `galends/minimum-runtime-seconds` annotation, so it can still be JIT-eligible. `0` disables the fallback (such a pod is left Pending, the historical behaviour) |
| `DEFAULT_USAGE_GROUP` | no | *(absent)* | Usage group to assume for a pod that names none. With `REQUIRED_GROUP_LABEL` set it stands in for the missing **label** — the pod matches that group's reservations on the reserved path exactly as if it carried the label, and a JIT lease for it is created under that group. With the label feature off it stands in for a missing `galends/usage-group` **annotation** (the JIT lease's group only). Unset disables the fallback |
| `SINGLETON_LEASE_ENABLED` | no | `true` | Hold a `coordination.k8s.io` Lease so a **second** controller instance refuses to run (two would issue duplicate toleration patches). A duplicate-instance guard, not leader election: there is no waiting to take over. Startup aborts (non-zero exit, kubelet backs off and retries) if another live instance holds the lease; if the coordination API is unreachable — e.g. an upgrade whose ClusterRole predates the leases rule — the controller logs a warning and runs unguarded. Set to `false` to disable |
| `K8S_TLS_STRICT_VERIFY` | no | `true` | OpenSSL strict X.509 verification on the connection to the Kubernetes API server. Python 3.13 enables these checks by default, and they require an Authority Key Identifier extension on the certificates the chain runs through — a cluster PKI built by older tooling may not carry one, and every API call then fails with `CERTIFICATE_VERIFY_FAILED ... Missing Authority Key Identifier` despite a correctly mounted service-account `ca.crt`. Regenerating the apiserver certificate with an AKID is the real fix; `false` is the escape hatch until then, and logs `k8s.tls_relaxed` at WARNING on every startup. **Not** `insecure-skip-tls-verify`: the chain is still verified against the mounted CA, validity dates still apply, and the hostname is still matched |
| `POD_NAME` | no | *(hostname)* | This pod's name, from the downward API; used as the singleton Lease holder identity. Falls back to `HOSTNAME`, then the system hostname |
| `POD_NAMESPACE` | no | *(service-account namespace)* | Namespace the singleton Lease is created in, from the downward API. Falls back to the in-cluster service-account namespace, then `default` |
| `LOG_LEVEL` | no | `INFO` | Python logging level for the controller. An unknown level logs `config.invalid` and falls back |
| `LIBRARY_LOG_LEVEL` | no | `WARNING` | Logging level for the HTTP and Kubernetes client libraries (`httpx`, `httpcore`, `urllib3`, `kubernetes`), whose verbose output is raw API request/response traces — so `LOG_LEVEL=DEBUG` shows the controller's own DEBUG events without them. Can only quieten the libraries below `LOG_LEVEL`, never raise them above it; set both to `DEBUG` to see the raw traces. An unknown level logs `config.invalid` and falls back |

> **Security note:** The controller needs a **`read_write`**-scoped service
> key: besides the read endpoints (`/api/reservations`, `/api/gpu-classes`),
> it calls `POST /api/reservations` (JIT lease requests) and
> `POST /api/reservations/{id}/cancel` (no-show / controller-revoked cancels).
> Always inject the key from a Kubernetes Secret rather than baking it into an
> image or ConfigMap.

---

## Running locally (out-of-cluster)

```bash
export RESERVATION_API_URL=https://gpures.example.edu
export RESERVATION_API_KEY=gpures_<your-key>
export KUBECONFIG=~/.kube/config
export TZ=America/Los_Angeles

uvicorn app.main:app --host 0.0.0.0 --port 8000
```

Verify the controller is alive:

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

---

## Inbound push API

The controller normally learns about reservation changes by **polling** the
reservation app every `RESERVATION_FETCH_INTERVAL` seconds. To propagate changes
faster (e.g. a cancellation or an owner reassignment), the reservation app
can **push** one or more updated reservation entries:

```
POST /api/reservations/push
Authorization: Bearer <INBOUND_API_TOKEN>
Content-Type: application/json

{ "reservations": [ <ReservationResponse>, … ] }
```

- Each entry uses the same shape the app already returns from
  `GET /api/reservations`. Entries are **upserted by id**; an entry whose
  `status` is not `"active"` (e.g. a cancellation) drops that reservation from
  the active set, and an in-window cancellation additionally **evicts the
  admitted pod** and frees its capacity — the same behaviour as a cancellation
  seen on a normal poll.
- This is a **partial delta**; bulk synchronisation remains a controller-initiated
  pull, and the next full poll is always the source of truth.
- The endpoint shares the `HTTP_PORT` listener (no extra port/Service), and
  responds `200` (with `{"applied", "cancelled", "total_active"}`), `401`
  (missing/invalid bearer), or `503` (endpoint disabled because
  `INBOUND_API_TOKEN` is unset).

```bash
curl -X POST http://localhost:8000/api/reservations/push \
  -H "Authorization: Bearer $INBOUND_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"reservations": [ … ]}'
```

To enable it under Helm, point `inboundApiTokenSecret.name` at a Secret holding
the token (see below). No additional RBAC is required.

---

## Preemption-risk forecast API

Answers, per controller-admitted pod, "how likely is this job to be preempted
during the remainder of the current hour and the next two full hours?" —
intended for the reservation app to poll and render to users.

```
GET /api/forecast/preemption-risk[?namespace=<username>]
Authorization: Bearer <INBOUND_API_TOKEN>
```

The forecast projects the same arithmetic the preemption sweep runs — booking
demand vs free GPUs at every upcoming reservation boundary — over three
calendar-aligned hourly buckets:

- A pod **within its runtime guarantee** has exactly **zero risk**
  (`state: "guaranteed"`); back-to-back chains are honoured.
- An **overstaying** pod's risk per boundary is
  `min(1, shortfall / eligible-pool GPUs)` — 0 whenever free capacity covers
  the demand, 1.0 when every eligible overstayer must be reclaimed — placed in
  the buckets its kill window (`boundary − PREEMPTION_LEAD_MINUTES …
  boundary`) touches and combined per bucket.

Trimmed example response:

```json
{
  "generated_at": "2026-07-15T14:20:00Z",
  "lead_minutes": 15,
  "selection_delegated": true,
  "buckets": [
    {"start": "…14:20Z", "end": "…15:00Z",
     "classes": {"h100": {"capacity": 8, "free": 1, "demand": 4,
                           "shortfall": 3, "eligible_pool_gpus": 5,
                           "pending_jit_gpus": 0}}},
    …
  ],
  "pods": [
    {"namespace": "alice", "name": "notebook-1", "uid": "…",
     "gpu_class": "h100", "gpu_count": 1, "reservation_id": 42,
     "guarantee_end": "2026-07-15T15:00:00Z",
     "buckets": [{"risk": 0.0, "state": "guaranteed"},
                  {"risk": 0.6, "state": "overstay"},
                  {"risk": 0.6, "state": "overstay"}]}
  ]
}
```

- Statuses: `200`; `401` (missing/invalid bearer); `503` either because
  `INBOUND_API_TOKEN` is unset **or** because a cluster snapshot (pods, node
  capacity) failed — the forecast never reports risk from unknown physical
  state, mirroring the sweep's fail-safe rule.
- `?namespace=` filters the `pods` list only (unknown namespace ⇒ empty list);
  the per-class bucket summaries stay cluster-global, since the risk
  denominators and demand drivers are global.
- Caveats: with victim selection delegated to the reservation app
  (`selection_delegated: true`), the numeric risk models the controller's
  uniform-random fallback — which pods are *at risk at all* is exact, the
  probability is an approximation.  Free capacity assumes running pods keep
  running, and multiple boundaries in one bucket combine as independent
  chances — both conservative.  `pending_jit_gpus` is informational pressure
  from pods awaiting a JIT lease; it is never folded into `shortfall`.

It shares the `HTTP_PORT` listener and the push API's bearer token — no
extra port, Service, or RBAC.

---

## Kubernetes deployment

### Helm (recommended)

A chart at `helm/gpu-reservation-controller/` renders the ServiceAccount,
ClusterRole/Binding, Deployment, and a Service for `/health`. The API-key
Secret must exist beforehand (step 1 below); everything in steps 2–3 is
covered by the chart:

```bash
helm install gpu-reservation-controller ./helm/gpu-reservation-controller \
  --namespace gpu-system --create-namespace \
  --set reservationApiUrl=https://gpures.example.edu \
  --set config.timezone=America/Los_Angeles
```

See `helm/gpu-reservation-controller/values.yaml` for all options (fetch
interval, lookahead, on-demand toggle, no-show timing, resources). The
manual manifests below remain valid for non-Helm deployments.

To enable the inbound push API, create a Secret holding the bearer token and
point the chart at it (leave it unset to keep the endpoint disabled):

```bash
kubectl create secret generic gpu-reservation-push-token \
  --namespace gpu-system --from-literal=token='<random-token>'
helm upgrade gpu-reservation-controller ./helm/gpu-reservation-controller \
  --reuse-values --set inboundApiTokenSecret.name=gpu-reservation-push-token
```

### 1 — Create the API-key Secret

```bash
kubectl create secret generic gpu-reservation-api-key \
  --namespace gpu-system \
  --from-literal=api-key='gpures_<your-key>'
```

### 2 — RBAC

The controller needs read access to pods across all namespaces, write access
(PATCH) to pods in namespaces where reservations are active, `delete` on pods
so it can evict pods whose reservation is cancelled mid-window or whose
runtime guarantee has elapsed and capacity is needed, `get`/`list` on
nodes so the preemption sweep can compute physical GPU capacity per class,
and `get`/`create`/`update` on `coordination.k8s.io` leases for the
singleton guard that stops a second instance from running.  The lease rule is
the only optional one: without it the controller logs a warning and runs
unguarded rather than failing to start.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: gpu-reservation-controller
rules:
  - apiGroups: [""]
    resources: ["pods"]
    verbs: ["get", "list", "watch", "patch", "delete"]
  - apiGroups: [""]
    resources: ["events"]
    verbs: ["create"]
  - apiGroups: [""]
    resources: ["nodes"]
    verbs: ["get", "list"]
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: gpu-reservation-controller
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: gpu-reservation-controller
subjects:
  - kind: ServiceAccount
    name: gpu-reservation-controller
    namespace: gpu-system
```

### 3 — Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: gpu-reservation-controller
  namespace: gpu-system
spec:
  # The controller holds in-memory queue state; running more than one replica
  # would result in duplicate toleration patches.  Keep this at 1, and use
  # Recreate so a rollout does not transiently run two (the default
  # RollingUpdate surge starts the new pod before stopping the old one).
  replicas: 1
  strategy:
    type: Recreate
  selector:
    matchLabels:
      app: gpu-reservation-controller
  template:
    metadata:
      labels:
        app: gpu-reservation-controller
    spec:
      serviceAccountName: gpu-reservation-controller
      containers:
        - name: controller
          # Registry-qualified on purpose: an unqualified name resolves to
          # docker.io/library/. Add imagePullSecrets below if the GHCR package
          # is private (it is, by default).
          image: ghcr.io/agt/gpu-reservation-controller:latest
          imagePullPolicy: Always  # `latest` is floating; see the Helm chart's values.yaml
          ports:
            - containerPort: 8000
          env:
            - name: RESERVATION_API_URL
              value: "https://gpures.example.edu"
            - name: RESERVATION_API_KEY
              valueFrom:
                secretKeyRef:
                  name: gpu-reservation-api-key
                  key: api-key
            - name: TZ
              value: "America/Los_Angeles"
            # Identity for the singleton Lease (SINGLETON_LEASE_ENABLED).
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: POD_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace
          livenessProbe:
            httpGet:
              path: /health
              port: 8000
            initialDelaySeconds: 30
            periodSeconds: 30
          resources:
            requests:
              cpu: 50m
              memory: 64Mi
            limits:
              cpu: 200m
              memory: 256Mi
```

### 4 — Taint GPU nodes

For each GPU node and GPU class participating in the reservation system:

```bash
kubectl taint node <gpu-node> \
  gpu-class-reservation=<label-value>:NoSchedule
```

`<label-value>` must match the `label_value` field in the reservation API's
GPU class record (e.g. `h100`, `a100-80gb`).

### 5 — Overriding a node's GPU capacity (optional)

The controller's only notion of how many GPUs physically exist is
`status.allocatable["nvidia.com/gpu"]`, summed per class over the tainted nodes
that are schedulable and Ready — a cordoned, terminating or **NotReady** node is
left out, so a GPU node that crashes stops counting as soon as Kubernetes marks
it NotReady, without anyone cordoning it (it logs `k8s.node_excluded` at DEBUG).
Pods still bound to a node that is left out count against nothing, and are never
chosen to free capacity, since deleting them frees none the scheduler can use.
A **node** annotation overrides the allocatable reading for one node:

```bash
# This node contributes 2 GPUs to its class, whatever allocatable says
kubectl annotate node <gpu-node> galends/force-node-capacity=2

# Mask the node's GPUs entirely without cordoning it
kubectl annotate node <gpu-node> galends/force-node-capacity=0 --overwrite

# Back to the allocatable count
kubectl annotate node <gpu-node> galends/force-node-capacity-
```

It exists for the cases where allocatable is not the number the reservation
system should account against:

- Some of a node's GPUs are failing or held back for non-reservation work, and
  the device plugin still advertises them all.
- A node's advertised count is wrong or unparseable.
- You want to drain a node *from the reservation system* while leaving the pods
  already on it — and every non-GPU pod — running.  Cordoning removes the node
  from the snapshot entirely and stops everything; `0` only stops the
  controller counting its GPUs.

**Semantics.** The value is a whole number ≥ 0 and replaces the allocatable
count for that node in every capacity calculation the controller makes: per-class
totals, free capacity, headroom, the app-vs-physical audit, and the per-node
feasibility guards all read the same snapshot.  A node tainted for more than one
class contributes the forced number to each of them — the annotation is per node,
not per class.  A negative or unparseable value is ignored, with a
`k8s.node_capacity_forced_invalid` **WARNING** naming the node; the node keeps
its allocatable count.  The annotation does **not** enrol a node: an untainted,
cordoned, terminating or NotReady node is still excluded.

**It is a replacement, not a cap** — a value above allocatable is honoured, which
is the operator's call and worth being deliberate about.  The controller will
then plan against GPUs the Kubernetes scheduler cannot place, so on-demand leases
it grants can leave their pods Pending.

**Interaction with the capacity audit.** Forcing a class *below* the reservation
app's `effective_gpus_today` makes that class read as over-committed, which is
what gates new JIT on-demand admissions for it (guard 4) and logs the hourly
`capacity_audit.mismatch` WARNING — usually the point of forcing capacity down.
Forcing it *up* to match the app conceals a real shortage instead of fixing it;
adjust the app-side class capacity for that.

No extra RBAC is needed — annotations arrive with the node LIST the controller
already performs.  Changes take effect on the next snapshot: within
`PREEMPTION_CHECK_INTERVAL` (60 s) for preemption and headroom, one
`QUEUE_PROCESSOR_INTERVAL` (300 s) for the admission guards, and one
`CAPACITY_CHECK_INTERVAL` (3600 s) for the audit.

---

## Pod requirements

Pods that should be managed by the controller must:

1. Run in a namespace whose name matches the **reservation owner's username**.
2. Carry the label `gpu-class=<label-value>` matching the reserved GPU class.
3. Request `nvidia.com/gpu` resources in their container spec.

Example pod fragment:

```yaml
metadata:
  namespace: jsmith          # must equal reservation username
  labels:
    gpu-class: h100          # must equal gpu_class.label_value
spec:
  containers:
    - name: trainer
      resources:
        requests:
          nvidia.com/gpu: "2"
        limits:
          nvidia.com/gpu: "2"
```

---

## Service key management

Service keys for the controller are managed via the reservation management app.
See `RESERVATION-API.md` §3 for the key lifecycle API, or use the CLI helper on
the reservation server:

```bash
python manage_service_keys.py create --name k8s-controller-prod
python manage_service_keys.py list
python manage_service_keys.py revoke --name k8s-controller-prod
```

---

## Project layout

```
gpu-reservation-controller/
├── app/
│   ├── __init__.py
│   ├── main.py               FastAPI app + four background asyncio tasks
│   ├── config.py             Config dataclass from environment variables
│   ├── schemas.py            Pydantic models for reservation API responses
│   ├── reservation_client.py httpx async client for the reservation API + JIT lease create/cancel
│   ├── k8s_client.py         Kubernetes client wrapper (watch, patch, occupancy snapshot)
│   └── controller.py         Shared state, queue, matching, window arithmetic
├── tests/                    pytest suite (controller logic, guards, no-show, JIT leases)
├── helm/gpu-reservation-controller/  Helm chart (Deployment, RBAC, Service)
├── docs/
│   ├── RESERVATION-API.md    Reservation management API specification
│   │                         (identical copy of docs/contracts/RESERVATION-API.md
│   │                         in gpu-reservation-app — update both together)
│   ├── SCHEDULING.md         Reservation-app scheduling behaviour reference (shared copy)
│   ├── POD-ANNOTATIONS.md    Pod-annotation reference for in-pod consumers
│   │                         (Jupyter/VS Code widgets) reading them via downwardAPI
│   └── lifecycle.mmd/.png    Pod lifecycle state diagram (Mermaid + rendered)
├── requirements.txt
├── requirements-dev.txt
├── pytest.ini
├── Dockerfile
├── OBSERVABILITY.md          Catalogue of every structured log point
├── CLAUDE.md                 Architecture reference for AI coding assistants
├── AGENTS.md                 Development standards for AI coding agents
└── README.md                 This file
```

## Improvement review

An August 2026 review covering both this repo and `gpu-reservation-app` —
centralization opportunities, code smells, and maintainability/ops changes — is
recorded in the app repo at `docs/REPO-REVIEW-2026-08.md`. Controller-specific
findings not yet addressed include the shared relink body behind `_adopt_pods` /
`_merge_ondemand_into_bookings`, the pod-annotation vocabulary spread across
three modules, and `reservation_lock` being held across outbound I/O.
