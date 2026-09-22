# Observability reference

Every log point the controller emits, grouped by the loop or flow it belongs to.

Line format and field meanings are defined in **`docs/LOG-FIELDS.md`** — the
canonical dictionary, shared verbatim with the reservation app. This file is the
inventory: *which* lines exist, at what level, and what each one tells you. Field
names below are the ones that appear on the line; look them up in the dictionary
for types and semantics.

`LOG_LEVEL` (default `INFO`) sets the threshold. Everything marked DEBUG below is
suppressed unless you set `LOG_LEVEL=DEBUG`. The HTTP and Kubernetes client
libraries (`httpx`, `httpcore`, `urllib3`, `kubernetes`) are held separately at
`LIBRARY_LOG_LEVEL` (default `WARNING`) — their verbose output is raw API
request/response traces, not controller events — and can only be quieter than
`LOG_LEVEL`, never louder; set both to `DEBUG` to see those traces. All timestamps and all window
arithmetic are UTC; `TZ` affects only how the timestamp column is rendered.

---

## Reading a line

```
2026-07-28 14:03:11,204 INFO     app.main: actor=controller trace=pod-1a2b3c4d5e event=pod.admitted ns=jdoe pod=notebook-0 rid=42 clabel=h100 gpus=2 free=6 reserved=8 until=2026-07-28T16:00:00+00:00
```

- **`actor=controller`** is constant — this daemon has one principal. The field
  exists so a controller line and an app line parse under the same grammar.
- **`trace=`** is the unit of work — see below.
- **`ns=` / `pod=`** identify the pod; **`poduid=`** is its Kubernetes UID.
- **`rid=`** is the reservation. It is the same id space the app calls `id=` on
  its own `reservation.*` events.

## Trace ids

Every line carries a `trace=` naming the unit of work that produced it. The
prefix says what kind:

| prefix | unit of work |
|---|---|
| `fetch-` | one reservation refresh cycle |
| `queue-` | one queue-processor tick (incl. the merge/adopt/lease work it fans out) |
| `sweep-` | one preemption sweep |
| `audit-` | one capacity audit |
| `gate-` | one on-demand gate warning pass (§5) |
| `jit-` | one JIT admission pass (a coalesced re-run gets its own id) |
| `pod-` | one pod watch event |
| `startup-` | the synchronous startup sequence |
| `push-` / `forecast-` | one inbound API request that supplied no id of its own |
| `-` | no unit of work in scope |

**The id crosses to the app.** It is sent as `X-Client-Trace` on every outbound
call, and the app's request middleware already read that header — so the app
logs its side of a controller-initiated operation under the *same* id. Inbound
is symmetric: a push from the app carrying a trace is logged here under the
app's id, so a user's cancel and the eviction it causes share one trace.

Because a watch stream interleaves, the trace is what separates two concurrent
pods' handling in the log; timestamps cannot.

Cross-repo joins:

```bash
# Everything one operation did, on both sides — including lines sharing no object.
grep 'trace=jit-1a2b3c4d5e' controller.log app.log

# One pod's whole life.
grep 'poduid=8f3a…' controller.log app.log

# A reservation from booking through admission to preemption.
grep -E 'event=reservation\.created id=42|rid=42' app.log controller.log
```

`trace` joins an **operation**; `rid` and `poduid` join an **object**. Both
matter: a JIT batch that grants three leases has one trace across six lines on
two sides, while `rid=42` follows that one reservation for its whole life.
`poduid` is the strongest object join — a JIT lease's idempotency key **is** the
pod UID, so `event=reservation.lease_created … poduid=X` in the app and
`event=lease.granted … ns=… pod=…` here refer to the same grant.

---

## 1. Startup and shutdown — `app/main.py` (`lifespan`)

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| WARNING | `config.invalid` | `name value reason detail` | An environment variable was junk, out of range, an unknown timezone or an unknown log level and the **default was used instead** (`reason=not_an_integer` \| `out_of_range` \| `unknown_timezone` \| `unknown_log_level`). Emitted from `Config.from_env`, so it precedes every other line. |
| INFO | `config.timezone` | `name value` | The timezone the human-readable Event and annotation prose renders in, and which variable decided it (`EVENT_DISPLAY_TIMEZONE` when set, else `TZ`). Logged because the default is *derived* from the process environment rather than read off a variable, so it is otherwise unknowable from the Deployment. Display only — every log field here, and every `galends/*` timestamp annotation, stays UTC. |
| INFO | `startup.initial_fetch` | — | Synchronous first fetch, before the loops launch. |
| INFO | `startup.initial_fetch_complete` | `reservations classes` | |
| INFO | `startup.noshow_armed` | `watched` | No-show deadlines armed. |
| ERROR | `startup.initial_fetch_failed` | `err retry_s` | Controller continues degraded; pod matching is delayed until a retry succeeds. |
| WARNING | `startup.capacity_audit_failed` | `err` | The synchronous first audit failed. |
| INFO | `startup.ready` | `loops` | All background loops running, named. |
| CRITICAL | `task.crashed` | `task err` | A supervised background loop died with an unhandled exception; `GET /health` turns 503 so the liveness probe restarts the pod. No in-process restart. |
| INFO | `shutdown.start` / `shutdown.complete` | — | |
| INFO | `k8s.auth` | `mode` (+ `path`) | `mode=kubeconfig` or `in_cluster`. |
| WARNING | `k8s.tls_relaxed` | `name detail` | `K8S_TLS_STRICT_VERIFY=false` — OpenSSL's strict X.509 checks are off for the Kubernetes API connection, so a cluster certificate missing an Authority Key Identifier is accepted. Chain, validity and hostname are still verified; this is not `verify_ssl: false`. Emitted once at startup, and only when the relaxation is in force. |

### Singleton lease — duplicate-instance guard (`SINGLETON_LEASE_ENABLED`)

Not leader election: the lease exists so a *second* controller refuses to run, because two instances would issue duplicate toleration patches.

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `singleton.acquired` | `name ns holder mode` (+ `age_s`) | `mode=created` \| `reacquired` (same pod, container restart) \| `takeover` (previous holder's lease expired — `age_s` is how stale it was). |
| INFO | `singleton.disabled` | — | The guard is switched off; nothing stops a duplicate instance. |
| WARNING | `singleton.acquire_failed` | `err` | Could not reach coordination.k8s.io (e.g. a 403 on an upgrade that predates the leases RBAC rule). **Fail-open**: the controller runs unguarded and keeps retrying. |
| CRITICAL | `singleton.held_by_other` | `name ns holder age_s` | Another live instance holds the lease; startup aborts and the process exits non-zero so crash-backoff paces the retry. |
| DEBUG | `singleton.renewed` | `name` | Routine renewal, every 20 s. |
| WARNING | `singleton.renew_failed` | `fails err` | First failure and every 30th (~10 min); the controller keeps running. |
| CRITICAL | `singleton.lost` | `name ns holder` (+ `age_s`) | Another instance took the lease — this one terminates immediately. |
| DEBUG | `k8s.lease_write` | `name mode` | The coordination API write behind the above. |

---

## 2. Reservation fetch — `app/main.py`, `app/reservation_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `fetch.start` | — | Confirms the loop is alive between INFO events. |
| INFO | `api.reservations_fetched` | `count active cancelled lookahead_days` | Paginated `status=all` fetch. |
| INFO | `class.resolved` | `cid class clabel` | Per-id fallback resolved a class missing from the bulk list. |
| WARNING | `class.unresolvable` | `cid reason` | No `label_value` — **pods for this class can never be matched** until it is configured. |
| INFO | `lease.preserved` | `count ids reason` | Locally-granted leases re-added because the snapshot predated the grant. |
| INFO | `fetch.complete` | `reservations classes` | |
| ERROR | `fetch.failed` | `err` | Whole cycle failed; previous state retained. |
| WARNING | `api.gpu_class_fetch_failed` / `api.gpu_class_parse_failed` | `cid` + `status` \| `err` | Per-class fallback. |
| WARNING | `api.gpu_classes_fetch_failed` / `api.gpu_classes_parse_failed` | `status` \| `err` | Bulk list; the previous cycle's maps are kept rather than losing all resolution. |

---

## 3. Reserved-path pod admission — `app/main.py`, `app/controller.py`, `app/k8s_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `pod.enqueued` | `ns pod rid start end reserved gpus` | Matched to a reservation, queued. |
| INFO | `pod.fast_path` | `ns pod rid` | ADDED inside an already-open window — toleration attempted immediately. |
| DEBUG | `pod.budget_full` | `ns pod rid gpus free reserved` | Retry in 2–5 min. |
| INFO | `pod.toleration_applied` | `ns pod tol_key tol_value booking_ref` | The patch landed. |
| INFO | `pod.admitted` | `ns pod rid clabel gpus free reserved until` | The one line to grep for a successful admission. |
| INFO | `pod.guarantee_recorded` | `ns pod guarantee_s until` | Informational annotations; Kubernetes enforces nothing. |
| INFO | `k8s.event_emitted` | `ns pod reason` (+ `guarantee_s until` \| `rid until` \| `clabel gpus`) | `reason=RuntimeGuaranteed` \| `BestEffortAdmitted` \| `Preempted` \| `ReservationCancelled` \| `ReservationReassigned` \| `OverstayRelinked` \| `OnDemandLeaseDenied`. `BestEffortAdmitted` replaces `RuntimeGuaranteed` for a pod admitted with no runtime guarantee (a `RuntimeGuaranteed` message would read "guaranteed for 0m00s, until \<the instant the pod started\>"). `OnDemandLeaseDenied` is the only `Warning`-type Event; it and `BestEffortAdmitted` are the two addressed to the pod's *owner* rather than to an operator. |
| INFO | `pod.dequeued` | `ns pod reason` | e.g. `toleration_already_present`. |
| INFO | `pod.queue_dropped` | `ns pod` + `rid reason` \| `phase reason` | Window expired, reservation cancelled with no replacement, or the pod went terminal. |
| INFO | `pod.requeued` | `ns pod reason old.rid new.rid` | Re-matched after its reservation was cancelled. |
| WARNING | `pod.admission_error` | `ns pod rid err` | Optimistic placement rolled back; entry stays queued. |
| WARNING | `pod.guarantee_record_failed` | `ns pod err` | Best-effort — the toleration is **not** revoked. |
| INFO | `pod.gate_removed` / DEBUG `pod.gate_absent` | `ns pod gate` | `POD_SCHEDULING_GATE_NAME` handling. |
| WARNING | `pod.gate_remove_failed` | `ns pod gate err` | Also best-effort. |
| DEBUG | `pod.routed_jit` | `ns pod clabel reason` | No admittable reservation → JIT queue. |
| DEBUG | `pod.left_pending` | `ns pod reason` | No match and not JIT-eligible (missing group label or minimum-runtime annotation). |
| ERROR | `pod.event_failed` | `watch_event ns pod err` | Handling one watch event raised; the event is skipped and the watch loop keeps consuming (`ns`/`pod` omitted when the object was too malformed to name). |

---

## 4. Occupancy — `app/controller.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `occupancy.placed` | `rid poduid gpus free reserved` | |
| INFO | `occupancy.released` | `rid poduid gpus` | |
| INFO | `occupancy.reconciled` | `reservations old.used new.used` | **INFO only when the count changed** — i.e. a missed watch event just self-healed. |
| DEBUG | `occupancy.reconciled` | `reservations used` | Steady state. |

---

## 5. JIT on-demand leases — `app/main.py`, `app/reservation_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `ondemand.candidate_added` | `ns pod poduid clabel gpus group` (+ `min_runtime_s` \| `best_effort`) | `min_runtime_s` is absent for a **best-effort** candidate, which sizes nothing; `best_effort` is emitted only when true. |
| DEBUG | `ondemand.candidate_removed` | `ns pod poduid` | |
| INFO | `ondemand.candidate_dropped` | `ns pod reason` (+ `phase` \| `detail`) | Terminal phase, or Pending for something no lease can fix (`detail` carries the scheduler's verdict). |
| DEBUG/INFO/WARNING | `ondemand.candidate_held` | `guard reason ns pod` (+ `clabel gpus node_free claimed nodes`) | **The guard number is the field** — see below. `claimed` (guard 5) is the GPUs already taken by earlier candidates in the same batch. |
| WARNING | `ondemand.gated` | `clabel guard reason dur_s candidates detail` (+ `app_gpus phys_gpus` \| `pods`) | **Every 60 s, one line per GPU class whose on-demand admission is paused**, for as long as it stays paused — see below. |
| ERROR | `ondemand.gate_warning_failed` | `err` | The warning pass raised; retried next minute. |
| DEBUG | `ondemand.schedule_verdict` | `ns pod` | Scheduler verdict arrived; re-attempting immediately. |
| INFO | `lease.denied` | `ns pod clabel gpus status detail` | App refused the ask as infeasible (409), or a transient network/5xx failure; cooldown 2–5 min. `detail` is the app's reason — absent when the app never answered. On a 409 it is also mirrored to the pod as an `OnDemandLeaseDenied` Event. |
| WARNING | `lease.error` | `ns pod clabel gpus status fails retry_s` | **A fault waiting cannot fix** — a 4xx that is not 409 (read-only service key, schema mismatch, unknown group). Exponential backoff to 30 min; `grep 'event=lease.error'` is how a misconfigured deployment announces itself. |
| INFO | `lease.granted` | `rid ns pod clabel gpus` (+ `lease_dur_s` \| `best_effort`) | `lease_dur_s` is absent for a **best-effort** admission, which reserves no window; `best_effort` is emitted only when true. |
| WARNING | `lease.admission_failed` | `rid ns pod detail` | Grant landed but admission did not — a compensating cancel follows. |
| INFO | `lease.teardown` | `rid class reason` | The lease's pod went away; the lease is cancelled. |
| INFO | `ondemand.unmet_demand` | `ns pod clabel gpus min_runtime_s submitted deleted waited_s` | **Demand the controller never satisfied** — the pod was deleted before any lease. |
| WARNING | `ondemand.selection_unavailable` | `fallback candidates` | Delegation call failed; granting all. |
| WARNING | `ondemand.unknown_grant` | `poduid` | App returned a uid that was never offered; ignored. |
| WARNING | `ondemand.pod_read_failed` | `ns pod err` | |
| INFO | `api.lease_denied` | `poduid status detail` | Client-side view of a routine 409 denial, with the reason the app gave. Pairs with `lease.denied` above. |
| WARNING | `api.lease_error` | `poduid status detail` | Client-side view of any other non-2xx, with the response body excerpt. Pairs with `lease.error` above. |
| WARNING | `api.lease_failed` / `api.lease_parse_failed` | `poduid err` | Network failure / unparseable response — the app never answered. |

**Guards** (`guard=`), all short-retry rather than drop:

| `guard=` | `reason=` | Meaning |
|---|---|---|
| 1 | `schedule_verdict_pending` | No `PodScheduled` verdict yet, so there is nothing to classify. Transient — the MODIFIED fast path shortens it to ~1 s. |
| 1 | `no_class_nodes` | The class is *known* to have no schedulable node carrying its reservation taint (fully drained/cordoned). Fail-open when unknown. |
| 3 | `stuck_holder_interlock` | A reservation holder is stuck Pending on this class. |
| 4 | `class_overcommitted` | App-side capacity exceeds physical (see §8). **Not applied to a best-effort candidate**, which consumes no app-side capacity to overcommit. |
| 5 | `no_single_node_fit` | No single node has enough free GPUs for the ask, net of `claimed` (GPUs taken by earlier candidates this batch). Applies to a ≥2-GPU ask, and to **every** best-effort ask — see below. Fail-open when unknown. |
| — | `class_id_unknown` | The `gpu-class` label has no numeric id yet. |

Guard 1 is the only one that also **drops** rather than holds: a candidate the
scheduler rules out for something a lease cannot fix leaves as
`ondemand.candidate_dropped reason=blocked_not_by_gpu_gating`, with the verdict
in `detail`.  Note what it deliberately does *not* key on — the string
`Insufficient nvidia.com/gpu`.  On a taint-gated cluster the scheduler rejects
the pod's own GPU nodes on our untolerated taint before it ever weighs their
resources, so that string never appears and every candidate used to sit at
`guard=1 reason=schedule_verdict_pending` forever, at DEBUG, granting nothing.
`grep 'event=ondemand.candidate_held guard=1'` should be a *transient*
population; a candidate that stays there across ticks is worth a look.

**`ondemand.gated` is the operator-facing summary of the guards above.**
Every minute, for each GPU class on which a *class-wide* gate is holding JIT
admission — guard 1 `no_class_nodes`, guard 3 `stuck_holder_interlock`, guard 4
`class_overcommitted` — the controller logs a WARNING whose `detail` states in
plain English what is paused, the cause, what to check (with `kubectl` commands)
and what lifts it.  It repeats whether or not any pod is waiting (`candidates=`
is how many are held right now, best-effort candidates excluded under guard 4,
which does not apply to them), because the pause is in force either way and the
one-shot transition lines (`capacity_audit.paused`, `interlock.activated`) are
easy to scroll past.  `dur_s` is how long this controller has seen the gate
(reset by a restart).  Guard 4 carries both sides of the mismatch
(`app_gpus`, `phys_gpus`, the latter from the last audit or queue tick); guard 3 carries the
stuck holder pods in `pods`.  Guard 5 is deliberately not reported: it is
per-ask fragmentation that clears as jobs finish, not a fault to act on.  Emitted
only when `ONDEMAND_LEASE_ENABLED` is on.  `grep 'event=ondemand.gated'` empty is
the healthy state.

`grep 'event=ondemand.candidate_held guard=4'` answers "how often is the capacity
audit blocking admission" without matching on message text.

Guard 5 is the **only** physical bound on best-effort admission, which is why it
applies there at every GPU count rather than only at ≥2.  A guaranteed lease is
bounded app-side — it holds calendar capacity, so the app will not sell the same
GPU-hour twice — but a best-effort reservation is zero-length and therefore
invisible to that arithmetic, so the app would admit an unbounded number of them
onto the same idle GPU.  `grep 'event=ondemand.candidate_held guard=5'` rising on
1-GPU asks is the cluster being physically full, not a misconfiguration.

---

## 6. Preemption sweep — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `preempt.boundary` | `boundary sweep clabel demand free kills` | **One line per GPU class**, not one carrying two dicts. |
| WARNING | `preempt.unmet` | `boundary sweep clabel short` | Demand uncovered after preempting every eligible overstayer. |
| INFO | `preempt.headroom` | `clabel demand free kills` | Anticipatory headroom pass. **One line per GPU class.** `demand` is the headroom *target* (`ceil(capacity × HEADROOM_TARGET_PERCENT / 100)`), not booking demand. No `boundary`/`sweep` — headroom is a standing goal, not a boundary phase. |
| WARNING | `preempt.headroom_unmet` | `clabel short` | Headroom target uncovered after preempting every eligible, notice-elapsed overstayer. Expected while notices are still ripening. |
| INFO | `pod.preempting` | `ns pod clabel gpus detail` | `detail` explains the overstay and either the triggering boundary or the headroom target. |
| INFO | `pod.deleted` | `ns pod` | |
| WARNING | `preempt.snapshot_failed` | `target err` | `target=pods` or `node_capacity`. **The whole sweep is skipped** — never kill on unknown physical state. |
| WARNING | `preempt.selection_unavailable` | `fallback` | App delegation failed; local uniform-random used. |
| WARNING | `preempt.unknown_victim` | `poduid` | App named a pod that was never offered; ignored. |
| ERROR | `preempt.sweep_failed` | `err` | |
| DEBUG | `preempt.demand_skipped` | `rid reason` | Reservation's class label unresolvable. |
| DEBUG | `guarantee.chain_skipped` | `rid reason` | Back-to-back chaining declined: the anchor reservation's class label is unresolvable, so its guarantee covers only its own window. Two classes that both fail to resolve would otherwise compare equal and chain across each other. |

`sweep=A` is the lead-time phase (proactive, `boundary > now`); `sweep=B` is
at-boundary. `phase=` on other lines means the **pod lifecycle phase** — the two
were deliberately given different keys.

---

## 7. Guarantee status, warnings, adoption and merge — `app/main.py`, `app/k8s_client.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `pod.guarantee_status` | `ns pod gstatus` | `gstatus=guaranteed|overstay`; patched only on a real transition. |
| WARNING | `pod.guarantee_status_failed` | `ns pod err` | Retried next tick. |
| INFO | `pod.facts_refreshed` | `ns pod end` | The pod's reservation changed *underneath* it without a re-link (a lease window extended in place), so the `galends/reservation-*` stamps were re-written. Patched only on a real change — a re-link stamps these through `pod.guarantee_recorded` instead. |
| WARNING | `pod.facts_refresh_failed` | `ns pod rid err` | Retried next tick. |
| INFO | `pod.termination_warned` | `ns pod at risk` | `at` is the projected kill instant. |
| INFO | `pod.termination_warning_cleared` | `ns pod` | Pod left the at-risk pool. |
| WARNING | `pod.termination_warning_failed` | `ns pod err` | |
| INFO | `pod.relinked` | `ns pod rid until reason` (+ the retired lease id) | `reason=adoption` (past-guarantee rescue) or `ondemand_merge` (a lease folded into a now-open booking, carrying an `old.` field for the lease it retired). |
| WARNING | `pod.relink_failed` / `pod.merge_failed` | `ns pod rid err` | |

---

## 8. Capacity audit — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| WARNING | `capacity_audit.mismatch` | `clabel app_gpus phys_gpus overcommitted` | **`overcommitted=true` is the direction that pauses admission**; `false` is under-provisioning, logged but harmless. |
| INFO | `capacity_audit.paused` / `capacity_audit.resumed` | `clabels` | Classes entering/leaving the JIT pause set. Emitted by the hourly audit **and** by any queue-processor tick that moves the set (`trace=queue-…`), so a pause lifts within one `QUEUE_PROCESSOR_INTERVAL` of the counts agreeing. |
| WARNING | `capacity_audit.snapshot_failed` | `target err` | Audit skipped; **the existing pause set is left unchanged** — a transient failure must never silently lift a pause. |
| ERROR | `capacity_audit.failed` | `err` | |

---

## 9. Queue processor tick — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `queue.tick` | `queued candidates` | |
| ERROR | `queue.tick_failed` | `err` | The whole tick raised; state from partial work stands and the loop retries next interval (same guard shape as the fetch/preemption/audit loops). |
| WARNING | `queue.snapshot_failed` | `target err` | `target=pods` or `node_inventory`; prior state kept. |
| WARNING | `interlock.activated` | `clabel guard count pods` | Guard-3 interlock on; JIT held for that class. |
| INFO | `interlock.cleared` | `clabel guard` | |
| DEBUG | `queue.node_feasibility` | `clabel node_free nodes` | One line per class: largest single-node opening (guard 5) and how many nodes back the class (guard 1). |

---

## 10. No-show tracking — `app/controller.py`, `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| DEBUG | `noshow.armed` | `rid reason deadline` | `reason=init|new`. |
| INFO | `noshow.declared` | `rid user clabel reason` | Queued for a durable app-side cancel. |
| INFO | `noshow.cancelled` | `rid` | The cancel landed — durably gone from the app's active set. |
| WARNING | `noshow.cancel_failed` | `rid` | Retried next tick. |
| INFO | `noshow.cancel_skipped` | `rid reason` | A pod appeared at the last second. |
| DEBUG | `noshow.cleared` | `rid` \| `ids` + `ns clabel reason` | Holder pod admitted — chained windows are cleared together. |
| INFO | `noshow.removed` / DEBUG `noshow.deadline_pruned` / `noshow.cancel_pruned` | `rid reason` | Left the active list. |
| DEBUG | `noshow.skipped` | `rid reason` | Window already ended; never declared. |

---

## 11. Cancellation and owner-change eviction — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `cancel.pods_relinked` | `rid count` | Adoption rescued pods instead of evicting them. |
| INFO | `cancel.evicting` | `rid count detail` | |
| INFO | `owner_change.evicting` | `rid count detail old.user` | Reservation reassigned to a teammate. |
| WARNING | `evict.snapshot_failed` | `target err` | The single pod snapshot both eviction planners share failed; eviction skipped this cycle. Replaces the former per-handler cancel/owner-change snapshot events — the two handlers now share one LIST. |
| WARNING | `k8s.event_failed` | `ns pod reason err` | Best-effort; deletion still attempted. |
| WARNING | `pod.delete_failed` | `ns pod err` | |

---

## 12. Inbound APIs — `app/main.py`

| Level | `event=` | Fields | Notes |
|---|---|---|---|
| INFO | `push.applied` | `upserts cancellations owner_changes reservations` | `POST /api/reservations/push`. |
| WARNING | `forecast.snapshot_failed` | `target err` | `GET /api/forecast/preemption-risk` → 503; never report risk from unknown physical state. |

---

## 13. Outbound API failures — `app/reservation_client.py`

All best-effort: every one of these returns `None`/`False` rather than raising.

| Level | `event=` | Fields |
|---|---|---|
| INFO | `api.cancel_already_gone` | `rid reason status` (404 treated as success) |
| WARNING | `api.cancel_failed` | `rid reason` + `status` \| `err` |
| WARNING | `api.victim_selection_failed` / `api.victim_selection_parse_failed` | `status` \| `err` |
| WARNING | `api.admission_selection_failed` / `api.admission_selection_parse_failed` | `status` \| `err` |
| WARNING | `api.overstay_report_failed` | `rid poduid` + `status` \| `err` |

---

## 14. Kubernetes API traces — `app/k8s_client.py`

DEBUG only, unless noted.

| Level | `event=` | Fields |
|---|---|---|
| DEBUG | `k8s.read_pod` | `ns pod` |
| DEBUG | `k8s.list_pods` / `k8s.list_pods_done` | `selector purpose` (+ `reason`) / `purpose count` (+ `rv`) — `reason` (watch seed only) is why a full re-LIST ran: `start`, `error`, `expired`, or `resync` |
| DEBUG | `k8s.list_nodes` | `purpose` |
| DEBUG | `k8s.node_inventory` | `clabel nodes total` — one line per class |
| DEBUG | `k8s.node_capacity_forced` | `node total` — the node's `galends/force-node-capacity` annotation replaced its allocatable count |
| DEBUG | `k8s.patch_pod` | `ns pod patch` + the patch's own fields |
| DEBUG | `k8s.create_event` / `k8s.delete_pod` | `ns pod` (+ `reason`) |
| DEBUG | `pod.already_gone` | `ns pod status` (404 on delete) |
| DEBUG | `pod.annotations_truncated` | `ns pod count` — the pod carries more `galends/*` annotations than a JIT admission ask forwards; `count` is how many keys were dropped (sorted order, so the same subset every attempt) |
| WARNING | `k8s.node_allocatable_invalid` | `node resource value` — treated as 0 |
| WARNING | `k8s.node_capacity_forced_invalid` | `node annotation value` — unparseable or negative `galends/force-node-capacity`; the node keeps its allocatable count |
| WARNING | `pod.annotation_invalid` | `ns pod annotation value` — malformed `galends/minimum-runtime-seconds` |
| DEBUG | `k8s.watch_open` / `k8s.watch_event` | `selector rv timeout_s mode` / `watch_event ns pod` — `mode=seed` after a LIST, `mode=resume` when continuing from the last resourceVersion (no LIST, no replay) |
| DEBUG | `k8s.watch_bookmark` | `rv` — server bookmark advanced the resourceVersion; never forwarded as a pod event |
| INFO | `k8s.watch_expired` | `rv` — HTTP 410: the resourceVersion expired server-side; re-LISTing immediately (no backoff) |
| WARNING | `k8s.watch_dropped` | `dropped` — bounded event queue was full; the oldest event was discarded (first drop and every 100th; the periodic resync re-LIST heals the gap) |
| WARNING | `k8s.watch_error` | `fails err retry_s` — **first failure and every 120th**, to bound spam during a sustained disconnect |
| DEBUG | `k8s.watch_ended` | `err retry_s` |

---

## Guarantees this inventory rests on

- **Every line goes through `kv()`**, enforced by `tests/test_log_grammar.py`,
  which also fails if a field is emitted that `docs/LOG-FIELDS.md` does not
  document. That test is what keeps this file from drifting again.
- **Fail-safe paths log and stop**: any snapshot failure skips the work rather
  than acting on unknown physical state. Grep `event=.*snapshot_failed` to find
  every instance of that pattern.
