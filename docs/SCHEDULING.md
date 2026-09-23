# GPU Reservation System — Scheduling Capabilities Summary

A FastAPI/SQLite single-lab GPU reservation app. Students book GPU time to build
and run assignments. This document captures the full set of scheduling
constraints, control levers, and allocation logic — written for review against
operations-research literature for configuration guidance.

## 1. The resource model (what is being scheduled)

- **GPU classes** (`gpu_classes`) — hardware tiers, each with a fixed integer
  `total_gpus` (the cluster pool for that tier). This is the supply. Multiple
  independent classes (e.g. H100, A100) are scheduled separately; there is no
  cross-class substitution. Each class carries a `su_rate_per_hour` (base Service
  Units per GPU per hour, default 0) and an optional `max_gpus_per_reservation`
  cap.
- **Capacity overrides** (`gpu_class_day_overrides`) — per-date-span capacity
  adjustments (holidays, maintenance, surge). Inclusive `date_start`/`date_end`,
  either bound nullable = unbounded. Overlaps resolve **narrowest-span-wins**;
  ties → largest `available_gpus`. This lets a short maintenance window override
  a long-standing baseline.

The physical pool (`total_gpus`, or a day override's `available_gpus`) is the
same for every user; headroom for maintenance or accommodations is now reserved
through the per-group and per-cluster GPU ceilings rather than a hidden buffer.

## 2. The time structure (how reservations are expressed)

Time is **continuous** — not a fixed slot grid. A reservation specifies an
arbitrary `start_dt`/`end_dt` pair in site-local wall-clock time. Two kinds
exist:

- **Web bookings** (`kind='booking'`) are snapped to whole hours (no
  minutes/seconds) by the API validator:
  - `start_dt` must be at least 15 minutes in the future (propagation lead time
    for the Kubernetes controller).
  - `end_dt > start_dt`; may cross midnight (e.g. 22:00 → 06:00).
  - Non-privileged members are subject to a **48-hour server-side cap** (the API
    returns 400 for requests exceeding 48 h). Admins and group managers are exempt
    and may create reservations longer than 48 h, typically via the admin
    reservation interface — up to the group's `max_reservation_hours` (default
    168 h), which binds every caller. The frontend booking wizard also enforces a
    48-hour ceiling for non-privileged users.
- **On-demand leases** (`kind='on_demand'`, requested by the Kubernetes
  controller for pending on-demand pods) are anchored at the app's "now" with an
  arbitrary **second-granularity** duration — no grid, no lead time, no 48-hour
  cap.
- The `date` column mirrors `start_dt.date()` for filtering.

Capacity is evaluated per **hour-aligned segment** across the booked interval
(segments never span an hour or date boundary), with **exact peak-concurrency**
counting — so cross-midnight ranges see each day's day-override capacity, and a
whole-hour booking conflicts with a sub-hour lease only when their intervals
genuinely overlap (the web UI simply shows a partially-consumed hour as
unavailable).

## 3. Service Unit (SU) pricing

Every GPU class has a `su_rate_per_hour` (base rate). System-wide
**SU discount schedules** (`su_discount_schedules`) define time-of-day multipliers
that reduce the effective rate during off-peak windows:

- Each schedule has `days_of_week` (JSON int list, 0=Mon), `start_time`/`end_time`
  (wall-clock; `end_time ≤ start_time` wraps past midnight), `multiplier`
  (0 ≤ m ≤ 1; effective rate = base × m; `0` = free window), optional inclusive
  `date_start`/`date_end`, and `is_active`.
- Each schedule must be **explicitly attached to one or more GPU classes** via the
  `su_discount_schedule_gpu_classes` join table (`gpu_class_ids` field on create/update).
  A schedule with no attached classes has no effect on any class, regardless of its
  `is_active` state.
- Among overlapping schedules covering the same GPU class at any instant, the same
  narrowest-span-wins resolution applies: narrowest date-window for discount; ties →
  highest multiplier (smallest discount).
- When no attached schedule covers the instant, the multiplier is **1.0** (full price).

The booking's **total SU cost** is computed at creation time and stored on the
reservation row as two fields that start equal:

```
effective_rate(hour) = base_rate × multiplier(hour)
su_cost_user  = Σ (effective_rate(hour) × gpu_count)  for each hour in [start_dt, end_dt)
su_cost_group = su_cost_user   (at creation; may diverge on a manager waive — see below)
```

`su_cost_user` is charged against the individual member's `su_budget`;
`su_cost_group` is charged against the group's `pool_su_budget`.
Both are used for budget enforcement and reporting.

`app/pricing.py` exposes `effective_multiplier`, `compute_su_cost`, and
`hourly_breakdown` (used by the availability timeline so the frontend can preview
cost before booking).

### Cancellation penalties

`su_cost_user` (and `su_cost_group`) are rewritten at cancellation time to the
**retained cost** = time already consumed (charged in full) + a penalty on the
*unused* reserved hours that fall within the next 24 h. Reserved time beyond
that 24 h window is always free, so cancelling well ahead costs nothing.

The penalty is computed by `_compute_cancellation_penalty` in
`app/routers/reservations.py`. Let:

- `used` = consumed hours (0 unless the reservation is in progress),
- `grace` = 2 h for a `kind='on_demand'` lease, 0 for a web booking (see
  "On-demand grace" below),
- `pen`  = unused reserved hours inside the next 24 h
  (`[max(start + grace, now), min(end, now+24h)]`),
- `committed = used + pen`.

The **exemption** (in hours) is `min(committed / 2, 8)`. Then:

- if `pen ≤ exemption` → **no penalty** (you still pay for `used`);
- otherwise the penalty is the window's SU cost scaled by `(pen − exemption) / pen`
  — i.e. a *fraction of cost*, with every hour forgiven proportionally.

`retained = used_cost + penalty`. Worked behaviour (base rate 1 SU/GPU·h, so
SU == hours):

| Situation at cancellation | Retained SU |
|---|---|
| ≥ 24 h before start | **0** |
| 4 h booking, not started, within 24 h | **2.0** (≈ 50 % — see below) |
| 24 h booking, just started | **16.0** (used 0 + 24 × (24−8)/24) |
| 12 h booking, cancelled 6 h in | **6.0** (used 6 + 0 penalty; `pen=6 ≤ exemption=6`) |
| 24 h booking, cancelled 20 h in | **20.0** (used 20 + 0 penalty) |
| 2 h **lease**, exited 10 min in | **0.17** (used 10 min + 0 penalty; the whole request is inside the grace) |
| 12 h **lease**, revoked at start | **5.0** (used 0 + 10 × (10−5)/10; `pen` starts 2 h in) |

A cancelled reservation with a non-zero penalty continues to count against the
member's `su_cost_user` balance until its `end_dt` passes — the same
renewable-ceiling model as active reservations. This prevents gaming the budget
by booking then cancelling at the last minute.

#### On-demand grace — `_CANCEL_ON_DEMAND_GRACE_H` (2 h)

A web booking is a claim its owner chose the length of; a `kind='on_demand'`
lease is not. The controller sizes a lease to the runtime a pod *requests*, and
a request is a ceiling, not a prediction — a job that asks for two hours and
converges in ten minutes is the normal case, not an abuse of the reservation.
Penalising the difference would charge the user for the accuracy of an estimate
the scheduler required them to make up front, and would push them to request
less time than their job might need, which is the opposite of what the lease
model wants.

So for `kind='on_demand'` only, the penalisable window opens `grace` hours after
the lease starts: unused time in `[start, start + 2 h]` contributes nothing to
`pen`. **Consumed time is untouched** — it is charged in full whether it falls
inside the grace or after it — so the ten-minute job pays for ten minutes and a
lease that runs its full two hours pays for two.

Only the *unused* remainder is forgiven, and only the leading hours: a lease long
enough to reach past the grace is penalised on the ordinary curve from there on
(the 12 h lease in the table). Web bookings are unaffected at any length: the
same 2 h window booked through the wizard and abandoned 10 minutes in still
retains 1.0 SU. `_CANCEL_ON_DEMAND_GRACE_H` in `app/routers/reservations.py` is
the lever; setting it to `0` restores the pre-grace behaviour exactly.

#### Design rationale and the three knobs

This policy replaced a simpler "100 % of consumed + 50 % of the next-24 h
remainder" rule. Three properties drove the redesign, and three constants
(declared at the top of the cancel helpers in `app/routers/reservations.py`,
alongside the on-demand grace above) remain the levers to retune:

1. **Penalty window — `_CANCEL_PENALTY_WINDOW_H` (24 h).** Only imminent
   capacity is scarce, so only reserved hours within this horizon are
   penalisable; everything past it is free. Widening it penalises further-out
   cancellations; narrowing it makes the system more forgiving. It also sets the
   floor the controller poll/guard intervals must stay under conceptually
   (capacity freed by a cancel is only useful if it can be re-booked in time).

2. **Exemption divisor — `_CANCEL_EXEMPTION_DIVISOR` (2).** The exemption is
   `committed / divisor`, where `committed = used + penalisable`. At `2` a
   *not-yet-started* booking (where `used = 0`) is charged exactly 50 % of its
   in-window cost, matching the old headline rate — so the change is invisible
   to the common "I booked and changed my mind" case. The exemption grows with
   *consumed* time, so an in-progress job that has already run longer than its
   remaining-within-24 h time pays **no** extra penalty (the `pen ≤ exemption`
   branch): you are not punished for releasing a job you have mostly used.
   Smaller divisors (1/3, 1/4) forgive less and bite harder on every case;
   we modelled all three before settling on 1/2.

3. **Exemption cap — `_CANCEL_EXEMPTION_CAP_H` (8 h).** Without a ceiling the
   exemption would scale with very long reservations, letting someone hold (say)
   a 48 h block and walk away cheaply. The cap engages only when
   `committed > 16 h`, so it leaves all small/medium bookings on the pure-1/2
   curve and pushes only genuinely large cancellations toward near-full charge
   (e.g. a just-started 24 h block retains 16 SU rather than 12). Lowering the
   cap discourages large speculative holds more aggressively; raising it (or
   removing it) is gentler on long legitimate jobs.

**Why "fraction of cost" and not "shave trailing hours".** The exemption is `E`
*hours* of relief, but with time-of-day discounts the SU value of an hour
varies, so *which* hours are forgiven matters. Three application methods were
compared:

- *trail* — drop the last `E` hours of the window. Position-dependent: it
  systematically favours users whose expensive hours sit late in the window and
  penalises those whose cheap (off-peak) hours do, for otherwise-identical
  bookings.
- *cheap* — forgive the `E` cheapest hours. The mirror image (keeps expensive
  hours billed); also order-sensitive in the opposite direction.
- *frac* (**chosen**) — scale the whole window cost by `(pen − E)/pen`. Every
  hour is forgiven in proportion, so the retained SU is a fixed fraction of the
  window's actual cost **regardless of where discounted hours fall**. This is
  the only rate-neutral, predictable option and the one implemented.

If discount-aware fairness ever stops mattering, `trail` is marginally cheaper
to compute; switching methods is a localised change in
`_compute_cancellation_penalty`.

**Waiving penalties.** Admins and group managers can waive the penalty:
- **At cancellation time** — the cancel modal shows the computed penalty and
  offers a waive checkbox.
- **After the fact** — `POST /api/reservations/{id}/waive-penalty` clears the
  penalty on an already-cancelled reservation. The admin reservations page shows
  an amber indicator on cancelled rows with a non-zero penalty.

The waiver scope differs by privilege level:
- **Manager waive** — zeroes `su_cost_user` only (the member's personal budget is freed); `su_cost_group` is retained (the group pool still carries the penalty). A manager's waive of a row whose user share is already zero — only the pool share remains, as after a manager's own waive-at-cancel — changes nothing and is refused with **403** rather than reported as a success.
- **Admin waive** — zeroes both `su_cost_user` and `su_cost_group` (full pardon).

## 4. Access scoping & shares (who can book what)

Allocation of the cluster to "courses" (as well as projects, labs, etc.) is done through **usage groups**
(`usage_groups`):

- **GPU class attachment** (`usage_group_gpu_classes`) — a group can only book
  GPU classes explicitly linked to it, or any class flagged `attach_all_groups`.
  This controls *which hardware tiers* a course sees.
- **Per-group GPU ceiling** (`usage_group_gpu_limits`) — `max_gpus` cap per
  (group, class) over an optional date span. `max_gpus=0` disables a class for
  that group. Same narrowest-span-wins overlap resolution → supports a temporary
  "deadline-week boost" overriding a semester-long baseline cap. This is the
  **course's share** of the cluster.
  - Effective hourly availability for a member =
    `min(cluster_capacity − peak_reserved, group_ceiling − group_peak_reserved)`.
    "Peak" = maximum concurrent GPUs across any one-hour bucket in the requested
    interval (not a sum of all overlapping reservations).

## 5. Quotas & booking-window constraints (per group, enforced at booking time)

All on `UsageGroup`, enforced for regular members in both
`POST /api/reservations` and surfaced in `GET /api/availability`:

| Lever | Meaning | Type |
|---|---|---|
| `valid_from` / `valid_until` | Date range the group can book within | Activation window |
| `min_days_ahead` | Earliest lead time before a start date (booking opens) | Booking horizon (lower) |
| `max_days_ahead` | Furthest ahead a start date can be booked | Booking horizon (upper) |
| `su_budget` | Cap on a member's Service Units (`su_cost_user`) **per budget window** (see `su_anchor_mode`); `NULL` = unlimited; skipped for privileged users | Per-member SU budget |
| `pool_su_budget` | Cap on the group's Service Units (`su_cost_group`) across all members, per the same window; `NULL` = unlimited; enforced for all non-admin users including managers | Group-wide SU pool |
| `su_anchor_mode` | How the SU budget accrues: `weekly` (default), `open`, `monthly`, `quarterly`, or `since_creation` — see table below. Each window carries its own allowance | Budget window |
| `on_demand_only` | Group accepts controller on-demand leases only; every web booking is refused with 400. **No privilege exemption** — managers and admins too (§7.1) | Creation-path restriction |
| `on_demand_auto_join` | An on-demand lease may enrol (and provision) the user it names instead of requiring existing membership. Affects that path alone (§7.1) | Roster admission |

Additional hard constraints:
- `start_dt` must be at least 15 minutes in the future.
- Start date must be today or future.
- Multiple reservations for the same user on the same day are allowed, subject to
  capacity and SU budget.
- Availability queries are capped at a 90-day date range.

### SU budget window (`su_anchor_mode`)

`su_anchor_mode` on `UsageGroup` selects how the budget accrues:

| Mode | Window | Character |
|---|---|---|
| `open` | none — the ceiling applies to reservations with `end_dt > now` | Renewable ceiling: SUs are freed when a reservation ends or is cancelled (no penalty) |
| `weekly` (default) | Monday 00:00 local → the following Monday | Resets each week |
| `monthly` | 1st of the month 00:00 local → the 1st of the next | Resets each month |
| `quarterly` | Jan 1 / Apr 1 / Jul 1 / Oct 1 00:00 local → the next quarter | Resets each calendar quarter |
| `since_creation` | `UsageGroup.created_at` → never closes | Cumulative — never resets |

**`weekly` is the code default for new groups** — it maps onto weekly assignment
rhythms and throttles demand near deadlines without requiring students to cancel
finished reservations to free headroom. Set `open` explicitly for groups that
want a renewable concurrent ceiling instead of a per-week depleting quota.

For the anchored modes the balance counts all `su_cost_user` accrued in the
window regardless of whether the reservation has ended — a **depleting quota**
over the window rather than a renewable concurrent ceiling.

#### Each window is its own allowance

**A reservation is charged to the budget window containing its `start_dt`, and to
no other window.** Booking three weeks ahead spends that week's budget and leaves
the current week's untouched; conversely, a member who has exhausted this week can
still plan next week's work.

This was not always so. The window originally had a *start* but no *end*, so the
balance counted the current window **and everything after it** against one
ceiling. A reservation booked for a future week therefore consumed the current
week's allowance — and every intervening week's as the anchor advanced — which
penalised exactly the planning-ahead the booking horizon exists to permit. The
half-open window (`timezone_utils.su_window_bounds`) is what closed that.

Two consequences worth stating:

- **The gate checks the window the *prospective* reservation falls in**, not the
  window "now" falls in, and the availability preview reports that window's
  figures for the date being previewed (`su_window_start` / `su_window_end` on the
  response name it). The dashboard's su-status card still shows the *current*
  window, with SU committed to later windows reported separately as `su_future` /
  `pool_su_future` so booking ahead is visible rather than merely absent.
- **`max_days_ahead` is now the lever that caps aggregate pre-booking.** Total
  future commitment is bounded by `su_budget` × the number of windows a member can
  reach, so a group that needs a hard ceiling on how far demand can be locked in
  should set a booking horizon. Without one, the reachable windows are unbounded.

#### Reservations that span a window boundary

A reservation crossing a boundary is **not** split across windows: it is charged
**wholly to the window containing its `start_dt`**. A weekly-mode booking running
Sunday 22:00 → Monday 20:00 charges 100 % to the earlier week, and the new week's
allowance is untouched even though most of its hours fall there.

Whole-reservation attribution is deliberate, over pro-rating by hours:

1. **It is an invariant.** Every reservation counts in exactly one window, and the
   window it counts in never changes. Sums stay stable across anchor rollovers,
   nothing is double-counted or dropped, and the bucket a reservation will charge
   is decidable at booking time — which is what the admission gate needs.
2. **Penalty rewrites stay well-defined.** A late cancellation rewrites
   `su_cost_user` to a retained value that no longer corresponds to particular
   hours (see §"Cancellation penalty"). Splitting that across windows would need
   an invented apportionment rule; attributing it whole needs none.
3. **The distortion is bounded.** Non-privileged web bookings cap at 48 h, so at
   most **one** boundary can be crossed. Multi-boundary spans arise only from
   privileged bookings (exempt from the budget gate anyway), long on-demand leases
   and *continue*, and the latter two anchor at "now" — so they start in the
   current window, which is where work consuming capacity now belongs.
4. **Gaming is self-limiting.** Starting a booking at Sunday 23:00 to shelter
   Monday's hours costs real headroom in the *old* week, and the 48 h cap bounds
   how much can be sheltered.

Pro-rated apportionment (splitting a spanning reservation by its hours in each
window) remains a localised future change if boundary gaming ever becomes real:
the attribution rule lives entirely in `budget.window_filters`.

The `since_creation` mode is the strictest: it is a single window that never
closes, so once SUs are spent they are never recovered (except by the
cancellation-penalty waiver mechanic above or by the admin increasing
`su_budget`). `open` and `since_creation` are the two modes with no *later*
window, so `su_future` is structurally zero for both.

`GET /api/groups/su-status` returns per-group breakdown fields: `su_used` (active
SUs already ended in the current window), `su_open` (active, not yet ended),
`su_cancelled` (late-cancel penalties still counting), `used_count`, `open_count`,
`su_remaining`, and `su_future` / `future_count` (committed to later windows);
plus pool-budget parallels `pool_su_budget`, `pool_su_used`, `pool_su_open`,
`pool_su_remaining`, and `pool_su_future`.

Note: late-cancel penalties (non-zero `su_cost_user` on cancelled rows) count
against the per-member budget until `end_dt` passes, regardless of anchor mode.
Likewise, `su_cost_group` on cancelled rows counts against the pool budget.

**SU quota boosts follow the same time basis.** A boost lifts the ceiling of the
window a reservation is charged to, so it is evaluated on the reservation's
**start date**, not the day the booking is placed — an offset dated to finals week
lets members book finals week in advance, and an offset active only today cannot
inflate the ceiling of every future window a member can reach.

## 6. Privilege tiers (constraint bypass)

- **Members** — all constraints above apply strictly.
- **Group managers & admins** ("privileged") — bypass `min/max_days_ahead` and
  `su_budget`; get a **±90-day grace window** around `valid_from`/`valid_until`.
  **Group membership is not bypassed**: admins and group managers must still be
  members of a group to book under it or query its availability. They still face
  hardware capacity, per-reservation GPU limits and the group's
  `max_reservation_hours`. Additionally:
  - **Penalty waiver** — can waive late-cancellation SU penalties at cancel
    time or after the fact via `POST /api/reservations/{id}/waive-penalty`.

## 7. Allocation logic

**Booking is greedy/first-come-first-served by users**, validated under a
serialized critical section (SQLite `BEGIN IMMEDIATE` via `write_intent()`) so
concurrent bookings for the last GPU(s) in a slot cannot both succeed. There is
**no optimization, priority, or fairness algorithm** in the booking path — it's
pure admission control: a request is accepted iff every constraint passes and
capacity remains at every instant in the requested interval. Controller-requested
on-demand leases pass through the *same* admission control (see §7.1).

### 7.1 On-demand leases (controller-requested reservations)

There is **no idle-capacity tiling** and no preemption of one reservation by
another. Instead, when the Kubernetes controller sees pending **on-demand** pods
it *requests a lease* from the app (`POST /api/reservations` with
`on_demand: true`, write-scope service key): user + group + GPU class + GPU
count + duration. The app anchors the lease at its own "now" and admits it with
the **same admission control as a web booking** — the three capacity tiers
(physical / cohort / group, including borrowing when the group's resolved
relaxation mode permits it, clamped by the per-class `relax_min_available` buffer
— or by `relax_min_available_junior` for a `junior` group),
the SU budget and pool gates, `max_gpus_per_reservation`, and the group validity
window — while timing policy (15-minute lead, whole-hour grid, 48-hour cap,
`min/max_days_ahead`) does not apply. Denials return 409; requests are
idempotent on an `idempotency_key`.

Two per-group flags qualify that symmetry, both default off and both
administrator-only. `usage_groups.on_demand_only` makes the group
**one-directional**: it accepts leases on this path and refuses web bookings
entirely (400 to every caller, group managers and administrators included), so
its allocation cannot be pre-committed by a human ahead of the pods it exists to
back. `usage_groups.on_demand_auto_join` relaxes the **membership** gate on this
path only — an unenrolled `username` is added as a member, and one with no
account is provisioned an ordinary account, rather than being refused — so the
group's roster is filled by the workload instead of curated ahead of it. Neither
flag touches capacity, the SU budgets, `max_reservation_hours`, or the validity
window, and neither changes any other creation path.

Consequences for the scheduling model:

- An admitted lease **holds its capacity until it ends or is cancelled** — a web
  booking cannot displace on-demand usage. The per-class borrowing buffer
  (`relax_min_available`) is the operator's lever for keeping headroom open for
  interactive users on a busy class. Note that a lease is anchored at "now",
  which is the *shallow* end of that buffer's lead-time ramp (below), so
  `relax_min_available` — not `relax_min_available_far` — is the value protecting
  interactive capacity. A `junior` group's leases are the exception: its buffer
  does not ramp, so they face the same floor as its bookings do at any lead time.
- Leases are charged Service Units and budget-gated exactly like bookings, so
  heavy on-demand use draws down the same per-member/team budget.
- The controller cancels a lease it no longer needs
  (`POST /api/reservations/{id}/cancel`, reason `controller-revoked`) or reports
  a holder who never ran pods (`no-show` — also usable against an
  already-started booking). The standard unwaived cancellation penalty applies,
  so a lease revoked near its end retains only the consumed time. This is the
  one place the lease and booking curves differ: a lease's first
  `_CANCEL_ON_DEMAND_GRACE_H` (2 h) carry **no** penalty on unused time (§3,
  "On-demand grace"), so a pod that exits well short of the runtime it requested
  is charged for what it ran and nothing more, and a revoke or no-show inside
  that grace costs the consumed time alone. Past the grace an early cancel
  retains a real charge as before.

## 8. What is NOT modeled (gaps for OR guidance)

- **No priorities, weights, or preemption** between users or groups.
- **No fairness mechanism** (no proportional sharing, max-min fairness, lottery,
  or aging) — purely FCFS within static per-group ceilings.
- **No dynamic pricing or quota adjustment** — SU rates and discount schedules
  are static admin-set values; group GPU ceilings are static (date-span overrides
  aside).
- **No waitlist / queue** — no demand signal is captured when potential bookings are turned away
- **No per-user-per-day GPU cap** (`max_gpus_per_user_per_day` is a documented
  deferred feature in CLAUDE.md).
- Concurrency limits are on **peak instantaneous GPU count** and **SU budget**
  (renewable or windowed depending on `su_anchor_mode`), not on throughput over
  time or fairness of access to scarce peak-hour windows.
- Duration cap is **48 hours for non-privileged members** (server-side 400).
  Admins and group managers are exempt from that cap but not from the group's
  `max_reservation_hours` (default 168 h, admin-set per group), which bounds a
  single reservation for **every** caller on every creation path — so a
  non-privileged member's effective cap is `min(48, max_reservation_hours)`.

## 9. Configuration levers an operator actually turns

Per **course (group)**: validity dates, booking horizon (`min/max_days_ahead`),
per-member SU budget (`su_budget`), group-wide SU pool (`pool_su_budget`), SU
budget window mode (`su_anchor_mode`), per-class GPU ceiling (with date-span
boosts), which GPU classes are visible, and — for a group that exists to back
on-demand work rather than calendar bookings — the `on_demand_only` and
`on_demand_auto_join` flags (§7.1). Set a `pool_su_budget` alongside auto-join:
the per-member budget bounds each member separately, so on a group whose
membership grows by itself the pool is the only ceiling on the total.

Per **GPU class**: total GPUs, `su_rate_per_hour` (base SU rate), optional
per-reservation GPU cap, and date-span capacity overrides (each with their own
`available_gpus`).

Per **SU discount schedule**: days-of-week, start/end time-of-day window
(midnight-wrap supported), multiplier (0 = free, 1 = full price), optional date
bounds, active flag, and the list of GPU classes the schedule applies to
(`gpu_class_ids`; a schedule with no attached classes has no effect).

Site-wide: timezone; the borrowing default (`relax_limits`, Admin → Settings —
on/off only) with per-group/per-cohort `relax_mode` overrides (`off`/`on`/`junior`,
unset = inherit); the borrowing time horizon
(`borrow_horizon_hours`, Admin → Settings — how far ahead borrowed headroom may
breach a group/cohort ceiling; `0` disables borrowing, a negative value means no
horizon); and each class's borrowing buffer.

That buffer is a **linear ramp over the horizon**, not a constant:
`relax_min_available` is the GPUs withheld from borrowing for an hour starting
now, `relax_min_available_far` the GPUs withheld for an hour starting at the
horizon, and hours in between interpolate (rounded up). What the buffer protects
is capacity for groups still inside their own ceilings who have not booked yet,
and how much of that demand is still coming is a function of lead time — an hour
twelve hours out will still absorb ordinary bookings, an hour thirty minutes out
will not. `relax_min_available_far` unset means **no ramp** (flat at
`relax_min_available`), which is not the same as zero; with no horizon at all
there is no span to interpolate over and the far value applies at every date.

Two consequences worth stating, because they are what make the ramp safe. The
buffer only ever *falls* as an hour approaches, so an already-admitted booking
can never become retroactively infeasible, and an availability preview can only
be more conservative than the submit that follows it. And because the ramp is
normalised to the horizon, shortening the horizon steepens every class's ramp
rather than merely truncating it.

A group or cohort set to **`junior`** borrows against a **second, fixed** buffer
instead: `relax_min_available_junior`, the same number at every lead time. Set
deeper than the pair above — which is what it is for — it means a junior scope
reaches idle capacity only once it runs past what the ordinary buffer was
protecting, so ordinary borrowers keep first claim on the shallower hours. Being
constant, it satisfies the falling-buffer property above trivially. It is bounded
by the same borrowing horizon, and it has no site-wide or per-scope resolution of
its own — it is per class only. `relax_min_available_junior` unset means the class
opens **no junior lane at all** (a junior scope borrows nothing there), which is a
third distinct null and makes the tier an explicit per-class opt-in rather than
something a class acquires by default.

## 10. Open questions, future work

1. How to set per-group GPU ceilings and open-SU budgets to balance utilization
   vs. fair student access under contention (especially near assignment deadlines).
2. How the booking-horizon window (`min/max_days_ahead`) interacts with
   peak-hour scarcity when multiple courses share a GPU class.
3. Whether an off-peak discount multiplier schedule effectively redistributes
   load, or whether deadline-driven demand is inelastic to pricing signals.
4. Whether the first-come-first-served model with per-group ceilings achieves
   adequate fairness, or whether a max-min fair share or lottery mechanism would
   better serve a multi-course lab environment.
5. Whether `su_anchor_mode = since_creation`, `weekly`, or `quarterly` gives better incentive
   alignment near assignment deadlines compared to the renewable-ceiling (`open`)
   default, and how the cancellation-penalty knobs (window / divisor / cap, see
   §3 "Cancellation penalties") affect no-show rates in practice.
6. Whether per-window budgets need a **carry-forward** (banking an unused quiet
   week toward a heavy one, admitting a booking in window *k* while cumulative
   commitment across windows 0..*k* stays within (*k*+1) × budget). It supports
   bursty project work that a flat per-window cap does not, at the cost of a
   meter that is materially harder to explain. Deferred pending evidence that
   real usage is bursty enough to need it.
7. Whether a spanning reservation should apportion its SU across the windows it
   covers rather than charging its start window whole (see §"Reservations that
   span a window boundary" for why it does not today, and where the rule lives).
