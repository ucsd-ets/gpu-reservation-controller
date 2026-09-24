# Log fields

The canonical field dictionary for **both** the reservation app and the
`gpu-reservation-controller` daemon. An identical copy lives at this same path
in the sibling repo — update both together, the same way `API.md` /
`docs/RESERVATION-API.md` are kept in step. Sharing the dictionary is the point:
a line from either side can be joined on the same key.

`app/log_fields.py` (also duplicated in both repos) is the only thing that
renders this grammar.

All 286 log call sites across the two repos emit this grammar, and
`tests/test_log_grammar.py` (present in both) enforces it: every call must render
through `kv()`, every field must appear in this dictionary, and every `event=`
must appear in that repo's `OBSERVABILITY.md`. Adding a field or a log point
without documenting it fails the suite.

A field marked *(planned)* below has a dictionary entry but no emitter yet.

---

## 1. Grammar

A line is:

```
<ts> <LEVEL> <logger>: actor=<actor> trace=<trace> event=<noun>.<verb> [k=v ...]
```

1. **`event=` is always the first message field** and is the only prose-derived
   one. Lower-snake noun, dot, verb: `event=reservation.created`,
   `event=pod.admitted`, `event=preempt.boundary`.
2. **One concept per field.** No parentheses, slashes, arrows or `a..b` ranges
   inside a value.
   - `pod ns/name` → `ns=jdoe pod=notebook-0`
   - `class=H100(id=2)` → `cid=2 class=H100`
   - `requested=2026-08-01..2026-08-07` → `req_from=2026-08-01 req_to=2026-08-07`
   - `user_id: 3 -> 7` → `old.uid=3 new.uid=7`
   - `reservation #42` → `rid=42`
3. **Absent means omitted.** `kv()` drops a `None` or empty value entirely
   rather than emitting `key=-` or `key=None`. A key that is not on the line was
   not known — which is how a fail-open guard stays distinguishable from one
   that measured zero. The two **envelope** fields are the documented exception:
   a formatter cannot omit a field, so `actor=-` / `trace=-` mean "not known".
4. **Values are quoted only when they need it.** A value containing a space,
   `=` or `"` is double-quoted with `\` and `"` escaped. Free text (`reason`,
   `err`, `detail`) therefore stays on one field without breaking the parse.
5. **Lists are comma-joined with no spaces** (`ids=3,7,9`). **Maps are fanned
   out to one line per key** — the preemption sweep emits one line per GPU
   class rather than one line carrying two dicts.
6. **Booleans** render `true` / `false`. **Timestamps** render ISO-8601;
   attach `tzinfo` before logging a wall-clock value so the offset is carried
   (the controller's datetimes are always UTC-aware; the app should convert via
   `timezone_utils.site_tz()`).
7. **Every value is scrubbed** of non-printable characters by `kv()` — newlines
   above all. Many values are attacker-influenced (submitted usernames, OAuth
   identities, SAML NameIDs, SICAD roster names, pod names and annotations), and
   the chokepoint is the helper so no call site can forget.

---

## 2. Key naming

Two tiers, so the short keys stay memorable rather than becoming a dozen
cryptic abbreviations.

**Tier 1 — typed short ids, for referring to *other* entities.** These recur
across many message families and are the only abbreviations to learn:

| key | entity |
|---|---|
| `uid` | user |
| `gid` | usage group |
| `cid` | GPU class |
| `rid` | reservation |
| `cohid` | cohort |
| `tid` | team |

**Tier 2 — `id=` is the primary key of whatever `event=` names.** The long-tail
entities each appear in one message family, so the event name already
disambiguates and no new abbreviation is needed:

```
event=date_range.updated id=7 from=2026-09-21 to=2026-12-11 propagated=14
event=su_boost.created id=3 uid=91 gid=12 su_offset=250
```

Corollary: on `event=reservation.*` the reservation is `id=`, not `rid=`. `rid=`
appears only where a non-reservation event points at one
(`event=overstay.recorded id=8 rid=42`, and every controller pod line).

**Names travel beside ids as separate fields**, never inside them:
`uid=91 user=jdoe`, `gid=12 group=cse151b`, `cid=2 class=H100`.

**GPU class has three distinct values** and therefore three keys: `cid` (numeric
app id), `class` (display name), `clabel` (Kubernetes node-label value, which is
all the controller could resolve before). Emitting them separately is what makes
an app line and a controller line joinable on class.

**`reason=` is scoped by `event=`.** Several unrelated enums share the key; the
event name says which. A bare `grep reason=` mixes them — scope to the event.
On the admission denials (`reservation.denied`, `lease.denied`,
`preflight.denied`, `continue.denied`) the enum is the **published denial
code** — the same closed vocabulary the client receives as `code` in the
response body — so an app line and a controller line join on the exact
string. It is `reason=` rather than a new key because the code *is* the
"why" this line already had a field for, and two keys for one concept is
what this dictionary exists to prevent.

---

## 3. Envelope

Rendered by the logging formatter, not by call sites.

| field | app | controller |
|---|---|---|
| timestamp / level / logger | `%(asctime)s %(levelname)s %(name)s` | same, level padded to 8 |
| `actor` | set by the auth layer after the session cookie resolves against the database: a username, `imp:<target>:<impersonator>`, or `-` (background work, machine callers, and any request rejected before authentication) | constant `controller` |
| `trace` | the `X-Client-Trace` header, whitelist-matched `[A-Za-z0-9_-]{1,36}`, else `-` | one per unit of work (`fetch-`/`queue-`/`sweep-`/`audit-`/`jit-`/`pod-`/`startup-`), sent outbound on every app call; an inbound header is adopted |

The actor is a database-resolved username, but it is interpolated by the
formatter, which cannot decide to quote — so it still goes through
`sanitize_log_token` (`app/request_context.py`), which drops non-printables and
replaces space, `=` and `"` so the value is safe bare. `kv()` quotes instead,
but only because it renders the message body. An unauthenticated request can no
longer put a name in the field at all.

**The trace crosses the process boundary**, which is what makes an operation
correlatable end to end rather than just an object. The controller mints one per
unit of work (`app/trace.py`) and sends it as `X-Client-Trace` on every call to
the app; the app's middleware already read that header, so no app-side change was
needed. Inbound works the same in reverse — the controller adopts the header off
a push. An inbound value is **whitelist-matched, not escaped**: it is
interpolated by the formatter, which cannot quote it, so a crafted value
containing a newline could otherwise forge log lines. Anything failing the
pattern is dropped in favour of a locally minted id.

Third-party loggers (uvicorn access lines, httpx, the Kubernetes client) do not
follow this grammar and are not expected to.

---

## 4. Dictionary

### Identity and entities

| key | type | meaning |
|---|---|---|
| `id` | int | primary key of the entity `event=` names |
| `uid` | int | user id |
| `user` | string | username (controller: also the pod's namespace) |
| `gid` | int | usage group id |
| `group` | string | usage group name |
| `src_gid` | int | lending usage group id (GPU loan; a loan names two groups, so neither can be plain `gid`) |
| `dst_gid` | int | borrowing usage group id (GPU loan) |
| `cid` | int | GPU class id |
| `class` | string | GPU class display name |
| `clabel` | string | GPU class Kubernetes node-label value |
| `cohid` | int | cohort id |
| `cohort` | string | cohort name |
| `rid` | int | reservation id, referenced from a non-reservation event |
| `tid` | int | team id |
| `extid` | string | SSO provider's stable identity |
| `rev` | string | Alembic schema revision id |
| `poduid` | string | Kubernetes pod UID; also a lease's idempotency key |
| `ns` | string | Kubernetes namespace |
| `pod` | string | Kubernetes pod name |
| `node` | string | Kubernetes node name |

### Outcome

| key | type | meaning |
|---|---|---|
| `status` | int | HTTP status code |
| `reason` | enum, scoped by `event=` | why — see §2 |
| `detail` | quoted string | free-text denial detail (`HTTPException.detail`) |
| `retryable` | bool | on an admission denial, whether waiting can ever admit the *same* request — `false` means only an administrator, or a different ask, resolves it. Pairs with the `reason=` denial code; both are the response envelope's fields verbatim (RESERVATION-API.md §4) |
| `err` | quoted string | exception text |
| `kind` | `booking` \| `on_demand` | reservation flavour |
| `provider` | `local` \| `jupyterhub` \| `google` \| `oidc` \| `saml` | auth path |
| `role` | `admin` \| `auditor` \| `user`, or `member` \| `manager` | scoped by `event=` |
| `scope` | `read_only` \| `read_write` | service-key privilege |
| `bootstrap` | bool | whether `BOOTSTRAP_ADMIN_USERNAME` granted the new account admin (emitted only when it did) |
| `direction` | `offer` \| `request` | which side proposed a GPU loan, and therefore which side approves it |
| `forced` | bool | whether an administrator overrode a GPU-loan accommodation check, leaving a group over its resulting limit (emitted only when they did) |
| `best_effort` | bool | whether an admission is best-effort — no runtime guarantee, zero SU (emitted only when it is) |
| `banner` | bool | whether an effective-date shift announces itself on screen; `false` means this line is the only record of it |

`reason` enums by event: `auth.login_failed` → `no_such_user`, `inactive`,
`wrong_provider`, `account_locked`, `bad_password`, `deactivated` ·
`auth.oauth_state_rejected` → `state_mismatch` ·
`auth.oauth_userinfo_failed` → `fetch_failed`, `missing_claim` ·
`auth.saml_acs_failed` → `missing_nameid`, else python3-saml's own free-text reason ·
`auth.csrf_rejected` → `missing_trace`, `invalid_trace` ·
`auth.session_revoked` → `self`, `admin`, `password_change` ·
`user.impersonation_ended` → `ended`, `target_invalid`, `revoked` ·
`user.bootstrap_skipped` → `admin_exists` · `email.loop_disabled` → `debug_date` ·
`db.migrated` → `fresh`, `adopted_existing`, `upgraded` ·
`reservation.cancelled` → `no-show`, `controller-revoked`, `pod-terminated`,
`superseded` · `overstay.recorded` → `pod-terminated`, `preempted`, `deleted` ·
`k8s.event` → `RuntimeGuaranteed`, `Preempted`, `ReservationCancelled`,
`ReservationReassigned`, `OverstayRelinked`, `ReservationRelinked`.

### Time

| key | type | meaning |
|---|---|---|
| `start` / `end` | ISO-8601 | reservation window |
| `from` / `to` | ISO-8601 date | inclusive span (date ranges, dated admin surfaces) |
| `req_from` / `req_to` | ISO-8601 date | availability span asked for |
| `srv_from` / `srv_to` | ISO-8601 date | availability span served after clipping |
| `boundary` | ISO-8601 UTC | reservation `slot_start` driving a sweep |
| `deadline` | ISO-8601 UTC | no-show deadline |
| `until` | ISO-8601 UTC | runtime-guarantee end instant |
| `at` | ISO-8601 UTC | projected termination-warning kill instant |
| `peak_at` | ISO-8601 UTC | first instant of a window at which `committed` peaks |
| `locked_until` | ISO-8601 | account lockout expiry |
| `date` | ISO-8601 date | the single calendar date a line is about (the effective date under a debug date shift) |
| `days` | int | signed whole-day count a value is shifted by (the debug effective-date offset) |
| `dur_s` | int seconds | duration, where a line carries only one |
| `min_runtime_s` | int seconds | a pod's `galends/minimum-runtime-seconds` ask |
| `lease_dur_s` | int seconds | granted JIT lease length (`min_runtime_s` + buffer) |
| `probe_min` | int minutes | best-effort admission probe window checked for capacity |
| `waited_s` | int seconds | how long a JIT candidate waited before being deleted unplaced |
| `guarantee_s` | int seconds | runtime-guarantee duration |

### Capacity

| key | type | meaning |
|---|---|---|
| `gpus` | int | GPU count requested or held |
| `reserved` | int | GPUs reserved on the reservation |
| `free` | int | GPUs free |
| `used` | int | GPUs in use |
| `total` | int | physical GPUs allocatable |
| `app_gpus` / `phys_gpus` | int | capacity-audit counts |
| `node_free` | int | largest single-node free GPUs for a class |
| `claimed` | int | GPUs already claimed earlier in the same admission batch, netted off `node_free` (emitted only when non-zero) |
| `demand` | int | GPUs demanded at a boundary, per class |
| `committed` | int | peak GPUs a class's reservations commit, placed or not, over a window (on-demand guard 4) |
| `kills` | int | victims selected at a boundary |
| `short` | int | GPUs still short after preempting every eligible overstayer |
| `sweep` | `A` \| `B` | preemption sweep phase (`phase=` is the pod lifecycle phase) |
| `phase` | `Pending` \| `Running` \| `Succeeded` \| `Failed` \| `Unknown` | pod lifecycle phase |
| `risk` | float 0..1 | termination-warning risk score |
| `gstatus` | `guaranteed` \| `overstay` | live guarantee standing |
| `borrowed` | int | peak GPUs admitted only because borrowing raised a ceiling (emitted only when it did) |
| `borrowed_gpu_h` | float | the same, integrated over the reservation's hours |
| `borrow_mode` | `off` \| `on` \| `junior` | resolved borrowing mode at admission |
| `borrow_tier` | `none` \| `group` \| `cohort` \| `both` | which ceiling the borrowing carried |

The four `borrow*` fields answer "did this reservation need borrowing to be
admitted?" — not "was borrowing available?", which is nearly always true and
therefore says nothing. They appear only on the reservation-creation events
(`reservation.created`, `reservation.lease_created`, `reservation.continued`)
and only when borrowing was **load-bearing**, i.e. the reservation would have
been refused under its own group/cohort ceilings. A creation line carrying no
`borrowed=` fit inside its ceilings. Only the app emits them; the controller is
not told which reservations borrowed, because it enforces no ceilings of its own.

### Service Units

| key | type | meaning |
|---|---|---|
| `su` | float | SU cost at creation |
| `su_user` / `su_group` | float | SU charged to the member / the group pool |
| `su_retained` | float | SU kept for already-consumed time |
| `su_offset` | float | signed per-member budget offset |
| `waived` | bool | whether a cancellation penalty was waived |

### Updates

| key | type | meaning |
|---|---|---|
| `chg` | comma list | which fields an update changed |
| `old.<field>` / `new.<field>` | scalar | before/after value of one changed field |

Rendered by `log_fields.changes({field: (old, new)})`, splatted into `kv()`.

A redacted field (password, Quill-managed HTML, `logo_data`, SMTP password)
appears in `chg=` with **no** `old.`/`new.` pair — pass `log_fields.REDACTED`
instead of a tuple. This one rule replaces the older `password changed` and
`[field changed]` conventions. A no-op update omits `chg=` entirely rather than
emitting the literal `no-op`.

Inside a change pair, `None` renders as `null` rather than being omitted — the
one deliberate exception to rule 3. A field going from unset to set must show
both sides, whereas everywhere else an absent value means "not known".

### Availability, bulk and counts

| key | type | meaning |
|---|---|---|
| `buckets` | int | hourly buckets returned (`0` = the window clipped everything) |
| `privileged` | bool | whether admin/manager booking rules applied |
| `n` | int | rows written by a bulk operation |
| `ids` | comma list of int | ids affected in bulk |
| `count` | int | fallback count — prefer a specific key wherever the event distinguishes them |
| `active` / `cancelled` | int | reservations by status in a fetch result |
| `watched` | int | reservations armed with a no-show deadline |
| `pods` | comma list | pod identifiers a line is reporting about (`ns.name`) |
| `fails` | int | consecutive failure counter (failed logins, watch-stream reconnects) |

### Auth, SICAD and Kubernetes traces

| key | type | meaning |
|---|---|---|
| `identity` | string | full OAuth identity before domain stripping |
| `nameid` | string | full SAML NameID before domain stripping |
| `domain` | string | domain part checked against the allowlist |
| `allowed` | comma list | configured domain allowlist |
| `errors` | comma list | python3-saml validation error codes |
| `acct_provider` | string | the account's *actual* `auth_provider`, on a `reason=wrong_provider` denial (`provider` stays the attempted path) |
| `course_id` | string | SICAD/AWSEd courseID backing a roster sync |
| `team` | string | SICAD team display name |
| `team_key` | string | SICAD `uniqueName`, the stable team upsert key |
| `src_role` | string | SICAD's upstream role value |
| `booking_ref` | string | `res-<id>` annotation value, where the raw string is the thing asserted |
| `gate` | string | SchedulingGate name |
| `selector` / `rv` / `timeout_s` | string / string / int | Kubernetes LIST+WATCH parameters |
| `watch_event` | `ADDED` \| `MODIFIED` \| `DELETED` | raw watch event type |
| `path` | string | filesystem path |
| `patch` | string | which patch a `k8s.patch_pod` is applying (`toleration`, `runtime_guarantee`, `guarantee_status`, `termination_warning`, `termination_warning_clear`, `gate_remove`) |
| `purpose` | string | why a LIST was issued (`watch_seed`, `tolerated_snapshot`, `gpu_inventory`) |
| `dropped` | int | watch events discarded so far by the bounded queue (running total) |
| `tol_key` / `tol_value` | string | the toleration being applied |
| `resource` / `value` | string / string | the Kubernetes resource name and the malformed value, on an unparseable allocatable |
| `nodes` | int | nodes carrying a GPU class, in a node-inventory line |

### Entity naming and provenance

| key | type | meaning |
|---|---|---|
| `name` | string | human-readable name of the entity `event=` names (group, cohort, class, date range, service key, …) |
| `source` | string | which subsystem drove a mutation (`sicad` on roster-driven user/membership changes) |
| `mode` | string | which variant of an operation ran (`kubeconfig` \| `in_cluster` for k8s auth; `seed` \| `resume` for a watch open; `created` \| `reacquired` \| `takeover` \| `renewed` for a singleton-lease write) |
| `section` | string | which part of a multi-part operation a line reports (config import/export) |
| `guard` | int | which JIT admission guard held a candidate (1, 3, 4, 5) |
| `annotation` | string | the annotation key a value was read from, on a parse failure |
| `url` | string | outbound URL being fetched |
| `level` | string | configured log level, at startup |
| `path` | string | filesystem path, or the configured `ROOT_PATH` |
| `headers` | comma list | header names a feature reads, on the test-harness warning |
| `loops` | comma list | background loops started |
| `groups` | comma list | usage-group names a sweep is about to process |

### Selection, counts and diagnostics

| key | type | meaning |
|---|---|---|
| `candidates` | int | size of a pool offered for selection (preemption victims, on-demand admission) |
| `selected` / `granted` | int | how many of them were chosen |
| `reservations` | int | reservations in the occupancy map |
| `fallback` | string | what was used instead when a delegated call was unavailable |
| `target` | string | which snapshot failed (`pods`, `node_capacity`) |
| `task` | string | asyncio background-task name (`pod-watch`, `queue-processor`, …) on a supervision line |
| `holder` | string | a coordination Lease's `holderIdentity` (the controller pod claiming singleton status) |
| `age_s` | int seconds | how long ago a Lease was last renewed |
| `source_id` / `source_kind` | int / `booking` \| `on_demand` | the reservation a *continue* was carried forward from |
| `superseded` | bool | whether that source still held future time and was cancelled penalty-exempt |
| `created` / `updated` | int | rows created / updated by one section of a config import |
| `propagated` | int | records re-dated by a date-range edit |
| `queued` | int | entries in the reserved work queue |
| `cancellations` / `owner_changes` / `upserts` | int | deltas applied by an inbound push |
| `classes` / `reservations` | int | counts of GPU classes / reservations after a refresh |
| `clabels` | comma list | GPU class labels entering or leaving the on-demand pause set |
| `overcommitted` | bool | whether a capacity mismatch is app-side-over-physical (the direction that pauses admission) |
| `max_gpus` | int | ceiling written by a group/cohort GPU limit |
| `offset_min` | int | reminder-email offset, in minutes before the window |
| `retry_s` | int seconds | how long until the next attempt |
| `submitted` / `deleted` | ISO-8601 UTC | when a JIT candidate's pod was created / removed |
| `date_ranges` / `gpu_classes` / `cohorts` / `usage_groups` / `discount_schedules` | int | per-section counts on a config export |
| `lookahead_days` | int | calendar days ahead a reservation fetch covered |

---

## 5. Adding a field

1. Add it here first — this file is the dictionary, not a description of what
   the code happens to do.
2. Reuse an existing key if the concept already has one. Two keys for one
   concept is the failure mode this document exists to prevent.
3. If the value is a map, fan it out to one line per key rather than inventing
   a nested encoding.
4. If the value is attacker-influenced, it needs no special handling — `kv()`
   scrubs everything. Do **not** pre-format it into the message string, which
   would bypass the chokepoint.
