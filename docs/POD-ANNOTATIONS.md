# Pod annotations — in-pod consumer reference

Audience: anyone building an **in-pod** consumer of the GPU reservation
controller's status — a JupyterLab extension, a VS Code status-bar item, a shell
prompt/MOTD, a checkpointing wrapper.

The controller stamps every pod it admits with a small set of annotations
describing **what reservation the pod is running under**, **how long its GPU
access is guaranteed**, and **whether it is currently at risk of being
terminated to free capacity**. Everything below is readable from inside the
container through a downward-API volume — no Kubernetes API access, no service
account, no network call to the reservation app.

> **All of these annotations are informational.** Nothing is enforced through
> Kubernetes (`spec.activeDeadlineSeconds` is never set), and the controller
> never reads them back to make a decision — it recomputes everything live from
> reservation state. Treat them as a best-effort heads-up, not a contract. See
> [Robustness rules](#8-robustness-rules) for what that means in practice.

---

## 1. Wiring: exposing annotations to the container

Project **all** of `metadata.annotations` into a single file, and each of the
three `termination-warning-*` keys into a file of its own — those are the ones a
consumer polls, and a file per key is readable without the parser below. Add to
the pod spec (this is the workload's own spec — JupyterHub
`singleuser.storage.extraVolumes`, a VS Code dev-container pod template, etc. —
the controller does not add it):

```yaml
spec:
  volumes:
    - name: podinfo
      downwardAPI:
        items:
          - path: annotations
            fieldRef:
              fieldPath: metadata.annotations
          - path: termination-warning-at
            fieldRef:
              fieldPath: metadata.annotations['galends/termination-warning-at']
          - path: termination-warning-risk
            fieldRef:
              fieldPath: metadata.annotations['galends/termination-warning-risk']
          - path: termination-warning-message
            fieldRef:
              fieldPath: metadata.annotations['galends/termination-warning-message']
  containers:
    - name: notebook
      volumeMounts:
        - name: podinfo
          mountPath: /etc/podinfo
          readOnly: true
```

That yields four files under `/etc/podinfo`: `annotations` (the whole map) plus
`termination-warning-at`, `termination-warning-risk` and
`termination-warning-message`.

**Use a volume, not `env:`.** Downward-API *environment variables* are resolved
once at container start and never change; every annotation here is written
*after* the pod starts and some change during its life. A downward-API *volume*
is refreshed by the kubelet.

### File format

One line per annotation, sorted by key, rendered by the kubelet as Go `%q`:

```
galends/admitted-at="2026-08-11T17:02:11Z"
galends/booking-reference="res-4812"
galends/gpu-class-name="H100"
galends/guarantee-status="guaranteed"
galends/guaranteed-until="2026-08-11T20:00:00Z"
galends/pod-runtime-limit-seconds="10800"
galends/reservation-end="2026-08-11T19:00:00Z"
galends/reservation-gpu-count="4"
galends/reservation-kind="booking"
galends/reservation-start="2026-08-11T17:00:00Z"
kubectl.kubernetes.io/last-applied-configuration="{\"apiVersion\":\"v1\",...}"
```

Every value is double-quoted, and `"`, `\`, newlines and tabs inside a value are
backslash-escaped — so **one annotation is always exactly one line**, even for
values that contain newlines (`last-applied-configuration` routinely does).
Split each line on the *first* `=`, then unquote. A minimal parser:

```python
import re

_ESCAPES = {'"': '"', "\\": "\\", "n": "\n", "t": "\t", "r": "\r"}

def parse_downward_annotations(path="/etc/podinfo/annotations") -> dict[str, str]:
    out = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            key, sep, raw = line.rstrip("\n").partition("=")
            if not sep or len(raw) < 2:
                continue
            out[key] = re.sub(
                r"\\(.)", lambda m: _ESCAPES.get(m.group(1), m.group(1)), raw[1:-1]
            )
    return out
```

### The three single-key files

A subscripted `fieldPath` projects one annotation's value on its own, and those
files are deliberately *not* in the format above:

```console
$ cat /etc/podinfo/termination-warning-at; echo
2026-08-21T17:30:16Z
$ cat /etc/podinfo/termination-warning-risk; echo
0.33
```

- **The value is raw** — no surrounding quotes, no backslash escaping, and no
  trailing newline. `$(cat …)` *is* the value; nothing has to unquote it.
- **An absent annotation is an empty file, not a missing one.** The kubelet
  creates all four files when the pod starts and writes an empty value for a key
  the pod does not carry — which is the normal state for these three, since they
  are present only while the pod is at risk (§2). Test with `-s` ("exists and is
  non-empty"), never `-f` or `-e`: those are true from the pod's first second.
- **All four files change together.** They are one volume, so the kubelet swaps
  the whole payload atomically — the single-key files can never disagree with
  each other or with the map.
- **Only these three earn a file.** Everything else in §2 is read as a set, when
  something renders — that is what the map is for. The warning keys are the ones
  that appear and vanish on their own while a job runs, and the thing most likely
  to be watching them is a shell script with no parser in it.

### Update semantics

- The kubelet refreshes the file on its sync loop — **up to ~60 s** (the
  kubelet's `syncFrequency`, default `1m`) after an annotation changes. Budget
  for this on top of the controller's own cadence (§7).
- The mount is the usual `..data` symlink swap: every file in `/etc/podinfo` is
  a symlink, and the whole set is replaced atomically on each update. An
  `inotify` watcher must watch the **directory** (`IN_MOVED_TO` / `IN_CREATE` on
  `..data`), not a file inode, or it will only ever fire once. Polling every
  15–30 s is simpler and perfectly adequate for these fields.
- Reads are atomic — you never see a half-written file — but the set is only
  *eventually* consistent as a group. Re-read the whole file each time and
  recompute state from the snapshot rather than diffing individual keys.

### Bailing out of a shell loop on the warning

A batch job that processes work in units — shards, epochs, images, sweeps —
usually needs nothing more than "stop starting new units once the controller
warns me". The single-key files make that a `test` against
`termination-warning-at`, checked between units:

```bash
#!/usr/bin/env bash
set -euo pipefail

PODINFO=${PODINFO:-/etc/podinfo}
BAIL_AT_RISK=${BAIL_AT_RISK:-50}    # percent; 0 bails on any warning at all

# Has the controller warned this pod?  -s is the operator that matters: the
# kubelet creates the file when the pod starts and leaves it EMPTY while the
# annotation is absent, so -f and -e are true from the first second and a loop
# guarded on either would bail immediately, on every run.
warned() { [[ -s "$PODINFO/termination-warning-at" ]]; }

# "0.33" -> 33.  The value always has two decimals, so deleting the dot scales
# by 100; 10# forces base 10, without which bash reads the leading zero of
# "008" as octal and errors out on a risk of 0.08 or 0.09.  Anything
# unreadable or unparseable counts as maximum risk (§8 rule 7).
risk_pct() {
    local raw
    raw=$(cat "$PODINFO/termination-warning-risk" 2>/dev/null) || raw=
    if [[ $raw =~ ^[01]\.[0-9]{2}$ ]]; then echo $(( 10#${raw/./} )); else echo 100; fi
}

stopped_before=
for shard in "${SHARDS[@]}"; do
    if warned && (( $(risk_pct) >= BAIL_AT_RISK )); then
        stopped_before=$shard
        break
    fi
    process_shard "$shard"          # each shard commits its own output
done

if [[ -n $stopped_before ]]; then
    printf '%s\n' "$(cat "$PODINFO/termination-warning-message")" >&2
    printf 'stopped before %s; rerun to pick up the rest\n' "$stopped_before" >&2
    exit 75                         # EX_TEMPFAIL — ask the submitter to requeue
fi
```

Four things about that loop:

- **Test every iteration; do not latch.** Warnings retract (§8 rule 2) — the
  user re-books, the incoming reservation no-shows, the pod is re-linked — and
  the file goes empty again. Re-running `warned` each time round is what lets
  the job carry on when that happens.
- **Check between units, never inside one.** This is the shell analogue of §6.5's
  step boundary: `break` is safe here only because `process_shard` finishes what
  it started. A unit longer than the notice (§7 — budget ~10 minutes, and there
  is no floor) needs the check moved inside it, or its own checkpoint.
- **Bailing is not the only option, and exiting is a legitimate one.** `break`
  plus `exit 75` hands the remaining work back to whatever submitted the job,
  frees the capacity the incoming reservation wanted, and skips the 30 s grace
  period entirely (§5). An interactive session should do the opposite: warn the
  user and keep going, since the pod may well survive (§8 rule 3).
- **Show the message, don't parse it.** `termination-warning-message` is a
  finished English sentence in the deployment's display zone (§5), so echoing
  it is right and reading a timestamp back out of it is not. If the script needs the
  instant, use `termination-warning-at`, which is UTC:
  `secs_left=$(( $(date -u -d "$(cat "$PODINFO/termination-warning-at")" +%s) - $(date -u +%s) ))`
  — and treat a negative result as "may be stopped at any time" (§8 rule 3),
  not as "already dead".

---

## 2. Annotations the controller writes

| Key | Written | Value | Lifecycle |
|-----|---------|-------|-----------|
| `galends/booking-reference` | at admission | `res-<id>`, e.g. `res-4812` — the reservation the pod is running under | Rewritten when the pod is re-linked to another reservation (adoption / lease→booking merge). Its **presence is the signal that the controller manages this pod**. |
| `galends/guarantee-status` | at admission, refreshed | `guaranteed` \| `overstay` | `guaranteed` at admission and again on a re-link; flips to `overstay` once the guarantee lapses, noticed on the controller's next queue tick (§7) — which is also how a best-effort pod reaches `overstay` (§3.1). Never removed while the pod lives. |
| `galends/guaranteed-until` | at admission, refreshed | absolute UTC instant, `YYYY-MM-DDTHH:MM:SSZ` | Kept *live* while `guaranteed` — it can move **later** if the user books an abutting follow-on window. Rewritten to the new reservation's guarantee on a re-link. Once `overstay` it is frozen at its now-past value. |
| `galends/pod-runtime-limit-seconds` | at admission and on each re-link | integer seconds, e.g. `10800`; `0` for a best-effort pod (§3.1) | The guaranteed *duration* at the moment it was recorded — rewritten when the pod is re-linked to another reservation, but **not** refreshed as the guarantee grows. For a countdown, use `guaranteed-until`, not this. |
| `galends/reservation-kind` | at admission, refreshed | `booking` \| `on_demand` \| `best_effort` | Describes the reservation the pod is *currently* linked to. Changes when the pod is re-linked to a **different** reservation, and is re-stamped when its **current** one is altered in place (a lease window extended). |
| `galends/reservation-start` / `galends/reservation-end` | at admission, refreshed | absolute UTC instant, same format | The reservation's **own** window — not the guarantee end. `-end` moves later when the reservation is extended in place. Same lifecycle otherwise. |
| `galends/reservation-gpu-count` | at admission, refreshed | integer, e.g. `4` | GPUs the *reservation* holds, not what the pod requested. Same lifecycle. |
| `galends/gpu-class-name` | at admission, refreshed | display name, e.g. `H100` | Same lifecycle. |
| `galends/admitted-at` | at first admission only | absolute UTC instant, same format | Written once and never rewritten — a re-link is not a new admission. Never removed while the pod lives. |
| `galends/termination-warning-at` | while at risk | absolute UTC instant, same format | **Appears and disappears.** Present only while the pod is in the at-risk pool; all three warning keys are removed together when the risk clears. |
| `galends/termination-warning-risk` | while at risk | decimal string in `(0, 1]`, 2 dp, e.g. `0.33` | Same lifecycle. |
| `galends/termination-warning-message` | while at risk | human-readable English sentence | Same lifecycle. Rendered deterministically from the other two plus the cause — the start of the booking that needs the GPUs (which is later than `-at` for a proactive kill), or holding GPUs free for on-demand jobs, where no booking is involved; safe to display verbatim. **The one value here not in the UTC wire format**: its instants read in the deployment's display zone (e.g. `2026-08-21 10:30:16 PDT` — or `… UTC` where the deployment sets none, §5), because it is prose for a person rather than a value to parse. Parse `-at` instead. |

### What each one means

**`galends/reservation-*` — what the pod is running under.** The `booking-reference`
names *which* reservation; these describe it.

`reservation-kind` is the one that changes your copy the most. `booking` means the
user reserved this window themselves, through the reservation app. `on_demand`
means no reservation was open when their pod started, so the controller requested a
short lease on their behalf, just-in-time — that lease is a real reservation, SU is
charged for it, and it is protected by the same runtime guarantee, but the user
never asked for it and will not recognise it from their calendar. Say so plainly
("started on an on-demand lease until 16:10") rather than calling it "your
reservation". `best_effort` means the pod asked to run with no guarantee at all
(§3.1): nothing is reserved, nothing is charged, and the pod is preemptible from
its first second.

`reservation-start`/`-end` are that reservation's **own** window, which is *not*
the same as `guaranteed-until`: the guarantee runs to the end of the back-to-back
chain, so a user with three abutting bookings has one window here and a
guarantee three windows long. Show the window for "what you booked" and the
guarantee for "how long you are safe".

`reservation-gpu-count` is how many GPUs the *reservation* holds — compare it
against the pod's own `nvidia.com/gpu` request to tell a user they are using 1 of
the 4 GPUs they booked. It is not a per-pod figure: a user running several pods
under one booking sees the same count on each.

**`galends/admitted-at` — when this pod got its GPU.** Written once, on the pod's
first admission, and never rewritten — including when the pod is re-linked to a
different reservation, and including when its reservation is extended in place.
That makes it the right anchor for a session-elapsed clock; `reservation-start`
is not (it moves on a re-link, and can predate the pod by hours when the pod
started mid-window).

**`galends/guaranteed-until` — the runtime guarantee.** The instant until which
this pod's GPU access is protected. It is the end of the pod's current
reservation window, extended through any directly back-to-back follow-on
bookings by the same owner for the same GPU class and GPU count (and the same
usage group, on a cluster that matches pods to reservations by group). Inside the
guarantee the pod is **never** preempted by the controller, however severe the
cluster shortfall.

Past that instant the pod is not killed either — it keeps running until a
booking starting on its GPU class actually needs the capacity, or, on a cluster
that holds GPUs free for on-demand jobs, that goal does (below). That is the
`overstay` state: still running, no longer protected.

**`galends/termination-warning-at` — the projected kill instant.** Present only
while the controller has identified this pod as an eligible victim in a GPU class
that is short on capacity. The value is the **earliest** instant the pod could
actually be deleted, whatever the cause. Not a scheduled execution time: the pod
may well survive it (§8).

There are two causes, and a consumer does not need to tell them apart — the key
means the same thing either way, and where both apply the **sooner** instant is
the one written:

- **An upcoming reservation boundary** needs the capacity. The instant is the
  start of the sweep's kill window, which opens `PREEMPTION_LEAD_MINUTES`
  (default 15) *before* the boundary, but never before the pod's own guarantee
  ends.
- **Anticipatory headroom** — the deployment holds a fixed percentage of each
  GPU class free for on-demand jobs that have not arrived yet
  (`HEADROOM_TARGET_PERCENT`, off by default). Here the instant is a **notice
  deadline**: `HEADROOM_NOTICE_MINUTES` from when the pod entered the at-risk
  pool. The pod is *not eligible to be killed at all* until it passes, so this is
  a firmer floor than the boundary case — and it is **sticky**, so it does not
  drift forward while the pod stays at risk.

In both cases the pod must already be past its runtime guarantee to be at risk;
a pod inside its guarantee is never a victim.

**`galends/termination-warning-risk` — how likely.** The fraction of the
eligible pool that has to be killed, `min(1, shortfall / pool_gpus)`.
`1.00` means the whole pool is needed and the pod will almost certainly be
picked; `0.20` means roughly a one-in-five chance. Pool *membership* is exact;
the number models uniform-random victim selection, which is the controller's
local fallback — when victim selection is delegated to the reservation app (the
default), the app's policy decides instead, and it currently takes best-effort
pods before leases and leases before bookings (§3.1). Render it as a coarse band
("possible" / "likely"), not as a precise probability.

## 3. Annotations the controller *reads* (job inputs)

Set by whoever creates the pod; the controller consumes them and never writes
them. Worth surfacing read-only in a UI, since they explain admission behaviour:

| Key | Purpose |
|-----|---------|
| `galends/minimum-runtime-seconds` | **Positive** integer. Required (unless the pod is best-effort, §3.1) for a pod to be admitted on demand, under a just-in-time lease, when none of its owner's reservations can take it — see *Wait or lease* below. Also sizes that lease: the runtime plus a buffer (`ONDEMAND_LEASE_BUFFER_MINUTES`, default 10). A pod without it waits for a matching reservation if its owner holds one (§5.4), and otherwise is told nothing will admit it (`NoReservation`, §5.3). `0` is **not** a way to ask for no guarantee — it is rejected with a `pod.annotation_invalid` warning; use `galends/runtime-guarantee` below. |
| `galends/runtime-guarantee` | `none` — "admit me with no runtime guarantee at all". See §3.1. Any other value is ignored with a `pod.annotation_invalid` warning in the controller's log. On a cluster without best-effort admission the annotation is ignored whatever it says, and not even read, so nothing is logged — the pod's own Events still say so wherever it changes what happens (§5.3). |
| `galends/usage-group` | The usage group a JIT lease is created under. Required for JIT eligibility unless the deployment identifies the group through a pod *label* instead (`REQUIRED_GROUP_LABEL`). |

A value the controller has to ignore is reported on the pod itself, not only in
the controller's log, wherever ignoring it changes what happens to the pod — see
`AnnotationIgnored` and `NoReservation` in §5.3.

**Wait or lease.** These annotations decide whether a pod *can* be admitted on
demand; whether it *is* depends first on its owner's bookings. The controller
routes a pod by taking the first of these that applies:

1. A booking of the owner's for the pod's GPU class (and usage group, where the
   cluster matches by group) that is open now, or opens within
   `ONDEMAND_HORIZON_MINUTES` (default 30), and has enough GPUs free for the pod:
   the pod waits for it — and is admitted at once if it is already open — **even
   if it qualifies for an on-demand lease**.
2. Otherwise, if the pod qualifies for on-demand admission, a lease is requested
   for it straight away. Each retry re-checks step 1, so a pod whose booking comes
   within the horizon while it waits switches to waiting for the booking.
3. Otherwise, if any booking of the owner's matches at all — further off, full,
   or holding fewer GPUs than the pod asks for — the pod waits for that (§5.4);
   on a cluster that offers on-demand admission its Event also says why it
   cannot go on demand instead.
4. Otherwise nothing will ever admit the pod as written: `NoReservation`, or
   `UnknownGpuClass` if its class label is wrong (§5.3).

A pod that starts on a lease before its owner's booking opens is moved onto the
booking as it opens, and the lease is retired (§5, §8 rule 6).

The pod's `gpu-class` **label** (not an annotation, so it is not in this file
unless you also project `metadata.labels`) names the GPU class.

A deployment may configure cluster-wide stand-ins for the minimum runtime and
the usage group (`DEFAULT_MINIMUM_RUNTIME_SECONDS` / `DEFAULT_USAGE_GROUP`), in
which case a pod carrying neither annotation is still JIT-eligible. Neither default is written
back to the pod, so a UI cannot tell from the annotations alone whether a value
came from the pod or from the deployment — read the absence of an annotation as
"whatever the cluster defaults to", not as "unset".

### 3.1 Running without a guarantee: `galends/runtime-guarantee: none`

Ordinarily a pod with no open reservation gets a **lease** requested on its
behalf: real reserved time, protected by a runtime guarantee, charged in Service
Units. That is the right trade for most work, and the wrong one for a job that
would rather start now and pay nothing — a short experiment, a notebook someone
is watching, anything that checkpoints.

```yaml
metadata:
  annotations:
    galends/runtime-guarantee: "none"
    galends/usage-group: "cse151b"        # still required
```

What that buys, and what it costs:

- **No Service Units are charged.** The reservation minted for the pod is a
  zero-length stub whose only job is to record that the pod was admitted.
- **No guarantee, from the first second.** The pod is a preemption candidate
  immediately, and nothing protects it — no minimum runtime applies. It is not
  preempted *arbitrarily*, though: the controller reclaims GPUs only when a
  booking starting on the pod's GPU class needs them, or — on a cluster that
  holds a share of each class free for on-demand jobs (`HEADROOM_TARGET_PERCENT`,
  off by default) — to restore that share. Another on-demand job arriving does
  not by itself displace anyone. When GPUs are reclaimed, which eligible pods go
  is by default the reservation service's choice, and its policy takes
  best-effort pods before leases and leases before bookings; if the controller
  cannot reach the service (or is configured not to ask), it picks at random
  among the eligible pods, best-effort or not. (The `BestEffortAdmitted` Event
  puts this more loosely — "as soon as any other job needs the capacity"; this
  is the precise version.)
- **It still has to fit.** Group membership, GPU-class access, per-reservation
  GPU caps and group validity dates all apply exactly as for a lease, and the
  controller will not admit the pod unless a node physically has the GPUs free.
  A refusal arrives as an `OnDemandLeaseDenied` Event (§5.1), same as any other,
  and a paused class as `OnDemandAdmissionPaused` (§5.2) — except the
  short-of-hardware pause, which does not hold a best-effort pod. It books no
  capacity in the reservation service, so a gap between that service's count
  and the hardware does not apply to it; a class with no node available at all
  still does.

`galends/minimum-runtime-seconds` is not required, and is ignored for sizing if
present — there is nothing to size. The two compose without conflict: a pod may
declare a runtime it *hopes* for and still waive the guarantee.

**How it reads once admitted.** Three annotations look odd and are not:
`galends/guaranteed-until` is the moment the pod was admitted,
`galends/pod-runtime-limit-seconds` is `0`, and `galends/guarantee-status` is
`overstay` for the rest of the pod's life. All three are literally true — the
guarantee ended when it began. The one wrinkle is that the admission write sets
`guarantee-status` to `guaranteed`, as every admission does, and it flips to
`overstay` only at the controller's next queue tick (§7), so for the first few
minutes it reads `guaranteed`; a UI that compares `guaranteed-until` with the
clock, as §4 does, is right from the first second. `galends/reservation-kind:
best_effort` is what distinguishes this from a job that really did outstay a
guarantee, so a UI should check that first and show something like
*"best-effort — no guarantee"* rather than *"overstaying"*. The admission Event
is `BestEffortAdmitted`, not `RuntimeGuaranteed`.

If the pod's owner then holds a booking of the class that is open with room
for the pod, the pod is moved onto it and gains that booking's guarantee
(`OverstayRelinked`, §5) — the same rescue as any other pod past its guarantee.

This is an opt-in the **deployment** must also enable (`BEST_EFFORT_ENABLED`);
where it is off, the annotation is ignored and the pod is handled as before.

### The whole `galends/` namespace leaves the cluster

The keys above are the ones the controller itself acts on, but they are not
the only ones it *sends*. When a pod is waiting for a just-in-time lease and the
deployment delegates that decision to the reservation app
(`ONDEMAND_DELEGATE_ADMISSION`), the controller offers the pod to the app along
with **every** annotation it carries under the `galends/` prefix — its own keys
from §2 included — plus the pod's creation time. The app may weigh any of them
when deciding which waiting pods to admit; a key it does not recognise is simply
ignored.

Two consequences worth knowing:

- **Treat `galends/` as a shared namespace, not scratch space.** An annotation
  you park there on a Pending pod is sent off-cluster to the reservation app.
  Anything private to your own tooling belongs under a prefix of your own, which
  the controller neither reads nor forwards.
- **Only a large one is clipped.** The controller sends at most 32 `galends/`
  keys (lowest-sorting first) and 1024 characters per value, so a pod with an
  unusual number of them, or a very long value, is offered a bounded view of
  itself. Nothing about admission changes as a result — the ask is unaffected —
  but a policy reading a truncated value sees the truncation.

Nothing here applies to an admitted pod: the annotations are read when a lease is
being sought, not while a job runs.

---

## 4. Deriving a display state

Everything a consumer needs is four fields. Recompute on every read:

```python
from datetime import datetime, timezone

def status(ann: dict[str, str], now=None):
    now = now or datetime.now(timezone.utc)
    ref = ann.get("galends/booking-reference")
    if not ref:
        return "unmanaged"                     # not admitted (yet) by the controller

    until = _parse_utc(ann.get("galends/guaranteed-until"))   # ...Z -> aware datetime
    in_guarantee = until is not None and until > now          # trust the clock, not the label
    at_risk = "galends/termination-warning-at" in ann

    if in_guarantee and not at_risk:
        return "guaranteed"      # protected until `until`
    if in_guarantee and at_risk:
        return "guarantee-ending"# protected now, flagged to be reclaimed when it lapses
    if at_risk:
        return "at-risk"         # past guarantee AND its GPUs wanted (a booking, or headroom)
    return "overstay"            # past guarantee, nothing wants the capacity right now
```

Suggested presentation:

| State | Tone | Copy |
|-------|------|------|
| `guaranteed` | neutral / green | "GPU reserved for another 2 h 14 m (until 20:00 UTC)." |
| `guarantee-ending` | amber | "Reservation ends 20:00 UTC; another job is booked to start then — this pod may be stopped from 19:45 UTC. Extend or re-book to keep the GPU." |
| `overstay` | neutral / grey | "Running past your reservation. The GPU is free for now, but this job can be stopped at any time to make room." |
| `at-risk` | red | Show `galends/termination-warning-message` verbatim, plus a countdown to `termination-warning-at`. |
| `unmanaged` | none | Show nothing. |

Countdowns should target `guaranteed-until` (state `guaranteed`) or
`termination-warning-at` (state `at-risk`), computed client-side against the
current time. Every timestamp you parse is UTC with an explicit `Z`; parse as
timezone-aware and render in the user's local zone.

The single exception is `galends/termination-warning-message`, whose instants
are *already* rendered for display — it is a finished sentence for a person, not
a field, which is why the advice for it is "display verbatim" and never "parse".
Its zone is whatever the controller deployment configures for display, and UTC
where it configures none (§5), so if your users are somewhere else, build your
own copy from `-at` and `-risk` rather than showing the message.

The state above is orthogonal to `reservation-kind`, which sets the *noun* in
that copy: a `guaranteed` pod on a `booking` has "your reservation until 20:00",
the same pod on an `on_demand` lease has "an on-demand lease until 20:00". Both
are real reservations charged in SU, so neither is "free" or "best-effort"
capacity — the difference is only whether the user asked for it. A
`best_effort` pod is the one that genuinely is: nothing reserved, nothing
charged, no guarantee at any point. It is only ever `overstay` or `at-risk` by
the logic above, so give it its own copy — *"best-effort — no guarantee"* — not
the overstay text, which would tell the user they ran past a reservation they
never had (§3.1).

Three things to *avoid* claiming in copy: don't say the job "will be terminated
at" the warning time (it is the earliest possible moment, not a schedule); don't
say an overstaying job is "over its limit" or "in violation" — overstay is a
normal, permitted mode; and don't describe an `on_demand` lease as the user's own
booking, since it will not appear in their calendar as one.

---

## 5. What the controller does when the moment arrives

A preempted pod is **deleted** (`DELETE` on the pod, normal graceful
termination — `SIGTERM`, then `terminationGracePeriodSeconds`). It is not
evicted, not restarted in place, not paused. A consumer that wants to checkpoint
should do it on the warning, not on `SIGTERM` — the grace period is whatever the
pod spec sets, typically 30 s. §6 covers how to do that for a PyTorch job.

The controller also emits Kubernetes **Events** against the pod
(`RuntimeGuaranteed` at admission, stating the guarantee — and again each time
the pod is re-linked, immediately before the re-link Event, stating the new one —
or `BestEffortAdmitted` in its place for a pod admitted with no guarantee
(§3.1), `OverstayRelinked` when a pod running past its
guarantee is re-linked to a reservation you have since booked,
`ReservationRelinked` when a pod is moved to another of your reservations for
any other reason — its on-demand lease merged into your booking as that booking
opened, or its reservation replaced by Extend — `Preempted` immediately before
deletion, `ReservationCancelled` and `ReservationReassigned` immediately before
a deletion no warning announces (below),
`OnDemandLeaseDenied` when a lease request is refused — §5.1 —
`OnDemandAdmissionPaused` when on-demand admission for the pod's GPU class is
on hold — §5.2 — `OnDemandLeaseRejected`, `UnknownGpuClass`,
`NoReservation`, `AnnotationIgnored` and `NoMatchingNode` when something about
the pod itself needs fixing, and `WaitingForNode` while it waits for room on the
nodes it asked for — §5.3 — and `WaitingForReservation`, `ReservationFull` and
`ReservationTooSmall` while the pod waits for one of your reservations — §5.4).
These are richer
than the annotations but need Kubernetes API access to read, so they are for
whoever runs `kubectl` — the pod's owner, an operator, a dashboard — rather than
for in-pod consumers. Being addressed to a person, their messages state times
in the deployment's display zone, with the zone named
(`2026-08-21 10:30:16 PDT`), rather than in the UTC wire format the annotations
carry. That zone is local only if the deployment sets one (`TZ`, or
`EVENT_DISPLAY_TIMEZONE` for the messages alone); one that sets neither — the
Helm chart's default — renders them in UTC (`2026-08-21 17:30:16 UTC`). The
Event examples in this document show a deployment set to US Pacific time.
§5.5 covers what the controller does to a reservation without any Event, and
§5.6 the states in which it says nothing at all.

Two deletions are **not** preceded by a termination warning, because nothing
predicts them — they follow a person's action on the reservation, and the pod
is deleted in the same step:

| Event | When | Message |
|---|---|---|
| `ReservationCancelled` | The reservation the pod runs under is cancelled while its window is open — by you, a group manager or an administrator — and you hold no other open reservation with room for the pod to move to | `Pod evicted: GPU reservation cancelled by user.` (`by another user` when someone else cancelled it; a machine reason such as `(reason: no-show)` is appended when there is one) |
| `ReservationReassigned` | The reservation is handed to a teammate (Team Mode) while its window is open, so its GPUs go to the new owner's pods | `Pod evicted: GPU reservation reassigned to <username>.` |

Both are ordinary graceful deletions (`SIGTERM`, then the grace period) with no
warning beforehand — one more reason the periodic checkpoint in §6.1 is the one
that is not optional. A reservation that is *replaced* rather than simply
cancelled — Extend supersedes it with a new one — does not evict: the pod is
moved onto the replacement and gets `ReservationRelinked` instead.

### 5.1 Why a pod is still Pending: `OnDemandLeaseDenied`

The Events above all concern a pod that was *admitted*. A pod that never gets
that far has the opposite problem, and it used to be invisible: a pod with no
reservation open gets an on-demand lease requested on its behalf, and when the
reservation app refuses that ask as infeasible it answers with a reason — not
enough GPUs left under the group's ceiling, an exhausted SU budget, a group the
user is not a member of. That reason reached the controller's log and stopped
there, so the owner saw a pod sitting Pending with nothing saying why.

The controller now mirrors it back onto the pod as a `Warning` Event, so it
shows up in the place a user already looks:

```console
$ kubectl describe pod my-training-job
...
Events:
  Type     Reason                Age    From                          Message
  ----     ------                ----   ----                          -------
  Warning  FailedScheduling      5m12s  default-scheduler             0/41 nodes are available: ...
  Warning  OnDemandLeaseDenied   4m58s  gpu-reservation-controller    On-demand GPU lease for 2 x a100 was denied by the reservation service: Only 1 GPU(s) available for this group at 2026-08-21 14:00 (group ceiling: 4). The pod stays Pending; the controller will keep retrying.
```

Three things worth knowing about it:

- **Whether waiting helps depends on why.** Most denials are about *load* —
  capacity, a budget window — and the controller keeps retrying on its own
  cadence (2–5 minutes); a pod denied for capacity usually gets in once someone
  else's job ends. When the reservation service knows when the denial clears (a
  budget window's end, a group's start date) the Event says so, and the
  controller waits for that rather than polling toward it. Some denials are
  *structural* — more GPUs than the class allows per reservation, a group the
  user is not a member of, a group whose term has ended — and the same request
  will be refused on every retry. Those Events say **waiting will not change
  this** instead of promising a retry, and end by suggesting you contact support
  if the reason looks wrong:

  ```text
  On-demand GPU lease for 8 x a100 was denied by the reservation service: Requested 8 GPU(s) but this class allows at most 4 per reservation. Waiting will not change this: the pod stays Pending until its request changes or an administrator changes what refused it. If the reason looks wrong, contact support.
  ```

  The controller still rechecks a structural denial, but backs off to at most
  once every 30 minutes — an administrator can change the answer, and the pod
  then gets in without being recreated.
- **It repeats, but not every retry.** An unchanged reason is restated at most
  once per `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` (default 30) — often enough
  that the Event does not silently age out of `kubectl describe` on a pod that
  is still stuck, rarely enough that it does not bury the pod's other Events. A
  reason that *changes* is reported immediately, because it is new information.
- **Only the app's own answers are reported.** A network failure or a
  controller misconfiguration (a read-only service key, say) is not the pod
  owner's problem and produces no Event; those go to the controller's log for an
  operator.  A request the app rejects because it does not recognise something
  the pod named — its usage group, most often — is reported too, as
  `OnDemandLeaseRejected` (§5.3), because that one *is* the owner's to fix.

### 5.2 When on-demand admission is paused: `OnDemandAdmissionPaused`

Sometimes the controller does not ask for a lease at all, because on-demand
admission for the pod's whole GPU class is on hold. Three situations do that:

- **No node of the class is available.** Every node of this GPU class is out of
  service — typically down for maintenance — so there is nowhere for the job to
  run. Admission resumes as soon as one is back.
- **The class is short of hardware.** The reservation service expects more GPUs
  of this class than are currently online — a node is down, say. Leases sold
  against GPUs that do not exist could never run, so on-demand admission for the
  class stops until the two agree again.
- **Reserved jobs are waiting.** A pod that already holds a *reservation* for
  this GPU class has been admitted but the cluster cannot place it. Reserved
  jobs go first, so no new on-demand jobs start on the class until the waiting
  ones are running.

None of these is anything the pod's owner did or can fix, so each pod held this way
gets a `Warning` Event saying so:

```console
$ kubectl describe pod my-training-job
...
Events:
  Type     Reason                   Age   From                        Message
  ----     ------                   ----  ----                        -------
  Warning  FailedScheduling         6m3s  default-scheduler           0/41 nodes are available: ...
  Warning  OnDemandAdmissionPaused  6m1s  gpu-reservation-controller  On-demand GPU admission for gpu-class a100 is paused: the reservation service expects more a100 GPUs than are currently online in the cluster (for example, a GPU node is down or under maintenance), so no new on-demand jobs are started on this GPU class until that is resolved. Nothing about this pod needs to change; it stays Pending and the controller keeps retrying on its own. If this persists, contact support.
```

- **Leave the pod where it is.** It is not rejected: the controller keeps
  retrying, and requests the lease itself as soon as the pause lifts.
  Deleting and recreating the pod gains nothing, and sends it to the back of
  the on-demand queue, which is ordered by pod creation time.
- **It repeats on the same schedule as §5.1** — at most once per
  `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` (default 30) while nothing changes,
  immediately when something does. The two Events share that schedule, so a pod
  that goes from paused to denied and back is told each time, and the newest
  Event always says what is holding it now.
- **"If this persists"** means the notice keeps coming back. The deployment may
  name its support contact at the end of the message (`SUPPORT_CONTACT`); if it
  does not, use your cluster's usual support channel. Mention the GPU class and
  the Event's reason — that is enough for an operator to find the cause, which
  the controller logs in full.

---

### 5.3 When the pod itself needs fixing

§5.1 and §5.2 report things outside the pod.  These five report the pod: the
controller cannot act on it *as written*, and waiting will not change that —
the pod has to be corrected (usually: fix it and recreate it) or, if it looks
right, reported to support.

| Event | What it means |
|---|---|
| `OnDemandLeaseRejected` | The reservation service did not recognise something the pod's on-demand request named: its **usage group** (the usual cause — a mistyped group label or `galends/usage-group` annotation), its user (the pod's namespace) or its GPU class. The Event quotes the service's reason, the user and group that were sent, and where the group came from — the pod's label, its annotation, or the cluster's default when the pod named none. If you hold a booking of this class under a *different* usage group, it says so. |
| `UnknownGpuClass` | The pod's `gpu-class` label is not a GPU class the reservation service knows, so no reservation can match it and it cannot be admitted on demand. The Event lists the classes that do exist. |
| `NoReservation` | No reservation matches the pod, and it does not qualify for on-demand admission either, so nothing will ever admit it as it stands. The Event gives every reason — on-demand admission is not enabled on this cluster; or the pod has no (or an invalid) `galends/minimum-runtime-seconds`; or it names no usage group — and names any booking you hold that the pod narrowly misses: the right class under another usage group, or another class. |
| `AnnotationIgnored` | One of the pod's `galends/*` annotations was invalid, or asks for something this cluster does not offer, and was ignored in a way that changes what happens: the cluster's default minimum runtime is used instead of yours, or the pod is admitted with a guaranteed runtime (charged like any on-demand lease) instead of on a best-effort basis.  A pod that waits for its reservation instead of being admitted now because of one is told in its §5.4 Event instead. |
| `NoMatchingNode` | The pod's `nodeSelector`, or the required part of its node affinity, rules out every schedulable node of its GPU class — a mistyped host name, a hardware label the class does not have, or a node that is cordoned — so it could not start there and no on-demand lease is requested for it. The Event quotes the constraint, and names any other GPU class whose nodes it *does* match, since asking for one class's hardware under another's `gpu-class` label is a common cause. The controller keeps checking on its queue interval (5 minutes by default), so a pod waiting on a cordoned node goes ahead once the node is back. |

```console
$ kubectl describe pod my-training-job
...
Events:
  Type     Reason                 Age   From                        Message
  ----     ------                 ----  ----                        -------
  Warning  FailedScheduling       2m5s  default-scheduler           0/41 nodes are available: ...
  Warning  OnDemandLeaseRejected  2m3s  gpu-reservation-controller  On-demand GPU lease for 1 x a100 was rejected by the reservation service: Usage group 'cse999' not found. The request named user jsmith (this pod's namespace) and usage group 'cse999' (from the pod's dsmlp/course label). You do have a gpu-class a100 reservation under usage group cse151b, but this pod's usage group is 'cse999'; set its dsmlp/course label to that group to use it. Waiting will not fix this: if the usage group is wrong, correct it and recreate the pod; if these look right, contact support.
```

- **They repeat on the schedule of §5.1** — a changed status at once, an
  unchanged one at most once per `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` (default
  30) — and share its throttle, so the newest of §5.1–§5.4 on a pod is always
  what is holding it now.  `AnnotationIgnored` keeps its own schedule beside
  them: it stays true whatever else holds the pod.
- **Fix, then recreate.**  The controller reads a pod's labels and annotations
  when it first sees the pod; editing them on a running pod (`kubectl label`,
  `kubectl annotate`) is not reliably picked up.  Change the spec the pod was
  created from and create it again.
- **Nothing is claimed that the controller cannot see.**  `UnknownGpuClass` and
  `NoReservation` are only reported once the controller has the reservation
  service's full class list and reservation list — so a pod created while the
  service is unreachable is told nothing rather than something false.
  `NoMatchingNode` likewise waits for the controller's first look at the
  cluster's nodes, and a constraint it cannot evaluate (an operator Kubernetes
  itself would reject) is treated as no constraint at all.

**A pod that narrows its nodes but can run on some of them** is not a problem
to fix, so it is told with a `Normal` Event instead:

| Event | Type | What it means |
|---|---|---|
| `WaitingForNode` | `Normal` | None of the nodes the pod's `nodeSelector` or node affinity allows has the GPUs it requests free right now, so no on-demand lease is requested yet — one would be charged while the pod waited. It is re-checked on the controller's queue interval (5 minutes by default), and a lease is requested once one of those nodes has room. Other nodes of the class may well be free: widening or removing the constraint lets the pod use them. |

A constraint that allows **every** node of the class (a `nodeSelector` on the
class's own hardware label, say) narrows nothing and changes nothing.  Only
on-demand admission checks placement: a pod waiting for one of your bookings
(§5.4) is admitted when the booking opens, and then waits for its nodes the
ordinary way, with kube-scheduler's `FailedScheduling` saying why.

### 5.4 While the pod waits for your reservation

A pod that matches one of your reservations waits for it: until its window
opens, or until one of its GPUs is free.  It gets an Event saying which, so the
newest thing on the pod is not kube-scheduler's "untolerated taint" — or a
`NoReservation` from before you booked.

| Event | Type | What it means |
|---|---|---|
| `WaitingForReservation` | `Normal` | The reservation has not opened yet.  The Event gives its id and window; the pod is admitted shortly after the window opens (within the controller's queue interval, 5 minutes by default). |
| `ReservationFull` | `Warning` | The reservation is open, but your other pods hold its GPUs.  The Event names them — a notebook server you forgot to stop is the usual cause.  The pod is admitted as soon as enough of them end; stop one to start it sooner. |
| `ReservationTooSmall` | `Warning` | The reservation holds fewer GPUs than the pod requests, so it can never admit it — the pod is queued on it only because you hold no larger one of the class.  Book a reservation of at least the pod's `nvidia.com/gpu` request, or lower the request and recreate the pod. |

```console
$ kubectl describe pod my-training-job
...
Events:
  Type     Reason            Age   From                        Message
  ----     ------            ----  ----                        -------
  Warning  FailedScheduling  48s   default-scheduler           0/41 nodes are available: ...
  Warning  ReservationFull   47s   gpu-reservation-controller  Your GPU reservation #4127 (1 x a100, 2026-08-21 09:00:00 PDT to 2026-08-21 17:00:00 PDT) is fully in use by your pod jupyter-jsmith. This pod will be admitted as soon as 1 GPU is free. It cannot be admitted on demand meanwhile: it has no galends/minimum-runtime-seconds annotation saying how long it needs to run, in seconds.
```

- **Why it cannot start on demand instead.**  When the pod waits only because
  it does not qualify for on-demand admission — the reservation is far off, full
  or too small — the Event ends by saying why not: no (or an invalid)
  `galends/minimum-runtime-seconds`, or no usage group.  Fix that and recreate
  the pod to be admitted on demand while you wait (charged like any on-demand
  lease).  On a cluster with on-demand admission switched off there is nothing
  sooner to offer, and the Event says nothing about it.  A pod whose reservation
  opens within `ONDEMAND_HORIZON_MINUTES` (default 30) and has room waits for it
  either way (§3, *Wait or lease*).
- **Same schedule as §5.1–§5.3**: a changed status at once — the window
  opening onto a full reservation, a different pod holding it — and an unchanged
  one at most once per `ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES` (default 30).
- **It follows your bookings.**  If a reservation that can take the pod *now*
  appears — you book one, or one of your other bookings frees up — the pod
  moves to it within the queue interval; you do not need to recreate it.

### 5.5 What happens to the reservation itself

Two things the controller does act on a *reservation* rather than a pod, so
they put nothing on any pod — they show up in the reservation app, as a
cancelled reservation, not in `kubectl describe`:

- **An unused booking is cancelled as a no-show.** A booking with no pod of
  yours running under it 15 minutes after it opens (`NOSHOW_TIMEOUT_MINUTES`)
  is cancelled in the reservation service with reason `no-show`, freeing the
  window for someone else. The same applies later in the window: once the last
  pod running under an open booking ends — it finished, you deleted it, a
  notebook server was culled for being idle — the booking is watched again, and
  if nothing starts under it for 30 minutes (`NOSHOW_GRACE_MINUTES`, counted
  from when the controller's periodic checks notice, which can add up to about
  10 minutes), the booking is cancelled the same way. (The same 30 minutes
  applies to every booking already open when the controller restarts.) A pod
  running under an earlier booking that directly abuts this one protects it
  too, since its guarantee already covers that window. Once cancelled, the booking is gone:
  a pod started afterwards no longer matches it, and goes on demand (charged) if
  it qualifies, or gets `NoReservation`. A no-show is charged like any other
  cancellation — see the reservation service's cancellation rules
  (`SCHEDULING.md` §3).
- **An on-demand lease ends with its pod.** A lease exists only to cover the one
  pod it was requested for, so when that pod finishes, is deleted or is
  preempted, the controller cancels the lease (reason `pod-terminated`) rather
  than letting it run out. You pay for the time the pod ran; unused time in the
  lease's first two hours is free, and past that part of the unused remainder
  may be charged (`SCHEDULING.md` §3, "On-demand grace") — so a
  `galends/minimum-runtime-seconds` far above what the job needs can cost SU. A
  booking is never cancelled because its pod ended. A lease is also retired early
  when its pod moves onto your booking as that booking opens
  (`ReservationRelinked`, §5); that cancel (`superseded`) charges only the time
  already used.

### 5.6 When the controller says nothing

A GPU pod can also sit Pending with no Event from the controller at all. That
is expected in these cases:

- **It has no `gpu-class` label**, or an empty one: the controller never looks
  at it.
- **The scheduler has not ruled on it yet** — the first few seconds after it is
  created.
- **The scheduler named something no reservation can fix** — `Insufficient cpu`
  or `memory`, a volume that cannot bind. The
  controller steps aside and kube-scheduler's own `FailedScheduling` Event is
  the one that says what is wrong; the controller looks at the pod again at its
  next resync, roughly every 10 minutes.
- **No single node has room for it.** A pod asking for 2 or more GPUs — or any
  best-effort pod — waits quietly while its class's free GPUs are spread across
  nodes with none holding enough on its own (a pod cannot span nodes). It is
  retried on the controller's queue interval (5 minutes by default) until one
  node does. (A pod whose own node selector or affinity is what rules the free
  nodes out is told, with `WaitingForNode` — §5.3.)
- **A lease was granted but the pod could not be admitted under it** (a
  transient Kubernetes error, say): the lease is cancelled at once (reason
  `controller-revoked`) and a fresh one requested 2–5 minutes later.
- **The reservation service cannot be reached**, or refuses the controller
  itself (its credentials, say) — an operator's problem, logged for them
  (§5.1). Where the deployment lets the reservation service choose which
  waiting pods to admit on demand (`ONDEMAND_DELEGATE_ADMISSION`), a pod it
  passes over for a round is not told either.
- **Its reservation went away while it waited** — the window ended, or the
  booking was cancelled with nothing to move to: its last §5.4 Event stands
  until the controller re-examines it at the next resync.
- **The controller has only just started** and has not yet loaded the
  reservation service's GPU classes and reservations (§5.3).
- **The controller is down.**

If a pod stays Pending well past these, with nothing from the controller, ask
support — with the pod's name, namespace and GPU class.

## 6. Acting on the warning: checkpointing a PyTorch job

This section is for the highest-value consumer of these annotations: a training
job that would rather write a checkpoint than lose an afternoon. Nothing here is
controller behaviour — it is guidance for the workload, and everything in it
degrades gracefully if the annotations never appear.

### 6.1 Three moments to save, and only one of them is optional

| Moment | Trigger | Why |
|--------|---------|-----|
| **Periodic** | every *N* steps | The baseline. Survives node failure, OOM, NCCL timeout, and the case where you get no warning at all. |
| **On warning** | `galends/termination-warning-at` appears (§4 state `at-risk`), or `guaranteed-until` is close | The window this controller gives you. Minutes, not seconds — enough for a real save. |
| **On `SIGTERM`** | pod deletion | Last resort only. |

**Do not build your strategy on `SIGTERM`.** A preempted pod is deleted with
normal graceful termination (§5), and `terminationGracePeriodSeconds` is
typically 30 s — long enough to flush a LoRA adapter, nowhere near long enough
to write a multi-GB optimizer state, and *far* short of a sharded save across
nodes. Treat the signal handler as a "flush whatever is already staged in CPU
memory" path, not as your checkpoint path. The annotation warning is what buys
you the time; the grace period is what you spend after the decision is already
made.

The corollary matters just as much: **the guarantee is not a deadline.** Past
`guaranteed-until` the job is not killed, it enters `overstay` and keeps running
until a booking actually needs the GPUs (or the headroom goal of §2 does). Do not exit at the
guarantee. Do tighten your cadence once you cross it, because from that instant
you are killable and the notice you get is bounded by §7, not by your own
planning.

### 6.2 What a resumable checkpoint has to contain

A checkpoint that restores only `model.state_dict()` resumes a *different* run.
Everything below is state the optimizer or the data pipeline carries, and
omitting any of it shows up as a loss spike at the resume point:

| Component | Call | Notes |
|-----------|------|-------|
| Model weights | `model.state_dict()` | Under `torch.compile`, keys gain an `_orig_mod.` prefix — save `model._orig_mod.state_dict()` (or strip the prefix on load), so the checkpoint stays loadable by an uncompiled model. |
| Optimizer | `optimizer.state_dict()` | The big one: Adam/AdamW carries two fp32 moments, so optimizer state is commonly **2–3× the model** in bytes. This is what makes checkpoint cost a training-loop design question rather than an afterthought. |
| LR scheduler | `scheduler.state_dict()` | Cheap and always forgotten. Without it a warmup/cosine schedule restarts and the loss jumps. |
| AMP scaler | `scaler.state_dict()` | `torch.amp.GradScaler` holds an adaptive loss scale; resuming without it re-converges through a few skipped steps. |
| Step / epoch counters | your own | The resume anchor, and what your cadence arithmetic is expressed in. |
| Data position | `StatefulDataLoader.state_dict()` | `torchdata`'s `StatefulDataLoader` is a drop-in `DataLoader` replacement that supports **mid-epoch** resume without replaying batches. It requires the same `num_workers` on load as on save. Without it, either accept re-seeing data or fast-forward the sampler by hand. |
| RNG state | `torch.get_rng_state()`, `torch.cuda.get_rng_state_all()`, `random.getstate()`, `numpy.random.get_state()` | Needed for bit-comparable resumes (dropout, augmentation, sampling). Skip deliberately if you don't need reproducibility — don't skip by accident. |
| EMA / metric state | your own | EMA weights, best-so-far metrics, early-stopping counters. |

Save the run's config next to the weights. A checkpoint you cannot identify six
weeks later is only half a checkpoint.

### 6.3 Write it so a kill mid-write cannot cost you the previous one

The failure this controller creates is precisely "process disappears while
writing." Two rules cover it:

**Single-file saves: write to a temporary path, `fsync`, then `os.replace`.**
`os.replace` is atomic within a filesystem, so the visible path is always either
the old checkpoint or the new one, never a truncated file. Skipping this is how
you get `RuntimeError: unexpected EOF` from the only checkpoint you had.

```python
import os, torch

def save_atomic(state: dict, path: str) -> None:
    tmp = f"{path}.tmp"
    with open(tmp, "wb") as fh:
        torch.save(state, fh)
        fh.flush()
        os.fsync(fh.fileno())          # data durable before the rename
    os.replace(tmp, path)              # atomic; same filesystem only
    dfd = os.open(os.path.dirname(path) or ".", os.O_RDONLY)
    try:
        os.fsync(dfd)                  # the rename itself durable
    finally:
        os.close(dfd)
```

**Sharded / directory saves: complete, then publish.** A distributed checkpoint
is a directory of per-rank shards, and there is no atomic rename for "all of
them landed." Write into a scratch directory and rename the *directory* when
every rank has finished, or write an explicit `DONE` marker as the last step and
have the resume path ignore any directory without one. With
`torch.distributed.checkpoint` (DCP), the `.metadata` file is written after the
shards, so its absence is a good "incomplete" signal — but an explicit marker you
control is the thing to rely on.

Then: **keep the last 2–3 checkpoints, and delete the old one only after the new
one is complete.** `save_total_limit`-style pruning that deletes eagerly turns a
mid-write kill into total loss. And write to a **PVC or shared filesystem** — an
`emptyDir` or node-local scratch path dies with the pod, which is exactly the
event you are checkpointing against.

### 6.4 Choosing a cadence

The classical answer is the Young/Daly interval: checkpoint every
`sqrt(2 · C · MTBF)`, where `C` is the wall-clock cost of one checkpoint and
`MTBF` is the mean time between interruptions. It balances the two failure modes
— saving so often that the save dominates, or so rarely that each interruption
costs hours.

Under this controller you can do better than a statistical guess, because
interruption is **announced**, not random:

- Set the periodic cadence from `C` alone — a common target is checkpoint
  overhead under ~5% of step time, which for a synchronous save means an
  interval of roughly `20 × C`. This covers the unannounced failures (node,
  NCCL, OOM).
- Let the **warning** cover the announced ones. That is what turns "lose up to
  one interval" into "lose up to one step."
- Measure `C` on your actual storage, once, and log it. Every number in this
  section is expressed in terms of it, and the value people assume is
  consistently optimistic.

If `C` is large enough that this arithmetic is uncomfortable, use **asynchronous
checkpointing** before you use a longer interval. `torch.distributed.checkpoint`
offers `dcp.async_save`, which stages tensors into CPU buffers and writes them
from a background thread while training continues — the blocking part of the
save drops to roughly the staging copy. Two caveats: it costs host RAM on the
order of one checkpoint's worth per rank, and you should keep **one outstanding
async save at a time** (wait on the previous future before issuing the next), or
the memory multiplies. Before exiting on a preemption, wait on the outstanding
future — an async save you didn't flush is not a checkpoint.

### 6.5 Wiring the annotations into the training loop

Poll the downward-API file (§1) on a background thread, derive the state (§4),
and expose a latch the loop reads at a **step boundary**. Never checkpoint from a
signal handler or a timer callback mid-backward: the state is inconsistent and,
under FSDP/DDP, saving is a collective that must be entered by every rank in the
same iteration.

```python
import threading, time
from datetime import datetime, timedelta, timezone

CHECKPOINT_COST = timedelta(seconds=90)      # measured, not guessed
LEAD = CHECKPOINT_COST * 2 + timedelta(seconds=60)

class PreemptionWatch:
    """Polls /etc/podinfo/annotations; publishes a boolean the loop can read."""

    def __init__(self, path="/etc/podinfo/annotations", interval=20.0):
        self._path, self._interval = path, interval
        self._deadline: datetime | None = None
        self._lock = threading.Lock()
        threading.Thread(target=self._poll, daemon=True).start()

    def _poll(self):
        while True:
            try:
                ann = parse_downward_annotations(self._path)   # §1
            except OSError:
                ann = {}
            at = _parse_utc(ann.get("galends/termination-warning-at"))
            try:
                risk = float(ann.get("galends/termination-warning-risk", "1"))
            except ValueError:
                risk = 1.0
            with self._lock:
                # None when the warning is absent — warnings retract (§8 rule 2)
                self._deadline = at if at is not None and risk >= 0.25 else None
            time.sleep(self._interval)

    def urgent(self, now=None) -> bool:
        now = now or datetime.now(timezone.utc)
        with self._lock:
            deadline = self._deadline
        return deadline is not None and now + LEAD >= deadline
```

```python
watch = PreemptionWatch()
warned = False

for step, batch in enumerate(loader, start=resume_step):
    train_step(batch)                                  # forward/backward/step

    urgent = watch.urgent()
    if dist.is_initialized():                          # see below — agree across ranks
        urgent = _any_rank(urgent)

    if step % CHECKPOINT_EVERY == 0 or (urgent and not warned):
        save_checkpoint(step)                          # atomic, §6.3
    warned = urgent                                    # re-arms if the warning clears
```

Four details in that loop earn their place:

1. **Debounce.** `warned` makes the urgent save fire once per warning episode,
   not on every step for the next fifteen minutes. Because it tracks the current
   value rather than latching, a warning that clears and returns (a different
   boundary, a re-booked window) correctly triggers a fresh save.
2. **`_any_rank` — agree across ranks.** Each pod carries its own annotations, so
   in a multi-pod job only some members may be flagged. But losing any one member
   kills the job, and a collective save that only some ranks enter **deadlocks**.
   OR-reduce the flag (`dist.all_reduce` on a `uint8`, or broadcast rank 0's view)
   and act on the aggregate.
3. **Risk as a band, not a number.** `termination-warning-risk` models uniform
   random victim selection and the app's policy may differ (§2). Thresholding it
   coarsely is right; scheduling around `0.31` vs `0.29` is not. If your
   checkpoints are cheap, ignore risk entirely and save on any warning.
4. **The warning is not the only cue.** Also save unconditionally as
   `guaranteed-until` approaches. Past that instant you are preemptible, and
   entering `overstay` with an hour-old checkpoint is a self-inflicted wound.

**Exiting voluntarily is a legitimate response** to a high-risk warning, and
often the better one for a batch job: checkpoint, exit 0, and let your submission
system resubmit. You free the capacity the incoming reservation wanted, you
choose your own stopping point, and you skip the 30 s grace-period scramble
entirely. For an interactive session (a notebook), don't — checkpoint and keep
working; the pod may well survive (§8 rule 3). §1 has the same shape for a job
with no Python in it: a shell loop that stops starting new units of work once
`termination-warning-at` is set.

### 6.6 By scenario

| Scenario | Checkpoint size | What to do |
|----------|-----------------|------------|
| **LoRA / PEFT fine-tune** | tens of MB (adapter only) | `C` is seconds. Save often, save on any warning, and don't over-engineer. `save_pretrained()` writes only trainable adapter params. To actually *resume* you still need optimizer + scheduler + step: with HF `Trainer` that means leaving `save_only_model=False` (the default) and resuming with `resume_from_checkpoint`. Keep the base model out of the checkpoint — reference it by id/path. |
| **Full fine-tune, single node (DDP)** | model + 2–3× optimizer | Save from rank 0 only for plain DDP (replicas are identical), or move to DCP if the optimizer state is large enough that a single-rank write is the bottleneck. This is the regime where the warning window pays for itself. |
| **From-scratch / large-model pre-training (FSDP, multi-node)** | 100s of GB, sharded | Use `torch.distributed.checkpoint` with `get_state_dict` / `set_state_dict` (they handle FSDP FQNs and sharded state for model *and* optimizer), and the `Stateful` protocol so DCP calls your `state_dict`/`load_state_dict` for you. Enable `dcp.async_save` for the periodic cadence. On a warning, prefer one synchronous save at a step boundary over racing an async one you may not get to flush. |
| **Long runs spanning reservations** | any | Design for restart, not for continuity. The reservation window is the natural unit: a run that resumes cleanly from disk can be scheduled across several windows and preempted between them at near-zero cost. Anchor elapsed-time accounting on `galends/admitted-at`, not `reservation-start` (§2). |
| **Inference / serving pods** | n/a | Nothing to checkpoint. Use the warning to drain: stop accepting work, finish in-flight requests, exit. |

### 6.7 Resume-side checklist

The save path gets all the attention; the resume path is where the bugs are.

- **Pick the newest *complete* checkpoint**, not the newest path. Verify the
  marker (§6.3), and fall back to the previous one on any load error — that is
  the entire reason for keeping more than one.
- **Restore everything you saved** (§6.2), in particular the scheduler and the
  data position. A resume that only restores weights is detectable in the loss
  curve.
- **`torch.load` defaults to `weights_only=True` from PyTorch 2.6.** A checkpoint
  containing anything beyond plain tensors and containers now needs
  `torch.serialization.safe_globals` to allowlist those types. Reach for that
  before reaching for `weights_only=False`, which permits arbitrary code execution
  on load. If you only need weights, `safetensors` sidesteps the question — but it
  stores tensors only, so optimizer state still goes through `torch.save`.
- **Test the resume path deliberately.** Kill a run at a random step, restart it,
  and check the loss curve is continuous. Preemption will run this test for you
  eventually; better it is not the first time.

### Further reading

- [Asynchronous saving with Distributed Checkpoint (DCP)](https://docs.pytorch.org/tutorials/recipes/distributed_async_checkpoint_recipe.html) — `dcp.async_save`, the `Stateful` protocol, `get_state_dict`/`set_state_dict`
- [6× faster async checkpointing in PyTorch](https://pytorch.org/blog/6x-faster-async-checkpointing/) — staging cost and memory trade-offs
- [`StatefulDataLoader`](https://meta-pytorch.org/data/main/torchdata.stateful_dataloader.html) — mid-epoch data-position resume
- [torchtitan checkpointing](https://github.com/pytorch/torchtitan/blob/main/docs/checkpoint.md) — a production from-scratch training loop's configuration surface
- [HF `Trainer` checkpointing](https://huggingface.co/docs/transformers/main_classes/trainer) and [PEFT integration](https://huggingface.co/docs/transformers/en/peft) — `save_steps`, `save_total_limit`, `resume_from_checkpoint`, adapter saves
- [Checkpointing à la Young/Daly: an overview](https://icl.utk.edu/files/publications/2022/icl-utk-1569-2022.pdf) — where the `sqrt(2·C·MTBF)` interval comes from and when it stops applying

---

## 7. Propagation latency

Annotations are reconciled on the controller's loops, then picked up by the
kubelet. Worst-case in-pod visibility, with default settings:

| Change | Controller cadence | + kubelet | Worst case |
|--------|--------------------|-----------|------------|
| Admission (`booking-reference`, `guaranteed-until`, `guarantee-status`, `admitted-at`, all `reservation-*`, `gpu-class-name`) | immediate, on admission | ~60 s | ~1 min |
| Re-link (`booking-reference` and every `reservation-*` key change together) | every queue-processor tick, `QUEUE_PROCESSOR_INTERVAL` = 300 s, or immediately during a preemption sweep | ~60 s | ~6 min |
| Reservation altered in place (`reservation-*` change, `booking-reference` does **not**) | every queue-processor tick, `QUEUE_PROCESSOR_INTERVAL` = 300 s | ~60 s | ~6 min |
| Termination warning appears / changes / clears | every preemption sweep, `PREEMPTION_CHECK_INTERVAL` = 60 s | ~60 s | ~2 min |
| `guarantee-status` flips to `overstay`; `guaranteed-until` extended by a follow-on booking | every queue-processor tick, `QUEUE_PROCESSOR_INTERVAL` = 300 s | ~60 s | ~6 min |

Two consequences worth designing around:

- **Don't trust `guarantee-status` for the in-guarantee test** — it can lag the
  actual expiry by minutes. Compare `guaranteed-until` against the wall clock
  yourself (as in §4) and use `guarantee-status` only as a corroborating hint.
- **Expect roughly 15 minutes of warning, not more.** Warnings look ahead
  `TERMINATION_WARNING_LEAD_MINUTES` (default 30) to the boundary, while the
  projected kill instant is `PREEMPTION_LEAD_MINUTES` (default 15) before it —
  so a warning typically appears ~15 minutes before the time it names, less
  propagation. A pod whose own guarantee ends exactly at the boundary gets up to
  the full 30. Design checkpoint prompts for a ~10-minute usable window.
- **There is no *floor* on the notice.** A reservation booked shortly before its
  own start puts its boundary inside the kill window immediately, so the warning
  and the kill can land within a sweep or two of each other — a couple of minutes
  of notice, not fifteen. An overstaying job holding work it cannot afford to
  lose should not rely on the warning alone; see §6.1.

---

## 8. Robustness rules

1. **Every key is optional.** Handle each one missing, at any time — including
   `booking-reference` on a pod the controller has not admitted yet, and the
   warning trio during the window between sweeps.
2. **Warnings retract.** The three `termination-warning-*` keys are deleted when
   the pod leaves the at-risk pool — the user extended or re-booked, the incoming
   reservation was cancelled or no-showed, demand evaporated, or the pod was
   re-linked to a new reservation. A UI that latches a red banner will show a
   false alarm indefinitely; clear it when the key disappears. **What cancels a
   pending termination is a new guarantee, and only two kinds of booking give
   one in time.** A booking of the owner's that directly *abuts* the pod's
   guarantee — same GPU class and GPU count (and usage group, on a cluster that
   matches by group), starting exactly when the guarantee ends — extends it as
   soon as the controller sees the booking, before it opens. A booking of the
   owner's for the class that is *open now* with room for the pod — which is what
   Extend in the reservation app creates — re-links the pod onto it (once its
   current guarantee has lapsed, and always before any kill). A booking that
   starts later without
   abutting rescues the pod only once it opens, which can be too late: the pod
   may even be preempted, up to `PREEMPTION_LEAD_MINUTES` early, to make room for
   that very booking. Either qualifying kind works right up to the moment the
   pod is deleted — the controller re-checks each pod's live guarantee on every
   sweep, so one it has seen first always wins — but it sees a new reservation
   within seconds only where the reservation service pushes changes to it, and
   otherwise at its next fetch (`RESERVATION_FETCH_INTERVAL`, 5 minutes by
   default).
3. **`termination-warning-at` can pass without anything happening.** The
   shortfall it was computed from may be gone by the time it arrives. Never
   count down to zero and declare the job dead; fall back to "may be stopped at
   any time" once the instant passes.
4. **The guarantee can move in both directions.** Usually later (an abutting
   follow-on booking). It can technically shrink — a window shortened
   server-side — so re-read rather than caching the first value you saw.
5. **`pod-runtime-limit-seconds` goes stale by design.** It is the duration
   when the guarantee was last recorded — at admission, or at the latest re-link
   — and is not refreshed in between, not even when an abutting booking extends
   the guarantee. Use `guaranteed-until` for anything the user sees.
6. **A pod's reservation can change under it.** `booking-reference` and every
   `reservation-*` key are rewritten together when the controller re-links a pod
   — to a window its user booked after the pod was already running, or from a
   just-in-time lease onto the matching booking once that booking opens. A pod
   can therefore go from `on_demand` to `booking` mid-session, with a new window
   and a new GPU count. Re-read the set rather than caching it at startup, and
   anchor anything cumulative (a session clock, an accrual estimate) on
   `admitted-at`, which does not move.
7. **Parse defensively.** `risk` is a decimal string (`float()` it, and clamp to
   `[0, 1]`); the timestamps are `YYYY-MM-DDTHH:MM:SSZ` (Python's
   `datetime.fromisoformat` accepts the `Z` suffix from 3.11 on); `res-<id>` and
   `reservation-gpu-count` are integers. Treat `reservation-kind` as an open set
   — match `booking`, `on_demand` and `best_effort` explicitly and fall back to
   neutral copy for anything else, rather than assuming a value you do not
   recognise is a lease.
   Ignore a value that does not parse instead of erroring the whole widget.
8. **Nothing here is authoritative.** These are best-effort stamps. For
   authoritative, richer risk data — per-hour buckets, cluster-wide class
   summaries — the controller exposes
   `GET /api/forecast/preemption-risk` (bearer-token guarded, see README), but
   that is a cluster-side API, not something an in-pod widget should call.
