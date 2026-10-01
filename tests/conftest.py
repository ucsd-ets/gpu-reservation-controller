"""Shared test factories and fixtures (CODE-REVIEW T1).

Before this file, ``_compute_window`` was byte-identical in five test modules and
the reservation factory existed in six drifted copies.  Everything here builds on
one canonical ``reservation()`` factory (explicit UTC windows) so the
``ReservationResponse`` shape has a single source of truth; the ``user_reservation``
/ ``ondemand_block`` / ``block`` wrappers exist only to keep call sites readable.

Import from test modules with ``from tests.conftest import ...``.
"""

from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone

from app.config import Config
from app.controller import ControllerState
from app.schemas import GpuClassBrief, GroupBrief, ReservationResponse, UserBrief

# ---------------------------------------------------------------------------
# Shared constants
# ---------------------------------------------------------------------------

GPU_CLASS_ID = 10
GPU_CLASS_LABEL = "h100"
OTHER_CLASS_ID = 20
OTHER_CLASS_LABEL = "a100"
USERNAME = "alice"
ADMIN_USERNAME = "sysadmin"
GROUP_NAME = "cse151b"    # usage-group name carried by JIT lease asks
FIXED_DATE = date(2024, 1, 15)    # past; window timing controlled via explicit `now`
FUTURE_DATE = date(2099, 6, 15)   # far future; slot_end always > datetime.now(utc)

_EPOCH = datetime(2024, 1, 1, tzinfo=timezone.utc)


# ---------------------------------------------------------------------------
# Window helpers
# ---------------------------------------------------------------------------


def window(
    day: date,
    start_time: str = "08:00:00",
    slot_index: int = 0,
    duration_minutes: int = 120,
) -> tuple[datetime, datetime]:
    """Return ``(start_utc, end_utc)`` from a date + ``HH:MM[:SS]`` + slot offset."""
    parts = start_time.split(":")
    minutes = int(parts[0]) * 60 + int(parts[1]) + slot_index * duration_minutes
    return window_from_minutes(minutes, duration_minutes, day)


def window_from_minutes(
    minutes_from_midnight: int,
    duration_minutes: int,
    day: date = FIXED_DATE,
) -> tuple[datetime, datetime]:
    """Return ``(start_utc, end_utc)`` from a minutes-past-midnight offset (UTC)."""
    midnight = datetime.combine(day, datetime.min.time()).replace(tzinfo=timezone.utc)
    start = midnight + timedelta(minutes=minutes_from_midnight)
    return start, start + timedelta(minutes=duration_minutes)


# ---------------------------------------------------------------------------
# Canonical reservation factory + readable wrappers
# ---------------------------------------------------------------------------


def reservation(
    res_id: int,
    *,
    start_utc: datetime,
    end_utc: datetime,
    kind: str = "booking",
    username: str = USERNAME,
    user_id: int = 1,
    with_user: bool | None = None,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    gpu_count: int = 2,
    status: str = "active",
    cancelled_by_id: int | None = None,
    cancel_reason: str | None = None,
    group: str | None = None,
    group_id: int | None = None,
    day: date | None = None,
) -> ReservationResponse:
    """Build a ``ReservationResponse`` with an explicit UTC window.

    A ``kind="booking"`` reservation carries a user by default; ``kind="reclaim"``
    carries none.  Pass ``with_user`` to override that coupling (a cancelled
    booking that keeps its user, or a reclaim owned by a user in a test).

    Pass ``group`` (the usage-group name) to attach a ``GroupBrief`` for
    REQUIRED_GROUP_LABEL tests; ``group_id`` defaults to ``res_id`` when a group
    name is given so distinct groups get distinct ids for chaining tests.
    """
    if with_user is None:
        with_user = kind == "booking"
    user = UserBrief(id=user_id, username=username) if with_user else None
    group_brief = None
    if group is not None:
        gid = group_id if group_id is not None else res_id
        group_brief = GroupBrief(id=gid, name=group)
    return ReservationResponse(
        id=res_id,
        user_id=user_id if with_user else None,
        user=user,
        group_id=group_brief.id if group_brief else group_id,
        group=group_brief,
        gpu_class_id=gpu_class_id,
        gpu_class=GpuClassBrief(id=gpu_class_id, name="H100", label_value=gpu_class_label),
        date=day or start_utc.date(),
        start_utc=start_utc,
        end_utc=end_utc,
        gpu_count=gpu_count,
        status=status,
        kind=kind,
        created_at=_EPOCH,
        updated_at=_EPOCH,
        cancelled_by_id=cancelled_by_id,
        cancel_reason=cancel_reason,
    )


def user_reservation(
    res_id: int,
    *,
    username: str = USERNAME,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    gpu_count: int = 2,
    slot_index: int = 0,
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
    user_id: int = 1,
    status: str = "active",
    cancelled_by_id: int | None = None,
    cancel_reason: str | None = None,
) -> ReservationResponse:
    """A ``kind="booking"`` reservation whose window comes from date + slot fields."""
    start_utc, end_utc = window(reservation_date, start_time, slot_index, duration_minutes)
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="booking",
        username=username,
        user_id=user_id,
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        status=status,
        cancelled_by_id=cancelled_by_id,
        cancel_reason=cancel_reason,
        day=reservation_date,
    )


def reclaim_reservation(
    res_id: int,
    *,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    gpu_count: int = 2,
    slot_index: int = 0,
    start_time: str = "08:00:00",
    duration_minutes: int = 120,
    reservation_date: date = FIXED_DATE,
) -> ReservationResponse:
    """A ``kind="reclaim"`` reservation whose window comes from date + slot fields."""
    start_utc, end_utc = window(reservation_date, start_time, slot_index, duration_minutes)
    return reservation(
        res_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="reclaim",
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        day=reservation_date,
    )


def ondemand_block(
    block_id: int,
    *,
    gpu_count: int = 2,
    slot_index: int = 0,
    duration_minutes: int = 120,
    date_offset_days: int = 0,
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
) -> ReservationResponse:
    """A ``kind="reclaim"`` block whose window is *today* (plus offset) at 08:00 UTC."""
    day = (datetime.now(timezone.utc) + timedelta(days=date_offset_days)).date()
    start_utc, end_utc = window(day, "08:00:00", slot_index, duration_minutes)
    return reservation(
        block_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind="reclaim",
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        day=day,
    )


def block(
    block_id: int,
    start_utc: datetime,
    end_utc: datetime,
    *,
    gpu_count: int = 2,
    kind: str = "reclaim",
    status: str = "active",
    gpu_class_id: int = GPU_CLASS_ID,
    gpu_class_label: str | None = None,
    user: bool = False,
) -> ReservationResponse:
    """A reservation with an explicit UTC window, for tests needing precise timing."""
    return reservation(
        block_id,
        start_utc=start_utc,
        end_utc=end_utc,
        kind=kind,
        with_user=user,
        gpu_class_id=gpu_class_id,
        gpu_class_label=gpu_class_label,
        gpu_count=gpu_count,
        status=status,
    )


# ---------------------------------------------------------------------------
# State helper
# ---------------------------------------------------------------------------


def make_state(
    *reservations: ReservationResponse,
    labels: dict[int, str] | None = None,
    required_group_label: str | None = None,
) -> ControllerState:
    """A ``ControllerState`` seeded with *reservations* and a gpu-class label map.

    Defaults the label map to ``{GPU_CLASS_ID: GPU_CLASS_LABEL}``; pass
    ``labels={}`` to exercise the unresolved-class path.  Pass
    ``required_group_label`` to enable the usage-group match constraint.
    """
    state = ControllerState()
    state.reservations = list(reservations)
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL} if labels is None else labels
    state.required_group_label = required_group_label
    return state


# ---------------------------------------------------------------------------
# Config helper
# ---------------------------------------------------------------------------


def make_config(**overrides) -> Config:
    """A ``Config`` with every required field filled in; override what matters.

    Nine test modules each carried a private ``_config()`` enumerating all
    fourteen required fields. Five were byte-identical and two had already
    forked — ``test_overstay_report`` adding ``overstay_report_enabled=True``
    and ``test_watch_release`` flipping ``ondemand_lease_enabled`` — which is
    exactly the drift a shared builder prevents: both of those are one-line
    overrides here.

    The real cost of the copies was not the duplication but the coupling: adding
    a required field to ``Config`` meant editing nine files, so the pressure was
    always to give new settings a default whether or not one made sense.
    """
    base = dict(
        reservation_api_url="http://reservations.local",
        reservation_api_key="gpures_test",
        reservation_fetch_interval=300,
        reservation_lookahead_days=7,
        kubeconfig_path=None,
        http_port=8000,
        ondemand_lease_enabled=True,
        noshow_timeout_minutes=15,
        noshow_grace_minutes=30,
        queue_processor_interval=300,
        scheduling_gate_name=None,
        inbound_api_token=None,
        preemption_lead_minutes=15,
        preemption_check_interval=60,
        # On in production.  Off here so a test's fake client need answer only
        # the creates it is about; a test of delegation opts in.
        ondemand_delegate_admission=False,
    )
    base.update(overrides)
    return Config(**base)


# ---------------------------------------------------------------------------
# Log-line field parser
# ---------------------------------------------------------------------------


def kv_fields(message: str) -> dict[str, str]:
    """Parse a ``log_fields.kv()``-rendered message back into a field dict.

    Assertions on log output should name the field they mean, not search the
    rendered string: ``"fails=1" in message`` is also true of ``fails=12``, and
    ``"status=409" in message`` is also true of ``status=4091``.  Parsing turns
    both into an exact comparison.

    Understands the two shapes ``kv()`` emits — a bare ``key=value`` and a
    quoted ``key="value with spaces"`` with ``\\`` / ``"`` escaped.  Values stay
    strings; compare against ``"409"`` rather than ``409``, since that is what
    the log actually carries.
    """
    fields: dict[str, str] = {}
    i, n = 0, len(message)
    while i < n:
        while i < n and message[i] == " ":
            i += 1
        start = i
        while i < n and message[i] not in ("=", " "):
            i += 1
        if i >= n or message[i] != "=":
            # A bare token with no '=' is not a field; skip it.
            i += 1
            continue
        key = message[start:i]
        i += 1  # consume '='
        if i < n and message[i] == '"':
            i += 1
            buf: list[str] = []
            while i < n and message[i] != '"':
                if message[i] == "\\" and i + 1 < n:
                    i += 1
                buf.append(message[i])
                i += 1
            i += 1  # consume closing quote
            fields[key] = "".join(buf)
        else:
            start = i
            while i < n and message[i] != " ":
                i += 1
            fields[key] = message[start:i]
    return fields


# ---------------------------------------------------------------------------
# Lock-holding call helpers
# ---------------------------------------------------------------------------


def run_locked(state: ControllerState, coro):
    """Await *coro* while holding ``state.reservation_lock``.

    Several helpers document "caller must hold ``state.reservation_lock``" and
    now enforce it with ``require_reservation_lock``.  A unit test driving one
    of them directly *is* that caller, so it has to take the lock — otherwise
    the test asserts behaviour under a state the helper is never legitimately
    in.  This is the seam that keeps that from being three lines at every call
    site.
    """
    async def _held():
        async with state.reservation_lock:
            return await coro

    return asyncio.run(_held())


def call_locked(state: ControllerState, fn, *args, **kwargs):
    """Call a *synchronous* lock-requiring helper with the lock held.

    The synchronous sibling of :func:`run_locked` — ``forecast_preemption_risk``
    is pure and sync, but still contractually requires the lock because its
    caller holds it across the surrounding snapshot work.
    """
    async def _held():
        async with state.reservation_lock:
            return fn(*args, **kwargs)

    return asyncio.run(_held())
