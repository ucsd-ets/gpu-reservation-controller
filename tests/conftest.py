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
import logging
import os
from datetime import date, datetime, timedelta, timezone

import pytest

# app.main builds its FastAPI app at import, and Config.from_env() raises
# without these.  Set here because this module is imported before any test
# module.  In the full suite, two modules happen to setdefault them at
# collection, so a module importing app.main without setting them passed there
# and failed when run on its own.  Tests of the missing-variable error delete
# them with monkeypatch first.
os.environ.setdefault("RESERVATION_API_URL", "http://reservations.local")
os.environ.setdefault("RESERVATION_API_KEY", "gpures_test")

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


# ---------------------------------------------------------------------------
# Swallowed watch-event guard
# ---------------------------------------------------------------------------

#: What ``pod_watch_loop`` logs when handling one watch event raises.
SWALLOWED_WATCH_EVENT = "pod.event_failed"

#: Marker for a test that makes ``pod_watch_loop`` swallow an event on purpose.
EXPECTS_SWALLOWED_WATCH_EVENT = "expects_pod_event_failed"


def pytest_configure(config):
    config.addinivalue_line(
        "markers",
        f"{EXPECTS_SWALLOWED_WATCH_EVENT}: the test feeds pod_watch_loop an event "
        f"that is meant to fail (logging {SWALLOWED_WATCH_EVENT}); unmarked, any "
        "such event fails the test, and marked, the test fails without one",
    )


def check_swallowed_watch_events(
    records: list[logging.LogRecord], *, expected: bool
) -> None:
    """Fail the current test unless its ``pod.event_failed`` records match *expected*.

    The decision behind :func:`no_swallowed_watch_events`, kept apart from the
    fixture so it can be tested without a nested pytest run.  Matched on the
    parsed ``event`` field, not a substring of the rendered line.
    """
    swallowed = [
        r for r in records
        if SWALLOWED_WATCH_EVENT in r.getMessage()
        and kv_fields(r.getMessage()).get("event") == SWALLOWED_WATCH_EVENT
    ]
    if swallowed and not expected:
        pytest.fail(
            "pod_watch_loop swallowed an exception, so the rest of that event's "
            "handling never ran and the test's assertions may have passed on a "
            "half-handled event:\n"
            + "\n".join(f"  {r.getMessage()}" for r in swallowed)
            + "\nFix the cause (often a SimpleNamespace config or pod stub missing "
            "an attribute the handler reads; prefer make_config()).  If the test "
            "poisons the event on purpose, mark it "
            f"@pytest.mark.{EXPECTS_SWALLOWED_WATCH_EVENT}.",
            pytrace=False,
        )
    if expected and not swallowed:
        pytest.fail(
            f"marked {EXPECTS_SWALLOWED_WATCH_EVENT}, but pod_watch_loop logged no "
            f"{SWALLOWED_WATCH_EVENT}: the event it poisons is now handled cleanly, "
            "so the test no longer exercises the guard",
            pytrace=False,
        )


@pytest.fixture(autouse=True)
def no_swallowed_watch_events(request, caplog):
    """Fail any test in which ``pod_watch_loop`` swallowed an event.

    The loop handles each watch event under its own ``try/except`` so that one
    bad event cannot kill the consumer: an exception partway through a handler
    is logged as ``pod.event_failed`` and the loop moves on to the next event.
    In a test, that means everything after the crash point silently never runs
    while assertions about state set *before* it still pass.  Three best-effort
    routing tests did exactly that: their ``SimpleNamespace`` config lacked a
    field the handler read just after adding the candidate, so the annotation
    notice and the admission batch never ran, and the tests passed anyway.

    Every test is checked, not just the ones that remember to ask, because
    the loop is driven from many modules, each through its own helper.  A test
    that poisons an event deliberately says so with
    ``@pytest.mark.expects_pod_event_failed``, and is held to it: a marked test
    that logs no such event fails too, so the marker cannot outlive the poison
    and quietly switch the guard off for a test that no longer needs it.

    **Limitation:** this reads caplog's records, so a test that raises caplog's
    level above ERROR around the loop (``caplog.at_level(logging.CRITICAL)``),
    or calls ``caplog.clear()`` after it, hides the event from this guard.
    """
    yield
    check_swallowed_watch_events(
        caplog.get_records("setup") + caplog.get_records("call"),
        expected=request.node.get_closest_marker(EXPECTS_SWALLOWED_WATCH_EVENT)
        is not None,
    )
