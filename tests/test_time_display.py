"""Local-time rendering of the human-readable Event and annotation prose.

The controller's timestamps are UTC everywhere that a machine reads them — a
stored ``datetime``, a ``galends/*`` timestamp annotation, a log field — and
local only in the sentences a person reads.  That split is the whole feature,
and it is easy to break in either direction: localising ``utc_iso`` would break
the annotation contract in ``docs/POD-ANNOTATIONS.md`` *and* the diff-and-skip
reconciles in ``main`` that rebuild those strings to decide whether to re-patch,
while leaving a message on ``utc_iso`` silently reverts the feature for that one
message.

So these tests pin both halves: what ``format_local`` renders, and — for each of
the four messages — that the prose moved while the annotation and log field it
travels with did not.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from zoneinfo import ZoneInfo

import pytest

from app import k8s_client
from app.config import Config, _env_tz, timezone_label
from app.controller import ControllerState, PodRuntimeView
from app.k8s_client import (
    emit_overstay_relinked_event,
    emit_runtime_guaranteed_event,
    format_local,
    local_display,
    set_display_timezone,
    utc_iso,
)

from tests.conftest import kv_fields


LA = ZoneInfo("America/Los_Angeles")

# 2026-08-21T17:30:16Z is 10:30:16 PDT (UTC-7, summer time).
SUMMER = datetime(2026, 8, 21, 17, 30, 16, tzinfo=timezone.utc)
# 2026-01-21T17:30:16Z is 09:30:16 PST (UTC-8, winter time).
WINTER = datetime(2026, 1, 21, 17, 30, 16, tzinfo=timezone.utc)


@pytest.fixture
def main(monkeypatch):
    """``app.main``, whose import builds an app and therefore needs the two
    required variables (the same seam every other main-touching test uses)."""
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-time-display")
    import app.main as main_module

    return main_module


@pytest.fixture
def process_tz(monkeypatch):
    """Set the *process* zone (TZ) for one test, as a Deployment would.

    ``time.tzset()`` is what makes a mid-process change take effect; without it
    the C library keeps the zone it resolved on first use.
    """
    def _set(name: str):
        monkeypatch.setenv("TZ", name)
        time.tzset()

    yield _set
    monkeypatch.undo()
    time.tzset()


@pytest.fixture
def display_tz():
    """Set the module-global display zone for one test and restore it after."""
    set_display_timezone(None)
    yield set_display_timezone
    set_display_timezone(None)


# ---------------------------------------------------------------------------
# format_local — the renderer
# ---------------------------------------------------------------------------


class TestFormatLocal:
    def test_renders_the_named_zone(self):
        assert format_local(SUMMER, LA) == "2026-08-21 10:30:16 PDT"

    def test_utc_is_rendered_as_utc_not_z(self):
        # Deliberately *not* utc_iso's format: this is prose, and the two must
        # stay visibly different so neither is mistaken for the other.
        assert format_local(SUMMER, timezone.utc) == "2026-08-21 17:30:16 UTC"

    def test_the_offset_is_re_derived_per_instant_not_frozen(self):
        # The daemon runs for months.  A zone resolved to a fixed offset at
        # startup would render every later instant at the offset in force on
        # boot -- 10:30 PDT would still read as 10:30 after the November
        # transition.  Both sides of the same zone, one call apart:
        assert format_local(SUMMER, LA) == "2026-08-21 10:30:16 PDT"
        assert format_local(WINTER, LA) == "2026-01-21 09:30:16 PST"

    def test_a_bare_fixed_offset_is_still_labelled(self):
        # No zone name to print, but an unlabelled local time is ambiguous, so
        # something identifying the offset has to survive.
        rendered = format_local(SUMMER, timezone(timedelta(hours=-7)))
        assert rendered.startswith("2026-08-21 10:30:16 ")
        assert rendered.rstrip().endswith("07:00")

    def test_none_follows_tz_and_still_re_derives_the_offset(self, process_tz):
        # The default path in every deployment that only sets TZ.  Both DST
        # sides again, because this branch resolves the zone through the C
        # library rather than through ZoneInfo.
        process_tz("America/Los_Angeles")
        assert format_local(SUMMER, None) == "2026-08-21 10:30:16 PDT"
        assert format_local(WINTER, None) == "2026-01-21 09:30:16 PST"

    def test_a_naive_instant_is_read_as_utc_like_utc_iso(self):
        # Defensive on both sides, but they have to agree: astimezone() would
        # otherwise read a naive instant as *local*, so the prose and the
        # annotation would describe two different moments.
        naive = SUMMER.replace(tzinfo=None)
        assert format_local(naive, LA) == format_local(SUMMER, LA)
        assert utc_iso(naive) == utc_iso(SUMMER)


class TestLocalDisplay:
    def test_defaults_to_utc_when_nothing_is_configured(self, display_tz):
        # An unconfigured deployment must render exactly what it rendered
        # before this feature existed, modulo the format.
        assert local_display(SUMMER) == format_local(SUMMER, None)

    def test_uses_the_configured_zone(self, display_tz):
        display_tz(LA)
        assert local_display(SUMMER) == "2026-08-21 10:30:16 PDT"

    def test_utc_iso_is_unaffected_by_the_display_zone(self, display_tz):
        # The load-bearing half: the annotation wire format and the log fields
        # must not move when the prose does.
        display_tz(LA)
        assert utc_iso(SUMMER) == "2026-08-21T17:30:16Z"


# ---------------------------------------------------------------------------
# EVENT_DISPLAY_TIMEZONE
# ---------------------------------------------------------------------------


class TestEnvTz:
    def test_absent_falls_through_to_the_process_zone(self, monkeypatch):
        monkeypatch.delenv("EVENT_DISPLAY_TIMEZONE", raising=False)
        assert _env_tz("EVENT_DISPLAY_TIMEZONE") is None

    def test_a_named_zone_is_resolved(self, monkeypatch):
        monkeypatch.setenv("EVENT_DISPLAY_TIMEZONE", "America/Los_Angeles")
        assert _env_tz("EVENT_DISPLAY_TIMEZONE") == LA

    def test_whitespace_is_stripped(self, monkeypatch):
        monkeypatch.setenv("EVENT_DISPLAY_TIMEZONE", "  America/Los_Angeles  ")
        assert _env_tz("EVENT_DISPLAY_TIMEZONE") == LA

    @pytest.mark.parametrize("raw", ["Not/AZone", "PDT", "../etc/passwd", "!"])
    def test_junk_does_not_kill_startup(self, monkeypatch, raw):
        # Same posture as _env_bool/_env_int: a display setting must never be
        # the reason a controller refuses to boot.
        monkeypatch.setenv("EVENT_DISPLAY_TIMEZONE", raw)
        assert _env_tz("EVENT_DISPLAY_TIMEZONE") is None

    def test_rejection_warns_and_names_the_variable(self, monkeypatch, caplog):
        monkeypatch.setenv("EVENT_DISPLAY_TIMEZONE", "Not/AZone")
        with caplog.at_level(logging.WARNING, logger="app.config"):
            assert _env_tz("EVENT_DISPLAY_TIMEZONE") is None
        record = next(
            r for r in caplog.records if "event=config.invalid" in r.getMessage()
        )
        fields = kv_fields(record.getMessage())
        assert fields["name"] == "EVENT_DISPLAY_TIMEZONE"
        assert fields["value"] == "Not/AZone"
        assert fields["reason"] == "unknown_timezone"

    def test_an_accepted_zone_is_silent(self, monkeypatch, caplog):
        monkeypatch.setenv("EVENT_DISPLAY_TIMEZONE", "UTC")
        with caplog.at_level(logging.WARNING, logger="app.config"):
            _env_tz("EVENT_DISPLAY_TIMEZONE")
        assert not [r for r in caplog.records if "config.invalid" in r.getMessage()]


class TestTimezoneLabel:
    def test_a_named_zone_reports_its_key(self):
        assert timezone_label(LA) == "America/Los_Angeles"

    def test_the_process_zone_reports_tz(self, monkeypatch):
        monkeypatch.setenv("TZ", "America/Los_Angeles")
        assert timezone_label(None) == "America/Los_Angeles"

    def test_the_process_zone_with_no_tz_reports_system(self, monkeypatch):
        monkeypatch.delenv("TZ", raising=False)
        assert timezone_label(None) == "system"


class TestConfigWiring:
    @pytest.fixture
    def env(self, monkeypatch):
        monkeypatch.setenv("RESERVATION_API_URL", "http://reservations.local")
        monkeypatch.setenv("RESERVATION_API_KEY", "gpures_test")
        return monkeypatch

    def test_default_is_the_process_zone(self, env):
        env.delenv("EVENT_DISPLAY_TIMEZONE", raising=False)
        assert Config.from_env().display_timezone is None

    def test_the_variable_is_honoured(self, env):
        env.setenv("EVENT_DISPLAY_TIMEZONE", "Asia/Kolkata")
        assert Config.from_env().display_timezone == ZoneInfo("Asia/Kolkata")

    def test_junk_leaves_the_process_zone(self, env):
        env.setenv("EVENT_DISPLAY_TIMEZONE", "Mars/Olympus_Mons")
        assert Config.from_env().display_timezone is None


# ---------------------------------------------------------------------------
# The four messages — prose local, companion value UTC
# ---------------------------------------------------------------------------


class _CapturingCore:
    """Minimal CoreV1Api stand-in that records the Events written through it."""

    def __init__(self):
        self.events: list = []

    def create_namespaced_event(self, namespace, body):
        self.events.append((namespace, body))
        return SimpleNamespace()


@pytest.fixture
def core(monkeypatch, display_tz):
    """A capturing API client with the display zone pinned to Los Angeles."""
    display_tz(LA)
    capturing = _CapturingCore()
    monkeypatch.setattr(k8s_client, "_core_v1", capturing)
    return capturing


POD = SimpleNamespace(metadata=SimpleNamespace(uid="uid-1"))


class TestRuntimeGuaranteedMessage:
    def test_the_event_reads_local(self, core):
        asyncio.run(
            emit_runtime_guaranteed_event(POD, "pod-1", "alice", 699, SUMMER)
        )
        _ns, event = core.events[0]
        assert "until 2026-08-21 10:30:16 PDT." in event.message
        assert "17:30:16Z" not in event.message

    def test_the_duration_is_untouched(self, core):
        # 699s = 11m39s.  Durations are timezone-free and must not be disturbed.
        asyncio.run(
            emit_runtime_guaranteed_event(POD, "pod-1", "alice", 699, SUMMER)
        )
        _ns, event = core.events[0]
        assert "guaranteed for 11m39s" in event.message

    def test_the_log_field_stays_utc(self, core, caplog):
        # until= joins this line to the app's own logs and to the
        # galends/guaranteed-until annotation the same admission writes.
        with caplog.at_level(logging.INFO, logger="app.k8s_client"):
            asyncio.run(
                emit_runtime_guaranteed_event(POD, "pod-1", "alice", 699, SUMMER)
            )
        record = next(
            r for r in caplog.records if "event=k8s.event_emitted" in r.getMessage()
        )
        assert kv_fields(record.getMessage())["until"] == "2026-08-21T17:30:16Z"


class TestReservationRelinkedMessage:
    def test_a_merge_does_not_call_the_pod_an_overstay(self, core):
        asyncio.run(k8s_client.emit_reservation_relinked_event(
            POD, "pod-1", "alice", 42, SUMMER,
            previous_reservation_id=41, cause="merge",
        ))
        _ns, event = core.events[0]
        assert event.reason == "ReservationRelinked"
        assert "overstay" not in event.message.lower()
        assert "lease #41" in event.message
        assert "guaranteed until 2026-08-21 10:30:16 PDT." in event.message

    def test_a_replaced_reservation_says_so(self, core):
        asyncio.run(k8s_client.emit_reservation_relinked_event(
            POD, "pod-1", "alice", 42, SUMMER,
            previous_reservation_id=41, cause="replaced",
        ))
        _ns, event = core.events[0]
        assert "reservation #41 was cancelled or replaced" in event.message
        assert "overstay" not in event.message.lower()


class TestOverstayRelinkedMessage:
    def test_the_event_reads_local(self, core):
        asyncio.run(
            emit_overstay_relinked_event(POD, "pod-1", "alice", 42, SUMMER)
        )
        _ns, event = core.events[0]
        assert "guaranteed until 2026-08-21 10:30:16 PDT." in event.message
        assert "17:30:16Z" not in event.message

    def test_the_log_field_stays_utc(self, core, caplog):
        with caplog.at_level(logging.INFO, logger="app.k8s_client"):
            asyncio.run(
                emit_overstay_relinked_event(POD, "pod-1", "alice", 42, SUMMER)
            )
        record = next(
            r for r in caplog.records if "event=k8s.event_emitted" in r.getMessage()
        )
        assert kv_fields(record.getMessage())["until"] == "2026-08-21T17:30:16Z"


class TestPreemptionMessage:
    def test_the_boundary_reads_local(self, main, display_tz):
        display_tz(LA)
        view = PodRuntimeView(
            uid="uid-1", namespace="alice", name="pod-1",
            gpu_class="a100", gpu_count=1, reservation_id=7,
            node_resident=True, terminating=False,
        )
        message = main._preemption_message(
            ControllerState(), view, SUMMER, SUMMER - timedelta(minutes=15)
        )
        assert "starting 2026-08-21 10:30:16 PDT:" in message
        assert "17:30:16Z" not in message


class TestTerminationWarningMessage:
    def test_the_projected_instant_reads_local(self, main, display_tz):
        display_tz(LA)
        message = main._termination_warning_message(SUMMER, "0.50")
        assert "as early as 2026-08-21 10:30:16 PDT to keep" in message
        assert "(risk 0.50)" in message
        assert "17:30:16Z" not in message

    def test_a_boundary_warning_names_the_booking_start_not_the_kill(
        self, main, display_tz
    ):
        # A proactive kill lands PREEMPTION_LEAD_MINUTES before the booking
        # starts, so the two instants differ and both must read correctly.
        display_tz(LA)
        boundary = SUMMER + timedelta(minutes=15)
        message = main._termination_warning_message(SUMMER, "0.50", boundary)
        assert "as early as 2026-08-21 10:30:16 PDT" in message
        assert "reservation starting at 2026-08-21 10:45:16 PDT" in message
        assert "starting then" not in message

    def test_a_headroom_warning_names_no_reservation(self, main, display_tz):
        display_tz(LA)
        message = main._termination_warning_message(SUMMER, "0.50")
        assert "reservation starting" not in message
        assert "on-demand jobs" in message

    def test_the_companion_annotation_stays_utc(self, main, core, display_tz):
        # -message is prose for a person; -at is a value a widget parses and a
        # value main's diff-and-skip rebuilds to decide whether to re-patch.
        # They describe one instant in two formats, deliberately.
        patches: list = []
        core.patch_namespaced_pod = lambda name, ns, body: patches.append(body)
        asyncio.run(
            k8s_client.annotate_termination_warning(
                "pod-1", "alice", SUMMER, "0.50",
                main._termination_warning_message(SUMMER, "0.50"),
            )
        )
        annotations = patches[0]["metadata"]["annotations"]
        assert annotations["galends/termination-warning-at"] == "2026-08-21T17:30:16Z"
        assert "2026-08-21 10:30:16 PDT" in annotations[
            "galends/termination-warning-message"
        ]
