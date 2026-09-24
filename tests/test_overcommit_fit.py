"""Guard 4 under ``ONDEMAND_OVERCOMMIT_FIT`` (the default).

On a GPU class the reservation app over-counts, an on-demand lease is admitted
only if it fits in the *physical* GPUs for its whole duration, alongside every
reservation already booked in that time -- rather than the whole class being
paused, which is what ``ONDEMAND_OVERCOMMIT_FIT=false`` still does (covered in
``test_capacity_audit.py`` and ``test_admission_paused_event.py``).

Three layers:

- ``ControllerState.peak_committed_gpus`` -- the calendar arithmetic, pure.
- ``main._preflight_ondemand_candidate`` / ``_run_ondemand_admission_once`` --
  which asks are held, including across a batch and across batches.
- ``plan_ondemand_gates`` / ``_warn_ondemand_gates`` -- the operator warning,
  which now reports guard 4 only while it is holding someone.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import pytest

from app.controller import ControllerState

from tests.conftest import (
    FIXED_DATE,
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    OTHER_CLASS_ID,
    OTHER_CLASS_LABEL,
    USERNAME,
    kv_fields,
    make_config,
    reservation,
)
from tests.test_admission_paused_event import _Recorder
from tests.test_jit_lease import _candidate, _gpu_only_condition, _main_module, _pod

T0 = datetime.combine(FIXED_DATE, datetime.min.time()).replace(
    tzinfo=timezone.utc
) + timedelta(hours=12)


def _h(hours: float) -> datetime:
    return T0 + timedelta(hours=hours)


def _res(res_id, start, end, *, gpus=2, kind="booking", class_id=GPU_CLASS_ID,
         username="bob"):
    return reservation(
        res_id, start_utc=start, end_utc=end, gpu_count=gpus, kind=kind,
        gpu_class_id=class_id, username=username, with_user=True,
    )


def _state(*reservations) -> ControllerState:
    state = ControllerState()
    state.gpu_class_labels = {
        GPU_CLASS_ID: GPU_CLASS_LABEL, OTHER_CLASS_ID: OTHER_CLASS_LABEL,
    }
    state.gpu_class_ids = {
        GPU_CLASS_LABEL: GPU_CLASS_ID, OTHER_CLASS_LABEL: OTHER_CLASS_ID,
    }
    state.reservations = list(reservations)
    return state


# ---------------------------------------------------------------------------
# peak_committed_gpus
# ---------------------------------------------------------------------------


class TestPeakCommittedGpus:
    def test_an_empty_calendar_commits_nothing(self):
        assert _state().peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(2)) == (0, T0)

    def test_a_booking_already_open_counts_from_the_start(self):
        state = _state(_res(1, _h(-1), _h(1), gpus=3))
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(2)) == (3, T0)

    def test_a_booking_starting_mid_window_raises_the_peak_there(self):
        # The case the present-time check would miss: nothing is committed now,
        # but a booking opens while the lease would still be running.
        state = _state(_res(1, _h(-1), _h(4), gpus=6), _res(2, _h(1), _h(3), gpus=6))
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(3)) == (12, _h(1))

    def test_the_window_is_half_open(self):
        # A booking starting exactly as the lease ends, or ending exactly as it
        # starts, never shares an instant with it.
        state = _state(_res(1, _h(2), _h(3), gpus=4), _res(2, _h(-1), T0, gpus=4))
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(2)) == (0, T0)

    def test_abutting_bookings_do_not_add_up(self):
        state = _state(_res(1, _h(-1), _h(1), gpus=4), _res(2, _h(1), _h(3), gpus=3))
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(3)) == (4, T0)

    def test_a_granted_lease_counts_like_a_booking(self):
        state = _state(_res(1, T0, _h(1), gpus=2, kind="on_demand"))
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(2))[0] == 2

    def test_a_best_effort_stub_holds_nothing(self):
        state = _state(_res(1, _h(0.5), _h(0.5), gpus=2, kind="best_effort"))
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(2))[0] == 0

    def test_another_class_is_not_counted(self):
        state = _state(_res(1, T0, _h(1), gpus=4, class_id=OTHER_CLASS_ID))
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(2))[0] == 0
        assert state.peak_committed_gpus(OTHER_CLASS_LABEL, T0, _h(2))[0] == 4

    def test_a_declared_no_show_is_not_counted(self):
        # As in boundary_demand: its capacity is already on-demand territory.
        state = _state(_res(1, _h(-1), _h(1), gpus=4))
        state.noshow_reservation_ids = {1}
        assert state.peak_committed_gpus(GPU_CLASS_LABEL, T0, _h(2))[0] == 0


# ---------------------------------------------------------------------------
# Preflight
# ---------------------------------------------------------------------------


def _preflight(monkeypatch, m, state, candidate, *, claimed=None, **config):
    recorder = _Recorder()

    async def fake_read_pod(name, namespace):
        return _pod(conditions=[_gpu_only_condition()])

    monkeypatch.setattr(m, "read_pod", fake_read_pod)
    monkeypatch.setattr(m, "emit_admission_paused_event", recorder)
    status, _ask = asyncio.run(m._preflight_ondemand_candidate(
        state, make_config(**config), candidate.pod_uid, candidate, claimed,
    ))
    return status, recorder


def _overcommitted(state, physical):
    state.overcommitted_gpu_classes = {GPU_CLASS_LABEL}
    state.gpu_class_capacity = {GPU_CLASS_LABEL: physical + 1}
    state.physical_gpu_capacity = {GPU_CLASS_LABEL: physical}
    return state


def _now_state(*spans_and_gpus):
    """Bookings by other users, relative to the real clock the preflight reads."""
    now = datetime.now(timezone.utc)
    return _state(*(
        _res(900 + i, now + timedelta(hours=a), now + timedelta(hours=b), gpus=g)
        for i, (a, b, g) in enumerate(spans_and_gpus)
    ))


class TestPreflightFit:
    def test_a_lightly_loaded_class_still_admits(self, monkeypatch):
        # The point of the change: one GPU down no longer stops on-demand work
        # on a class with room to spare.
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state((-1, 4, 6)), physical=15)
        status, rec = _preflight(
            monkeypatch, m, state, _candidate(gpu_requested=4, min_runtime_seconds=3 * 3600),
        )
        assert status == m._PREFLIGHT_READY
        assert rec.calls == []

    def test_a_booking_that_opens_mid_lease_holds_it(self, monkeypatch, caplog):
        # 15 physical, 16 in the app.  6 committed now and 6 more from +1h: the
        # app would sell a 4-GPU, 3-hour lease (12 + 4 = 16), but the booking at
        # +1h would then find no GPU to land on.
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state((-1, 4, 6), (1, 3, 6)), physical=15)
        candidate = _candidate(gpu_requested=4, min_runtime_seconds=3 * 3600)
        before = candidate.next_attempt_at

        with caplog.at_level(logging.INFO, logger="app.main"):
            status, rec = _preflight(monkeypatch, m, state, candidate)

        assert status == m._PREFLIGHT_RETRY
        assert candidate.next_attempt_at > before
        assert candidate.held_by_overcommit is True
        [line] = [
            kv_fields(r.getMessage()) for r in caplog.records
            if "event=ondemand.candidate_held" in r.getMessage()
        ]
        assert (line["guard"], line["reason"]) == ("4", "overcommit_no_fit")
        assert (line["committed"], line["phys_gpus"], line["gpus"]) == ("12", "15", "4")
        assert line["peak_at"] == state.reservations[1].start_utc.isoformat()
        assert "claimed" not in line
        assert [c["guard"] for c in rec.calls] == [4]
        assert rec.calls[0]["message"] == m._admission_paused_message(
            4, GPU_CLASS_LABEL, make_config()
        )

    def test_the_same_ask_ending_before_that_booking_is_admitted(self, monkeypatch):
        # 20 min + the 10 min lease buffer ends well before +1h.
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state((-1, 4, 6), (1, 3, 6)), physical=15)
        status, _rec = _preflight(
            monkeypatch, m, state, _candidate(gpu_requested=4, min_runtime_seconds=1200),
        )
        assert status == m._PREFLIGHT_READY

    def test_an_exact_fit_is_admitted(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state((-1, 4, 11)), physical=15)
        status, _rec = _preflight(monkeypatch, m, state, _candidate(gpu_requested=4))
        assert status == m._PREFLIGHT_READY

    def test_one_gpu_over_is_held(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state((-1, 4, 12)), physical=15)
        status, _rec = _preflight(monkeypatch, m, state, _candidate(gpu_requested=4))
        assert status == m._PREFLIGHT_RETRY

    def test_asks_passed_earlier_in_the_batch_count(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state(), physical=4)
        with caplog.at_level(logging.INFO, logger="app.main"):
            status, _rec = _preflight(
                monkeypatch, m, state, _candidate(gpu_requested=2),
                claimed={GPU_CLASS_LABEL: 3},
            )
        assert status == m._PREFLIGHT_RETRY
        [line] = [
            kv_fields(r.getMessage()) for r in caplog.records
            if "event=ondemand.candidate_held" in r.getMessage()
        ]
        assert line["claimed"] == "3"

    def test_a_class_with_no_physical_entry_has_no_gpus(self, monkeypatch):
        # Overcommitted with no physical entry means no schedulable node at all.
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state(), physical=0)
        state.physical_gpu_capacity = {}
        status, _rec = _preflight(monkeypatch, m, state, _candidate())
        assert status == m._PREFLIGHT_RETRY

    def test_a_class_the_app_counts_correctly_is_left_to_the_app(self, monkeypatch):
        # Not over-counted: the app's own calendar check is against the right
        # number already, so guard 4 does not second-guess it.
        m = _main_module(monkeypatch)
        state = _now_state((-1, 4, 12))
        state.physical_gpu_capacity = {GPU_CLASS_LABEL: 4}
        status, _rec = _preflight(monkeypatch, m, state, _candidate(gpu_requested=4))
        assert status == m._PREFLIGHT_READY

    def test_with_the_flag_off_even_a_fitting_ask_is_held(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state(), physical=15)
        with caplog.at_level(logging.INFO, logger="app.main"):
            status, rec = _preflight(
                monkeypatch, m, state, _candidate(), ondemand_overcommit_fit=False,
            )
        assert status == m._PREFLIGHT_RETRY
        [line] = [
            kv_fields(r.getMessage()) for r in caplog.records
            if "event=ondemand.candidate_held" in r.getMessage()
        ]
        assert line["reason"] == "class_overcommitted"
        assert "committed" not in line
        assert "is paused" in rec.calls[0]["message"]

    def test_the_hold_mark_describes_the_latest_attempt_only(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _overcommitted(_now_state((-1, 4, 15)), physical=15)
        candidate = _candidate()
        _preflight(monkeypatch, m, state, candidate)
        assert candidate.held_by_overcommit is True

        state.reservations = []  # the booking ended; the ask now fits
        status, _rec = _preflight(monkeypatch, m, state, candidate)
        assert status == m._PREFLIGHT_READY
        assert candidate.held_by_overcommit is False


class TestAcrossBatches:
    """``claimed`` bounds one batch; the upserted lease bounds the next."""

    def _run(self, monkeypatch, m, state, granted):
        async def fake_read_pod(name, namespace):
            return _pod(conditions=[_gpu_only_condition()])

        async def fake_grant(state, client, config, uid, candidate, ask):
            now = datetime.now(timezone.utc)
            state.reservations.append(_res(
                700 + len(granted), now, now + timedelta(seconds=ask.duration_seconds),
                gpus=ask.gpu_count, kind="on_demand", username=USERNAME,
            ))
            granted.append(uid)
            return True

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "emit_admission_paused_event", _Recorder())
        monkeypatch.setattr(m, "_grant_and_admit", fake_grant)
        asyncio.run(m._run_ondemand_admission_once(
            state, None, make_config(ondemand_delegate_admission=False),
        ))

    def test_only_what_fits_is_granted(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _overcommitted(_state(), physical=4)
        for uid in ("uid-a", "uid-b"):
            state.ondemand_candidates[uid] = _candidate(uid, gpu_requested=3)
        granted: list[str] = []

        self._run(monkeypatch, m, state, granted)
        assert granted == ["uid-a"]
        assert state.ondemand_candidates["uid-b"].held_by_overcommit is True

        # The next batch sees the lease just granted, not an empty calendar.
        state.ondemand_candidates["uid-b"].next_attempt_at = datetime.now(timezone.utc)
        self._run(monkeypatch, m, state, granted)
        assert granted == ["uid-a"]


# ---------------------------------------------------------------------------
# The operator warning
# ---------------------------------------------------------------------------


NOW = T0


class TestGateWarning:
    def test_an_overcounted_class_holding_nobody_is_not_reported(self):
        state = _overcommitted(_state(), physical=8)
        state.ondemand_candidates = {"a": _candidate("a")}  # waiting, not held
        assert state.plan_ondemand_gates(NOW, overcommit_fit=True) == []

    def test_only_held_candidates_are_counted(self):
        state = _overcommitted(_state(), physical=8)
        held = _candidate("a")
        held.held_by_overcommit = True
        state.ondemand_candidates = {"a": held, "b": _candidate("b")}

        [gate] = state.plan_ondemand_gates(NOW, overcommit_fit=True)

        assert (gate.label, gate.guard, gate.reason) == (
            GPU_CLASS_LABEL, 4, "overcommit_no_fit",
        )
        assert gate.waiting == 1
        assert (gate.app_gpus, gate.phys_gpus) == (9, 8)

    def test_it_is_re_dated_once_it_stops_holding(self):
        state = _overcommitted(_state(), physical=8)
        held = _candidate("a")
        held.held_by_overcommit = True
        state.ondemand_candidates = {"a": held}
        assert state.plan_ondemand_gates(NOW, overcommit_fit=True)[0].since == NOW

        held.held_by_overcommit = False
        later = NOW + timedelta(minutes=5)
        assert state.plan_ondemand_gates(later, overcommit_fit=True) == []
        held.held_by_overcommit = True
        assert state.plan_ondemand_gates(later, overcommit_fit=True)[0].since == later

    def test_the_line_says_limited_not_paused(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        state = _overcommitted(_state(), physical=8)
        held = _candidate("a")
        held.held_by_overcommit = True
        state.ondemand_candidates = {"a": held}

        with caplog.at_level(logging.WARNING, logger="app.main"):
            m._warn_ondemand_gates(state, make_config())

        [f] = [
            kv_fields(r.getMessage()) for r in caplog.records
            if "event=ondemand.gated" in r.getMessage()
        ]
        assert (f["guard"], f["reason"], f["candidates"]) == ("4", "overcommit_no_fit", "1")
        detail = f["detail"]
        assert "are limited" in detail and "paused" not in detail
        assert "1 pending pod(s) do not" in detail
        assert "9 GPUs" in detail and "only 8" in detail
        assert "ONDEMAND_OVERCOMMIT_FIT" in detail
        assert "as soon as its lease fits" in detail


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


class TestConfigFlag:
    def _from_env(self, monkeypatch, value=None):
        from app.config import Config

        monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
        monkeypatch.setenv("RESERVATION_API_KEY", "test-key")
        if value is None:
            monkeypatch.delenv("ONDEMAND_OVERCOMMIT_FIT", raising=False)
        else:
            monkeypatch.setenv("ONDEMAND_OVERCOMMIT_FIT", value)
        return Config.from_env()

    def test_defaults_on(self, monkeypatch):
        assert self._from_env(monkeypatch).ondemand_overcommit_fit is True

    @pytest.mark.parametrize("raw", ["0", "false", "no", "off", " FALSE "])
    def test_falsy_words_restore_the_blanket_pause(self, monkeypatch, raw):
        assert self._from_env(monkeypatch, raw).ondemand_overcommit_fit is False

    def test_junk_falls_back_to_the_default(self, monkeypatch):
        assert self._from_env(monkeypatch, "maybe").ondemand_overcommit_fit is True
