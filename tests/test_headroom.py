"""Tests for anticipatory headroom preemption.

Headroom holds ``HEADROOM_TARGET_PERCENT`` of each GPU class free for on-demand
jobs that have **not arrived yet**, rather than waiting for a booking boundary
to demand the capacity.  Two halves are exercised here:

- the pure planners in ``controller.py`` (``headroom_shortfall_by_class``,
  ``plan_headroom_candidates``, ``plan_headroom_warnings``), and
- the sweep integration in ``main._run_preemption_sweep`` — the notice gate, the
  ``HEADROOM_CHECK_INTERVAL`` throttle, and the interaction with the
  termination-warning reconcile.

Kubernetes-boundary coroutines are monkeypatched at the ``app.main`` module
level, the same convention ``test_preemption_sweep.py`` uses.  Time is always
passed in explicitly; nothing here reads the wall clock.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone

from app.controller import PodRuntimeView
from app.k8s_client import utc_iso

from tests.conftest import make_config, GPU_CLASS_LABEL, USERNAME, kv_fields
from tests.conftest import make_state as _state
from tests.conftest import reservation
from tests.test_preemption_sweep import (
    _main_module,
    _patch_snapshots,
    _patch_warnings,
    _pod,
)

# "Now" for every test.  Nothing derives a time from the wall clock.
H = datetime(2024, 3, 5, 12, 0, tzinfo=timezone.utc)

# 10 physical GPUs at 10% → a target of 1 GPU held free.
CAPACITY = {GPU_CLASS_LABEL: 10}


def _config(**overrides):
    """A headroom-enabled config; override what a test actually cares about."""
    base = dict(
        headroom_target_percent=10,
        headroom_notice_minutes=15,
        headroom_check_interval=600,
    )
    base.update(overrides)
    return make_config(**base)


def _ended_booking(res_id: int, *, gpu_count: int = 1, username: str = USERNAME):
    """A booking whose window closed an hour ago — its pod is an overstayer.

    Deliberately far enough in the past that ``upcoming_boundaries`` and
    ``forecast_boundaries`` both ignore it, so a sweep over these reservations
    runs for headroom reasons *only*.
    """
    return reservation(
        res_id,
        start_utc=H - timedelta(hours=3),
        end_utc=H - timedelta(hours=1),
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
        username=username,
        user_id=1,
    )


def _view(uid: str, *, reservation_id: int, gpu_count: int = 1,
          warning_at: datetime | None = None) -> PodRuntimeView:
    return PodRuntimeView(
        uid=uid,
        namespace=USERNAME,
        name=f"pod-{uid}",
        gpu_class=GPU_CLASS_LABEL,
        gpu_count=gpu_count,
        reservation_id=reservation_id,
        node_resident=True,
        terminating=False,
        termination_warning_at=warning_at,
    )


# ---------------------------------------------------------------------------
# Pure planners
# ---------------------------------------------------------------------------


class TestHeadroomTargetMath:
    def test_target_rounds_up_so_a_small_class_still_reserves_a_gpu(self):
        state = _state()
        target, _, _ = state.headroom_shortfall_by_class({"a": 15}, [], 10)
        assert target["a"] == 2  # ceil(1.5), not 1

    def test_a_fully_free_class_has_no_shortfall(self):
        state = _state()
        _, free, shortfall = state.headroom_shortfall_by_class(CAPACITY, [], 10)
        assert free[GPU_CLASS_LABEL] == 10
        assert shortfall == {}

    def test_shortfall_is_target_minus_free(self):
        state = _state(_ended_booking(1))
        pods = [_view(f"p{i}", reservation_id=1) for i in range(10)]
        _, free, shortfall = state.headroom_shortfall_by_class(CAPACITY, pods, 10)
        assert free[GPU_CLASS_LABEL] == 0
        assert shortfall == {GPU_CLASS_LABEL: 1}

    def test_a_class_with_unknown_physical_capacity_is_skipped(self):
        state = _state()
        target, _, shortfall = state.headroom_shortfall_by_class({}, [], 10)
        assert target == {} and shortfall == {}


class TestHeadroomCandidates:
    def test_a_pod_inside_its_guarantee_is_never_a_candidate(self):
        """The invariant that must never regress, at any shortfall."""
        live = reservation(
            1,
            start_utc=H - timedelta(hours=1),
            end_utc=H + timedelta(hours=5),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(live)
        pods = [
            _view(f"p{i}", reservation_id=1, warning_at=H - timedelta(hours=1))
            for i in range(10)
        ]
        need = state.plan_headroom_candidates(CAPACITY, pods, H, 10)
        assert need.kills_needed_by_class == {GPU_CLASS_LABEL: 1}
        assert need.candidates_by_class[GPU_CLASS_LABEL] == []

    def test_an_overstayer_with_an_elapsed_notice_is_a_candidate(self):
        state = _state(_ended_booking(1))
        pods = [
            _view(f"p{i}", reservation_id=1, warning_at=H - timedelta(minutes=1))
            for i in range(10)
        ]
        need = state.plan_headroom_candidates(CAPACITY, pods, H, 10)
        assert len(need.candidates_by_class[GPU_CLASS_LABEL]) == 10

    def test_an_overstayer_with_no_notice_yet_is_not_a_candidate(self):
        state = _state(_ended_booking(1))
        pods = [_view(f"p{i}", reservation_id=1) for i in range(10)]
        need = state.plan_headroom_candidates(CAPACITY, pods, H, 10)
        assert need.candidates_by_class[GPU_CLASS_LABEL] == []

    def test_an_overstayer_still_inside_its_notice_is_not_a_candidate(self):
        state = _state(_ended_booking(1))
        pods = [
            _view(f"p{i}", reservation_id=1, warning_at=H + timedelta(minutes=5))
            for i in range(10)
        ]
        need = state.plan_headroom_candidates(CAPACITY, pods, H, 10)
        assert need.candidates_by_class[GPU_CLASS_LABEL] == []

    def test_the_notice_gate_can_be_bypassed(self):
        """What the sweep does when TERMINATION_WARNING_ENABLED is off."""
        state = _state(_ended_booking(1))
        pods = [_view(f"p{i}", reservation_id=1) for i in range(10)]
        need = state.plan_headroom_candidates(
            CAPACITY, pods, H, 10, require_elapsed_notice=False
        )
        assert len(need.candidates_by_class[GPU_CLASS_LABEL]) == 10

    def test_a_terminating_pod_is_not_a_candidate(self):
        from dataclasses import replace

        state = _state(_ended_booking(1))
        pods = [
            replace(
                _view(f"p{i}", reservation_id=1, warning_at=H - timedelta(minutes=1)),
                terminating=True,
            )
            for i in range(10)
        ]
        need = state.plan_headroom_candidates(CAPACITY, pods, H, 10)
        # Terminating pods free their GPUs, so there is no shortfall at all.
        assert need.kills_needed_by_class == {}


class TestHeadroomWarnings:
    def test_the_full_eligible_pool_is_warned_not_just_the_shortfall(self):
        state = _state(_ended_booking(1))
        pods = [_view(f"p{i}", reservation_id=1) for i in range(10)]
        warnings = state.plan_headroom_warnings(
            CAPACITY, pods, H, 10, timedelta(minutes=15)
        )
        assert len(warnings) == 10  # shortfall is 1, but any of them could be picked

    def test_a_fresh_pod_is_given_now_plus_the_notice(self):
        state = _state(_ended_booking(1))
        pods = [_view(f"p{i}", reservation_id=1) for i in range(10)]
        warnings = state.plan_headroom_warnings(
            CAPACITY, pods, H, 10, timedelta(minutes=15)
        )
        assert warnings["p0"].terminate_at == H + timedelta(minutes=15)

    def test_an_existing_deadline_is_kept_not_pushed_forward(self):
        """Stickiness — otherwise the deadline recedes and never elapses."""
        state = _state(_ended_booking(1))
        stamped = H - timedelta(minutes=3)
        pods = [
            _view(f"p{i}", reservation_id=1, warning_at=stamped) for i in range(10)
        ]
        later = state.plan_headroom_warnings(
            CAPACITY, pods, H + timedelta(minutes=30), 10, timedelta(minutes=15)
        )
        assert later["p0"].terminate_at == stamped

    def test_risk_is_shortfall_over_pool_gpus(self):
        state = _state(_ended_booking(1))
        pods = [_view(f"p{i}", reservation_id=1) for i in range(10)]
        warnings = state.plan_headroom_warnings(
            CAPACITY, pods, H, 10, timedelta(minutes=15)
        )
        assert warnings["p0"].risk == 0.1  # 1 GPU short over a 10-GPU pool

    def test_an_in_guarantee_pool_is_not_warned(self):
        live = reservation(
            1,
            start_utc=H - timedelta(hours=1),
            end_utc=H + timedelta(hours=5),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
        )
        state = _state(live)
        pods = [_view(f"p{i}", reservation_id=1) for i in range(10)]
        assert state.plan_headroom_warnings(
            CAPACITY, pods, H, 10, timedelta(minutes=15)
        ) == {}


# ---------------------------------------------------------------------------
# Sweep integration
# ---------------------------------------------------------------------------


def _run(m, state, config, *, pods, now, capacity=None):
    return asyncio.run(m._run_preemption_sweep(state, config, now=now))


class TestSweepDisabledByDefault:
    def test_zero_percent_takes_no_snapshots_when_nothing_else_is_in_scope(
        self, monkeypatch
    ):
        m = _main_module(monkeypatch)
        state = _state(_ended_booking(1))

        async def _boom(_key, _group_label_key=None):
            raise AssertionError("headroom must not force a snapshot when disabled")

        monkeypatch.setattr(m, "snapshot_tolerated_pods", _boom)
        monkeypatch.setattr(m, "snapshot_node_gpu_capacity", _boom)

        asyncio.run(
            m._run_preemption_sweep(state, make_config(), now=H)
        )


class TestNoticeGate:
    def _setup(self, monkeypatch, *, pods):
        m = _main_module(monkeypatch)
        state = _state(_ended_booking(1))
        deleted, events = _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        writes, clears = _patch_warnings(monkeypatch, m)
        return m, state, deleted, events, writes, clears

    def test_first_evaluation_warns_and_kills_nothing(self, monkeypatch):
        pods = [_pod(f"p{i}", booking_reference="res-1", reservation_id=1)
                for i in range(10)]
        m, state, deleted, _, writes, _ = self._setup(monkeypatch, pods=pods)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        assert deleted == []
        assert len(writes) == 10
        # Every pod is told the same deadline: now + HEADROOM_NOTICE_MINUTES.
        assert {w[2] for w in writes} == {H + timedelta(minutes=15)}

    def test_a_pod_whose_notice_has_elapsed_is_killed(self, monkeypatch):
        stamped = utc_iso(H - timedelta(minutes=1))
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10")
            for i in range(10)
        ]
        m, state, deleted, events, _, _ = self._setup(monkeypatch, pods=pods)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        assert len(deleted) == 1  # shortfall is exactly 1 GPU
        assert "headroom" in events[0][2]

    def test_a_pod_still_inside_its_notice_is_spared(self, monkeypatch):
        stamped = utc_iso(H + timedelta(minutes=10))
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10")
            for i in range(10)
        ]
        m, state, deleted, _, _, _ = self._setup(monkeypatch, pods=pods)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        assert deleted == []

    def test_the_deadline_is_stable_across_ticks_so_it_is_not_re_patched(
        self, monkeypatch
    ):
        """Second tick must be a no-op patch-wise, or the deadline never lands."""
        pods = [_pod(f"p{i}", booking_reference="res-1", reservation_id=1)
                for i in range(10)]
        m, state, _, _, writes, clears = self._setup(monkeypatch, pods=pods)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))
        first = {w[1]: w[2] for w in writes}
        writes.clear()

        # The pods now carry what tick one wrote.
        stamped = utc_iso(H + timedelta(minutes=15))
        message = m._termination_warning_message(H + timedelta(minutes=15), "0.10")
        pods[:] = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10",
                 termination_warning_message=message)
            for i in range(10)
        ]
        asyncio.run(
            m._run_preemption_sweep(state, _config(), now=H + timedelta(minutes=11))
        )

        assert writes == [], "an unchanged warning must not be re-patched"
        assert clears == []
        assert set(first.values()) == {H + timedelta(minutes=15)}

    def test_notice_is_bypassed_when_termination_warnings_are_disabled(
        self, monkeypatch
    ):
        pods = [_pod(f"p{i}", booking_reference="res-1", reservation_id=1)
                for i in range(10)]
        m, state, deleted, _, _, _ = self._setup(monkeypatch, pods=pods)

        asyncio.run(m._run_preemption_sweep(
            state, _config(termination_warning_enabled=False), now=H
        ))

        assert len(deleted) == 1


class TestThrottle:
    def test_a_second_sweep_before_the_interval_does_not_re_evaluate(
        self, monkeypatch
    ):
        stamped = utc_iso(H - timedelta(minutes=1))
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10")
            for i in range(10)
        ]
        m = _main_module(monkeypatch)
        state = _state(_ended_booking(1))
        deleted, _ = _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))
        assert len(deleted) == 1
        assert state.headroom_last_eval == H

        # 60 s later — the sweep's own cadence, well inside the 600 s interval.
        asyncio.run(
            m._run_preemption_sweep(state, _config(), now=H + timedelta(seconds=60))
        )
        assert len(deleted) == 1, "headroom re-evaluated inside its own interval"

    def test_the_interval_elapsing_re_evaluates(self, monkeypatch):
        stamped = utc_iso(H - timedelta(minutes=1))
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10")
            for i in range(10)
        ]
        m = _main_module(monkeypatch)
        state = _state(_ended_booking(1))
        deleted, _ = _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))
        asyncio.run(
            m._run_preemption_sweep(state, _config(), now=H + timedelta(seconds=600))
        )
        assert len(deleted) == 2

    def test_a_failed_snapshot_does_not_consume_the_interval(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _state(_ended_booking(1))

        async def _boom(_key, _group_label_key=None):
            raise RuntimeError("apiserver down")

        monkeypatch.setattr(m, "snapshot_tolerated_pods", _boom)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))
        assert state.headroom_last_eval is None


class TestWarningReconcileInteraction:
    def test_a_headroom_warning_survives_a_boundary_only_tick(self, monkeypatch):
        """The livelock guard.

        Headroom warnings are recomputed on *every* sweep, not only when the
        throttled kill evaluation runs.  If they were skipped on a tick the
        sweep ran for a boundary, ``_apply_termination_warnings`` would clear
        the annotation as stale and the notice could never ripen into a kill.
        """
        stamped_at = H + timedelta(minutes=15)
        m = _main_module(monkeypatch)
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=utc_iso(stamped_at),
                 termination_warning_risk="0.10",
                 termination_warning_message=m._termination_warning_message(
                     stamped_at, "0.10"))
            for i in range(10)
        ]
        # A boundary in a *different* GPU class: it forces the sweep to run
        # without generating any demand — or any boundary warning — for the
        # h100 pods, so the only thing that can keep their annotation alive is
        # the headroom warning being recomputed on this tick.
        other_class = reservation(
            2,
            start_utc=H + timedelta(minutes=10),
            end_utc=H + timedelta(hours=2),
            gpu_count=1,
            gpu_class_id=999,
            gpu_class_label="other-class",
            username="someone-else",
            user_id=9,
        )
        state = _state(_ended_booking(1), other_class)
        deleted, _ = _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        writes, clears = _patch_warnings(monkeypatch, m)

        # Headroom already evaluated, so only the boundary drives this tick.
        state.headroom_last_eval = H
        asyncio.run(
            m._run_preemption_sweep(state, _config(), now=H + timedelta(seconds=30))
        )

        assert deleted == []
        assert clears == [], "a headroom notice was cleared by a boundary-only tick"
        assert writes == [], "the unchanged notice should not be re-patched either"

    def test_a_headroom_warning_survives_a_quiet_tick(self, monkeypatch):
        """The same guard on a tick with *nothing* in scope.

        A standing warning keeps the sweep running on otherwise-quiet ticks so
        stale ones can be cleared (``termination_warnings_outstanding``).  That
        reconcile must still recompute headroom warnings, or it would clear a
        live notice every minute and it could never ripen into a kill.
        """
        stamped_at = H + timedelta(minutes=15)
        m = _main_module(monkeypatch)
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=utc_iso(stamped_at),
                 termination_warning_risk="0.10",
                 termination_warning_message=m._termination_warning_message(
                     stamped_at, "0.10"))
            for i in range(10)
        ]
        state = _state(_ended_booking(1))
        deleted, _ = _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        writes, clears = _patch_warnings(monkeypatch, m)

        state.headroom_last_eval = H  # not due: nothing but the flag runs this tick
        asyncio.run(
            m._run_preemption_sweep(state, _config(), now=H + timedelta(seconds=30))
        )

        assert deleted == []
        assert clears == [], "a headroom notice was cleared by a quiet tick"
        assert writes == []
        assert state.termination_warnings_outstanding is True

    def test_the_sooner_of_two_deadlines_wins(self, monkeypatch):
        """A pod at risk from both sources is told the earlier instant."""
        pods = [_pod(f"p{i}", booking_reference="res-1", reservation_id=1)
                for i in range(10)]
        m = _main_module(monkeypatch)
        # A booking 25 min out: beyond PREEMPTION_LEAD_MINUTES (15) so nothing is
        # killed this tick, but inside TERMINATION_WARNING_LEAD_MINUTES (30) so it
        # still warns.  Its projected kill instant is boundary − lead = now+10m,
        # which is sooner than the now+15m headroom notice.
        future = reservation(
            2,
            start_utc=H + timedelta(minutes=25),
            end_utc=H + timedelta(hours=2),
            gpu_count=4,
            gpu_class_label=GPU_CLASS_LABEL,
            username="someone-else",
            user_id=9,
        )
        state = _state(_ended_booking(1), future)
        deleted, _ = _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        writes, _ = _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        assert deleted == [], "the boundary is beyond the kill window"
        assert len(writes) == 10
        assert {w[2] for w in writes} == {H + timedelta(minutes=10)}, (
            "the boundary's sooner deadline must win over the headroom notice"
        )


class TestBoundaryAndHeadroomTogether:
    def test_a_boundary_victim_is_not_also_selected_by_headroom(self, monkeypatch):
        """Boundary kills count as freed capacity and are never double-selected."""
        stamped = utc_iso(H - timedelta(minutes=1))
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10")
            for i in range(10)
        ]
        m = _main_module(monkeypatch)
        # A 1-GPU booking opening in 10 minutes: demand 1, free 0 → one kill.
        future = reservation(
            2,
            start_utc=H + timedelta(minutes=10),
            end_utc=H + timedelta(hours=2),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username="someone-else",
            user_id=9,
        )
        state = _state(_ended_booking(1), future)
        deleted, _ = _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        # The boundary kill frees 1 GPU, which satisfies the 1-GPU headroom
        # target on its own — so headroom adds nothing and kills no one twice.
        assert len(deleted) == 1
        assert len(set(deleted)) == len(deleted)


class TestExtensionAbortsTermination:
    """A user who extends must stop being a victim, on every extension shape."""

    def _warned_pods(self):
        stamped = utc_iso(H - timedelta(minutes=1))
        return [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10")
            for i in range(10)
        ]

    def test_an_abutting_follow_on_booking_spares_the_pod(self, monkeypatch):
        """Chaining grows guarantee_end, so the pod leaves the eligible pool."""
        m = _main_module(monkeypatch)
        extension = reservation(
            2,
            start_utc=H - timedelta(hours=1),   # abuts res-1's end exactly
            end_utc=H + timedelta(hours=2),
            gpu_count=1,
            gpu_class_label=GPU_CLASS_LABEL,
            username=USERNAME,
            user_id=1,
        )
        state = _state(_ended_booking(1), extension)
        deleted, _ = _patch_snapshots(
            monkeypatch, m, pods=self._warned_pods(), capacity=CAPACITY
        )
        writes, clears = _patch_warnings(monkeypatch, m)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        assert deleted == [], "an extended reservation must abort the kill"
        assert len(clears) == 10, "the stale warning must be cleared"

    def test_a_non_abutting_rebooking_is_adopted_and_spares_the_pod(
        self, monkeypatch
    ):
        """Adoption runs above victim planning, so the re-linked pod is safe."""
        m = _main_module(monkeypatch)
        # Starts *now* with a gap after res-1's end — chaining cannot reach it,
        # so only plan_pod_adoptions can rescue this pod.
        rebooked = reservation(
            2,
            start_utc=H,
            end_utc=H + timedelta(hours=2),
            gpu_count=10,
            gpu_class_label=GPU_CLASS_LABEL,
            username=USERNAME,
            user_id=1,
        )
        state = _state(_ended_booking(1), rebooked)
        deleted, _ = _patch_snapshots(
            monkeypatch, m, pods=self._warned_pods(), capacity=CAPACITY
        )
        _patch_warnings(monkeypatch, m)

        relinked: list[str] = []

        async def _apply_toleration(name, namespace, pod, key, value, ref):
            relinked.append(ref)

        async def _annotate_guarantee(*a, **k):
            pass

        async def _emit_relinked(*a, **k):
            pass

        monkeypatch.setattr(m, "apply_toleration", _apply_toleration)
        monkeypatch.setattr(m, "annotate_runtime_guarantee", _annotate_guarantee)
        monkeypatch.setattr(m, "emit_overstay_relinked_event", _emit_relinked)
        monkeypatch.setattr(m, "emit_reservation_relinked_event", _emit_relinked)
        monkeypatch.setattr(m, "is_terminal_phase", lambda _pod: False)

        asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        assert relinked, "the pod should have been adopted onto the new booking"
        assert all(r == "res-2" for r in relinked)
        assert deleted == [], "an adopted pod must never be a headroom victim"


class TestHeadroomLogging:
    def test_one_line_per_class_naming_target_free_and_kills(
        self, monkeypatch, caplog
    ):
        stamped = utc_iso(H - timedelta(minutes=1))
        pods = [
            _pod(f"p{i}", booking_reference="res-1", reservation_id=1,
                 termination_warning_at=stamped, termination_warning_risk="0.10")
            for i in range(10)
        ]
        m = _main_module(monkeypatch)
        state = _state(_ended_booking(1))
        _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        _patch_warnings(monkeypatch, m)

        with caplog.at_level(logging.INFO, logger="app.main"):
            asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        lines = [
            kv_fields(r.getMessage())
            for r in caplog.records
            if "event=preempt.headroom " in r.getMessage() + " "
        ]
        assert len(lines) == 1
        assert lines[0]["clabel"] == GPU_CLASS_LABEL
        assert lines[0]["demand"] == "1"   # the headroom target, not booking demand
        assert lines[0]["free"] == "0"
        assert lines[0]["kills"] == "1"

    def test_an_uncoverable_target_logs_unmet(self, monkeypatch, caplog):
        # Overstayers exist but none has an elapsed notice, so nothing is killable.
        pods = [_pod(f"p{i}", booking_reference="res-1", reservation_id=1)
                for i in range(10)]
        m = _main_module(monkeypatch)
        state = _state(_ended_booking(1))
        _patch_snapshots(monkeypatch, m, pods=pods, capacity=CAPACITY)
        _patch_warnings(monkeypatch, m)

        with caplog.at_level(logging.WARNING, logger="app.main"):
            asyncio.run(m._run_preemption_sweep(state, _config(), now=H))

        unmet = [
            kv_fields(r.getMessage())
            for r in caplog.records
            if "event=preempt.headroom_unmet" in r.getMessage()
        ]
        assert len(unmet) == 1
        assert unmet[0]["short"] == "1"
