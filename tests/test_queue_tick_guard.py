"""Whole-tick exception guard in ``queue_processor_loop``.

The tick body now lives in ``_run_queue_tick`` and the loop guards it the same
way the fetch/preemption/audit loops guard theirs: an exception is logged
(``queue.tick_failed``) and the loop retries next interval instead of dying.

Also covers the physical-state maps the tick refreshes for the JIT guards
(``class_node_counts`` for guard 1b, ``node_free_by_class`` for guard 5), whose
fail-safe is that a snapshot failure leaves the previous values standing.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

from app.controller import ControllerState

from tests.conftest import make_config

from tests.test_watch_release import _config


class _Stop(Exception):
    """Sentinel to break out of the infinite loop after the tick under test."""


def test_tick_exception_is_logged_and_loop_survives(monkeypatch, caplog):
    import app.main as main_module

    tick_calls = 0

    async def _boom_tick(state, client, config):
        nonlocal tick_calls
        tick_calls += 1
        raise RuntimeError("tick exploded")

    sleep_calls = 0

    async def _fake_sleep(seconds):
        nonlocal sleep_calls
        sleep_calls += 1
        if sleep_calls > 1:  # tick 1 ran (and raised); stop before tick 2
            raise _Stop()

    monkeypatch.setattr(main_module, "_run_queue_tick", _boom_tick)
    monkeypatch.setattr(asyncio, "sleep", _fake_sleep)

    with caplog.at_level(logging.ERROR, logger="app.main"):
        with pytest.raises(_Stop):
            asyncio.run(
                main_module.queue_processor_loop(ControllerState(), None, _config())
            )

    # Reaching the second sleep proves the RuntimeError did not kill the loop.
    assert tick_calls == 1
    assert any("queue.tick_failed" in r.getMessage() for r in caplog.records)


def test_extracted_tick_keeps_narrow_snapshot_guard(monkeypatch, caplog):
    """The pure move preserved the pre-existing per-snapshot fail-safe."""
    import app.main as main_module

    async def _fail_snapshot(*a, **kw):
        raise RuntimeError("api down")

    monkeypatch.setattr(main_module, "snapshot_tolerated_pods", _fail_snapshot)

    with caplog.at_level(logging.WARNING, logger="app.main"):
        asyncio.run(main_module._run_queue_tick(ControllerState(), None, _config()))

    assert any("queue.snapshot_failed" in r.getMessage() for r in caplog.records)


def _tick_with_inventory(monkeypatch, inventory, state):
    """Run one tick with a stubbed pod snapshot and node inventory."""
    import app.main as main_module

    async def _no_pods(*a, **kw):
        return []

    async def _inventory(*a, **kw):
        if isinstance(inventory, Exception):
            raise inventory
        return inventory

    monkeypatch.setattr(main_module, "snapshot_tolerated_pods", _no_pods)
    monkeypatch.setattr(main_module, "snapshot_node_gpu_inventory", _inventory)
    # The JIT guards' maps are only refreshed when the on-demand path is on.
    config = make_config(ondemand_lease_enabled=True)
    asyncio.run(main_module._run_queue_tick(state, None, config))
    return state


def test_tick_refreshes_class_node_counts(monkeypatch):
    """Guard 1b's map comes from the same inventory guard 5's does."""
    state = _tick_with_inventory(
        monkeypatch, {"xtra": {"n31": 4}, "h100": {"n1": 8, "n2": 8}}, ControllerState()
    )

    assert state.class_node_counts == {"xtra": 1, "h100": 2}
    assert state.node_free_by_class == {"xtra": 4, "h100": 8}


def test_tick_records_a_drained_class_as_zero(monkeypatch):
    """Zero nodes and zero free GPUs are different facts; both are recorded."""
    state = _tick_with_inventory(monkeypatch, {"xtra": {}}, ControllerState())

    assert state.class_node_counts == {"xtra": 0}
    assert state.node_free_by_class == {"xtra": 0}


def test_failed_inventory_leaves_prior_maps_intact(monkeypatch, caplog):
    """Fail-safe: never re-decide admission from unknown physical state."""
    state = ControllerState()
    state.class_node_counts = {"xtra": 1}
    state.node_free_by_class = {"xtra": 3}

    with caplog.at_level(logging.WARNING, logger="app.main"):
        _tick_with_inventory(monkeypatch, RuntimeError("nodes down"), state)

    assert state.class_node_counts == {"xtra": 1}
    assert state.node_free_by_class == {"xtra": 3}
    assert any("queue.snapshot_failed" in r.getMessage() for r in caplog.records)


# ---------------------------------------------------------------------------
# Guard 4 re-checked on the tick, not only by the hourly audit
# ---------------------------------------------------------------------------


def test_tick_lifts_an_overcommit_pause_once_capacity_returns(monkeypatch, caplog):
    """A repaired node resumes admission within one tick, not up to an hour."""
    state = ControllerState()
    state.gpu_class_capacity = {"h100": 16}
    state.overcommitted_gpu_classes = {"h100"}

    with caplog.at_level(logging.INFO, logger="app.main"):
        _tick_with_inventory(monkeypatch, {"h100": {"n1": 8, "n2": 8}}, state)

    assert state.overcommitted_gpu_classes == set()
    assert state.physical_gpu_capacity == {"h100": 16}
    assert any("event=capacity_audit.resumed" in r.getMessage() for r in caplog.records)


def test_tick_engages_a_pause_without_repeating_the_mismatch_warning(monkeypatch, caplog):
    """The tick moves the pause set; the per-class mismatch WARNING stays hourly."""
    state = ControllerState()
    state.gpu_class_capacity = {"h100": 16}

    with caplog.at_level(logging.INFO, logger="app.main"):
        _tick_with_inventory(monkeypatch, {"h100": {"n1": 8}}, state)

    assert state.overcommitted_gpu_classes == {"h100"}
    assert state.physical_gpu_capacity == {"h100": 8}
    messages = [r.getMessage() for r in caplog.records]
    assert any("event=capacity_audit.paused" in m for m in messages)
    assert not any("event=capacity_audit.mismatch" in m for m in messages)


def test_tick_inventory_failure_leaves_the_pause_set(monkeypatch):
    """Fail-safe: never lift a pause on unknown physical state."""
    state = ControllerState()
    state.gpu_class_capacity = {"h100": 16}
    state.overcommitted_gpu_classes = {"h100"}

    _tick_with_inventory(monkeypatch, RuntimeError("api down"), state)

    assert state.overcommitted_gpu_classes == {"h100"}
