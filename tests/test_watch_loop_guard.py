"""Per-event exception guard in ``pod_watch_loop``.

Before the guard, one exception escaping an event handler killed the consumer
task silently — and the ``events()`` generator's ``finally`` then tore down the
watch thread with it, a permanent admission outage that ``/health`` never
reflected.  These tests prove a poisoned event is logged and skipped while the
loop keeps consuming, using the same ``_FakeWatcher`` harness as
``tests/test_watch_release.py``.

The guard's cost is that a *test* whose event fails halfway still passes on
whatever ran before the failure, so ``tests/conftest.py`` fails any test that
logs ``pod.event_failed`` unless it is marked ``expects_pod_event_failed`` --
as the poisoning tests here are.  The last class tests that check.
"""

from __future__ import annotations

import asyncio
import logging
from types import SimpleNamespace

import pytest

from app.controller import ControllerState

from tests.conftest import check_swallowed_watch_events
from tests.test_watch_release import _FakeWatcher, _config, _pod


def _run(monkeypatch, state, events):
    import app.main as main_module

    monkeypatch.setattr(main_module, "PodWatcher", lambda **kw: _FakeWatcher(events))
    asyncio.run(main_module.pod_watch_loop(state, None, _config()))


@pytest.mark.expects_pod_event_failed
def test_poisoned_event_is_skipped_and_later_events_processed(monkeypatch, caplog):
    state = ControllerState()
    state.record_placement(42, "uid-del", 2)

    # Event 1 raises inside the handler body (no ``labels`` attribute →
    # AttributeError); event 2 is a normal DELETED that must still release
    # occupancy, proving the loop survived event 1.
    poisoned = SimpleNamespace(
        metadata=SimpleNamespace(uid="uid-bad", name="pod-bad", namespace="alice")
    )
    events = [("ADDED", poisoned), ("DELETED", _pod("uid-del"))]

    with caplog.at_level(logging.ERROR, logger="app.main"):
        _run(monkeypatch, state, events)

    assert "uid-del" not in state.occupancy.get(42, {})
    failures = [r for r in caplog.records if "pod.event_failed" in r.getMessage()]
    assert len(failures) == 1
    msg = failures[0].getMessage()
    assert "watch_event=ADDED" in msg
    assert "ns=alice" in msg and "pod=pod-bad" in msg


@pytest.mark.expects_pod_event_failed
def test_malformed_object_logs_without_names(monkeypatch, caplog):
    """A pod too malformed to even name (no ``metadata``) still just logs."""
    state = ControllerState()
    events = [("MODIFIED", SimpleNamespace()), ("DELETED", _pod("uid-x"))]

    with caplog.at_level(logging.ERROR, logger="app.main"):
        _run(monkeypatch, state, events)

    failures = [r for r in caplog.records if "pod.event_failed" in r.getMessage()]
    assert len(failures) == 1
    msg = failures[0].getMessage()
    # kv() omits None fields — no ns=/pod= rather than ns=None.
    assert "watch_event=MODIFIED" in msg
    assert "ns=" not in msg and "pod=" not in msg


def test_loop_exits_cleanly_when_stream_ends(monkeypatch):
    """Generator exhaustion (test harness / shutdown) exits without raising."""
    state = ControllerState()
    _run(monkeypatch, state, [])


# ---------------------------------------------------------------------------
# The suite-wide check that a test's watch loop swallowed nothing
# ---------------------------------------------------------------------------


def _record(message: str) -> logging.LogRecord:
    return logging.LogRecord("app.main", logging.ERROR, __file__, 0, "%s", (message,), None)


_SWALLOWED = _record('event=pod.event_failed watch_event=ADDED ns=alice pod=pod-1 err="boom"')


class TestSwallowedEventCheck:
    """``check_swallowed_watch_events``, which the conftest fixture runs after
    every test.  That it recognises the loop's real record is shown by the two
    marked tests above, which it would fail if it did not."""

    def test_an_unexpected_swallowed_event_fails_the_test(self):
        with pytest.raises(pytest.fail.Exception, match='err="boom"'):
            check_swallowed_watch_events([_SWALLOWED], expected=False)

    def test_an_expected_swallowed_event_passes(self):
        check_swallowed_watch_events([_SWALLOWED], expected=True)

    def test_a_test_with_nothing_swallowed_passes(self):
        check_swallowed_watch_events(
            [_record("event=pod.fast_path ns=alice pod=pod-1 rid=7")], expected=False
        )

    def test_a_marker_without_the_event_fails_the_test(self):
        # A stale marker would otherwise switch the check off for that test.
        with pytest.raises(pytest.fail.Exception, match="no longer exercises"):
            check_swallowed_watch_events([], expected=True)

    def test_only_the_event_field_counts(self):
        # The name inside another field's value is not the event.
        check_swallowed_watch_events(
            [_record('event=queue.tick_failed err="after pod.event_failed"')],
            expected=False,
        )
