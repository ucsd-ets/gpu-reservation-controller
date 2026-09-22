"""``LIBRARY_LOG_LEVEL`` — holding the client libraries' wire traces apart.

``LOG_LEVEL`` is a root-logger setting, so ``LOG_LEVEL=DEBUG`` used to turn on
``kubernetes.client.rest`` response-body dumps, per-request ``urllib3`` /
``httpcore`` lines and the rest alongside the controller's own DEBUG events.
These tests pin the separate library level and its "stricter of the two" rule.
"""

from __future__ import annotations

import logging

import pytest

from app.config import Config, _env_log_level
from app.main import _LIBRARY_LOGGERS, _configure_logging

from tests.conftest import kv_fields


@pytest.fixture
def env(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://reservations.local")
    monkeypatch.setenv("RESERVATION_API_KEY", "gpures_test")
    monkeypatch.delenv("LOG_LEVEL", raising=False)
    monkeypatch.delenv("LIBRARY_LOG_LEVEL", raising=False)
    return monkeypatch


@pytest.fixture
def restore_levels():
    """_configure_logging mutates process-global loggers; put them back."""
    names = ("", *_LIBRARY_LOGGERS)
    saved = {n: logging.getLogger(n).level for n in names}
    yield
    for n, level in saved.items():
        logging.getLogger(n).setLevel(level)


class TestEnvLogLevel:
    def test_default_when_unset(self, env):
        assert Config.from_env().library_log_level == "WARNING"

    @pytest.mark.parametrize("raw", ["debug", " Debug ", "DEBUG"])
    def test_case_and_whitespace_insensitive(self, env, raw):
        env.setenv("LIBRARY_LOG_LEVEL", raw)
        assert Config.from_env().library_log_level == "DEBUG"

    def test_unknown_level_warns_and_falls_back(self, env, caplog):
        env.setenv("LIBRARY_LOG_LEVEL", "verbose")
        with caplog.at_level(logging.WARNING, logger="app.config"):
            assert _env_log_level("LIBRARY_LOG_LEVEL", "WARNING") == "WARNING"
        record = next(r for r in caplog.records if "event=config.invalid" in r.getMessage())
        fields = kv_fields(record.getMessage())
        assert fields["name"] == "LIBRARY_LOG_LEVEL"
        assert fields["reason"] == "unknown_log_level"


class TestConfigureLogging:
    def _levels(self):
        return {n: logging.getLogger(n).getEffectiveLevel() for n in _LIBRARY_LOGGERS}

    def test_debug_root_keeps_libraries_quiet_by_default(self, env, restore_levels):
        env.setenv("LOG_LEVEL", "DEBUG")
        _configure_logging(Config.from_env())
        assert logging.getLogger("app.main").getEffectiveLevel() == logging.DEBUG
        assert set(self._levels().values()) == {logging.WARNING}

    def test_kubernetes_rest_logger_inherits_the_library_level(self, env, restore_levels):
        # The body dumps come from a child logger; it must not escape the clamp.
        env.setenv("LOG_LEVEL", "DEBUG")
        _configure_logging(Config.from_env())
        assert not logging.getLogger("kubernetes.client.rest").isEnabledFor(logging.DEBUG)

    def test_both_debug_restores_raw_traces(self, env, restore_levels):
        env.setenv("LOG_LEVEL", "DEBUG")
        env.setenv("LIBRARY_LOG_LEVEL", "DEBUG")
        _configure_logging(Config.from_env())
        assert set(self._levels().values()) == {logging.DEBUG}

    def test_library_level_cannot_exceed_root_verbosity(self, env, restore_levels):
        # LOG_LEVEL stays the ceiling: asking for library DEBUG under an INFO
        # root yields INFO, not a flood of traces with nothing else.
        env.setenv("LOG_LEVEL", "INFO")
        env.setenv("LIBRARY_LOG_LEVEL", "DEBUG")
        _configure_logging(Config.from_env())
        assert set(self._levels().values()) == {logging.INFO}

    def test_stricter_root_still_silences_library_warnings(self, env, restore_levels):
        env.setenv("LOG_LEVEL", "ERROR")
        _configure_logging(Config.from_env())
        assert set(self._levels().values()) == {logging.ERROR}
