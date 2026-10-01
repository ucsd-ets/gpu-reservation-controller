"""Environment parsing in ``app/config.py``.

The boolean vocabulary was always deliberately tolerant — a recognised word
wins, anything else falls back to the default — but the twelve numeric settings
were parsed with a bare ``int()``, which is the opposite posture in both
directions: junk raised ``ValueError`` and killed startup, while ``0`` and
negatives were accepted silently.  A ``PREEMPTION_CHECK_INTERVAL`` of ``0`` is a
busy loop against the Kubernetes API.

These tests pin the tolerance and the per-setting floors.  Floors are not
uniform on purpose: a zero *interval* is meaningless, a zero *lead*/*grace* is a
legitimate "off".
"""

from __future__ import annotations

import logging

import pytest

from app.config import Config, _env_bool, _env_int

from tests.conftest import kv_fields


REQUIRED = {
    "RESERVATION_API_URL": "http://reservations.local",
    "RESERVATION_API_KEY": "gpures_test",
}


@pytest.fixture
def env(monkeypatch):
    """Only the two required variables set; everything else at its default."""
    for name, value in REQUIRED.items():
        monkeypatch.setenv(name, value)
    return monkeypatch


# ---------------------------------------------------------------------------
# _env_int — the helper
# ---------------------------------------------------------------------------


class TestEnvInt:
    def test_absent_uses_default(self, monkeypatch):
        monkeypatch.delenv("SOME_INT", raising=False)
        assert _env_int("SOME_INT", 42) == 42

    @pytest.mark.parametrize("raw", ["", "   "])
    def test_empty_uses_default(self, monkeypatch, raw):
        monkeypatch.setenv("SOME_INT", raw)
        assert _env_int("SOME_INT", 42) == 42

    def test_whitespace_is_stripped(self, monkeypatch):
        monkeypatch.setenv("SOME_INT", "  7  ")
        assert _env_int("SOME_INT", 42) == 7

    @pytest.mark.parametrize("raw", ["abc", "1.5", "12x", "--3"])
    def test_junk_falls_back_instead_of_raising(self, monkeypatch, raw):
        # The old bare int() raised here, killing the process at startup.
        monkeypatch.setenv("SOME_INT", raw)
        assert _env_int("SOME_INT", 42) == 42

    @pytest.mark.parametrize("raw", ["0", "-1", "-9999"])
    def test_below_minimum_falls_back(self, monkeypatch, raw):
        monkeypatch.setenv("SOME_INT", raw)
        assert _env_int("SOME_INT", 42) == 42

    def test_minimum_zero_admits_zero_but_not_negatives(self, monkeypatch):
        monkeypatch.setenv("SOME_INT", "0")
        assert _env_int("SOME_INT", 42, minimum=0) == 0
        monkeypatch.setenv("SOME_INT", "-1")
        assert _env_int("SOME_INT", 42, minimum=0) == 42

    def test_above_maximum_falls_back(self, monkeypatch):
        monkeypatch.setenv("SOME_INT", "70000")
        assert _env_int("SOME_INT", 8000, maximum=65535) == 8000

    def test_boundary_values_are_admitted(self, monkeypatch):
        monkeypatch.setenv("SOME_INT", "65535")
        assert _env_int("SOME_INT", 8000, maximum=65535) == 65535
        monkeypatch.setenv("SOME_INT", "1")
        assert _env_int("SOME_INT", 8000, maximum=65535) == 1

    @pytest.mark.parametrize(
        "raw,reason", [("abc", "not_an_integer"), ("0", "out_of_range")]
    )
    def test_rejection_warns_and_names_the_variable(self, monkeypatch, caplog, raw, reason):
        # An operator who set a value and saw no effect has to be able to find
        # out it was ignored.
        monkeypatch.setenv("SOME_INT", raw)
        with caplog.at_level(logging.WARNING, logger="app.config"):
            assert _env_int("SOME_INT", 42) == 42

        record = next(r for r in caplog.records if "event=config.invalid" in r.getMessage())
        fields = kv_fields(record.getMessage())
        assert fields["name"] == "SOME_INT"
        assert fields["value"] == raw
        assert fields["reason"] == reason

    def test_accepted_value_is_silent(self, monkeypatch, caplog):
        monkeypatch.setenv("SOME_INT", "7")
        with caplog.at_level(logging.WARNING, logger="app.config"):
            assert _env_int("SOME_INT", 42) == 7
        assert not [r for r in caplog.records if "config.invalid" in r.getMessage()]


# ---------------------------------------------------------------------------
# _env_bool — unchanged, pinned alongside so the two postures stay symmetric
# ---------------------------------------------------------------------------


class TestEnvBool:
    @pytest.mark.parametrize("raw", ["1", "true", "TRUE", "  yes  ", "on"])
    def test_truthy_words(self, monkeypatch, raw):
        monkeypatch.setenv("SOME_BOOL", raw)
        assert _env_bool("SOME_BOOL", False) is True

    @pytest.mark.parametrize("raw", ["0", "false", "NO", "off"])
    def test_falsy_words(self, monkeypatch, raw):
        monkeypatch.setenv("SOME_BOOL", raw)
        assert _env_bool("SOME_BOOL", True) is False

    @pytest.mark.parametrize("raw", ["maybe", "", "2"])
    def test_junk_keeps_the_default(self, monkeypatch, raw):
        monkeypatch.setenv("SOME_BOOL", raw)
        assert _env_bool("SOME_BOOL", True) is True
        assert _env_bool("SOME_BOOL", False) is False


# ---------------------------------------------------------------------------
# Config.from_env — the numeric settings
# ---------------------------------------------------------------------------


# (env var, Config attribute, default, whether zero is a legitimate value)
NUMERIC_SETTINGS = [
    ("RESERVATION_FETCH_INTERVAL", "reservation_fetch_interval", 300, False),
    ("RESERVATION_LOOKAHEAD_DAYS", "reservation_lookahead_days", 7, False),
    ("HTTP_PORT", "http_port", 8000, False),
    ("NOSHOW_TIMEOUT_MINUTES", "noshow_timeout_minutes", 15, True),
    ("NOSHOW_GRACE_MINUTES", "noshow_grace_minutes", 30, True),
    ("QUEUE_PROCESSOR_INTERVAL", "queue_processor_interval", 300, False),
    ("PREEMPTION_LEAD_MINUTES", "preemption_lead_minutes", 15, True),
    ("PREEMPTION_CHECK_INTERVAL", "preemption_check_interval", 60, False),
    ("TERMINATION_WARNING_LEAD_MINUTES", "termination_warning_lead_minutes", 30, True),
    ("ONDEMAND_HORIZON_MINUTES", "ondemand_horizon_minutes", 30, True),
    ("ONDEMAND_LEASE_BUFFER_MINUTES", "ondemand_lease_buffer_minutes", 10, True),
    ("CAPACITY_CHECK_INTERVAL", "capacity_check_interval", 3600, False),
    # 0 disables anticipatory headroom entirely, so it is a legitimate value;
    # a 0-minute notice legitimately means "no notice gate"; but a 0-second
    # evaluation interval is a busy loop, so that one floors at 1.
    ("HEADROOM_TARGET_PERCENT", "headroom_target_percent", 0, True),
    ("HEADROOM_NOTICE_MINUTES", "headroom_notice_minutes", 15, True),
    ("HEADROOM_CHECK_INTERVAL", "headroom_check_interval", 600, False),
    # 0 is this one's "off": no stand-in for a missing minimum-runtime annotation.
    ("DEFAULT_MINIMUM_RUNTIME_SECONDS", "default_min_runtime_seconds", 0, True),
    # Not a busy loop at 0 (it rides the fetch cycle), but 0 would mean "never
    # cache the rosters", so it floors at 1 like every other interval.
    (
        "DEFAULT_USAGE_GROUP_REFRESH_INTERVAL",
        "default_usage_group_refresh_interval",
        14400,
        False,
    ),
    # 0 legitimately means "restate the denial reason on every retry" -- noisy,
    # but a coherent choice; ONDEMAND_DENIAL_EVENT_ENABLED is the off switch.
    (
        "ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES",
        "ondemand_denial_event_repeat_minutes",
        30,
        True,
    ),
]


class TestConfigNumericSettings:
    def test_every_numeric_setting_is_covered(self):
        # Guards against the next setting being added with a bare int().
        import inspect

        source = inspect.getsource(Config.from_env)
        assert "int(os.environ" not in source, "a numeric setting bypasses _env_int"
        assert source.count("_env_int(") == len(NUMERIC_SETTINGS)

    @pytest.mark.parametrize("var,attr,default,_zero_ok", NUMERIC_SETTINGS)
    def test_default_applies_when_unset(self, env, var, attr, default, _zero_ok):
        env.delenv(var, raising=False)
        assert getattr(Config.from_env(), attr) == default

    @pytest.mark.parametrize("var,attr,default,_zero_ok", NUMERIC_SETTINGS)
    def test_junk_does_not_kill_startup(self, env, var, attr, default, _zero_ok):
        env.setenv(var, "not-a-number")
        assert getattr(Config.from_env(), attr) == default

    @pytest.mark.parametrize("var,attr,default,_zero_ok", NUMERIC_SETTINGS)
    def test_negative_falls_back(self, env, var, attr, default, _zero_ok):
        env.setenv(var, "-5")
        assert getattr(Config.from_env(), attr) == default

    @pytest.mark.parametrize("var,attr,default,zero_ok", NUMERIC_SETTINGS)
    def test_zero_is_honoured_only_where_it_means_something(
        self, env, var, attr, default, zero_ok
    ):
        # An interval of 0 is a busy loop against the Kubernetes API, so it is
        # rejected; a lead/grace/horizon of 0 legitimately disables a behaviour.
        env.setenv(var, "0")
        assert getattr(Config.from_env(), attr) == (0 if zero_ok else default)

    @pytest.mark.parametrize("var,attr,default,_zero_ok", NUMERIC_SETTINGS)
    def test_a_valid_value_is_honoured(self, env, var, attr, default, _zero_ok):
        env.setenv(var, "11")
        assert getattr(Config.from_env(), attr) == 11

    def test_http_port_rejects_an_unbindable_port(self, env):
        env.setenv("HTTP_PORT", "70000")
        assert Config.from_env().http_port == 8000


class TestDefaultUsageGroupList:
    """``DEFAULT_USAGE_GROUP`` is an ordered fallback list, comma-separated."""

    def test_unset_means_no_fallback(self, env):
        env.delenv("DEFAULT_USAGE_GROUP", raising=False)
        assert Config.from_env().default_usage_groups == ()

    def test_a_single_name_is_a_list_of_one(self, env):
        # The form every existing deployment uses keeps working unchanged.
        env.setenv("DEFAULT_USAGE_GROUP", "cse151b")
        assert Config.from_env().default_usage_groups == ("cse151b",)

    def test_order_is_kept(self, env):
        env.setenv("DEFAULT_USAGE_GROUP", "research-a,research-b,dsmlp-public")
        assert Config.from_env().default_usage_groups == (
            "research-a", "research-b", "dsmlp-public",
        )

    def test_whitespace_and_empty_entries_are_dropped(self, env):
        env.setenv("DEFAULT_USAGE_GROUP", " research-a , ,research-b,, ")
        assert Config.from_env().default_usage_groups == ("research-a", "research-b")

    def test_a_repeat_keeps_its_first_position(self, env):
        # A later mention could only ever be shadowed by the first.
        env.setenv("DEFAULT_USAGE_GROUP", "b,a,b")
        assert Config.from_env().default_usage_groups == ("b", "a")

    @pytest.mark.parametrize("raw", ["", "  ", ",", " , ,"])
    def test_nothing_but_separators_is_unset(self, env, raw):
        env.setenv("DEFAULT_USAGE_GROUP", raw)
        assert Config.from_env().default_usage_groups == ()


class TestConfigRequired:
    def test_missing_url_raises(self, monkeypatch):
        monkeypatch.delenv("RESERVATION_API_URL", raising=False)
        monkeypatch.setenv("RESERVATION_API_KEY", "gpures_test")
        with pytest.raises(RuntimeError, match="RESERVATION_API_URL"):
            Config.from_env()

    def test_missing_key_raises(self, monkeypatch):
        monkeypatch.setenv("RESERVATION_API_URL", "http://reservations.local")
        monkeypatch.delenv("RESERVATION_API_KEY", raising=False)
        with pytest.raises(RuntimeError, match="RESERVATION_API_KEY"):
            Config.from_env()
