"""Coverage for Config.from_env and the unified reserved admission path (T6).

``Config.from_env`` had no tests (required-var errors, falsy parsing, the
renamed int/bool settings); neither did ``_try_apply_toleration`` itself — the
budget check, optimistic record/rollback, the already-tolerated dequeue, and the
terminal-phase drop added when the two admission paths were unified.

The async tests import ``app.main`` after setting the required env vars, because
importing it runs ``create_app()`` (and ``Config.from_env``) at module load.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from app.controller import ControllerState, QueueEntry
from tests.conftest import GPU_CLASS_ID, GPU_CLASS_LABEL, USERNAME, reservation

_CONFIG_ENV = [
    "RESERVATION_API_URL",
    "RESERVATION_API_KEY",
    "RESERVATION_FETCH_INTERVAL",
    "RESERVATION_LOOKAHEAD_DAYS",
    "KUBECONFIG",
    "HTTP_PORT",
    "ONDEMAND_LEASE_ENABLED",
    "NOSHOW_TIMEOUT_MINUTES",
    "NOSHOW_GRACE_MINUTES",
    "QUEUE_PROCESSOR_INTERVAL",
    "POD_SCHEDULING_GATE_NAME",
    "PREEMPTION_LEAD_MINUTES",
    "PREEMPTION_CHECK_INTERVAL",
    "SINGLETON_LEASE_ENABLED",
    "POD_NAME",
    "POD_NAMESPACE",
    "HOSTNAME",
    "LOG_LEVEL",
    "LIBRARY_LOG_LEVEL",
]


def _clean_env(monkeypatch):
    for key in _CONFIG_ENV:
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# Config.from_env
# ---------------------------------------------------------------------------


class TestConfigFromEnv:
    def test_missing_url_raises(self, monkeypatch):
        from app.config import Config

        _clean_env(monkeypatch)
        monkeypatch.setenv("RESERVATION_API_KEY", "k")
        with pytest.raises(RuntimeError, match="RESERVATION_API_URL"):
            Config.from_env()

    def test_missing_key_raises(self, monkeypatch):
        from app.config import Config

        _clean_env(monkeypatch)
        monkeypatch.setenv("RESERVATION_API_URL", "http://x")
        with pytest.raises(RuntimeError, match="RESERVATION_API_KEY"):
            Config.from_env()

    def test_defaults_and_url_stripped(self, monkeypatch):
        from app.config import Config

        _clean_env(monkeypatch)
        monkeypatch.setenv("RESERVATION_API_URL", "http://x/")
        monkeypatch.setenv("RESERVATION_API_KEY", "gpures_k")
        c = Config.from_env()
        assert c.reservation_api_url == "http://x"   # trailing slash stripped
        assert c.log_level == "INFO"
        assert c.library_log_level == "WARNING"
        assert c.noshow_timeout_minutes == 15
        assert c.noshow_grace_minutes == 30
        assert c.queue_processor_interval == 300
        assert c.http_port == 8000
        assert c.ondemand_lease_enabled is True
        assert c.scheduling_gate_name is None
        assert c.preemption_lead_minutes == 15
        assert c.preemption_check_interval == 60
        # Delegation is opt-in: default off until the app ships the endpoint.
        assert c.ondemand_delegate_admission is False

    def _base_env(self, monkeypatch):
        _clean_env(monkeypatch)
        monkeypatch.setenv("RESERVATION_API_URL", "http://x")
        monkeypatch.setenv("RESERVATION_API_KEY", "k")

    @pytest.mark.parametrize("name,attr,default,override", [
        ("HTTP_PORT", "http_port", 8000, 9001),
        ("QUEUE_PROCESSOR_INTERVAL", "queue_processor_interval", 300, 45),
        ("NOSHOW_TIMEOUT_MINUTES", "noshow_timeout_minutes", 15, 7),
        ("NOSHOW_GRACE_MINUTES", "noshow_grace_minutes", 30, 42),
    ])
    def test_int_setting_override(self, monkeypatch, name, attr, default,
                                  override):
        # Each renamed int setting reads its canonical env var, defaulting when
        # unset and honoring an explicit override.
        from app.config import Config

        self._base_env(monkeypatch)
        assert getattr(Config.from_env(), attr) == default

        self._base_env(monkeypatch)
        monkeypatch.setenv(name, str(override))
        assert getattr(Config.from_env(), attr) == override

    @pytest.mark.parametrize("value,expected", [
        ("false", False), ("0", False), ("no", False), ("FALSE", False),
        ("true", True), ("1", True), ("anything-else", True),
    ])
    def test_ondemand_flag_falsy_parsing(self, monkeypatch, value, expected):
        from app.config import Config

        _clean_env(monkeypatch)
        monkeypatch.setenv("RESERVATION_API_URL", "http://x")
        monkeypatch.setenv("RESERVATION_API_KEY", "k")
        monkeypatch.setenv("ONDEMAND_LEASE_ENABLED", value)
        assert Config.from_env().ondemand_lease_enabled is expected

    @pytest.mark.parametrize("value,expected", [
        ("true", True), ("1", True), ("yes", True), ("TRUE", True),
        ("false", False), ("0", False), ("no", False), ("anything-else", False),
    ])
    def test_ondemand_delegate_admission_truthy_parsing(self, monkeypatch, value, expected):
        # Opt-in flag: only explicit truthy values enable it (mirrors the
        # inverse of the falsy-default flags).
        from app.config import Config

        _clean_env(monkeypatch)
        monkeypatch.setenv("RESERVATION_API_URL", "http://x")
        monkeypatch.setenv("RESERVATION_API_KEY", "k")
        monkeypatch.setenv("ONDEMAND_DELEGATE_ADMISSION", value)
        assert Config.from_env().ondemand_delegate_admission is expected

    def test_singleton_lease_defaults_on(self, monkeypatch):
        from app.config import Config

        self._base_env(monkeypatch)
        c = Config.from_env()
        assert c.singleton_lease_enabled is True
        assert c.pod_name is None and c.pod_namespace is None

    @pytest.mark.parametrize("value,expected", [
        ("false", False), ("0", False), ("off", False),
        ("true", True), ("anything-else", True),
    ])
    def test_singleton_lease_flag_parsing(self, monkeypatch, value, expected):
        from app.config import Config

        self._base_env(monkeypatch)
        monkeypatch.setenv("SINGLETON_LEASE_ENABLED", value)
        assert Config.from_env().singleton_lease_enabled is expected

    def test_pod_identity_prefers_downward_api_over_hostname(self, monkeypatch):
        from app.config import Config

        self._base_env(monkeypatch)
        monkeypatch.setenv("HOSTNAME", "container-host")
        assert Config.from_env().pod_name == "container-host"  # fallback

        monkeypatch.setenv("POD_NAME", "controller-7d9f")
        monkeypatch.setenv("POD_NAMESPACE", "gpu-system")
        c = Config.from_env()
        assert c.pod_name == "controller-7d9f"
        assert c.pod_namespace == "gpu-system"


# ---------------------------------------------------------------------------
# _try_apply_toleration (reserved path)
# ---------------------------------------------------------------------------


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-admission")
    import app.main as main_module

    return main_module


def _pod(*, uid="uid-1", phase="Pending", tolerations=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name="pod-1", namespace=USERNAME,
            annotations=None, labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(phase=phase, conditions=None),
        spec=SimpleNamespace(
            tolerations=tolerations if tolerations is not None else [],
            containers=[],
            scheduling_gates=None,
        ),
    )


def _open_reservation(gpu_count=2):
    now = datetime.now(timezone.utc)
    return reservation(
        1,
        start_utc=now - timedelta(minutes=30),
        end_utc=now + timedelta(minutes=30),
        gpu_count=gpu_count,
        gpu_class_label=GPU_CLASS_LABEL,
    )


def _entry(res, *, uid="uid-1", gpu=1):
    return QueueEntry(
        pod_uid=uid, pod_name="pod-1", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=gpu,
        reservation=res, next_attempt_at=datetime.now(timezone.utc),
    )


def _state_with(res):
    state = ControllerState()
    state.reservations = [res]
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    return state


class TestTryApplyToleration:
    def test_budget_full_backs_off_without_reading_pod(self, monkeypatch):
        m = _main_module(monkeypatch)
        res = _open_reservation(gpu_count=2)
        state = _state_with(res)
        state.record_placement(res.id, "other-pod", 2)   # fully occupied
        entry = _entry(res, gpu=1)
        before = entry.next_attempt_at

        read_called = []

        async def fake_read_pod(name, namespace):
            read_called.append((name, namespace))
            return _pod()

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        result = asyncio.run(m._try_apply_toleration(state, "uid-1", entry))

        assert result is False
        assert read_called == []                 # never got past the budget check
        assert entry.next_attempt_at > before    # backoff scheduled
        assert "uid-1" not in state.occupancy.get(res.id, {})

    def test_terminal_pod_is_dropped_without_patching(self, monkeypatch):
        m = _main_module(monkeypatch)
        res = _open_reservation()
        state = _state_with(res)
        entry = _entry(res)

        applied = []

        async def fake_read_pod(name, namespace):
            return _pod(phase="Succeeded")

        async def fake_apply(*args, **kwargs):
            applied.append(args)

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "apply_toleration", fake_apply)
        result = asyncio.run(m._try_apply_toleration(state, "uid-1", entry))

        assert result is True                    # dequeued
        assert applied == []                     # completed pod never patched
        assert state.available(res) == 2         # optimistic record rolled back

    def test_successful_apply_records_and_stamps_reserved_booking(self, monkeypatch):
        m = _main_module(monkeypatch)
        res = _open_reservation()
        state = _state_with(res)
        entry = _entry(res, gpu=1)
        calls = {}

        async def fake_read_pod(name, namespace):
            return _pod(phase="Pending", tolerations=[])

        async def fake_apply(pod_name, namespace, pod, key, value, booking):
            calls["booking"] = booking
            calls["value"] = value

        async def fake_annotate(
            pod_name, namespace, seconds, guaranteed_until, facts, admitted_at=None
        ):
            calls["seconds"] = seconds
            calls["guaranteed_until"] = guaranteed_until
            calls["facts"] = facts
            calls["admitted_at"] = admitted_at

        async def fake_emit(*args, **kwargs):
            pass

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "apply_toleration", fake_apply)
        monkeypatch.setattr(m, "annotate_runtime_guarantee", fake_annotate)
        monkeypatch.setattr(m, "emit_runtime_guaranteed_event", fake_emit)
        result = asyncio.run(m._try_apply_toleration(state, "uid-1", entry))

        assert result is True
        assert calls["booking"] == "res-1"       # make_booking_reference round-trip
        assert calls["value"] == GPU_CLASS_LABEL
        assert calls["seconds"] > 0              # never 0 (B3 floor, now in _record_guarantee)
        assert calls["guaranteed_until"] == res.end_utc  # no chain, window end
        assert state.available(res) == 1         # 1 GPU now recorded as used
        # The reservation's own facts ride the same patch, describing the
        # reservation (its window, its reserved count) rather than the pod.
        assert calls["facts"].kind == res.kind
        assert calls["facts"].start_utc == res.start_utc
        assert calls["facts"].end_utc == res.end_utc
        assert calls["facts"].gpu_count == res.gpu_count
        assert calls["facts"].gpu_class_name == res.gpu_class.name
        # A genuine admission stamps admitted-at (unlike a re-link).
        assert calls["admitted_at"] is not None

    def test_already_tolerated_pod_is_dequeued_without_reapplying(self, monkeypatch):
        m = _main_module(monkeypatch)
        res = _open_reservation()
        state = _state_with(res)
        entry = _entry(res)
        existing_tol = SimpleNamespace(
            key="gpu-class-reservation", value=GPU_CLASS_LABEL, effect="NoSchedule",
        )
        applied = []

        async def fake_read_pod(name, namespace):
            return _pod(phase="Pending", tolerations=[existing_tol])

        async def fake_apply(*args, **kwargs):
            applied.append(args)

        monkeypatch.setattr(m, "read_pod", fake_read_pod)
        monkeypatch.setattr(m, "apply_toleration", fake_apply)
        result = asyncio.run(m._try_apply_toleration(state, "uid-1", entry))

        assert result is True
        assert applied == []                     # already had the toleration
