"""Runtime configuration sourced entirely from environment variables."""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from datetime import tzinfo
from typing import Optional
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .log_fields import kv

log = logging.getLogger(__name__)

# Boolean env-var vocabulary, shared with the reservation app's
# ``config_utils`` (keep the two in step): a recognised truthy/falsy word wins,
# anything else — including junk — falls back to the flag's default.
_TRUTHY = frozenset({"1", "true", "yes", "on"})
_FALSY = frozenset({"0", "false", "no", "off"})


def _env_bool(name: str, default: bool) -> bool:
    """Read a boolean-ish environment variable with an explicit default.

    A recognised truthy/falsy word (see ``_TRUTHY`` / ``_FALSY``) wins;
    anything else — including junk — falls back to ``default``.  This keeps the
    controller's boolean vocabulary in step with the reservation app's
    ``config_utils``.
    """
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.strip().lower()
    if value in _TRUTHY:
        return True
    if value in _FALSY:
        return False
    return default


def _env_int(
    name: str,
    default: int,
    *,
    minimum: int = 1,
    maximum: Optional[int] = None,
) -> int:
    """Read an integer environment variable, falling back on junk or out-of-range.

    The numeric settings used to be parsed with a bare ``int()``, which is the
    opposite posture to ``_env_bool`` above: junk raised ``ValueError`` at
    startup, and — worse — a ``0`` or a negative was accepted silently.  A
    ``PREEMPTION_CHECK_INTERVAL`` or ``QUEUE_PROCESSOR_INTERVAL`` of ``0`` is a
    busy loop hammering the Kubernetes API, which is a far worse outcome than
    ignoring the value.

    This mirrors the reservation app's ``config_utils._env_positive_float`` —
    "the ``env_bool`` tolerance posture, applied to numbers" — and keeps the two
    repos' vocabularies in step.  *minimum* is per-setting rather than uniform:
    a zero interval is a busy loop, but a zero grace/lead genuinely means "no
    grace", so only the settings where zero is meaningless floor at 1.

    A rejected value logs at WARNING naming the variable, because an operator
    who set it and saw no effect needs to know it was ignored.
    """
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        value = int(raw)
    except ValueError:
        log.warning("%s", kv(
            event="config.invalid", name=name, value=raw,
            reason="not_an_integer", detail=f"using default {default}",
        ))
        return default
    if value < minimum or (maximum is not None and value > maximum):
        log.warning("%s", kv(
            event="config.invalid", name=name, value=value,
            reason="out_of_range", detail=f"using default {default}",
        ))
        return default
    return value


def timezone_label(tz: Optional[tzinfo]) -> str:
    """Name *tz* for a log line, where ``None`` means the process's local zone.

    A ``ZoneInfo`` carries the IANA key it was built from; the process-local
    fallback has no such name, so it is reported as whatever ``TZ`` says (the
    variable that actually decides it) or ``system`` when even that is unset.
    """
    key = getattr(tz, "key", None)
    if key:
        return str(key)
    return os.environ.get("TZ") or "system"


def _env_tz(name: str, default: Optional[tzinfo] = None) -> Optional[tzinfo]:
    """Read an IANA timezone name, falling back to *default* on an unknown zone.

    The same tolerant posture as ``_env_bool`` / ``_env_int``: a typo'd or
    unshipped zone must not kill startup over a *display* setting, so it logs
    ``config.invalid`` and falls back.

    ``None`` — the default default — means "the process's local zone", which is
    what ``TZ`` sets.  It is deliberately not resolved to a fixed ``tzinfo``
    here: a zone captured at startup would freeze the UTC offset in force at
    that moment, and this daemon runs across DST transitions.  Deferring to
    ``datetime.astimezone(None)`` at render time re-derives the offset per
    instant instead.
    """
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    try:
        return ZoneInfo(raw)
    except (ZoneInfoNotFoundError, ValueError, OSError):
        log.warning("%s", kv(
            event="config.invalid", name=name, value=raw,
            reason="unknown_timezone",
            detail=f"using {timezone_label(default)}",
        ))
        return default


_LOG_LEVELS = ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL")


def _env_log_level(name: str, default: str) -> str:
    """Read a logging level name, falling back to *default* on an unknown one.

    The same tolerant posture as ``_env_bool`` / ``_env_int``: a typo'd level
    must not kill startup over a *logging* setting, so it logs
    ``config.invalid`` and falls back.  Case-insensitive; returned upper-case.
    """
    raw = (os.environ.get(name) or "").strip()
    if not raw:
        return default
    value = raw.upper()
    if value not in _LOG_LEVELS:
        log.warning("%s", kv(
            event="config.invalid", name=name, value=raw,
            reason="unknown_log_level", detail=f"using default {default}",
        ))
        return default
    return value


@dataclass(frozen=True)
class Config:
    reservation_api_url: str
    reservation_api_key: str
    reservation_fetch_interval: int   # seconds between refresh cycles
    reservation_lookahead_days: int   # how many calendar days ahead to fetch
    kubeconfig_path: Optional[str]    # None → use in-cluster service account
    http_port: int                    # bind port for the whole FastAPI listener
    ondemand_lease_enabled: bool      # enable/disable the JIT on-demand lease path
    noshow_timeout_minutes: int    # minutes after slot_start before no-show is declared
    noshow_grace_minutes: int      # grace period when controller starts mid-window
    queue_processor_interval: int  # seconds between queue-processor ticks
    scheduling_gate_name: Optional[str]  # SchedulingGate to remove on admission; None = disabled
    inbound_api_token: Optional[str]  # bearer token for the inbound push API; None = endpoint disabled
    preemption_lead_minutes: int   # lead time before a slot boundary for phase-A preemption
    preemption_check_interval: int  # seconds between preemption sweeps
    pod_adoption_enabled: bool = True  # re-link overstay pods to a user's new booking
    ondemand_merge_enabled: bool = True  # merge a JIT lease's pod into a now-open matching booking
    termination_warning_enabled: bool = True  # annotate pods at risk of demand-driven preemption
    termination_warning_lead_minutes: int = 30  # warning look-ahead, decoupled from PREEMPTION_LEAD_MINUTES
    # Surface the app's 409 denial reason on a JIT lease as a Warning Event on
    # the waiting pod, so its owner can see why it is still Pending.  The repeat
    # interval throttles an *unchanged* reason (the retry cadence is 2-5 min, far
    # faster than anyone needs to be told the same thing); a reason that changes
    # is emitted at once regardless, and 0 means emit on every denial.  The
    # interval governs the admission-paused Event below too -- the name predates
    # it -- so every status update on a pending pod runs on one cadence.
    ondemand_denial_event_enabled: bool = True
    ondemand_denial_event_repeat_minutes: int = 30
    # Tell the owner of a pod held by a class-wide on-demand pause (guard 1b's
    # drained class, guard 3's stuck-holder interlock, guard 4's capacity
    # overcommit) with a Warning Event, suggesting they contact support if it
    # persists.
    ondemand_pause_event_enabled: bool = True
    # How a pod's owner reaches support -- an email address or URL -- named in
    # that suggestion.  None = the suggestion names no one.
    support_contact: Optional[str] = None
    # Tell the owner of a pod the controller cannot act on as written with a
    # Warning Event: its gpu-class label names no class the app knows
    # (UnknownGpuClass), no reservation matches it and it does not qualify for
    # on-demand admission (NoReservation), or one of its galends/* annotations
    # was ignored (AnnotationIgnored).  On the denial Event's repeat cadence.
    pod_problem_event_enabled: bool = True
    # Tell the owner of a pod queued for one of their reservations what it is
    # waiting on: the window has not opened (WaitingForReservation, a Normal
    # Event), their other pods hold its GPUs (ReservationFull), or it holds
    # fewer GPUs than the pod requests (ReservationTooSmall).  On the denial
    # Event's repeat cadence.
    reservation_wait_event_enabled: bool = True
    preemption_delegate_selection: bool = True  # ask the app to choose victims; local random fallback
    ondemand_delegate_admission: bool = False  # ask the app which pending pods to admit; grant-all fallback
    # Honour galends/runtime-guarantee=none by admitting the pod under a
    # zero-length, zero-SU kind="best_effort" reservation.  Ships **off**, like
    # ondemand_delegate_admission above: the app must be running a build that
    # serves the best-effort create shape, and against one that is not, every
    # such candidate would take a non-retryable 4xx into lease.error backoff.
    # Unlike the node-level galends/force-node-capacity, whose annotation is its
    # own opt-in because only a cluster admin can set it, this one is a *pod*
    # annotation any user can write — so enabling the path is the operator's
    # decision, not the pod author's.
    best_effort_enabled: bool = False
    required_group_label: Optional[str] = None  # pod label naming the usage group to match; None = disabled
    # Fallbacks for the two things a pod must name itself before the controller
    # can mint a JIT lease on its behalf.  Both ship disabled, so an unconfigured
    # deployment keeps the "a pod that doesn't say is left Pending" behaviour.
    default_min_runtime_seconds: int = 0  # stand-in for a missing galends/minimum-runtime-seconds; 0 = disabled
    default_usage_group: Optional[str] = None  # stand-in for a missing group label/annotation; None = disabled
    ondemand_horizon_minutes: int = 30    # JIT trigger: reserved-match horizon before requesting a lease
    ondemand_lease_buffer_minutes: int = 10  # added to a pod's minimum-runtime when sizing a JIT lease
    capacity_check_interval: int = 3600  # seconds between app-side vs physical capacity audits
    headroom_target_percent: int = 0  # % of each class's physical GPUs to hold free; 0 = disabled
    headroom_notice_minutes: int = 15  # notice a headroom victim gets before it becomes killable
    headroom_check_interval: int = 600  # seconds between headroom evaluations (throttles the sweep)
    overstay_report_enabled: bool = False  # report overstay durations to the app for analysis (ships dark)
    singleton_lease_enabled: bool = True  # hold a coordination Lease so a duplicate instance refuses to run
    k8s_tls_strict_verify: bool = True  # OpenSSL strict X.509 checks on the Kubernetes API connection
    pod_name: Optional[str] = None  # this pod's name (downward API); lease holder identity
    pod_namespace: Optional[str] = None  # this pod's namespace (downward API); where the Lease lives
    # Timezone the human-readable Event/annotation prose renders in.  None =
    # the process's local zone (TZ).  Display only: every stored instant,
    # every galends/* timestamp annotation and every log field stays UTC.
    display_timezone: Optional[tzinfo] = None
    log_level: str = "INFO"        # root log level (LOG_LEVEL)
    # Level for the HTTP/Kubernetes client libraries (LIBRARY_LOG_LEVEL), whose
    # DEBUG output is raw wire traces.  Can only quieten them below LOG_LEVEL.
    library_log_level: str = "WARNING"

    @classmethod
    def from_env(cls) -> "Config":
        url = os.environ.get("RESERVATION_API_URL", "").rstrip("/")
        key = os.environ.get("RESERVATION_API_KEY", "")
        if not url:
            raise RuntimeError(
                "RESERVATION_API_URL environment variable is required"
            )
        if not key:
            raise RuntimeError(
                "RESERVATION_API_KEY environment variable is required"
            )

        # Floors are per-setting on purpose.  An interval of 0 is a busy loop,
        # so those floor at 1; a lead/grace/horizon of 0 is a meaningful "off",
        # so those floor at 0 and only reject negatives.
        return cls(
            reservation_api_url=url,
            reservation_api_key=key,
            reservation_fetch_interval=_env_int("RESERVATION_FETCH_INTERVAL", 300),
            reservation_lookahead_days=_env_int("RESERVATION_LOOKAHEAD_DAYS", 7),
            kubeconfig_path=os.environ.get("KUBECONFIG") or None,
            http_port=_env_int("HTTP_PORT", 8000, maximum=65535),
            ondemand_lease_enabled=_env_bool("ONDEMAND_LEASE_ENABLED", True),
            best_effort_enabled=_env_bool("BEST_EFFORT_ENABLED", False),
            noshow_timeout_minutes=_env_int("NOSHOW_TIMEOUT_MINUTES", 15, minimum=0),
            noshow_grace_minutes=_env_int("NOSHOW_GRACE_MINUTES", 30, minimum=0),
            queue_processor_interval=_env_int("QUEUE_PROCESSOR_INTERVAL", 300),
            scheduling_gate_name=os.environ.get("POD_SCHEDULING_GATE_NAME") or None,
            required_group_label=os.environ.get("REQUIRED_GROUP_LABEL") or None,
            # 0 is the meaningful "off" here — get_pod_min_runtime_seconds
            # already rejects a non-positive annotation, so a zero default is
            # indistinguishable from having no default at all.
            default_min_runtime_seconds=_env_int(
                "DEFAULT_MINIMUM_RUNTIME_SECONDS", 0, minimum=0
            ),
            default_usage_group=os.environ.get("DEFAULT_USAGE_GROUP") or None,
            inbound_api_token=os.environ.get("INBOUND_API_TOKEN") or None,
            preemption_lead_minutes=_env_int("PREEMPTION_LEAD_MINUTES", 15, minimum=0),
            preemption_check_interval=_env_int("PREEMPTION_CHECK_INTERVAL", 60),
            pod_adoption_enabled=_env_bool("POD_ADOPTION_ENABLED", True),
            ondemand_merge_enabled=_env_bool("ONDEMAND_MERGE_ENABLED", True),
            termination_warning_enabled=_env_bool("TERMINATION_WARNING_ENABLED", True),
            termination_warning_lead_minutes=_env_int(
                "TERMINATION_WARNING_LEAD_MINUTES", 30, minimum=0
            ),
            ondemand_denial_event_enabled=_env_bool(
                "ONDEMAND_DENIAL_EVENT_ENABLED", True
            ),
            ondemand_denial_event_repeat_minutes=_env_int(
                "ONDEMAND_DENIAL_EVENT_REPEAT_MINUTES", 30, minimum=0
            ),
            ondemand_pause_event_enabled=_env_bool(
                "ONDEMAND_PAUSE_EVENT_ENABLED", True
            ),
            support_contact=(os.environ.get("SUPPORT_CONTACT") or "").strip() or None,
            pod_problem_event_enabled=_env_bool("POD_PROBLEM_EVENT_ENABLED", True),
            reservation_wait_event_enabled=_env_bool(
                "RESERVATION_WAIT_EVENT_ENABLED", True
            ),
            preemption_delegate_selection=_env_bool(
                "PREEMPTION_DELEGATE_SELECTION", True
            ),
            ondemand_delegate_admission=_env_bool(
                "ONDEMAND_DELEGATE_ADMISSION", False
            ),
            ondemand_horizon_minutes=_env_int(
                "ONDEMAND_HORIZON_MINUTES", 30, minimum=0
            ),
            ondemand_lease_buffer_minutes=_env_int(
                "ONDEMAND_LEASE_BUFFER_MINUTES", 10, minimum=0
            ),
            capacity_check_interval=_env_int("CAPACITY_CHECK_INTERVAL", 3600),
            # A percentage floors at 0 ("hold nothing", the disabled default) and
            # caps at 100; a notice of 0 means "no notice gate, kill on sight";
            # but an evaluation interval of 0 is a busy loop, so that floors at 1.
            headroom_target_percent=_env_int(
                "HEADROOM_TARGET_PERCENT", 0, minimum=0, maximum=100
            ),
            headroom_notice_minutes=_env_int(
                "HEADROOM_NOTICE_MINUTES", 15, minimum=0
            ),
            headroom_check_interval=_env_int("HEADROOM_CHECK_INTERVAL", 600),
            overstay_report_enabled=_env_bool("OVERSTAY_REPORT_ENABLED", False),
            singleton_lease_enabled=_env_bool("SINGLETON_LEASE_ENABLED", True),
            k8s_tls_strict_verify=_env_bool("K8S_TLS_STRICT_VERIFY", True),
            # POD_NAME comes from the downward API in-cluster; HOSTNAME is the
            # pod name inside a container anyway, so it is a natural fallback.
            pod_name=os.environ.get("POD_NAME") or os.environ.get("HOSTNAME") or None,
            pod_namespace=os.environ.get("POD_NAMESPACE") or None,
            # Unset falls through to the process local zone, so setting TZ
            # alone (which the chart already wires) is enough to localise the
            # prose; the explicit variable is for keeping logs on UTC while
            # events read local.
            display_timezone=_env_tz("EVENT_DISPLAY_TIMEZONE"),
            log_level=_env_log_level("LOG_LEVEL", "INFO"),
            library_log_level=_env_log_level("LIBRARY_LOG_LEVEL", "WARNING"),
        )
