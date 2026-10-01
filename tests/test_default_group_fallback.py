"""``DEFAULT_USAGE_GROUP`` as an ordered, per-user fallback.

A pod that names no usage group of its own (no ``REQUIRED_GROUP_LABEL`` label,
or no ``galends/usage-group`` annotation with that feature off) used to fall
back to one fixed group.  The setting is now a comma-separated list, walked in
order for the pod's owner: the first group they are a member of -- in any role
-- wins, and a group with ``on_demand_auto_join`` admits everyone, since the app
would enrol them on their first lease.  Nothing else is judged: no validity
window, budget or class access, which stay the app's to decide on the
reservation itself.

The controller learns membership from ``GET /api/groups``, refreshed every
``DEFAULT_USAGE_GROUP_REFRESH_INTERVAL``; each resolution in between is an
in-memory walk of the list (``ControllerState.default_usage_group_for``).

Nothing here touches a real cluster or the app.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import httpx
import pytest

from app.controller import (
    ControllerState,
    DefaultGroupRoster,
    OnDemandCandidate,
    build_default_group_rosters,
)
from app.k8s_client import NO_RESERVATION_REASON
from app.reservation_client import ReservationClient
from app.schemas import GroupDetail

from tests.conftest import (
    GPU_CLASS_ID,
    GPU_CLASS_LABEL,
    USERNAME,
    kv_fields,
    make_config,
    reservation,
)

GROUP_LABEL_NAME = "dsmlp/course"
MIN_RUNTIME = "galends/minimum-runtime-seconds"
USAGE_GROUP = "galends/usage-group"
FALLBACK = ("research-a", "research-b", "dsmlp-public")
NOW = datetime(2026, 10, 1, 12, 0, tzinfo=timezone.utc)


def _main_module(monkeypatch):
    monkeypatch.setenv("RESERVATION_API_URL", "http://localhost:9999")
    monkeypatch.setenv("RESERVATION_API_KEY", "test-key-default-groups")
    import app.main as main_module

    return main_module


def _roster(name, *, members=(), auto_join=False, active=True, gid=1):
    return DefaultGroupRoster(
        name=name, group_id=gid, is_active=active, auto_join=auto_join,
        members=frozenset(members),
    )


def _group(name, *, members=(), auto_join=False, active=True, gid=1, role="member"):
    """One GET /api/groups entry, as the app serialises it (extra fields and all)."""
    return {
        "id": gid,
        "name": name,
        "description": None,
        "is_active": active,
        "on_demand_auto_join": auto_join,
        "on_demand_only": False,
        "su_budget": 200,
        "members": [
            {"id": 100 + i, "username": u, "role": role, "team_name": None}
            for i, u in enumerate(members)
        ],
        "gpu_classes": [{"id": GPU_CLASS_ID, "name": "H100", "label_value": "h100"}],
    }


def _state(*rosters, order=FALLBACK) -> ControllerState:
    state = ControllerState()
    state.default_usage_groups = order
    state.default_group_rosters = {r.name: r for r in rosters}
    return state


# ---------------------------------------------------------------------------
# ControllerState.default_usage_group_for -- the per-user walk
# ---------------------------------------------------------------------------


class TestResolution:
    def test_nothing_configured_resolves_nothing(self):
        state = ControllerState()
        assert state.default_usage_group_for(USERNAME) is None
        # ...and None is then simply the answer, not a gap.
        assert state.default_groups_known

    def test_before_the_rosters_load_nothing_resolves_and_that_is_known(self):
        state = ControllerState()
        state.default_usage_groups = FALLBACK
        assert state.default_usage_group_for(USERNAME) is None
        assert not state.default_groups_known

    def test_the_first_group_the_user_belongs_to_wins(self):
        state = _state(
            _roster("research-a", members=("bob",)),
            _roster("research-b", members=(USERNAME,)),
            _roster("dsmlp-public", auto_join=True),
        )
        assert state.default_usage_group_for(USERNAME) == "research-b"
        assert state.default_usage_group_for("bob") == "research-a"

    def test_order_is_the_configured_order(self):
        rosters = (
            _roster("research-a", members=(USERNAME,)),
            _roster("research-b", members=(USERNAME,)),
        )
        assert _state(*rosters).default_usage_group_for(USERNAME) == "research-a"
        assert (
            _state(*rosters, order=("research-b", "research-a"))
            .default_usage_group_for(USERNAME)
            == "research-b"
        )

    def test_an_auto_join_group_admits_anyone(self):
        state = _state(
            _roster("research-a", members=("bob",)),
            _roster("dsmlp-public", auto_join=True),
        )
        assert state.default_usage_group_for("someone-new") == "dsmlp-public"

    def test_an_auto_join_group_shadows_everything_after_it(self):
        state = _state(
            _roster("dsmlp-public", auto_join=True),
            _roster("research-a", members=(USERNAME,)),
            order=("dsmlp-public", "research-a"),
        )
        assert state.default_usage_group_for(USERNAME) == "dsmlp-public"

    def test_no_group_admits_the_user(self):
        state = _state(
            _roster("research-a", members=("bob",)),
            _roster("research-b", members=("carol",)),
        )
        assert state.default_usage_group_for(USERNAME) is None
        assert state.default_groups_known

    def test_an_inactive_group_admits_no_one(self):
        # The app answers every lease naming an inactive group with a 404, and
        # an auto-join flag does not change that.
        state = _state(
            _roster("research-a", members=(USERNAME,), active=False),
            _roster("dsmlp-public", auto_join=True, active=False),
            _roster("research-b", members=(USERNAME,)),
            order=("research-a", "dsmlp-public", "research-b"),
        )
        assert state.default_usage_group_for(USERNAME) == "research-b"

    def test_a_name_the_app_has_no_group_for_is_skipped(self):
        state = _state(_roster("research-b", members=(USERNAME,)))
        assert "research-a" not in state.default_group_rosters
        assert state.default_usage_group_for(USERNAME) == "research-b"


class TestBuildRosters:
    def test_only_listed_groups_are_kept(self):
        groups = [
            GroupDetail.model_validate(_group("research-a", members=(USERNAME,))),
            GroupDetail.model_validate(_group("cse151b", members=(USERNAME,))),
        ]
        rosters = build_default_group_rosters(FALLBACK, groups)
        assert set(rosters) == {"research-a"}
        assert rosters["research-a"].members == frozenset({USERNAME})

    def test_a_manager_is_a_member(self):
        # Membership is any role -- the app's own lease path checks for a
        # membership row, not its role.
        groups = [GroupDetail.model_validate(
            _group("research-a", members=(USERNAME,), role="manager")
        )]
        rosters = build_default_group_rosters(FALLBACK, groups)
        assert rosters["research-a"].admits(USERNAME)

    def test_flags_are_carried_and_inactive_groups_kept(self):
        groups = [
            GroupDetail.model_validate(_group("research-a", active=False, gid=7)),
            GroupDetail.model_validate(_group("dsmlp-public", auto_join=True, gid=9)),
        ]
        rosters = build_default_group_rosters(FALLBACK, groups)
        assert rosters["research-a"] == DefaultGroupRoster(
            name="research-a", group_id=7, is_active=False, auto_join=False,
            members=frozenset(),
        )
        assert rosters["dsmlp-public"].auto_join


# ---------------------------------------------------------------------------
# ReservationClient.fetch_groups
# ---------------------------------------------------------------------------


def _client(handler) -> ReservationClient:
    config = make_config()
    client = ReservationClient(config)
    client._client = httpx.AsyncClient(
        base_url=config.reservation_api_url, transport=httpx.MockTransport(handler),
    )
    return client


class TestFetchGroups:
    def test_returns_only_the_named_groups(self):
        seen: list[str] = []

        def handler(request: httpx.Request) -> httpx.Response:
            seen.append(request.url.path)
            return httpx.Response(200, json=[
                _group("cse151b", members=("bob",)),
                _group("research-a", members=(USERNAME,), gid=3),
                _group("dsmlp-public", auto_join=True, gid=4),
            ])

        groups = asyncio.run(_client(handler).fetch_groups(FALLBACK))
        assert seen == ["/api/groups"]
        assert [g.name for g in groups] == ["research-a", "dsmlp-public"]
        assert groups[0].members[0].username == USERNAME
        assert groups[1].on_demand_auto_join

    def test_an_unwanted_group_is_never_validated(self):
        # Malformed, but not asked for: it must not sink the refresh.
        def handler(request):
            return httpx.Response(200, json=[
                {"name": "cse151b", "members": "not-a-list"},
                _group("research-a", members=(USERNAME,)),
            ])

        groups = asyncio.run(_client(handler).fetch_groups(FALLBACK))
        assert [g.name for g in groups] == ["research-a"]

    def test_an_app_without_the_auto_join_flag_reads_as_members_only(self):
        def handler(request):
            payload = _group("research-a", members=(USERNAME,))
            del payload["on_demand_auto_join"]
            return httpx.Response(200, json=[payload])

        [group] = asyncio.run(_client(handler).fetch_groups(FALLBACK))
        assert group.on_demand_auto_join is False

    @pytest.mark.parametrize(
        "response, event",
        [
            (httpx.Response(500, json={"detail": "boom"}), "api.groups_fetch_failed"),
            (httpx.Response(200, json={"detail": "not a list"}), "api.groups_parse_failed"),
            (httpx.Response(200, text="<html>proxy</html>"), "api.groups_parse_failed"),
            (
                httpx.Response(200, json=[{"name": "research-a", "id": "x"}]),
                "api.groups_parse_failed",
            ),
        ],
    )
    def test_a_failure_is_none_and_logged(self, caplog, response, event):
        caplog.set_level(logging.WARNING, logger="app.reservation_client")
        result = asyncio.run(_client(lambda request: response).fetch_groups(FALLBACK))
        assert result is None
        assert [kv_fields(r.getMessage())["event"] for r in caplog.records] == [event]

    def test_a_network_failure_is_none(self, caplog):
        def handler(request):
            raise httpx.ConnectError("refused")

        caplog.set_level(logging.WARNING, logger="app.reservation_client")
        assert asyncio.run(_client(handler).fetch_groups(FALLBACK)) is None
        assert kv_fields(caplog.records[0].getMessage())["event"] == "api.groups_fetch_failed"


# ---------------------------------------------------------------------------
# main._refresh_default_group_rosters -- the cache
# ---------------------------------------------------------------------------


class _GroupsClient:
    """Answers fetch_groups from a canned payload (None = the app failed)."""

    def __init__(self, groups):
        self.groups = groups
        self.calls: list[tuple[str, ...]] = []

    async def fetch_groups(self, names):
        self.calls.append(tuple(names))
        if self.groups is None:
            return None
        if isinstance(self.groups, Exception):
            raise self.groups
        return [GroupDetail.model_validate(g) for g in self.groups]


def _refresh(m, state, client, config, now=NOW):
    asyncio.run(m._refresh_default_group_rosters(state, client, config, now))


def _events(caplog):
    return [kv_fields(r.getMessage()) for r in caplog.records if r.name == "app.main"]


class TestRefresh:
    def _config(self, **overrides):
        return make_config(default_usage_groups=FALLBACK, **overrides)

    def _state(self):
        state = ControllerState()
        state.default_usage_groups = FALLBACK
        return state

    def test_nothing_is_fetched_without_a_fallback(self, monkeypatch):
        m = _main_module(monkeypatch)
        client = _GroupsClient([])
        _refresh(m, ControllerState(), client, make_config())
        assert client.calls == []

    def test_the_first_refresh_loads_and_logs_the_order(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        caplog.set_level(logging.INFO, logger="app.main")
        state = self._state()
        client = _GroupsClient([
            _group("research-b", members=(USERNAME, "bob"), gid=5),
            _group("dsmlp-public", auto_join=True, gid=6),
        ])
        _refresh(m, state, client, self._config())

        assert client.calls == [FALLBACK]
        assert state.default_group_rosters_at == NOW
        assert state.default_usage_group_for(USERNAME) == "research-b"
        assert state.default_usage_group_for("someone-new") == "dsmlp-public"
        events = _events(caplog)
        # One line per listed group, in list order: the walk a pod takes.
        assert [(e["event"], e["name"]) for e in events] == [
            ("default_group.unusable", "research-a"),
            ("default_group.loaded", "research-b"),
            ("default_group.loaded", "dsmlp-public"),
        ]
        assert events[0]["reason"] == "not_found"
        assert (events[1]["id"], events[1]["count"], events[1]["reason"]) == (
            "5", "2", "members",
        )
        assert events[2]["reason"] == "auto_join"

    def test_an_inactive_group_is_reported(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        caplog.set_level(logging.WARNING, logger="app.main")
        state = self._state()
        client = _GroupsClient([
            _group("research-a", members=(USERNAME,), active=False, gid=2),
            _group("research-b", gid=3),
            _group("dsmlp-public", gid=4),
        ])
        _refresh(m, state, client, self._config())
        [line] = _events(caplog)
        assert (line["event"], line["name"], line["id"], line["reason"]) == (
            "default_group.unusable", "research-a", "2", "inactive",
        )
        assert state.default_usage_group_for(USERNAME) is None

    def test_rosters_are_cached_for_the_refresh_interval(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = self._state()
        client = _GroupsClient([_group("research-a", members=(USERNAME,))])
        config = self._config(default_usage_group_refresh_interval=4 * 3600)

        _refresh(m, state, client, config, NOW)
        _refresh(m, state, client, config, NOW + timedelta(hours=4) - timedelta(seconds=1))
        assert len(client.calls) == 1  # still fresh: no request at all

        _refresh(m, state, client, config, NOW + timedelta(hours=4))
        assert len(client.calls) == 2
        assert state.default_group_rosters_at == NOW + timedelta(hours=4)

    def test_a_membership_change_is_seen_at_the_next_refresh(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = self._state()
        config = self._config(default_usage_group_refresh_interval=3600)
        client = _GroupsClient([
            _group("research-b", members=(USERNAME,)),
            _group("dsmlp-public", auto_join=True),
        ])
        _refresh(m, state, client, config, NOW)
        assert state.default_usage_group_for(USERNAME) == "research-b"

        # They join research-a; until the cache expires, nothing changes.
        client.groups = [
            _group("research-a", members=(USERNAME,)),
            _group("research-b", members=(USERNAME,)),
            _group("dsmlp-public", auto_join=True),
        ]
        _refresh(m, state, client, config, NOW + timedelta(minutes=30))
        assert state.default_usage_group_for(USERNAME) == "research-b"
        _refresh(m, state, client, config, NOW + timedelta(hours=1))
        assert state.default_usage_group_for(USERNAME) == "research-a"

    def test_a_failed_refresh_keeps_the_previous_rosters(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = self._state()
        config = self._config(default_usage_group_refresh_interval=3600)
        client = _GroupsClient([_group("research-a", members=(USERNAME,))])
        _refresh(m, state, client, config, NOW)

        client.groups = None  # the app is unreachable
        later = NOW + timedelta(hours=2)
        _refresh(m, state, client, config, later)
        assert state.default_usage_group_for(USERNAME) == "research-a"
        assert state.default_group_rosters_at == NOW  # so it is still due...
        _refresh(m, state, client, config, later + timedelta(minutes=5))
        assert len(client.calls) == 3  # ...and retried on the next cycle

    def test_a_failed_first_refresh_leaves_them_unknown(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = self._state()
        _refresh(m, state, _GroupsClient(None), self._config())
        assert not state.default_groups_known
        assert state.default_group_rosters_at is None

    def test_an_unexpected_error_is_contained(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        caplog.set_level(logging.ERROR, logger="app.main")
        state = self._state()
        _refresh(m, state, _GroupsClient(RuntimeError("bug")), self._config())
        [line] = _events(caplog)
        assert line["event"] == "default_group.refresh_failed"
        assert state.default_group_rosters is None


class _Stop(Exception):
    """Breaks out of the fetch loop after the cycle under test."""


class TestFetchLoopWiring:
    def test_rosters_refresh_even_when_the_reservation_fetch_fails(self, monkeypatch):
        # The two are separate reads: a reservation fetch that failed says
        # nothing about the groups, and must not hold their refresh back.
        m = _main_module(monkeypatch)
        refreshed: list[datetime] = []
        sleeps = 0

        async def fake_sleep(seconds):
            nonlocal sleeps
            sleeps += 1
            if sleeps > 1:
                raise _Stop()

        async def failing_fetch(*args, **kwargs):
            raise RuntimeError("app down")

        async def record_refresh(state, client, config, now):
            refreshed.append(now)

        monkeypatch.setattr(asyncio, "sleep", fake_sleep)
        monkeypatch.setattr(m, "_refresh_reservations", failing_fetch)
        monkeypatch.setattr(m, "_refresh_default_group_rosters", record_refresh)

        with pytest.raises(_Stop):
            asyncio.run(m.reservation_fetch_loop(
                ControllerState(), None, make_config(default_usage_groups=FALLBACK),
            ))
        assert len(refreshed) == 1


# ---------------------------------------------------------------------------
# pod_watch_loop -- routing a pod that names no group
# ---------------------------------------------------------------------------


class _FakeWatcher:
    def __init__(self, events):
        self._events = events

    async def events(self):
        for ev in self._events:
            yield ev


class _Recorder:
    """Stands in for ``emit_pending_pod_event``."""

    def __init__(self):
        self.calls: list[dict] = []

    async def __call__(self, uid, name, namespace, message, *, reason,
                       gpu_class=None, gpu_count=None):
        self.calls.append(dict(reason=reason, message=message))


def _pod(uid="uid-1", *, namespace=USERNAME, labels=None, annotations=None):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name=f"pod-{uid}", namespace=namespace,
            labels={"gpu-class": GPU_CLASS_LABEL, **(labels or {})},
            annotations=annotations,
            creation_timestamp=NOW,
            deletion_timestamp=None,
        ),
        status=SimpleNamespace(phase="Pending", conditions=None),
        spec=SimpleNamespace(
            tolerations=[],
            containers=[SimpleNamespace(
                resources=SimpleNamespace(requests={"nvidia.com/gpu": "1"})
            )],
            scheduling_gates=None,
        ),
    )


def _watch(monkeypatch, m, config, state, *pods):
    rec = _Recorder()

    async def no_admission(*args, **kwargs):
        pass

    async def no_apply(*args, **kwargs):
        return False

    monkeypatch.setattr(
        m, "PodWatcher", lambda **kw: _FakeWatcher([("ADDED", p) for p in pods])
    )
    monkeypatch.setattr(m, "_run_ondemand_admission", no_admission)
    monkeypatch.setattr(m, "_try_apply_toleration", no_apply)
    monkeypatch.setattr(m, "emit_pending_pod_event", rec)
    state.required_group_label = config.required_group_label
    state.default_usage_groups = config.default_usage_groups
    state.gpu_class_labels = {GPU_CLASS_ID: GPU_CLASS_LABEL}
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    state.gpu_classes_known = True
    state.reservations_known = True
    asyncio.run(m.pod_watch_loop(state, None, config))
    return rec


def _with_rosters(*rosters) -> ControllerState:
    state = ControllerState()
    state.default_group_rosters = {r.name: r for r in rosters}
    return state


ROSTERS = (
    _roster("research-a", members=("bob",)),
    _roster("research-b", members=(USERNAME,)),
    _roster("dsmlp-public", auto_join=True),
)


class TestRoutingWithTheLabelFeature:
    def _config(self, groups=FALLBACK, **overrides):
        return make_config(
            required_group_label=GROUP_LABEL_NAME, default_usage_groups=groups,
            **overrides,
        )

    def test_a_labelless_pod_gets_its_owners_first_group(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _with_rosters(*ROSTERS)
        _watch(
            monkeypatch, m, self._config(), state,
            _pod("uid-1", annotations={MIN_RUNTIME: "600"}),
            _pod("uid-2", namespace="bob", annotations={MIN_RUNTIME: "600"}),
            _pod("uid-3", namespace="carol", annotations={MIN_RUNTIME: "600"}),
        )
        by_ns = {
            c.pod_namespace: (c.group_label, c.usage_group, c.usage_group_source)
            for c in state.ondemand_candidates.values()
        }
        assert by_ns == {
            USERNAME: ("research-b", "research-b", "default"),
            "bob": ("research-a", "research-a", "default"),
            # Belongs to nothing listed: the auto-join group takes anyone.
            "carol": ("dsmlp-public", "dsmlp-public", "default"),
        }

    def test_the_pods_own_label_wins_over_the_list(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _with_rosters(*ROSTERS)
        _watch(
            monkeypatch, m, self._config(), state,
            _pod(labels={GROUP_LABEL_NAME: "cse251a"}, annotations={MIN_RUNTIME: "600"}),
        )
        candidate = state.ondemand_candidates["uid-1"]
        assert (candidate.usage_group, candidate.usage_group_source) == ("cse251a", "label")

    def test_the_resolved_group_is_also_the_booking_lookup(self, monkeypatch):
        # The fallback stands in for the label wholesale: an open booking under
        # the owner's resolved group is matched; one under a group they merely
        # also listed is not the pod's.
        m = _main_module(monkeypatch)
        now = datetime.now(timezone.utc)
        state = _with_rosters(*ROSTERS)
        state.reservations = [
            reservation(
                1, start_utc=now - timedelta(minutes=5), end_utc=now + timedelta(hours=2),
                group="research-a",
            ),
            reservation(
                2, start_utc=now - timedelta(minutes=5), end_utc=now + timedelta(hours=2),
                group="research-b",
            ),
        ]
        _watch(
            monkeypatch, m, self._config(), state,
            _pod(annotations={MIN_RUNTIME: "600"}),
        )
        assert state.task_queue["uid-1"].reservation.id == 2
        assert state.task_queue["uid-1"].group_label == "research-b"
        assert state.ondemand_candidates == {}

    def test_a_user_no_group_admits_is_told_so(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _with_rosters(*ROSTERS[:2])  # no auto-join group listed
        rec = _watch(
            monkeypatch, m, self._config(groups=FALLBACK[:2]), state,
            _pod(namespace="carol", annotations={MIN_RUNTIME: "600"}),
        )
        assert state.ondemand_candidates == {}
        [call] = rec.calls
        assert call["reason"] == NO_RESERVATION_REASON
        assert (
            f"it has no {GROUP_LABEL_NAME} label naming its usage group, and you are "
            f"not a member of any of the cluster's default usage groups"
        ) in call["message"]
        # The operator's groups are not named to someone outside them.
        for name in FALLBACK:
            assert name not in call["message"]

    def test_nothing_is_said_before_the_rosters_load(self, monkeypatch):
        # Whether a default group admits the owner is not known yet, so "this
        # pod has no group" would blame them for the controller's gap.
        m = _main_module(monkeypatch)
        state = ControllerState()  # rosters never loaded
        rec = _watch(
            monkeypatch, m, self._config(), state,
            _pod(namespace="carol", annotations={MIN_RUNTIME: "600"}),
        )
        assert state.ondemand_candidates == {}
        assert rec.calls == []

    def test_a_pod_with_its_own_group_is_still_told_before_they_load(self, monkeypatch):
        # Its group is its own, so the unloaded rosters say nothing about it.
        m = _main_module(monkeypatch)
        state = ControllerState()
        rec = _watch(
            monkeypatch, m, self._config(), state,
            _pod(labels={GROUP_LABEL_NAME: "cse251a"}),  # no minimum runtime
        )
        assert [c["reason"] for c in rec.calls] == [NO_RESERVATION_REASON]
        assert "default usage groups" not in rec.calls[0]["message"]


class TestRoutingWithoutTheLabelFeature:
    def test_the_list_stands_in_for_the_annotation(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _with_rosters(*ROSTERS)
        _watch(
            monkeypatch, m, make_config(default_usage_groups=FALLBACK), state,
            _pod(annotations={MIN_RUNTIME: "600"}),
        )
        candidate = state.ondemand_candidates["uid-1"]
        assert (candidate.usage_group, candidate.usage_group_source) == (
            "research-b", "default",
        )
        assert candidate.group_label is None  # the match axis stays off

    def test_the_pods_own_annotation_wins(self, monkeypatch):
        m = _main_module(monkeypatch)
        state = _with_rosters(*ROSTERS)
        _watch(
            monkeypatch, m, make_config(default_usage_groups=FALLBACK), state,
            _pod(annotations={MIN_RUNTIME: "600", USAGE_GROUP: "cse251a"}),
        )
        assert state.ondemand_candidates["uid-1"].usage_group == "cse251a"


# ---------------------------------------------------------------------------
# Preflight -- a waiting candidate follows its owner's current fallback
# ---------------------------------------------------------------------------


def _pending_pod(uid="uid-1"):
    return SimpleNamespace(
        metadata=SimpleNamespace(
            uid=uid, name=f"pod-{uid}", namespace=USERNAME,
            annotations=None, labels={"gpu-class": GPU_CLASS_LABEL},
        ),
        status=SimpleNamespace(
            phase="Pending",
            conditions=[SimpleNamespace(
                type="PodScheduled", status="False", reason="Unschedulable",
                message="0/10 nodes are available: 5 Insufficient nvidia.com/gpu.",
            )],
        ),
        spec=SimpleNamespace(tolerations=[], containers=[], scheduling_gates=None),
    )


def _candidate(*, usage_group, source="default", group_label=None):
    return OnDemandCandidate(
        pod_uid="uid-1", pod_name="pod-uid-1", pod_namespace=USERNAME,
        gpu_class_label=GPU_CLASS_LABEL, gpu_requested=1, min_runtime_seconds=600,
        pod_created_at=NOW, next_attempt_at=NOW,
        group_label=group_label, usage_group=usage_group, usage_group_source=source,
        lease_error_count=3, awaiting_capacity=True,
    )


def _preflight(monkeypatch, m, state, candidate, config):
    async def fake_read_pod(name, namespace):
        return _pending_pod()

    monkeypatch.setattr(m, "read_pod", fake_read_pod)
    state.gpu_class_ids = {GPU_CLASS_LABEL: GPU_CLASS_ID}
    state.default_usage_groups = config.default_usage_groups
    state.required_group_label = config.required_group_label
    return asyncio.run(m._preflight_ondemand_candidate(
        state, config, candidate.pod_uid, candidate,
    ))


class TestPreflightReresolves:
    def test_a_group_the_owner_has_left_is_replaced(self, monkeypatch, caplog):
        # Asked under research-a; since then they left it, and the refreshed
        # rosters put them in research-b.
        m = _main_module(monkeypatch)
        caplog.set_level(logging.INFO, logger="app.main")
        config = make_config(
            required_group_label=GROUP_LABEL_NAME, default_usage_groups=FALLBACK,
        )
        state = _with_rosters(*ROSTERS)
        candidate = _candidate(usage_group="research-a", group_label="research-a")

        status, ask = _preflight(monkeypatch, m, state, candidate, config)

        assert status == m._PREFLIGHT_READY
        assert ask.group_name == "research-b"
        assert (candidate.usage_group, candidate.group_label) == ("research-b", "research-b")
        # A different ask: the old one's fault backoff and capacity queue do
        # not carry over.
        assert (candidate.lease_error_count, candidate.awaiting_capacity) == (0, False)
        [line] = [e for e in _events(caplog) if e["event"] == "ondemand.group_changed"]
        assert (line["old.group"], line["new.group"]) == ("research-a", "research-b")

    def test_an_unchanged_group_is_left_alone(self, monkeypatch):
        m = _main_module(monkeypatch)
        config = make_config(default_usage_groups=FALLBACK)
        state = _with_rosters(*ROSTERS)
        candidate = _candidate(usage_group="research-b")

        status, ask = _preflight(monkeypatch, m, state, candidate, config)

        assert ask.group_name == "research-b"
        assert candidate.lease_error_count == 3  # nothing reset
        assert candidate.group_label is None  # match axis off: never set

    def test_a_candidate_no_group_admits_any_more_is_dropped(self, monkeypatch, caplog):
        m = _main_module(monkeypatch)
        caplog.set_level(logging.INFO, logger="app.main")
        config = make_config(default_usage_groups=FALLBACK[:2])
        state = _with_rosters(_roster("research-a", members=("bob",)))
        candidate = _candidate(usage_group="research-b")

        status, ask = _preflight(monkeypatch, m, state, candidate, config)

        assert (status, ask) == (m._PREFLIGHT_REMOVE, None)
        [line] = [e for e in _events(caplog) if e["event"] == "ondemand.candidate_dropped"]
        assert line["reason"] == "no_default_group"

    def test_a_group_the_pod_named_itself_is_not_second_guessed(self, monkeypatch):
        m = _main_module(monkeypatch)
        config = make_config(default_usage_groups=FALLBACK)
        state = _with_rosters(*ROSTERS)
        candidate = _candidate(usage_group="cse251a", source="annotation")

        status, ask = _preflight(monkeypatch, m, state, candidate, config)

        assert ask.group_name == "cse251a"


# ---------------------------------------------------------------------------
# What the pod's owner reads
# ---------------------------------------------------------------------------


class TestMessages:
    def test_the_default_source_says_where_the_group_came_from(self, monkeypatch):
        m = _main_module(monkeypatch)
        assert m._usage_group_source_phrase("default", make_config()) == (
            "chosen from the cluster's default usage groups, as the pod names none"
        )

    @pytest.mark.parametrize(
        "groups, known, says",
        [
            ((), True, False),          # no fallback configured
            (FALLBACK, True, True),     # configured, and none admits the owner
            (FALLBACK, False, False),   # configured, but not known yet
        ],
    )
    def test_the_no_group_clause(self, monkeypatch, groups, known, says):
        m = _main_module(monkeypatch)
        [clause] = m._ondemand_ineligibility(
            make_config(default_usage_groups=groups),
            min_runtime=600, best_effort=False, usage_group=None, problems=[],
            has_usage_group_annotation=False, default_groups_known=known,
        )
        assert clause.startswith(f"it has no {USAGE_GROUP} annotation naming its usage group")
        assert ("default usage groups" in clause) is says
