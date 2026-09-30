"""Tests for the reservation HTTP client boundary.

Uses httpx.MockTransport (built in — no pytest-httpx dependency) to drive the
async client without a live server.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone

import httpx
import pytest

from app.config import Config
from app.reservation_client import AdmissionSelection, ReservationClient, _response_detail
from app.schemas import (
    OnDemandAdmissionCandidate,
    OnDemandAdmissionRequest,
    OnDemandReservationRequest,
    OverstayReportRequest,
)

from tests.conftest import kv_fields as _fields, make_config, reservation as _reservation


def _only_lease_record(caplog) -> logging.LogRecord:
    """The single api.lease_* record emitted by one client call."""
    records = [
        r for r in caplog.records
        if r.name == "app.reservation_client" and "event=api.lease_" in r.getMessage()
    ]
    assert len(records) == 1, [r.getMessage() for r in records]
    return records[0]


def _config(**overrides) -> Config:
    """Thin alias for the shared builder in tests/conftest."""
    return make_config(**overrides)


def _client_with_handler(config: Config, handler) -> ReservationClient:
    client = ReservationClient(config)
    # Swap the live AsyncClient for one backed by a MockTransport.
    client._client = httpx.AsyncClient(
        base_url=config.reservation_api_url,
        transport=httpx.MockTransport(handler),
    )
    return client


# ---------------------------------------------------------------------------
# B2 — fetch date bounds are UTC-derived and widened by one day
# ---------------------------------------------------------------------------


def test_fetch_reservations_date_start_is_utc_and_widened():
    """date_start must be (UTC today - 1 day); date_end must be UTC today + lookahead.

    Regression for CODE-REVIEW-2026-07 B2: date.today() used the process TZ and
    could drop reservations whose window is open right now.
    """
    captured: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured.update(dict(request.url.params))
        return httpx.Response(200, json=[])

    config = _config(reservation_lookahead_days=7)
    client = _client_with_handler(config, handler)

    before = datetime.now(timezone.utc).date()
    asyncio.run(client.fetch_reservations())
    after = datetime.now(timezone.utc).date()

    # Allow for a midnight rollover between the two now() samples.
    expected_starts = {(before - timedelta(days=1)).isoformat(),
                       (after - timedelta(days=1)).isoformat()}
    expected_ends = {(before + timedelta(days=7)).isoformat(),
                    (after + timedelta(days=7)).isoformat()}
    assert captured["date_start"] in expected_starts
    assert captured["date_end"] in expected_ends
    assert captured["status"] == "all"

    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# B9 — malformed payloads honor the "None on error" contract
# ---------------------------------------------------------------------------


def test_fetch_gpu_class_returns_none_on_invalid_json():
    """Non-JSON body (ValueError/JSONDecodeError) must also return None (B9)."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.fetch_gpu_class(1)) is None
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# fetch_gpu_classes — full class list for the JIT label → id reverse map
# ---------------------------------------------------------------------------


def test_fetch_gpu_classes_returns_list():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/gpu-classes"
        return httpx.Response(
            200,
            json=[
                # First class carries total_gpus; second omits it entirely.
                {"id": 10, "name": "H100", "label_value": "h100", "total_gpus": 8},
                {"id": 20, "name": "A100", "label_value": "a100"},
            ],
        )

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.fetch_gpu_classes())
    assert result is not None
    assert [(c.id, c.label_value) for c in result] == [(10, "h100"), (20, "a100")]
    # total_gpus is parsed when present, and defaults to None when absent.
    assert result[0].total_gpus == 8
    assert result[1].total_gpus is None
    asyncio.run(client.aclose())


def test_fetch_gpu_classes_parses_effective_gpus_today():
    # The audit reads the override-resolved count, so it has to survive parsing.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                # A maintenance window has halved this class for today.
                {
                    "id": 10,
                    "name": "H100",
                    "label_value": "h100",
                    "total_gpus": 80,
                    "effective_gpus_today": 40,
                },
                # An app predating the field publishes only the default.
                {"id": 20, "name": "A100", "label_value": "a100", "total_gpus": 8},
            ],
        )

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.fetch_gpu_classes())
    assert result is not None
    assert (result[0].total_gpus, result[0].effective_gpus_today) == (80, 40)
    assert result[0].audit_gpus == 40
    assert result[1].effective_gpus_today is None
    assert result[1].audit_gpus == 8
    asyncio.run(client.aclose())


def test_fetch_gpu_classes_returns_none_on_http_error():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.fetch_gpu_classes()) is None
    asyncio.run(client.aclose())


def test_fetch_gpu_classes_returns_none_on_invalid_json():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"not json")

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.fetch_gpu_classes()) is None
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# create_ondemand_reservation — JIT lease request
# ---------------------------------------------------------------------------


def _ondemand_request(**overrides) -> OnDemandReservationRequest:
    base = dict(
        username="alice",
        group_name="cse151b",
        gpu_class_id=10,
        gpu_count=1,
        duration_seconds=1200,
        on_demand=True,
        idempotency_key="pod-uid-1",
        notes="on-demand lease for pod alice/train-1",
    )
    base.update(overrides)
    return OnDemandReservationRequest(**base)


def test_create_ondemand_reservation_201_returns_reservation():
    now = datetime.now(timezone.utc)
    res = _reservation(7, start_utc=now, end_utc=now + timedelta(minutes=20))

    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/reservations"
        body = json.loads(request.content)
        assert body["idempotency_key"] == "pod-uid-1"
        assert body["on_demand"] is True
        # group_name is a required natural key on the app side; notes carry
        # which pod the lease covers for admin traceability.
        assert body["group_name"] == "cse151b"
        assert body["notes"] == "on-demand lease for pod alice/train-1"
        return httpx.Response(201, json=res.model_dump(mode="json"))

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert result.granted and result.reservation.id == 7
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_409_is_a_routine_denial(caplog):
    """A capacity/policy denial degrades without raising, and stays at INFO."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(409, json={"detail": "no capacity"})

    client = _client_with_handler(_config(), handler)
    with caplog.at_level(logging.DEBUG, logger="app.reservation_client"):
        result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert not result.granted
    assert result.status == 409
    assert result.retryable is True

    record = _only_lease_record(caplog)
    assert record.levelno == logging.INFO
    assert _fields(record.getMessage())["event"] == "api.lease_denied"
    asyncio.run(client.aclose())


@pytest.mark.parametrize("status", [401, 403, 404, 422])
def test_create_ondemand_reservation_non_409_is_an_error(caplog, status):
    """A fault waiting cannot fix: WARNING, carries the body, not retryable.

    These used to be indistinguishable from a 409 — same INFO line, same bare
    None — so a read-only service key retried every 2-5 min forever below the
    level anyone alerts on.
    """
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json={"detail": "write scope required"})

    client = _client_with_handler(_config(), handler)
    with caplog.at_level(logging.DEBUG, logger="app.reservation_client"):
        result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert not result.granted
    assert result.status == status
    assert result.retryable is False

    record = _only_lease_record(caplog)
    assert record.levelno == logging.WARNING
    fields = _fields(record.getMessage())
    assert fields["event"] == "api.lease_error"
    assert fields["status"] == str(status)
    assert fields["detail"] == "write scope required"
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_5xx_is_an_error_but_retryable():
    # The app is broken rather than refusing: loud, but waiting may still fix it.
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream unavailable")

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert not result.granted
    assert result.status == 503
    assert result.retryable is True
    asyncio.run(client.aclose())


def test_create_ondemand_reservation_timeout_has_no_status():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.create_ondemand_reservation(_ondemand_request()))
    assert not result.granted
    # The app never answered, so there is no status to reason about — and a
    # network blip must stay retryable.
    assert result.status is None
    assert result.retryable is True
    asyncio.run(client.aclose())


def test_lease_error_detail_truncates_a_large_body():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, text="x" * 5000)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.create_ondemand_reservation(_ondemand_request())).status == 400
    assert len(_response_detail(httpx.Response(400, text="x" * 5000))) <= 201
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# select_ondemand_admissions — app-delegated JIT admission selection
# ---------------------------------------------------------------------------


def _admission_request(**overrides) -> OnDemandAdmissionRequest:
    base = dict(
        pod_uid="pod-uid-1",
        pod_created_at=datetime(2026, 8, 21, 17, 0, tzinfo=timezone.utc),
        pod_annotations={"galends/minimum-runtime-seconds": "1200"},
        username="alice",
        group_name=None,
        gpu_class_id=10,
        gpu_count=1,
        duration_seconds=1200,
    )
    base.update(overrides)
    return OnDemandAdmissionRequest(
        candidates=[
            OnDemandAdmissionCandidate(**base),
            OnDemandAdmissionCandidate(
                pod_uid="pod-uid-2",
                pod_created_at=datetime(2026, 8, 21, 17, 5, tzinfo=timezone.utc),
                username="bob", gpu_class_id=10,
                gpu_count=2, duration_seconds=600,
            ),
        ]
    )


def test_select_ondemand_admissions_200_returns_granted_uids():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/reservations/ondemand-admission"
        body = json.loads(request.content)
        assert [c["pod_uid"] for c in body["candidates"]] == ["pod-uid-1", "pod-uid-2"]
        # The pod evidence rides on the wire alongside the ask: a creation time
        # the app can age a candidate by, and the pod's galends annotations.
        first, second = body["candidates"]
        assert first["pod_created_at"] == "2026-08-21T17:00:00Z"
        assert first["pod_annotations"] == {"galends/minimum-runtime-seconds": "1200"}
        assert second["pod_created_at"] == "2026-08-21T17:05:00Z"
        # A pod declaring no galends annotation serialises an empty map, never null.
        assert second["pod_annotations"] == {}
        # Which create would follow a grant rides along too.
        assert first["best_effort"] is False
        return httpx.Response(200, json={"granted_pod_uids": ["pod-uid-2"]})

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.select_ondemand_admissions(_admission_request()))
    # An app predating ``withheld`` reads as withholding nothing it explained.
    assert result == AdmissionSelection(granted=["pod-uid-2"], withheld={})
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_keeps_the_app_order():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, json={"granted_pod_uids": ["pod-uid-2", "pod-uid-1"], "withheld": []},
        )

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.select_ondemand_admissions(_admission_request()))
    assert result.granted == ["pod-uid-2", "pod-uid-1"]
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_reads_each_withheld_envelope():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "granted_pod_uids": [],
            "withheld": [
                {"pod_uid": "pod-uid-1", "detail": "Fits on its own, but not after...",
                 "code": "outranked", "retryable": True},
                {"pod_uid": "pod-uid-2", "detail": "This lease costs 4 SU but ...",
                 "code": "su_budget_member", "retryable": True,
                 "not_before": "2026-08-24T07:00:00Z"},
            ],
        })

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.select_ondemand_admissions(_admission_request()))
    outranked = result.withheld["pod-uid-1"]
    assert outranked.outranked and outranked.app_retryable is True
    budget = result.withheld["pod-uid-2"]
    assert not budget.outranked
    assert budget.code == "su_budget_member"
    assert budget.not_before == datetime(2026, 8, 24, 7, 0, tzinfo=timezone.utc)
    # As the 409 the create would have returned, to reuse the denial path.
    denial = budget.as_denial()
    assert denial.status == 409 and denial.retryable and not denial.structural
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_skips_an_unreadable_withheld_entry():
    """One malformed entry must not discard the app's grants with it."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "granted_pod_uids": ["pod-uid-1"],
            "withheld": [
                "not an object",
                {"detail": "names no pod"},
                {"pod_uid": "pod-uid-2", "code": 7, "retryable": "no",
                 "not_before": "never", "detail": "x" * 5000},
            ],
        })

    client = _client_with_handler(_config(), handler)
    result = asyncio.run(client.select_ondemand_admissions(_admission_request()))
    assert result.granted == ["pod-uid-1"]
    assert set(result.withheld) == {"pod-uid-2"}
    entry = result.withheld["pod-uid-2"]
    # Wrong types are dropped, not trusted: read as retryable, per the contract.
    assert entry.code is None and entry.app_retryable is None and entry.not_before is None
    assert len(entry.detail) <= 201
    assert entry.as_denial().retryable
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_empty_list_is_respected():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"granted_pod_uids": []})

    client = _client_with_handler(_config(), handler)
    # Empty (grant none) is a valid decision — distinct from None (fall back).
    result = asyncio.run(client.select_ondemand_admissions(_admission_request()))
    assert result is not None and result.granted == []
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_404_returns_none():
    """A missing endpoint (older app) degrades to None → caller grants all."""
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"detail": "not found"})

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.select_ondemand_admissions(_admission_request())) is None
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_timeout_returns_none():
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("timed out", request=request)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.select_ondemand_admissions(_admission_request())) is None
    asyncio.run(client.aclose())


def test_select_ondemand_admissions_bad_body_returns_none():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": "shape"})

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.select_ondemand_admissions(_admission_request())) is None
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# cancel_reservation — no-show / controller-revoked
# ---------------------------------------------------------------------------


def test_cancel_reservation_200_returns_true():
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/reservations/42/cancel"
        assert json.loads(request.content) == {"reason": "no-show"}
        return httpx.Response(200, json={"status": "cancelled"})

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.cancel_reservation(42, "no-show")) is True
    asyncio.run(client.aclose())


def test_cancel_reservation_already_gone_404_treated_as_success():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.cancel_reservation(42, "controller-revoked")) is True
    asyncio.run(client.aclose())


def test_cancel_reservation_error_returns_false():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client_with_handler(_config(), handler)
    assert asyncio.run(client.cancel_reservation(42, "no-show")) is False
    asyncio.run(client.aclose())


# ---------------------------------------------------------------------------
# report_overstay — analysis-only, best-effort
# ---------------------------------------------------------------------------


def _overstay_req() -> OverstayReportRequest:
    start = datetime(2026, 7, 19, 17, 0, 0, tzinfo=timezone.utc)
    return OverstayReportRequest(
        pod_uid="pod-abc",
        gpu_count=1,
        start_utc=start,
        end_utc=start + timedelta(minutes=30),
        end_reason="preempted",
    )


def test_report_overstay_posts_to_reservation_path():
    seen: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(200, json={"id": 1})

    client = _client_with_handler(_config(), handler)
    ok = asyncio.run(client.report_overstay(4412, _overstay_req()))
    assert ok is True
    assert seen["path"] == "/api/reservations/4412/overstay"
    assert seen["body"]["pod_uid"] == "pod-abc"
    assert seen["body"]["end_reason"] == "preempted"
    asyncio.run(client.aclose())


def test_report_overstay_swallows_errors():
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500)

    client = _client_with_handler(_config(), handler)
    # Best-effort: a non-2xx returns False and never raises.
    assert asyncio.run(client.report_overstay(42, _overstay_req())) is False
    asyncio.run(client.aclose())
