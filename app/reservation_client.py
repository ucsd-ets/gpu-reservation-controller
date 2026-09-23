"""Async HTTP client for the GPU Reservation management API.

Implements only the endpoints the controller needs:
  - GET /api/reservations  — paginated list of all (active + cancelled) reservations
  - GET /api/gpu-classes/{id}  — per-class detail including label_value
  - GET /api/gpu-classes  — full class list (JIT label → id resolution)
  - POST /api/reservations  — create a JIT on-demand booking
  - POST /api/reservations/ondemand-admission  — ask the app which pending pods to admit
  - POST /api/reservations/preemption-victims  — ask the app which overstay pods to preempt
  - POST /api/reservations/{id}/cancel  — cancel a reservation (no-show / revoke)
  - POST /api/reservations/{id}/overstay  — report an ended overstay's duration (analysis-only)
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from pydantic import ValidationError

from .config import Config
from .schemas import (
    GpuClassDetail,
    OnDemandAdmissionRequest,
    OnDemandAdmissionResponse,
    BestEffortReservationRequest,
    OnDemandReservationRequest,
    OverstayReportRequest,
    PreemptionSelectionRequest,
    PreemptionSelectionResponse,
    ReservationResponse,
)

from . import trace
from .log_fields import kv

log = logging.getLogger(__name__)

# The app's documented denial code for an infeasible on-demand lease: capacity,
# SU budget, or a policy ceiling.  Routine, expected, and worth retrying — every
# other status is a fault that waiting will not fix.
LEASE_DENIED_STATUS = 409

# The app's answer to a lease ask naming a user, usage group or GPU class it does
# not recognise (or holds inactive).  Not retryable, like every non-409 4xx --
# but unlike a read-only key or a schema mismatch, every name it can refer to
# came off the pod (its namespace, its group label or annotation, its gpu-class
# label), so it is the one such fault worth telling the pod's owner about.
LEASE_NOT_FOUND_STATUS = 404


@dataclass(frozen=True)
class LeaseAttempt:
    """Outcome of one ``create_ondemand_reservation`` call.

    Carries the HTTP status alongside the result so the caller can tell a
    routine 409 denial (retry — capacity may free up) from a fault that will
    never resolve on its own (a read-only service key, a schema mismatch after
    an app upgrade, an unknown group name).  Both used to collapse into a bare
    ``None``, which is why a misconfigured deployment retried forever while
    logging only at INFO.
    """

    reservation: Optional[ReservationResponse] = None
    status: Optional[int] = None  # None when the app never answered (network/parse)
    # Excerpt of the app's ``HTTPException.detail`` on a non-2xx, or None when
    # the app never answered.  Carried rather than only logged because the 409
    # text is the only statement of *why* the ask was infeasible ("Only 2 GPU(s)
    # available for this group at ...", "SU budget exceeded"), and the caller
    # surfaces it to the pod's owner as a Kubernetes Event.
    detail: Optional[str] = None
    # The admission-denial envelope (RESERVATION-API.md, "Admission-denial
    # envelope"), read off a 409: which gate refused (``code``), whether waiting
    # can ever admit this same ask (``app_retryable``), and the instant the
    # denial is known to clear (``not_before``).  Each is None when the app sent
    # none -- an older app, or a denial outside the admission gates.
    code: Optional[str] = None
    app_retryable: Optional[bool] = None
    not_before: Optional[datetime] = None

    @property
    def granted(self) -> bool:
        return self.reservation is not None

    @property
    def structural(self) -> bool:
        """The app refused this ask and said waiting will never change the answer.

        A 409 carrying ``retryable: false`` -- more GPUs than the class allows,
        a group the user is not in, a group whose validity has ended.  Only an
        administrator, or a different ask, gets past it.  An absent flag is
        read as retryable, as the contract requires: a wrong "retryable" costs
        one wasted attempt, a wrong "permanent" strands a job that would run.
        """
        return self.status == LEASE_DENIED_STATUS and self.app_retryable is False

    @property
    def retryable(self) -> bool:
        """Whether waiting is a plausible fix.

        A 409 is the app saying "not right now" -- unless its envelope says
        otherwise (``structural``); a network error or a 5xx may be transient.
        A 4xx that is not 409 is a fault in the request or the credential, so
        backing off hard beats hammering the app every 2–5 min.
        """
        if self.status is None:
            return True
        if self.structural:
            return False
        return self.status == LEASE_DENIED_STATUS or self.status >= 500


_DETAIL_MAX_CHARS = 200

# Rendered into a log line when an error response carries nothing to quote.  A
# log field cannot be empty and still read as deliberate, but this is our own
# placeholder rather than something the app said, so it must not travel any
# further -- ``LeaseAttempt.detail`` reports it as None ("the app gave no
# reason") so it can never surface as a reason to a user.
_NO_RESPONSE_BODY = "no body"


def _response_detail(response: httpx.Response) -> str:
    """A short, log-safe excerpt of an error response body.

    The app answers a rejection with a JSON ``detail``; anything else (a proxy's
    HTML error page, an empty body) degrades to the raw text.  Truncated because
    a misconfigured route can return a full page, and ``kv()`` would render the
    whole thing on one line.
    """
    try:
        payload = response.json()
    except ValueError:
        payload = None
    detail = payload.get("detail") if isinstance(payload, dict) else None
    text = str(detail) if detail is not None else (response.text or "").strip()
    if len(text) > _DETAIL_MAX_CHARS:
        text = text[:_DETAIL_MAX_CHARS] + "…"
    return text or _NO_RESPONSE_BODY


def _denial_envelope(
    response: httpx.Response,
) -> tuple[Optional[str], Optional[bool], Optional[datetime]]:
    """Read ``(code, retryable, not_before)`` off an admission-denial body.

    Tolerant by design: every field is optional, and a value of the wrong type
    is dropped rather than trusted, so a malformed envelope degrades to exactly
    the pre-envelope behaviour (status-only classification) instead of raising.
    """
    try:
        payload = response.json()
    except ValueError:
        return None, None, None
    if not isinstance(payload, dict):
        return None, None, None
    code = payload.get("code")
    code = code if isinstance(code, str) and code else None
    retryable = payload.get("retryable")
    retryable = retryable if isinstance(retryable, bool) else None
    not_before: Optional[datetime] = None
    raw = payload.get("not_before")
    if isinstance(raw, str):
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            parsed = None
        if parsed is not None:
            not_before = (
                parsed.replace(tzinfo=timezone.utc)
                if parsed.tzinfo is None
                else parsed.astimezone(timezone.utc)
            )
    return code, retryable, not_before


async def _attach_trace(request: httpx.Request) -> None:
    """Add ``X-Client-Trace`` so the app logs this request under our trace.

    The app's request middleware already reads this header, so nothing on its
    side had to change for a controller-initiated call to become correlatable.
    No header is sent when no unit of work is in scope.
    """
    for header, value in trace.outbound_headers().items():
        request.headers[header] = value


class ReservationClient:
    def __init__(self, config: Config) -> None:
        self._lookahead_days = config.reservation_lookahead_days
        # Client-level default timeout (CODE-REVIEW D8) so a future endpoint added
        # without an explicit timeout doesn't silently inherit httpx's 5 s default;
        # per-request timeouts below override it where a different value is wanted.
        self._client = httpx.AsyncClient(
            base_url=config.reservation_api_url,
            headers={"X-API-Key": config.reservation_api_key},
            timeout=httpx.Timeout(10.0),
            # Stamp the in-scope trace onto every outbound request from one
            # place rather than at each of the eight call sites — the same
            # chokepoint reasoning as kv() scrubbing values. A hook also cannot
            # be forgotten by an endpoint added later.
            event_hooks={"request": [_attach_trace]},
        )

    async def aclose(self) -> None:
        """Close the underlying HTTP connection pool."""
        await self._client.aclose()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_reservations(self) -> list[ReservationResponse]:
        """Return all reservations (active and cancelled) from today through lookahead window.

        Raises:
            httpx.HTTPStatusError / httpx.RequestError: on API or network failure.
                Unlike ``fetch_gpu_class`` (which degrades to ``None``), a failed
                reservation fetch propagates so the refresh cycle aborts rather
                than acting on an empty reservation list.
        """
        # UTC everywhere: date.today() would use the process TZ and drop
        # currently-open reservations east of UTC (see CODE-REVIEW-2026-07 B2).
        #
        # The API selects by window **overlap**, so a reservation that started
        # before ``date_start`` and is still running inside the range comes back
        # on its own -- the one-day widening below is no longer what keeps a
        # currently-open window in view, and must not be read as though it were.
        # It is kept only as slack for the app's local clock differing from UTC
        # by up to a day at the range edges; the range is a *day* filter on the
        # app's own calendar, and neither side is worth a timezone round-trip to
        # tighten.
        today = datetime.now(timezone.utc).date()
        start = today - timedelta(days=1)
        end = today + timedelta(days=self._lookahead_days)
        results: list[ReservationResponse] = []
        offset = 0
        limit = 200

        while True:
            resp = await self._client.get(
                "/api/reservations",
                params={
                    "status": "all",
                    "date_start": start.isoformat(),
                    "date_end": end.isoformat(),
                    "limit": limit,
                    "offset": offset,
                },
                timeout=15.0,
            )
            resp.raise_for_status()
            page = [ReservationResponse.model_validate(r) for r in resp.json()]
            results.extend(page)
            if len(page) < limit:
                break
            offset += limit

        active = sum(1 for r in results if r.status == "active")
        log.info("%s", kv(
            event="api.reservations_fetched", count=len(results), active=active,
            cancelled=len(results) - active, lookahead_days=self._lookahead_days,
        ))
        return results

    async def fetch_gpu_class(self, gpu_class_id: int) -> Optional[GpuClassDetail]:
        """Return detail for a single GPU class, or None on error."""
        try:
            resp = await self._client.get(f"/api/gpu-classes/{gpu_class_id}")
            resp.raise_for_status()
            return GpuClassDetail.model_validate(resp.json())
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.gpu_class_fetch_failed", cid=gpu_class_id,
                status=exc.response.status_code,
            ))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.gpu_class_fetch_failed", cid=gpu_class_id, err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            # Malformed / unparseable payload (ValueError covers JSONDecodeError):
            # honor the documented "None on error" contract instead of letting it
            # abort the whole refresh cycle (B9).
            log.warning("%s", kv(event="api.gpu_class_parse_failed", cid=gpu_class_id, err=exc))
            return None

    async def fetch_gpu_classes(self) -> Optional[list[GpuClassDetail]]:
        """Return the full GPU class list, or None on error.

        Feeds the JIT lease path's label → id reverse map
        (``ControllerState.gpu_class_ids``): a pod carries only its gpu-class
        *label*, but creating an on-demand reservation needs the numeric
        ``gpu_class_id``.  Degrades to ``None`` like ``fetch_gpu_class`` so a
        transient failure does not abort the reservation refresh cycle.
        """
        try:
            resp = await self._client.get("/api/gpu-classes")
            resp.raise_for_status()
            return [GpuClassDetail.model_validate(c) for c in resp.json()]
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(event="api.gpu_classes_fetch_failed", status=exc.response.status_code))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.gpu_classes_fetch_failed", err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(event="api.gpu_classes_parse_failed", err=exc))
            return None

    async def create_ondemand_reservation(
        self, req: OnDemandReservationRequest
    ) -> LeaseAttempt:
        """Request a JIT on-demand booking — a *guaranteed*, SU-charged lease.

        Idempotent by ``req.idempotency_key`` (the admitting pod's UID): the
        app returns the original reservation for a repeated key rather than a
        duplicate, so the caller can safely retry with the same request.
        """
        return await self._post_reservation_create(req)

    async def create_best_effort_reservation(
        self, req: BestEffortReservationRequest
    ) -> LeaseAttempt:
        """Request a best-effort admission — a zero-length, zero-SU stub.

        For a pod that declared ``galends/runtime-guarantee: none``.  The
        returned reservation's window is already over, which is exactly what
        makes the pod preemptible from its first tick.

        Idempotent by ``req.idempotency_key`` (the pod's UID), as for a lease.

        Shares :meth:`_post_reservation_create` with the lease path verbatim, so
        the two get identical status classification, backoff and denial-event
        plumbing — a best-effort 409 reaches the pod's owner as a Kubernetes
        Event by the same route, with no second implementation to keep in step.
        """
        return await self._post_reservation_create(req)

    async def _post_reservation_create(self, req) -> LeaseAttempt:
        """POST a reservation-create body and classify the outcome.

        Shared by the on-demand lease and best-effort admission paths; *req* is
        any request model carrying an ``idempotency_key``.

        Returns a :class:`LeaseAttempt` rather than a bare reservation, because
        the caller must distinguish a 409 — the app's routine "infeasible right
        now", worth retrying — from any other status, which is a fault that will
        not resolve on its own.  Only the 409 is logged at INFO; everything else
        is a WARNING carrying the response body, so a read-only key or a schema
        mismatch is visible to an operator watching at WARNING rather than
        buried in a per-pod INFO line every 2–5 minutes forever.
        """
        try:
            resp = await self._client.post(
                "/api/reservations",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return LeaseAttempt(
                reservation=ReservationResponse.model_validate(resp.json()),
                status=resp.status_code,
            )
        except httpx.HTTPStatusError as exc:
            status = exc.response.status_code
            detail = _response_detail(exc.response)
            if status == LEASE_DENIED_STATUS:
                code, app_retryable, not_before = _denial_envelope(exc.response)
                log.info("%s", kv(
                    event="api.lease_denied", poduid=req.idempotency_key,
                    status=status, reason=code, retryable=app_retryable,
                    detail=detail,
                ))
                return LeaseAttempt(
                    status=status,
                    detail=None if detail == _NO_RESPONSE_BODY else detail,
                    code=code,
                    app_retryable=app_retryable,
                    not_before=not_before,
                )
            log.warning("%s", kv(
                event="api.lease_error", poduid=req.idempotency_key,
                status=status, detail=detail,
            ))
            return LeaseAttempt(
                status=status,
                detail=None if detail == _NO_RESPONSE_BODY else detail,
            )
        except httpx.RequestError as exc:
            log.warning("%s", kv(
                event="api.lease_failed", poduid=req.idempotency_key, err=exc,
            ))
            return LeaseAttempt()
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(
                event="api.lease_parse_failed", poduid=req.idempotency_key, err=exc,
            ))
            return LeaseAttempt()

    async def select_preemption_victims(
        self, req: PreemptionSelectionRequest
    ) -> Optional[list[str]]:
        """Ask the app to choose which overstay candidates to preempt.

        Returns the selected pod UIDs (possibly empty — the app may deliberately
        spare pods), or ``None`` on any failure (endpoint missing on an older
        app, network error, non-2xx, unparseable body).  A ``None`` return tells
        the caller to fall back to local random selection; an empty list is a
        deliberate app decision and is respected.  Degrades like the other
        client calls so a transient failure never aborts the preemption sweep.
        """
        try:
            resp = await self._client.post(
                "/api/reservations/preemption-victims",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return PreemptionSelectionResponse.model_validate(resp.json()).victim_pod_uids
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.victim_selection_failed", status=exc.response.status_code,
            ))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.victim_selection_failed", err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(event="api.victim_selection_parse_failed", err=exc))
            return None

    async def select_ondemand_admissions(
        self, req: OnDemandAdmissionRequest
    ) -> Optional[list[str]]:
        """Ask the app which pending pods to grant JIT on-demand admission.

        Returns the granted pod UIDs (possibly empty — the app may deliberately
        grant none), or ``None`` on any failure (endpoint missing on an older
        app, network error, non-2xx, unparseable body).  A ``None`` return tells
        the caller to fall back to granting every offered candidate (today's
        greedy per-pod behaviour); an empty list is a deliberate app decision and
        is respected.  Degrades like the other client calls so a transient
        failure never strands the admission batch.
        """
        try:
            resp = await self._client.post(
                "/api/reservations/ondemand-admission",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return OnDemandAdmissionResponse.model_validate(resp.json()).granted_pod_uids
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.admission_selection_failed", status=exc.response.status_code,
            ))
            return None
        except httpx.RequestError as exc:
            log.warning("%s", kv(event="api.admission_selection_failed", err=exc))
            return None
        except (ValidationError, ValueError) as exc:
            log.warning("%s", kv(event="api.admission_selection_parse_failed", err=exc))
            return None

    async def cancel_reservation(self, reservation_id: int, reason: str) -> bool:
        """Cancel *reservation_id* (no-show or controller-revoked); True on success.

        Idempotent: cancelling an already-cancelled id is treated as success
        (the app's own idempotent-cancel semantics; any 2xx is a success, and a
        404 means the reservation is already gone, which is the outcome we
        wanted anyway).
        """
        try:
            resp = await self._client.post(
                f"/api/reservations/{reservation_id}/cancel",
                json={"reason": reason},
                timeout=15.0,
            )
            if resp.status_code == 404:
                log.info("%s", kv(
                    event="api.cancel_already_gone", rid=reservation_id,
                    reason=reason, status=404,
                ))
                return True
            resp.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.cancel_failed", rid=reservation_id, reason=reason,
                status=exc.response.status_code,
            ))
            return False
        except httpx.RequestError as exc:
            log.warning("%s", kv(
                event="api.cancel_failed", rid=reservation_id, reason=reason, err=exc,
            ))
            return False

    async def report_overstay(
        self, reservation_id: int, req: OverstayReportRequest
    ) -> bool:
        """Record an ended overstay against *reservation_id*; True on success.

        Analysis-only and strictly best-effort: any failure (endpoint absent on
        an older app, network error, non-2xx, unparseable) is logged and
        swallowed so it never blocks pod teardown or preemption.  Idempotent
        app-side on ``req.pod_uid`` — a repeat for the same terminating pod is a
        harmless no-op.
        """
        try:
            resp = await self._client.post(
                f"/api/reservations/{reservation_id}/overstay",
                json=req.model_dump(mode="json"),
                timeout=15.0,
            )
            resp.raise_for_status()
            return True
        except httpx.HTTPStatusError as exc:
            log.warning("%s", kv(
                event="api.overstay_report_failed", rid=reservation_id,
                poduid=req.pod_uid, status=exc.response.status_code,
            ))
            return False
        except httpx.RequestError as exc:
            log.warning("%s", kv(
                event="api.overstay_report_failed", rid=reservation_id,
                poduid=req.pod_uid, err=exc,
            ))
            return False

