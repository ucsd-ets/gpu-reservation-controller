
This file documents development standards for AI coding agents (Claude Code, Codex, etc.).
Read this alongside `CLAUDE.md` for full context.

---

## IMPORTANT — Initial project setup

As a first step before any work in this repository, you **must** create a local
Python virtual environment:

```bash
uv venv .venv && source .venv/bin/activate && uv pip install -r requirements-dev.lock
```

An external hook ensures all subsequent shell commands run inside this virtual
environment — but you must create it first.

Install from the **lock**, not `requirements-dev.txt`.  The requirements files
are lower-bound only, so installing them directly gives you whatever resolved
that morning — which is not what the image ships, and not what CI tested.  The
dev lock also carries `pytest`, which `requirements.txt` alone does not.

### Changing a dependency

Edit the requirements file, then recompile **both** locks — the dev lock is
compiled against the prod lock as a constraint, so it stays a superset at
identical versions:

```bash
uv pip compile requirements.txt -o requirements.lock
uv pip compile requirements-dev.txt -c requirements.lock -o requirements-dev.lock
```

`tests/test_dependency_lock.py` fails the build if a requirements file declares
something its lock does not pin, or if the two locks disagree on a version.

---

## What not to do

- Do not add `__pycache__`, `*.pyc`, or `.env` to version control — they are gitignored.
- Do not install packages with bare `pip install`; always use `uv pip install` inside the activated venv.
- Do not add a database, ORM, authentication layer, or frontend.  This is a
  pure daemon with no persistent state.  See CLAUDE.md for the rationale.
- Do not add `SQLAlchemy`, `argon2-cffi`, `python-jose`, or any web-framework
  machinery beyond plain routes on the module-global `app` in `app/main.py`
  (no `APIRouter` objects, no static files).  Current routes: `GET /health`,
  `POST /api/reservations/push`, `GET /api/forecast/preemption-risk`.

---

## Project-specific dev notes

### Running the controller locally (out-of-cluster)

```bash
export RESERVATION_API_URL=http://localhost:8001   # point at a local dev API
export RESERVATION_API_KEY=gpures_<key>
export KUBECONFIG=~/.kube/config                   # or a test cluster kubeconfig
export TZ=America/Los_Angeles
uvicorn app.main:app --port 8000 --reload
```

`--reload` is fine for development; do not use it in production.

### Verifying the health endpoint

```bash
curl http://localhost:8000/health
# {"status":"ok"}
```

### Module responsibilities — before adding code, read the right file

| Module | Owns |
|--------|------|
| `config.py` | All environment-variable parsing; add new settings here |
| `schemas.py` | Pydantic models for the reservation API; mirror RESERVATION-API.md §6 |
| `reservation_client.py` | All HTTP calls to the reservation management API |
| `k8s_client.py` | All Kubernetes API calls (watch, list, patch, singleton Lease); no business logic |
| `controller.py` | Business logic: matching, time-window arithmetic, queue management |
| `resources.py` | What one unit of a GPU class is, and converting Kubernetes quantities to units — pure, no `kubernetes` import |
| `main.py` | Wiring only: creates state, starts tasks, owns the FastAPI app |

### Kubernetes client note

`k8s_client.py` uses the **synchronous** `kubernetes` Python client, run in a
thread-pool executor via `asyncio.get_running_loop().run_in_executor(None, ...)`.
Do not call `_core_v1.*` directly from async code without wrapping in an executor.

### Time-window arithmetic

All datetime objects are **UTC-aware** (`timezone.utc` from the standard library).
The reservation API now supplies `start_utc` / `end_utc` directly on each
`ReservationResponse`; `slot_start` and `slot_end` in `controller.py` simply
return those fields.  Every `datetime.now()` call uses `datetime.now(timezone.utc)`.
Do not introduce naive datetimes or `pytz`/`zoneinfo` — `datetime.timezone.utc`
is sufficient.

### Testing

The core logic modules (`controller.py`, `resources.py`, `schemas.py`, `config.py`) have no
Kubernetes or HTTP dependencies and can be exercised with plain `pytest` and
in-process mocks.  For the boundary modules the suite stays dependency-free
(`requirements-dev.txt` is `pytest` only): `reservation_client.py` is tested with
httpx's built-in `httpx.MockTransport` (see `tests/test_reservation_client.py`),
and `k8s_client.py` / the `main.py` coroutines are driven with `monkeypatch`,
`SimpleNamespace` pod stubs, and `asyncio.run` (see `tests/test_guards.py`,
`tests/test_watch_release.py`).  No `pytest-httpx` or kubernetes fake client is
required or installed.
