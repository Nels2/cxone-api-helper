#!/usr/bin/env python3
"""
Small CXone bearer-token proxy for homepage.dev customapi.

Run:
  pip install fastapi uvicorn httpx python-dotenv
  uvicorn server:app --host 0.0.0.0 --port 8787

Required env:
  CXONE_USERNAME=...
  CXONE_PASSWORD=...
  CXONE_CLIENT_ID=...
  CXONE_CLIENT_SECRET=...

Optional env:
  CXONE_AUTH_URL=https://cxone.niceincontact.com/auth/token
  CXONE_API_BASE_URL=https://api-<area>.<domain>
  CXONE_API_VERSION=v30.0
  CXONE_AUTH_SEND_JSON=false
  CXONE_VERIFY_TLS=true
  CXONE_TOKEN_SKEW_SECONDS=60
  CXONE_ROUTE_MAP_JSON='{"agents":"https://api-na1.niceincontact.com/incontactapi/services/v30.0/agents"}'

Usage examples:
  GET  /health
  GET  /token/status
  POST /token/refresh
  GET  /job/agents
  GET  /job/agent_states
  GET  /proxy/incontactapi/services/v30.0/agents/states
  GET  /proxy?path=/incontactapi/services/v30.0/agents/states
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping
from urllib.parse import urljoin, urlparse

import httpx
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, Response
from pydantic import BaseModel


load_dotenv()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("dash-ccm-cxone-proxy")


AUTH_URL = os.getenv("CXONE_AUTH_URL", "https://cxone.niceincontact.com/auth/token")
API_BASE_URL = os.getenv("CXONE_API_BASE_URL", "").rstrip("/")
API_VERSION = os.getenv("CXONE_API_VERSION", "v30.0")

AUTH_SEND_JSON = os.getenv("CXONE_AUTH_SEND_JSON", "false").lower() in {
    "1",
    "true",
    "yes",
    "y",
}

VERIFY_TLS = os.getenv("CXONE_VERIFY_TLS", "true").lower() not in {
    "0",
    "false",
    "no",
    "n",
}

TOKEN_SKEW_SECONDS = int(os.getenv("CXONE_TOKEN_SKEW_SECONDS", "60"))
REQUEST_TIMEOUT_SECONDS = float(os.getenv("CXONE_REQUEST_TIMEOUT_SECONDS", "30"))
SUMMARY_CACHE_SECONDS = int(os.getenv("CXONE_SUMMARY_CACHE_SECONDS", "45"))
AGENT_ID_ALLOWLIST_RAW = os.getenv("CXONE_AGENT_IDS", "").strip()


REQUIRED_ENV = [
    "CXONE_USERNAME",
    "CXONE_PASSWORD",
    "CXONE_CLIENT_ID",
    "CXONE_CLIENT_SECRET",
]


DEFAULT_ROUTES: dict[str, str] = {
    # 1. Get Agents - Call Center Only
    "agents": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/agents?teamId=11320079",
    "agents_call_center": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/agents?teamId=11320079",
    "agents_all": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/agents",

    # 2. Get Longest in Queue
    "longest_in_queue": "https://home-c200.nice-incontact.com/ReportService/DataDownloadHandler.ashx?CDST=7zAveaYEymBw%2f0TZFNp2Ec3K9VOYs3WUQNhzXd3q7J%2bojag4145x%2bZ%2bbPUlXUDjlBhW4%2f43fmvfDAQC2yzRNFdo1%2btisSSshAg3UpJOoDqRAVIQk1B14m7yqLsTlJEJ4WHZX4aHLH7yP6VVzuMw2bXGjNj%2fjpLJhaRglYf%2b0RqL3L8F7TXYAQhsv%2bVyKrgBNnHDpYQt12YLARf0%2bZNqOvYvv%2f2i7CZEF9NXPFIE5VDy2yDy7T5MAL6w%2bLAhXr57w&PresetDate=1&Format=CSV&IncludeHeaders=True&AppendDate=True",

    # 3. Get Agents Real-Time States
    "agent_states": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/agents/states",
    "agents_realtime_states": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/agents/states",

    # 4. Get Call Center Team Performance
    "team_performance": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/teams/11320079/performance-total",
    "call_center_team_performance": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/teams/11320079/performance-total",

    # 5. Get Call Backs
    "callbacks": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/callbacks",
    "call_backs": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/callbacks",

    # 6. Get Agent Performance - Daily
    "agent_performance_daily": "https://api-c200.nice-incontact.com/incontactapi/services/v33.0/wfm-data/skills/agent-performance",

    # 7. Get SLA Summary - Daily
    "sla_summary_daily": "https://api-c200.nice-incontact.com/incontactapi/services/v33.0/skills/sla-summary",

    # 8. Get Active Calls
    "active_calls": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/realTime/contacts/active",

    # 9. Get Completed Callbacks
    "completed_callbacks": "https://api-c200.nice-incontact.com/incontactapi/services/v33.0/contacts/completed?teamId=11320079",

    # 10. Get Agent Daily Scorecard
    "agent_daily_scorecard": "https://api-c200.nice-incontact.com/incontactapi/services/v33.0/wfm-data/agents/scorecards",

    # 11. Get Agent Daily State History
    "agent_daily_state_history": "https://api-c200.nice-incontact.com/incontactapi/services/v33.0/wfm-data/agents/scorecards",

    # 12. Get Agent Daily State Details
    "agent_daily_state_details": "https://api-c200.nice-incontact.com/incontactapi/services/v33.0/contacts",

    # 13. Get Agent Daily Interaction History
    "agent_daily_interaction_history": "https://api-c200.nice-incontact.com/incontactapi/services/v33.0/interaction-history",

    # Extra
    "teams": "https://api-na1.niceincontact.com/incontactapi/services/v30.0/teams",
}


DATE_WINDOW_JOBS = {
    "team_performance",
    "call_center_team_performance",
    "agent_performance_daily",
    "sla_summary_daily",
    "completed_callbacks",
    "agent_daily_scorecard",
    "agent_daily_state_history",
    "agent_daily_state_details",
    "agent_daily_interaction_history",
}


def _load_route_map() -> dict[str, str]:
    routes = dict(DEFAULT_ROUTES)

    raw = os.getenv("CXONE_ROUTE_MAP_JSON", "").strip()
    if not raw:
        return routes

    try:
        extra = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"CXONE_ROUTE_MAP_JSON is not valid JSON: {exc}") from exc

    if not isinstance(extra, dict):
        raise RuntimeError("CXONE_ROUTE_MAP_JSON must be a JSON object mapping route names to paths")

    for key, value in extra.items():
        if not isinstance(key, str) or not isinstance(value, str):
            raise RuntimeError("CXONE_ROUTE_MAP_JSON keys and values must be strings")

        routes[key] = value

    return routes


ROUTES = _load_route_map()


class TokenStatus(BaseModel):
    has_token: bool
    token_type: str | None = None
    expires_at_epoch: int | None = None
    seconds_remaining: int
    auth_url: str
    api_base_url_set: bool
    auth_payload_mode: str


@dataclass
class TokenCache:
    access_token: str | None = None
    token_type: str = "Bearer"
    refresh_token: str | None = None
    id_token: str | None = None
    issued_token_type: str | None = None
    expires_at: float = 0.0
    raw: dict[str, Any] = field(default_factory=dict)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def seconds_remaining(self) -> int:
        return max(0, int(self.expires_at - time.time()))

    def valid(self) -> bool:
        return bool(self.access_token) and time.time() < (self.expires_at - TOKEN_SKEW_SECONDS)

@dataclass
class SummaryCache:
    agents_summary: dict[str, Any] | None = None
    agents_summary_expires_at: float = 0.0
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def agents_valid(self) -> bool:
        return self.agents_summary is not None and time.time() < self.agents_summary_expires_at

cache = TokenCache()
summary_cache = SummaryCache()
app = FastAPI(title="dash-ccm CXone helper", version="1.0.0")


def _missing_env() -> list[str]:
    return [name for name in REQUIRED_ENV if not os.getenv(name)]


def _auth_payload() -> dict[str, str]:
    missing = _missing_env()
    if missing:
        raise HTTPException(
            status_code=500,
            detail=f"Missing required environment variables: {', '.join(missing)}",
        )

    return {
        "grant_type": os.getenv("CXONE_GRANT_TYPE", "password"),
        "username": os.environ["CXONE_USERNAME"],
        "password": os.environ["CXONE_PASSWORD"],
        "client_id": os.environ["CXONE_CLIENT_ID"],
        "client_secret": os.environ["CXONE_CLIENT_SECRET"],
    }


def _redacted_token_response(data: Mapping[str, Any]) -> dict[str, Any]:
    safe: dict[str, Any] = {}

    for key, value in data.items():
        if key in {"access_token", "refresh_token", "id_token"} and value:
            safe[key] = "<redacted>"
        else:
            safe[key] = value

    return safe


def _iso_utc(dt: datetime) -> str:
    """
    Return ISO 8601 UTC timestamp with Z suffix.

    Example:
      2026-05-08T22:15:30Z
    """
    return (
        dt.astimezone(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _inject_default_date_params(params: dict[str, str], job_name: str | None = None) -> dict[str, str]:
    """
    Inject default ISO 8601 date window params when missing.

    NICE/CXone expects lowercase:
      endDate
      startDate
    """
    now = datetime.now(timezone.utc)

    if job_name == "team_performance" or job_name == "call_center_team_performance":
        # Team performance in your Bruno export uses a 15-minute window.
        start = now - timedelta(minutes=15)
    else:
        # Daily/reporting endpoints use last 24 hours.
        start = now - timedelta(days=1)

    now_iso = _iso_utc(now)
    start_iso = _iso_utc(start)

    params.setdefault("endDate", now_iso)
    params.setdefault("startDate", start_iso)

    return params

def _agent_id_allowlist() -> set[int]:
    """
    Optional allowlist for agents.

    Env:
      CXONE_AGENT_IDS=43888126,43888127,43986490
    """
    if not AGENT_ID_ALLOWLIST_RAW:
        return set()

    agent_ids: set[int] = set()

    for item in AGENT_ID_ALLOWLIST_RAW.split(","):
        item = item.strip()
        if not item:
            continue

        try:
            agent_ids.add(int(item))
        except ValueError:
            log.warning("Ignoring invalid CXONE_AGENT_IDS entry: %s", item)

    return agent_ids

async def refresh_access_token() -> None:
    payload = _auth_payload()

    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_SECONDS,
        verify=VERIFY_TLS,
    ) as client:
        try:
            if AUTH_SEND_JSON:
                response = await client.post(
                    AUTH_URL,
                    json=payload,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                )
            else:
                response = await client.post(
                    AUTH_URL,
                    data=payload,
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/x-www-form-urlencoded",
                    },
                )
        except httpx.HTTPError as exc:
            raise HTTPException(
                status_code=502,
                detail=f"CXone auth request failed: {exc}",
            ) from exc

    if response.status_code >= 400:
        detail = response.text[:2000]
        raise HTTPException(
            status_code=502,
            detail=f"CXone auth failed with HTTP {response.status_code}: {detail}",
        )

    try:
        data = response.json()
    except ValueError as exc:
        raise HTTPException(
            status_code=502,
            detail="CXone auth response was not JSON",
        ) from exc

    # Some CXone responses may be wrapped in an array, because apparently
    # consistency was not in the project budget.
    if isinstance(data, list):
        if not data or not isinstance(data[0], dict):
            raise HTTPException(
                status_code=502,
                detail="CXone auth returned a JSON array but not an object inside it",
            )

        data = data[0]

    if not isinstance(data, dict):
        raise HTTPException(
            status_code=502,
            detail="CXone auth response was not a JSON object",
        )

    access_token = data.get("access_token")
    if not access_token:
        raise HTTPException(
            status_code=502,
            detail=f"CXone auth response had no access_token: {_redacted_token_response(data)}",
        )

    expires_in = int(data.get("expires_in") or 3600)

    cache.access_token = str(access_token)
    cache.token_type = str(data.get("token_type") or "Bearer")
    cache.refresh_token = data.get("refresh_token")
    cache.id_token = data.get("id_token")
    cache.issued_token_type = data.get("issued_token_type")
    cache.expires_at = time.time() + expires_in
    cache.raw = data

    log.info("Refreshed CXone access token; expires in %s seconds", expires_in)


async def get_access_token() -> str:
    if cache.valid():
        return cache.access_token or ""

    async with cache.lock:
        if not cache.valid():
            await refresh_access_token()

    return cache.access_token or ""


def _require_api_base() -> str:
    if not API_BASE_URL:
        raise HTTPException(
            status_code=500,
            detail=(
                "CXONE_API_BASE_URL is not set. Set it to your discovered CXone API base, "
                "for example https://api-na1.niceincontact.com."
            ),
        )

    return API_BASE_URL


def _is_allowed_absolute_url(url: str) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()

    if parsed.scheme != "https" or not host:
        return False

    return host.endswith(".niceincontact.com") or host.endswith(".nice-incontact.com")


def _clean_path(path: str) -> str:
    if not path:
        raise HTTPException(status_code=400, detail="Missing target path")

    if path.startswith("http://") or path.startswith("https://"):
        raise HTTPException(
            status_code=400,
            detail="Only relative CXone API paths are allowed here",
        )

    return "/" + path.lstrip("/")


def _target_url(path: str, *, allow_absolute: bool = False) -> str:
    if path.startswith("http://") or path.startswith("https://"):
        if not allow_absolute or not _is_allowed_absolute_url(path):
            raise HTTPException(
                status_code=400,
                detail="Absolute URL is not allowed for this request",
            )

        return path

    base = _require_api_base().rstrip("/") + "/"
    return urljoin(base, _clean_path(path).lstrip("/"))

async def _cxone_request(
    method: str,
    path: str,
    *,
    params: dict[str, str] | None = None,
    content: bytes | None = None,
    headers: dict[str, str] | None = None,
    allow_absolute: bool = False,
) -> httpx.Response:
    """
    Make an authenticated CXone request.

    Handles:
      - token fetch
      - bearer injection
      - one token refresh retry on 401
    """
    token = await get_access_token()

    request_headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    if headers:
        request_headers.update(headers)

    target_url = _target_url(path, allow_absolute=allow_absolute)

    async with httpx.AsyncClient(
        timeout=REQUEST_TIMEOUT_SECONDS,
        verify=VERIFY_TLS,
    ) as client:
        response = await client.request(
            method=method.upper(),
            url=target_url,
            params=params,
            content=content,
            headers=request_headers,
        )

        if response.status_code == 401:
            async with cache.lock:
                await refresh_access_token()
                token = cache.access_token or ""

            request_headers["Authorization"] = f"Bearer {token}"

            response = await client.request(
                method=method.upper(),
                url=target_url,
                params=params,
                content=content,
                headers=request_headers,
            )

    return response

def _copy_query_params(request: Request, drop: set[str] | None = None) -> dict[str, str]:
    drop = drop or set()

    return {
        key: value
        for key, value in request.query_params.multi_items()
        if key not in drop
    }


async def _forward(
    request: Request,
    path: str,
    params: dict[str, str] | None = None,
    *,
    allow_absolute: bool = False,
) -> Response:
    method = request.method.upper()
    body = await request.body()

    headers: dict[str, str] = {
        "Accept": request.headers.get("accept", "application/json"),
    }

    content_type = request.headers.get("content-type")
    if content_type and method not in {"GET", "HEAD"}:
        headers["Content-Type"] = content_type

    response = await _cxone_request(
        method,
        path,
        params=params if params is not None else _copy_query_params(request),
        content=body if method not in {"GET", "HEAD"} else None,
        headers=headers,
        allow_absolute=allow_absolute,
    )

    excluded_headers = {
        "content-encoding",
        "content-length",
        "transfer-encoding",
        "connection",
    }

    passthrough_headers = {
        key: value
        for key, value in response.headers.items()
        if key.lower() not in excluded_headers
    }

    return Response(
        content=response.content,
        status_code=response.status_code,
        headers=passthrough_headers,
    )
    
def _safe_len(value: Any) -> int:
    if isinstance(value, list):
        return len(value)

    if isinstance(value, dict):
        return len(value)

    return 0


def _safe_sum(rows: list[dict[str, Any]], key: str) -> float:
    total = 0.0

    for row in rows:
        value = row.get(key, 0)
        if isinstance(value, int | float):
            total += float(value)

    return total


def _safe_avg(rows: list[dict[str, Any]], key: str) -> float:
    values: list[float] = []

    for row in rows:
        value = row.get(key)
        if isinstance(value, int | float):
            values.append(float(value))

    if not values:
        return 0.0

    return round(sum(values) / len(values), 2)


def _find_by_name(rows: list[dict[str, Any]], key: str, wanted: str) -> dict[str, Any] | None:
    wanted_lower = wanted.lower()

    for row in rows:
        value = str(row.get(key, "")).lower()
        if value == wanted_lower:
            return row

    return None


def _json_or_error(job_name: str, response: httpx.Response) -> dict[str, Any]:
    try:
        data = response.json()
    except ValueError:
        return {
            "ok": False,
            "job": job_name,
            "status_code": response.status_code,
            "error": "Response was not JSON",
            "body_preview": response.text[:500],
        }

    if response.status_code >= 400:
        return {
            "ok": False,
            "job": job_name,
            "status_code": response.status_code,
            "error": data,
        }

    if not isinstance(data, dict):
        return {
            "ok": False,
            "job": job_name,
            "status_code": response.status_code,
            "error": "Response JSON was not an object",
            "data_type": type(data).__name__,
        }

    return {
        "ok": True,
        "job": job_name,
        "status_code": response.status_code,
        "data": data,
    }


async def _fetch_job_json(job_name: str) -> dict[str, Any]:
    route = ROUTES.get(job_name)

    if not route:
        return {
            "ok": False,
            "job": job_name,
            "status_code": 404,
            "error": f"No route configured for job '{job_name}'",
        }

    params: dict[str, str] = {}

    if job_name in DATE_WINDOW_JOBS:
        params = _inject_default_date_params(params, job_name)

    response = await _cxone_request(
        "GET",
        route,
        params=params,
        allow_absolute=True,
    )

    return _json_or_error(job_name, response)

@app.get("/health")
async def health() -> dict[str, Any]:
    return {
        "ok": True,
        "missing_env": _missing_env(),
        "api_base_url_set": bool(API_BASE_URL),
        "auth_url": AUTH_URL,
        "auth_payload_mode": "json" if AUTH_SEND_JSON else "form-url-encoded",
        "routes_configured": sorted([key for key, value in ROUTES.items() if value]),
        "routes_missing_paths": sorted([key for key, value in ROUTES.items() if not value]),
        "date_window_jobs": sorted(DATE_WINDOW_JOBS),
    }


@app.get("/token/status", response_model=TokenStatus)
async def token_status() -> TokenStatus:
    return TokenStatus(
        has_token=bool(cache.access_token),
        token_type=cache.token_type if cache.access_token else None,
        expires_at_epoch=int(cache.expires_at) if cache.access_token else None,
        seconds_remaining=cache.seconds_remaining(),
        auth_url=AUTH_URL,
        api_base_url_set=bool(API_BASE_URL),
        auth_payload_mode="json" if AUTH_SEND_JSON else "form-url-encoded",
    )


@app.post("/token/refresh")
async def token_refresh() -> dict[str, Any]:
    async with cache.lock:
        await refresh_access_token()

    return {
        "ok": True,
        "seconds_remaining": cache.seconds_remaining(),
        "token_type": cache.token_type,
    }


@app.api_route("/job/{job_name}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def job(job_name: str, request: Request) -> Response:
    route = ROUTES.get(job_name)

    if route is None:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown job '{job_name}'. Known jobs: {sorted(ROUTES.keys())}",
        )

    if not route:
        raise HTTPException(
            status_code=501,
            detail=(
                f"Job '{job_name}' exists as a placeholder, but no CXone path is configured. "
                "Set CXONE_ROUTE_MAP_JSON to map it to the exact NICE endpoint path."
            ),
        )

    params = _copy_query_params(request)

    # Inject automatic date/time params only for reporting/daily endpoints.
    # Live endpoints do not need this clutter.
    if job_name in DATE_WINDOW_JOBS:
        params = _inject_default_date_params(params, job_name)

    # Allows route templates like:
    #   /agents/{agentId}/state-history
    # Then call:
    #   /job/some_route?agentId=123
    for key, value in list(params.items()):
        token = "{" + key + "}"
        if token in route:
            route = route.replace(token, value)
            params.pop(key, None)

    if "{" in route or "}" in route:
        raise HTTPException(
            status_code=400,
            detail=f"Missing required path parameter for route template: {route}",
        )

    return await _forward(
        request,
        route,
        params=params,
        allow_absolute=True,
    )

@app.get("/summary/call-center")
async def call_center_summary() -> dict[str, Any]:
    """
    Combined dashboard-friendly summary for Homepage.

    This intentionally returns flat, boring JSON because Homepage likes that,
    and honestly, for once Homepage is right.
    """
    jobs = [
        "agents",
        "agent_states",
        "completed_callbacks",
        "agent_performance_daily",
        "sla_summary_daily",
        "agent_daily_scorecard",
        "agent_daily_state_details",
    ]

    results = await asyncio.gather(
        *[_fetch_job_json(job_name) for job_name in jobs],
        return_exceptions=True,
    )

    by_job: dict[str, dict[str, Any]] = {}

    for job_name, result in zip(jobs, results):
        if isinstance(result, Exception):
            by_job[job_name] = {
                "ok": False,
                "job": job_name,
                "status_code": 500,
                "error": str(result),
            }
        else:
            by_job[job_name] = result

    agents_data = by_job.get("agents", {}).get("data", {})
    states_data = by_job.get("agent_states", {}).get("data", {})
    callbacks_data = by_job.get("completed_callbacks", {}).get("data", {})
    performance_data = by_job.get("agent_performance_daily", {}).get("data", {})
    sla_data = by_job.get("sla_summary_daily", {}).get("data", {})
    scorecard_data = by_job.get("agent_daily_scorecard", {}).get("data", {})
    contacts_data = by_job.get("agent_daily_state_details", {}).get("data", {})

    agents = agents_data.get("agents", [])
    agent_states = states_data.get("agentStates", [])
    completed_callbacks = callbacks_data.get("completedContacts", [])
    skills_performance = performance_data.get("skillsPerformance", [])
    service_level_summaries = sla_data.get("serviceLevelSummaries", [])
    scorecards = scorecard_data.get("wfmScorecardStats", [])
    contacts = contacts_data.get("contacts", [])

    if not isinstance(agents, list):
        agents = []

    if not isinstance(agent_states, list):
        agent_states = []

    if not isinstance(completed_callbacks, list):
        completed_callbacks = []

    if not isinstance(skills_performance, list):
        skills_performance = []

    if not isinstance(service_level_summaries, list):
        service_level_summaries = []

    if not isinstance(scorecards, list):
        scorecards = []

    if not isinstance(contacts, list):
        contacts = []

    account_services_sla = _find_by_name(
        service_level_summaries,
        "skillName",
        "Account Services",
    )

    total_sla_contacts = _safe_sum(service_level_summaries, "totalContacts")
    total_within_sla = _safe_sum(service_level_summaries, "contactsWithinSLA")
    total_out_of_sla = _safe_sum(service_level_summaries, "contactsOutOfSLA")

    if total_within_sla + total_out_of_sla > 0:
        overall_sla_percent = round(
            (total_within_sla / (total_within_sla + total_out_of_sla)) * 100,
            2,
        )
    else:
        overall_sla_percent = 0.0

    total_handled = _safe_sum(skills_performance, "totalHandled")
    total_handled_time = _safe_sum(skills_performance, "totalHandledTime")
    total_acw_time = _safe_sum(skills_performance, "totalACWTime")

    if total_handled > 0:
        avg_handle_seconds = round(total_handled_time / total_handled, 2)
        avg_acw_seconds = round(total_acw_time / total_handled, 2)
    else:
        avg_handle_seconds = 0.0
        avg_acw_seconds = 0.0

    abandoned_contacts = [
        contact for contact in contacts
        if isinstance(contact, dict) and contact.get("abandoned") is True
    ]

    active_contacts = [
        contact for contact in contacts
        if isinstance(contact, dict) and contact.get("isActive") is True
    ]

    unavailable_states = [
        state for state in agent_states
        if isinstance(state, dict)
        and str(state.get("agentStateName", "")).lower() == "unavailable"
    ]

    available_states = [
        state for state in agent_states
        if isinstance(state, dict)
        and str(state.get("agentStateName", "")).lower() == "available"
    ]

    inbound_contacts = [
        contact for contact in contacts
        if isinstance(contact, dict) and contact.get("isOutbound") is False
    ]

    outbound_contacts = [
        contact for contact in contacts
        if isinstance(contact, dict) and contact.get("isOutbound") is True
    ]

    summary = {
        "ok": True,
        "generatedAt": _iso_utc(datetime.now(timezone.utc)),

        # Simple counts
        "agents": len(agents),
        "agentStates": len(agent_states),
        "availableAgents": len(available_states),
        "unavailableAgents": len(unavailable_states),
        "completedCallbacks": len(completed_callbacks),
        "agentPerformanceRows": len(skills_performance),
        "slaSkills": int(sla_data.get("totalRecords") or len(service_level_summaries)),
        "scorecardAgents": len(scorecards),
        "contacts": int(contacts_data.get("totalRecords") or len(contacts)),
        "contactRows": len(contacts),

        # SLA
        "slaTotalContacts": int(total_sla_contacts),
        "slaWithin": int(total_within_sla),
        "slaOut": int(total_out_of_sla),
        "slaPercent": overall_sla_percent,

        # Account Services SLA, because that is probably your main queue
        "accountServicesContacts": int(account_services_sla.get("totalContacts", 0)) if account_services_sla else 0,
        "accountServicesWithinSla": int(account_services_sla.get("contactsWithinSLA", 0)) if account_services_sla else 0,
        "accountServicesOutOfSla": int(account_services_sla.get("contactsOutOfSLA", 0)) if account_services_sla else 0,
        "accountServicesSlaPercent": float(account_services_sla.get("serviceLevel", 0.0)) if account_services_sla else 0.0,

        # Performance
        "totalHandled": int(total_handled),
        "avgHandleSeconds": avg_handle_seconds,
        "avgAcwSeconds": avg_acw_seconds,

        # Contacts
        "abandonedContacts": len(abandoned_contacts),
        "activeContacts": len(active_contacts),
        "inboundContacts": len(inbound_contacts),
        "outboundContacts": len(outbound_contacts),
        "avgQueueSeconds": _safe_avg(contacts, "inQueueSeconds"),
        "avgContactDurationSeconds": _safe_avg(contacts, "totalDurationSeconds"),

        # Scorecard totals
        "scorecardTotalCalls": int(_safe_sum(scorecards, "totalCalls")),
        "scorecardActualCalls": int(_safe_sum(scorecards, "actualCalls")),
        "scorecardIdleSeconds": round(_safe_sum(scorecards, "idleTime"), 2),
        "scorecardAuxSeconds": round(_safe_sum(scorecards, "auxTime"), 2),

        # Debug health
        "sources": {
            job_name: {
                "ok": result.get("ok", False),
                "statusCode": result.get("status_code"),
            }
            for job_name, result in by_job.items()
        },
    }

    failed_sources = {
        job_name: result
        for job_name, result in by_job.items()
        if not result.get("ok", False)
    }

    if failed_sources:
        summary["ok"] = False
        summary["failedSources"] = failed_sources

    return summary

async def _build_agents_summary() -> dict[str, Any]:
    """
    Per-agent dashboard summary.

    Combines:
      - agents
      - agent_states
      - agent_performance_daily
      - agent_daily_scorecard
      - agent_daily_state_details

    Returns a clean agents[] list for Homepage dynamic-list or other dashboards.
    """
    jobs = [
        "agents",
        "agent_states",
        "agent_performance_daily",
        "agent_daily_scorecard",
        "agent_daily_state_details",
    ]

    results = await asyncio.gather(
        *[_fetch_job_json(job_name) for job_name in jobs],
        return_exceptions=True,
    )

    by_job: dict[str, dict[str, Any]] = {}

    for job_name, result in zip(jobs, results):
        if isinstance(result, Exception):
            by_job[job_name] = {
                "ok": False,
                "job": job_name,
                "status_code": 500,
                "error": str(result),
            }
        else:
            by_job[job_name] = result

    agents_data = by_job.get("agents", {}).get("data", {})
    states_data = by_job.get("agent_states", {}).get("data", {})
    performance_data = by_job.get("agent_performance_daily", {}).get("data", {})
    scorecard_data = by_job.get("agent_daily_scorecard", {}).get("data", {})
    contacts_data = by_job.get("agent_daily_state_details", {}).get("data", {})

    agents = agents_data.get("agents", [])
    agent_states = states_data.get("agentStates", [])
    skills_performance = performance_data.get("skillsPerformance", [])
    scorecards = scorecard_data.get("wfmScorecardStats", [])
    contacts = contacts_data.get("contacts", [])

    if not isinstance(agents, list):
        agents = []

    if not isinstance(agent_states, list):
        agent_states = []

    if not isinstance(skills_performance, list):
        skills_performance = []

    if not isinstance(scorecards, list):
        scorecards = []

    if not isinstance(contacts, list):
        contacts = []

    rollup: dict[int, dict[str, Any]] = {}

    def ensure_agent(agent_id: Any) -> dict[str, Any] | None:
        if not isinstance(agent_id, int):
            return None

        if agent_id not in rollup:
            rollup[agent_id] = {
                "agentId": agent_id,
                "name": f"Agent {agent_id}",
                "firstName": "",
                "lastName": "",
                "teamName": "",
                "state": "Unknown",
                "stateStartDate": None,

                # Performance
                "totalHandled": 0,
                "totalHandledSeconds": 0.0,
                "totalAcwSeconds": 0.0,
                "avgHandleSeconds": 0.0,
                "avgAcwSeconds": 0.0,

                # Scorecard
                "scorecardTotalCalls": 0,
                "scorecardActualCalls": 0,
                "scorecardIdleSeconds": 0.0,
                "scorecardAuxSeconds": 0.0,
                "scorecardTalkSeconds": 0.0,
                "scorecardHoldSeconds": 0.0,

                # Contacts
                "contacts": 0,
                "inboundContacts": 0,
                "outboundContacts": 0,
                "abandonedContacts": 0,
                "activeContacts": 0,
                "totalQueueSeconds": 0.0,
                "avgQueueSeconds": 0.0,
                "totalContactDurationSeconds": 0.0,
                "avgContactDurationSeconds": 0.0,
            }

        return rollup[agent_id]

    # Base agent list
    for agent in agents:
        if not isinstance(agent, dict):
            continue

        agent_id = agent.get("agentId")
        row = ensure_agent(agent_id)

        if row is None:
            continue

        first_name = str(agent.get("firstName") or "")
        last_name = str(agent.get("lastName") or "")
        full_name = f"{first_name} {last_name}".strip()

        row["firstName"] = first_name
        row["lastName"] = last_name

        if full_name:
            row["name"] = full_name

        if agent.get("teamName"):
            row["teamName"] = agent.get("teamName")

    # Current states
    for state in agent_states:
        if not isinstance(state, dict):
            continue

        agent_id = state.get("agentId")
        row = ensure_agent(agent_id)

        if row is None:
            continue

        first_name = str(state.get("firstName") or row.get("firstName") or "")
        last_name = str(state.get("lastName") or row.get("lastName") or "")
        full_name = f"{first_name} {last_name}".strip()

        if full_name:
            row["name"] = full_name
            row["firstName"] = first_name
            row["lastName"] = last_name

        row["state"] = (
            state.get("agentStateName")
            or state.get("stateName")
            or state.get("agentState")
            or row["state"]
        )

        row["stateStartDate"] = (
            state.get("startDate")
            or state.get("stateStartDate")
            or state.get("lastUpdateTime")
            or row["stateStartDate"]
        )

        if state.get("teamName"):
            row["teamName"] = state.get("teamName")

    # Agent performance daily
    for perf in skills_performance:
        if not isinstance(perf, dict):
            continue

        agent_id = perf.get("agentId")
        row = ensure_agent(agent_id)

        if row is None:
            continue

        first_name = str(perf.get("firstName") or row.get("firstName") or "")
        last_name = str(perf.get("lastName") or row.get("lastName") or "")
        full_name = f"{first_name} {last_name}".strip()

        if full_name:
            row["name"] = full_name
            row["firstName"] = first_name
            row["lastName"] = last_name

        total_handled = perf.get("totalHandled", 0)
        total_handled_time = perf.get("totalHandledTime", 0)
        total_acw_time = perf.get("totalACWTime", 0)

        if isinstance(total_handled, int | float):
            row["totalHandled"] += int(total_handled)

        if isinstance(total_handled_time, int | float):
            row["totalHandledSeconds"] += float(total_handled_time)

        if isinstance(total_acw_time, int | float):
            row["totalAcwSeconds"] += float(total_acw_time)

    # Scorecards
    for scorecard in scorecards:
        if not isinstance(scorecard, dict):
            continue

        agent_id = scorecard.get("agentId")
        row = ensure_agent(agent_id)

        if row is None:
            continue

        total_calls = scorecard.get("totalCalls", 0)
        actual_calls = scorecard.get("actualCalls", 0)
        idle_time = scorecard.get("idleTime", 0)
        aux_time = scorecard.get("auxTime", 0)
        talk_time = scorecard.get("talkTime", 0)
        hold_time = scorecard.get("holdTime", 0)

        if isinstance(total_calls, int | float):
            row["scorecardTotalCalls"] += int(total_calls)

        if isinstance(actual_calls, int | float):
            row["scorecardActualCalls"] += int(actual_calls)

        if isinstance(idle_time, int | float):
            row["scorecardIdleSeconds"] += float(idle_time)

        if isinstance(aux_time, int | float):
            row["scorecardAuxSeconds"] += float(aux_time)

        if isinstance(talk_time, int | float):
            row["scorecardTalkSeconds"] += float(talk_time)

        if isinstance(hold_time, int | float):
            row["scorecardHoldSeconds"] += float(hold_time)

    # Contact details
    for contact in contacts:
        if not isinstance(contact, dict):
            continue

        agent_id = contact.get("agentId")
        row = ensure_agent(agent_id)

        if row is None:
            continue

        first_name = str(contact.get("firstName") or row.get("firstName") or "")
        last_name = str(contact.get("lastName") or row.get("lastName") or "")
        full_name = f"{first_name} {last_name}".strip()

        if full_name:
            row["name"] = full_name
            row["firstName"] = first_name
            row["lastName"] = last_name

        if contact.get("teamName"):
            row["teamName"] = contact.get("teamName")

        row["contacts"] += 1

        if contact.get("isOutbound") is True:
            row["outboundContacts"] += 1
        else:
            row["inboundContacts"] += 1

        if contact.get("abandoned") is True:
            row["abandonedContacts"] += 1

        if contact.get("isActive") is True:
            row["activeContacts"] += 1

        in_queue_seconds = contact.get("inQueueSeconds", 0)
        duration_seconds = contact.get("totalDurationSeconds", 0)

        if isinstance(in_queue_seconds, int | float):
            row["totalQueueSeconds"] += float(in_queue_seconds)

        if isinstance(duration_seconds, int | float):
            row["totalContactDurationSeconds"] += float(duration_seconds)

    # Final calculations
    agent_rows = []

    for row in rollup.values():
        total_handled = row["totalHandled"]
        contacts_count = row["contacts"]

        if total_handled > 0:
            row["avgHandleSeconds"] = round(row["totalHandledSeconds"] / total_handled, 2)
            row["avgAcwSeconds"] = round(row["totalAcwSeconds"] / total_handled, 2)

        if contacts_count > 0:
            row["avgQueueSeconds"] = round(row["totalQueueSeconds"] / contacts_count, 2)
            row["avgContactDurationSeconds"] = round(
                row["totalContactDurationSeconds"] / contacts_count,
                2,
            )

        row["scorecardIdleSeconds"] = round(row["scorecardIdleSeconds"], 2)
        row["scorecardAuxSeconds"] = round(row["scorecardAuxSeconds"], 2)
        row["scorecardTalkSeconds"] = round(row["scorecardTalkSeconds"], 2)
        row["scorecardHoldSeconds"] = round(row["scorecardHoldSeconds"], 2)
        row["totalHandledSeconds"] = round(row["totalHandledSeconds"], 2)
        row["totalAcwSeconds"] = round(row["totalAcwSeconds"], 2)
        row["totalQueueSeconds"] = round(row["totalQueueSeconds"], 2)
        row["totalContactDurationSeconds"] = round(row["totalContactDurationSeconds"], 2)

        agent_rows.append(row)

    wanted_agent_ids = _agent_id_allowlist()

    if wanted_agent_ids:
        agent_rows = [
            agent
            for agent in agent_rows
            if isinstance(agent.get("agentId"), int)
            and agent["agentId"] in wanted_agent_ids
        ]

    agent_rows.sort(key=lambda item: str(item.get("name", "")).lower())

    response: dict[str, Any] = {
        "ok": True,
        "generatedAt": _iso_utc(datetime.now(timezone.utc)),
        "agentCount": len(agent_rows),
        "agentFilterEnabled": bool(wanted_agent_ids),
        "agentFilterIds": sorted(wanted_agent_ids),
        "agents": agent_rows,
        "sources": {
            job_name: {
                "ok": result.get("ok", False),
                "statusCode": result.get("status_code"),
            }
            for job_name, result in by_job.items()
        },
    }

    failed_sources = {
        job_name: result
        for job_name, result in by_job.items()
        if not result.get("ok", False)
    }

    if failed_sources:
        response["ok"] = False
        response["failedSources"] = failed_sources

    return response

@app.get("/summary/agents")
async def agents_summary(force_refresh: bool = False) -> dict[str, Any]:
    """
    Cached per-agent summary.

    Use:
      /summary/agents
      /summary/agents?force_refresh=true
    """
    if not force_refresh and summary_cache.agents_valid():
        cached = dict(summary_cache.agents_summary or {})
        cached["cached"] = True
        cached["cacheSecondsRemaining"] = max(
            0,
            int(summary_cache.agents_summary_expires_at - time.time()),
        )
        return cached

    async with summary_cache.lock:
        if not force_refresh and summary_cache.agents_valid():
            cached = dict(summary_cache.agents_summary or {})
            cached["cached"] = True
            cached["cacheSecondsRemaining"] = max(
                0,
                int(summary_cache.agents_summary_expires_at - time.time()),
            )
            return cached

        fresh = await _build_agents_summary()
        fresh["cached"] = False
        fresh["cacheSecondsRemaining"] = SUMMARY_CACHE_SECONDS

        summary_cache.agents_summary = fresh
        summary_cache.agents_summary_expires_at = time.time() + SUMMARY_CACHE_SECONDS

        return fresh

@app.get("/summary/agents/table")
async def agents_table_summary(force_refresh: bool = False) -> dict[str, Any]:
    """
    Homepage-friendly agent table.

    Returns rows shaped for customapi dynamic-list:
      agents[].name
      agents[].label

    Uses visual status indicators because Homepage customapi dynamic-list
    does not cleanly support per-row CSS classes from API data.
    """
    all_agents = await agents_summary(force_refresh=force_refresh)

    agents = all_agents.get("agents", [])
    if not isinstance(agents, list):
        agents = []

    def status_icon(state: str) -> str:
        normalized = state.strip().lower()

        if normalized in {"available", "ready"}:
            return "🟢"

        if normalized in {"loggedout", "logged out", "offline"}:
            return "🔴"

        if normalized in {"unavailable", "notready", "not ready"}:
            return "🟡"

        if normalized in {"incontact", "in contact", "working", "busy"}:
            return "🔵"

        return "⚪"

    def short_state(state: str) -> str:
        normalized = state.strip()

        replacements = {
            "LoggedOut": "Out",
            "Logged Out": "Out",
            "Unavailable": "Unavail",
            "Available": "Avail",
            "InContact": "Call",
            "In Contact": "Call",
        }

        return replacements.get(normalized, normalized or "Unknown")

    rows: list[dict[str, Any]] = []

    for agent in agents:
        if not isinstance(agent, dict):
            continue

        name = str(agent.get("name") or f"Agent {agent.get('agentId', '')}").strip()
        state = str(agent.get("state") or "Unknown").strip()

        total_handled = int(agent.get("totalHandled") or 0)
        contacts = int(agent.get("contacts") or 0)
        active_contacts = int(agent.get("activeContacts") or 0)
        avg_queue = round(float(agent.get("avgQueueSeconds") or 0))
        avg_handle = round(float(agent.get("avgHandleSeconds") or 0))

        icon = status_icon(state)
        state_short = short_state(state)

        # Compact row label for far-away readability.
        label_parts = [
            f"{icon} {state_short}",
            f"H:{total_handled}",
            f"C:{contacts}",
        ]

        if active_contacts:
            label_parts.append(f"Active:{active_contacts}")

        if avg_queue:
            label_parts.append(f"Q:{avg_queue}s")

        if avg_handle:
            label_parts.append(f"AHT:{avg_handle}s")

        rows.append(
            {
                "agentId": agent.get("agentId"),
                "name": name,
                "label": "  │  ".join(label_parts),
                "state": state,
                "stateIcon": icon,
                "stateShort": state_short,
                "totalHandled": total_handled,
                "contacts": contacts,
                "activeContacts": active_contacts,
                "avgQueueSeconds": avg_queue,
                "avgHandleSeconds": avg_handle,
            }
        )

    rows.sort(key=lambda item: item["name"].lower())

    return {
        "ok": True,
        "cached": all_agents.get("cached", False),
        "cacheSecondsRemaining": all_agents.get("cacheSecondsRemaining", 0),
        "generatedAt": all_agents.get("generatedAt"),
        "agentCount": len(rows),
        "agents": rows,
    }

@app.get("/summary/agents/scorecard-table")
async def agents_scorecard_table_summary(force_refresh: bool = False) -> dict[str, Any]:
    """
    Homepage-friendly scorecard table.

    Returns:
      agents[].name
      agents[].label
    """
    all_agents = await agents_summary(force_refresh=force_refresh)

    agents = all_agents.get("agents", [])
    if not isinstance(agents, list):
        agents = []

    rows: list[dict[str, Any]] = []

    for agent in agents:
        if not isinstance(agent, dict):
            continue

        name = str(agent.get("name") or f"Agent {agent.get('agentId', '')}").strip()

        total_calls = int(agent.get("scorecardTotalCalls") or 0)
        actual_calls = int(agent.get("scorecardActualCalls") or 0)
        idle_seconds = round(float(agent.get("scorecardIdleSeconds") or 0))
        aux_seconds = round(float(agent.get("scorecardAuxSeconds") or 0))
        talk_seconds = round(float(agent.get("scorecardTalkSeconds") or 0))

        # Convert long seconds into rough hours for far-away readability.
        idle_hours = round(idle_seconds / 3600, 1)
        aux_hours = round(aux_seconds / 3600, 1)
        talk_minutes = round(talk_seconds / 60, 1)

        label_parts = [
            f"Calls:{total_calls}",
            f"Actual:{actual_calls}",
            f"Idle:{idle_hours}h",
            f"Aux:{aux_hours}h",
            f"Talk:{talk_minutes}m",
        ]

        rows.append(
            {
                "agentId": agent.get("agentId"),
                "name": name,
                "label": "  │  ".join(label_parts),
                "scorecardTotalCalls": total_calls,
                "scorecardActualCalls": actual_calls,
                "scorecardIdleSeconds": idle_seconds,
                "scorecardAuxSeconds": aux_seconds,
                "scorecardTalkSeconds": talk_seconds,
                "scorecardIdleHours": idle_hours,
                "scorecardAuxHours": aux_hours,
                "scorecardTalkMinutes": talk_minutes,
            }
        )

    rows.sort(key=lambda item: item["name"].lower())

    return {
        "ok": True,
        "cached": all_agents.get("cached", False),
        "cacheSecondsRemaining": all_agents.get("cacheSecondsRemaining", 0),
        "generatedAt": all_agents.get("generatedAt"),
        "agentCount": len(rows),
        "agents": rows,
    }


@app.get("/summary/agents/{agent_id}")
async def agent_summary_by_id(agent_id: int, force_refresh: bool = False) -> dict[str, Any]:
    """
    Return one agent summary by agent ID.

    This uses the cached /summary/agents data so Homepage can safely create
    multiple per-agent widgets without hammering CXone like a confused woodpecker.
    """
    all_agents = await agents_summary(force_refresh=force_refresh)

    agents = all_agents.get("agents", [])
    if not isinstance(agents, list):
        agents = []

    for agent in agents:
        if not isinstance(agent, dict):
            continue

        if agent.get("agentId") == agent_id:
            return {
                "ok": True,
                "cached": all_agents.get("cached", False),
                "cacheSecondsRemaining": all_agents.get("cacheSecondsRemaining", 0),
                "generatedAt": all_agents.get("generatedAt"),
                "agentId": agent.get("agentId"),
                "name": agent.get("name"),
                "firstName": agent.get("firstName"),
                "lastName": agent.get("lastName"),
                "teamName": agent.get("teamName"),
                "state": agent.get("state"),
                "stateStartDate": agent.get("stateStartDate"),

                "totalHandled": agent.get("totalHandled", 0),
                "totalHandledSeconds": agent.get("totalHandledSeconds", 0.0),
                "totalAcwSeconds": agent.get("totalAcwSeconds", 0.0),
                "avgHandleSeconds": agent.get("avgHandleSeconds", 0.0),
                "avgAcwSeconds": agent.get("avgAcwSeconds", 0.0),

                "scorecardTotalCalls": agent.get("scorecardTotalCalls", 0),
                "scorecardActualCalls": agent.get("scorecardActualCalls", 0),
                "scorecardIdleSeconds": agent.get("scorecardIdleSeconds", 0.0),
                "scorecardAuxSeconds": agent.get("scorecardAuxSeconds", 0.0),
                "scorecardTalkSeconds": agent.get("scorecardTalkSeconds", 0.0),
                "scorecardHoldSeconds": agent.get("scorecardHoldSeconds", 0.0),

                "contacts": agent.get("contacts", 0),
                "inboundContacts": agent.get("inboundContacts", 0),
                "outboundContacts": agent.get("outboundContacts", 0),
                "abandonedContacts": agent.get("abandonedContacts", 0),
                "activeContacts": agent.get("activeContacts", 0),
                "avgQueueSeconds": agent.get("avgQueueSeconds", 0.0),
                "avgContactDurationSeconds": agent.get("avgContactDurationSeconds", 0.0),
            }

    raise HTTPException(
        status_code=404,
        detail=f"No agent found with agentId {agent_id}",
    )

@app.post("/summary/cache/clear")
async def clear_summary_cache() -> dict[str, Any]:
    summary_cache.agents_summary = None
    summary_cache.agents_summary_expires_at = 0.0

    return {
        "ok": True,
        "message": "Summary cache cleared",
    }

@app.api_route("/proxy/{path:path}", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_path(path: str, request: Request) -> Response:
    return await _forward(request, path)


@app.api_route("/proxy", methods=["GET", "POST", "PUT", "PATCH", "DELETE"])
async def proxy_query(
    request: Request,
    path: str = Query(..., description="Relative CXone API path"),
) -> Response:
    return await _forward(
        request,
        path,
        params=_copy_query_params(request, drop={"path"}),
    )


@app.exception_handler(HTTPException)
async def http_exception_handler(_: Request, exc: HTTPException) -> JSONResponse:
    return JSONResponse(
        status_code=exc.status_code,
        content={
            "ok": False,
            "error": exc.detail,
        },
    )
