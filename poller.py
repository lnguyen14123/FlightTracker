"""FlightPulse-K8s, Phase 1: OpenSky poller + raw archive (local, $0).

Every POLL_INTERVAL seconds it:
  1. gets an OAuth2 bearer token (cached, refreshed before expiry),
  2. calls /states/all for the New York TRACON box with extended=1,
  3. archives the untouched response as gzip JSON (your replay dataset),
  4. cleans each state vector and hands the clean states to a sink.

Phase 1's sink appends NDJSON files. In Phase 2 swap `write_ndjson` for a
Redis Streams XADD (with maxlen ~ 50000) -- nothing else needs to change.

Run:   python poller.py            # FastAPI + background poller on :8000
       python poller.py --once     # one poll, print a summary, exit
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import gzip
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

import httpx
from fastapi import FastAPI

log = logging.getLogger("flightpulse.poller")

# New York TRACON box: JFK, LGA, EWR, Teterboro
BBOX = {"lamin": 40.2, "lamax": 41.3, "lomin": -74.8, "lomax": -73.2}

API_URL = "https://opensky-network.org/api/states/all"
TOKEN_URL = (
    "https://auth.opensky-network.org/auth/realms/opensky-network"
    "/protocol/openid-connect/token"
)

# OpenSky aircraft category codes (index 17, only present with extended=1).
CATEGORY_NAMES = {
    0: "No information",
    1: "No ADS-B category information",
    2: "Light",
    3: "Small",
    4: "Large",
    5: "High vortex large (B757)",
    6: "Heavy",
    7: "High performance",
    8: "Rotorcraft",
    9: "Glider",
    10: "Lighter-than-air",
    11: "Parachutist",
    12: "Ultralight",
    14: "UAV",
    15: "Space vehicle",
    16: "Emergency vehicle",
    17: "Service vehicle",
    18: "Point obstacle",
    19: "Cluster obstacle",
    20: "Line obstacle",
}

POSITION_SOURCES = {0: "ADS-B", 1: "ASTERIX", 2: "MLAT", 3: "FLARM"}


# --------------------------------------------------------------------------
# Config
# --------------------------------------------------------------------------
@dataclass
class Config:
    client_id: str | None = None
    client_secret: str | None = None
    poll_interval: float = 30.0
    archive_dir: Path = Path("./archive")
    clean_dir: Path = Path("./clean")
    api_url: str = API_URL
    token_url: str = TOKEN_URL

    @classmethod
    def from_env(cls) -> "Config":
        cid = os.getenv("OPENSKY_CLIENT_ID")
        secret = os.getenv("OPENSKY_CLIENT_SECRET")
        cred_file = os.getenv("OPENSKY_CREDENTIALS_FILE")
        if cred_file and not (cid and secret):
            # OpenSky's downloadable credentials.json (key names: clientId/clientSecret)
            data = json.loads(Path(cred_file).read_text())
            cid = data.get("clientId") or data.get("client_id")
            secret = data.get("clientSecret") or data.get("client_secret")
        return cls(
            client_id=cid,
            client_secret=secret,
            poll_interval=float(os.getenv("POLL_INTERVAL", "30")),
            archive_dir=Path(os.getenv("ARCHIVE_DIR", "./archive")),
            clean_dir=Path(os.getenv("CLEAN_DIR", "./clean")),
        )


# --------------------------------------------------------------------------
# Auth: OAuth2 client credentials, short-lived tokens
# --------------------------------------------------------------------------
class TokenManager:
    def __init__(self, client_id: str | None, client_secret: str | None, token_url: str):
        self.client_id = client_id
        self.client_secret = client_secret
        self.token_url = token_url
        self._token: str | None = None
        self._expires_at = 0.0

    @property
    def anonymous(self) -> bool:
        return not (self.client_id and self.client_secret)

    async def get(self, client: httpx.AsyncClient, force: bool = False) -> str | None:
        if self.anonymous:
            return None
        # refresh 60 s early so a token never expires mid-request
        if force or not self._token or time.monotonic() >= self._expires_at - 60:
            r = await client.post(
                self.token_url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self.client_id,
                    "client_secret": self.client_secret,
                },
            )
            r.raise_for_status()
            body = r.json()
            self._token = body["access_token"]
            self._expires_at = time.monotonic() + float(body.get("expires_in", 1800))
            log.info("refreshed OpenSky token (expires in %ss)", body.get("expires_in"))
        return self._token


class RateLimited(Exception):
    def __init__(self, retry_after: float):
        super().__init__(f"rate limited, retry after {retry_after}s")
        self.retry_after = retry_after


# --------------------------------------------------------------------------
# Cleaning
# --------------------------------------------------------------------------
def clean_state(s: Any, poll_time: int) -> dict | None:
    """Turn one OpenSky state-vector array into a clean dict, or None to skip.

    Fixes from the design review:
      * null longitude/latitude -> skip the state (never coerce to 0.0)
      * null callsign -> "" (never the string "None")
      * null time_position -> fall back to last_contact (it is a primary-key part)
      * category read from index 17 (only present when extended=1)
      * vertical_rate, squawk, spi, position_source are kept
    """
    if not isinstance(s, list) or len(s) < 17:
        return None
    icao24 = s[0]
    lon, lat = s[5], s[6]
    if not icao24 or lon is None or lat is None:
        return None

    time_position = s[3]
    from_last_contact = False
    if time_position is None:
        time_position = s[4] if s[4] is not None else poll_time
        from_last_contact = True

    category = s[17] if len(s) > 17 else None
    pos_src = s[16]

    return {
        "icao24": str(icao24).lower(),
        "callsign": (s[1] or "").strip(),
        "origin_country": s[2] or "",
        "time_position": int(time_position),
        "time_position_from_last_contact": from_last_contact,
        "last_contact": int(s[4]) if s[4] is not None else int(time_position),
        "longitude": float(lon),
        "latitude": float(lat),
        "baro_altitude": s[7],
        "geo_altitude": s[13],
        "on_ground": bool(s[8]),
        "velocity": s[9],
        "true_track": s[10],
        "vertical_rate": s[11],
        "squawk": s[14],
        "spi": bool(s[15]),
        "position_source": pos_src,
        "position_source_name": POSITION_SOURCES.get(pos_src),
        "category": category,
        "category_name": CATEGORY_NAMES.get(category, "Unknown") if category is not None else None,
        "poll_time": poll_time,
    }


def clean_response(data: dict) -> tuple[list[dict], int]:
    """Return (clean_states, dropped_count) for a raw /states/all response."""
    raw_states = data.get("states") or []  # OpenSky returns null when empty
    poll_time = data.get("time") or int(time.time())
    clean = [c for s in raw_states if (c := clean_state(s, poll_time)) is not None]
    return clean, len(raw_states) - len(clean)


# --------------------------------------------------------------------------
# Archive + sink
# --------------------------------------------------------------------------
def archive_raw(archive_dir: Path, data: dict, fetched_at: float, params: dict) -> Path:
    """Write the untouched response (plus fetch metadata) as gzip JSON.

    Layout: archive/YYYY-MM-DD/HHMMSS_<epoch>.json.gz. `fetched_at_epoch` lets the
    Phase 3 replay script reproduce the original gaps and speed them up 40x.
    """
    ts = datetime.fromtimestamp(fetched_at, tz=timezone.utc)
    day_dir = Path(archive_dir) / ts.strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    path = day_dir / f"{ts.strftime('%H%M%S')}_{int(fetched_at)}.json.gz"
    wrapper = {
        "fetched_at": ts.isoformat(),
        "fetched_at_epoch": fetched_at,
        "request": {"url": API_URL, "params": params},
        "response": data,
    }
    tmp = path.with_suffix(".tmp")
    with gzip.open(tmp, "wt", encoding="utf-8") as f:
        json.dump(wrapper, f, separators=(",", ":"))
    tmp.replace(path)  # atomic: a crash never leaves a half-written archive file
    return path


def write_ndjson(clean_dir: Path, states: list[dict]) -> None:
    """Phase 1 sink. Replaced by a Redis XADD in Phase 2."""
    if not states:
        return
    Path(clean_dir).mkdir(parents=True, exist_ok=True)
    day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    with open(Path(clean_dir) / f"states-{day}.ndjson", "a", encoding="utf-8") as f:
        for st in states:
            f.write(json.dumps(st, separators=(",", ":")) + "\n")


# --------------------------------------------------------------------------
# Polling
# --------------------------------------------------------------------------
@dataclass
class Stats:
    polls_ok: int = 0
    polls_failed: int = 0
    last_poll_epoch: float | None = None
    last_states_total: int = 0
    last_states_kept: int = 0
    last_states_dropped: int = 0
    last_category_counts: dict = field(default_factory=dict)
    credits_remaining: str | None = None
    last_error: str | None = None


async def fetch_states(
    client: httpx.AsyncClient, tm: TokenManager, cfg: Config
) -> tuple[dict, httpx.Headers, dict]:
    params = {**BBOX, "extended": 1}
    for attempt in (1, 2):
        token = await tm.get(client, force=(attempt == 2))
        headers = {"Authorization": f"Bearer {token}"} if token else {}
        r = await client.get(cfg.api_url, params=params, headers=headers)
        if r.status_code == 401 and token and attempt == 1:
            log.warning("401 from OpenSky; forcing token refresh and retrying once")
            continue
        if r.status_code == 429:
            retry = r.headers.get("X-Rate-Limit-Retry-After-Seconds", "60")
            raise RateLimited(float(retry))
        r.raise_for_status()
        return r.json(), r.headers, params
    raise RuntimeError("unreachable")


async def poll_cycle(
    client: httpx.AsyncClient,
    tm: TokenManager,
    cfg: Config,
    stats: Stats,
    sink: Callable[[Path, list[dict]], None] = write_ndjson,
) -> list[dict]:
    data, headers, params = await fetch_states(client, tm, cfg)
    fetched_at = time.time()

    # Archive FIRST so a bug in cleaning can never cost you replay data.
    archive_raw(cfg.archive_dir, data, fetched_at, params)

    clean, dropped = clean_response(data)
    sink(cfg.clean_dir, clean)

    counts: dict[str, int] = {}
    for st in clean:
        key = st["category_name"] or "Not provided"
        counts[key] = counts.get(key, 0) + 1

    stats.polls_ok += 1
    stats.last_poll_epoch = fetched_at
    stats.last_states_total = len(clean) + dropped
    stats.last_states_kept = len(clean)
    stats.last_states_dropped = dropped
    stats.last_category_counts = counts
    stats.credits_remaining = headers.get("X-Rate-Limit-Remaining", stats.credits_remaining)
    stats.last_error = None
    log.info("poll ok: kept=%d dropped=%d credits_remaining=%s",
             len(clean), dropped, stats.credits_remaining)
    return clean


async def run_poller(cfg: Config, stats: Stats, stop: asyncio.Event) -> None:
    tm = TokenManager(cfg.client_id, cfg.client_secret, cfg.token_url)
    if tm.anonymous:
        log.warning(
            "No OpenSky credentials set: anonymous access has a much smaller daily "
            "credit allowance and will not sustain a 30 s poll all day."
        )
    failures = 0
    next_tick = time.monotonic()
    async with httpx.AsyncClient(timeout=20.0) as client:
        while not stop.is_set():
            delay_override: float | None = None
            try:
                await poll_cycle(client, tm, cfg, stats)
                failures = 0
            except RateLimited as e:
                stats.polls_failed += 1
                stats.last_error = str(e)
                log.warning("%s", e)
                delay_override = e.retry_after
            except Exception as e:  # network blip, 5xx, bad JSON: keep running
                failures += 1
                stats.polls_failed += 1
                stats.last_error = f"{type(e).__name__}: {e}"
                log.exception("poll failed (%d in a row)", failures)
                delay_override = min(cfg.poll_interval * 2 ** min(failures, 4), 300)

            if delay_override is not None:
                delay = delay_override
                next_tick = time.monotonic() + delay
            else:
                next_tick += cfg.poll_interval  # fixed cadence, no drift
                delay = next_tick - time.monotonic()
                if delay < 0:  # fell behind; don't burst-poll to catch up
                    next_tick = time.monotonic()
                    delay = 0
            with contextlib.suppress(asyncio.TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=delay)


# --------------------------------------------------------------------------
# FastAPI wrapper (health + status for later Kubernetes probes)
# --------------------------------------------------------------------------
cfg = Config.from_env()
stats = Stats()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stop = asyncio.Event()
    task = asyncio.create_task(run_poller(cfg, stats, stop))
    try:
        yield
    finally:
        stop.set()
        await task


app = FastAPI(title="FlightPulse poller", lifespan=lifespan)


@app.get("/health")
def health() -> dict:
    age = None if stats.last_poll_epoch is None else round(time.time() - stats.last_poll_epoch, 1)
    healthy = age is not None and age < cfg.poll_interval * 4
    return {"healthy": healthy, "seconds_since_last_poll": age}


@app.get("/status")
def status() -> dict:
    return asdict(stats)


# --------------------------------------------------------------------------
async def _once() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    tm = TokenManager(cfg.client_id, cfg.client_secret, cfg.token_url)
    async with httpx.AsyncClient(timeout=20.0) as client:
        await poll_cycle(client, tm, cfg, stats)
    print(json.dumps(asdict(stats), indent=2))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--once", action="store_true", help="poll once and exit")
    args = ap.parse_args()
    if args.once:
        asyncio.run(_once())
    else:
        import uvicorn

        uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8000")))
