"""Phase 1 tests: run with `pytest -q`. No network needed."""
import asyncio
import gzip
import json
from pathlib import Path

import httpx
import pytest

import poller


def state(over=None):
    """A 18-element extended state vector with sane defaults."""
    base = {
        0: "a1b2c3", 1: "DAL123  ", 2: "United States", 3: 1000, 4: 1001,
        5: -73.9, 6: 40.7, 7: 3000.0, 8: False, 9: 120.0, 10: 270.0,
        11: -5.0, 12: None, 13: 3100.0, 14: "1200", 15: False, 16: 0, 17: 6,
    }
    base.update(over or {})
    return [base[i] for i in range(18)]


# ---- cleaning ----------------------------------------------------------
def test_good_state_is_mapped():
    c = poller.clean_state(state(), 1002)
    assert c["icao24"] == "a1b2c3"
    assert c["callsign"] == "DAL123"
    assert c["category"] == 6 and c["category_name"] == "Heavy"
    assert c["vertical_rate"] == -5.0
    assert c["squawk"] == "1200"
    assert c["position_source_name"] == "ADS-B"
    assert c["time_position"] == 1000 and not c["time_position_from_last_contact"]


@pytest.mark.parametrize("over", [{5: None}, {6: None}, {5: None, 6: None}])
def test_null_lon_or_lat_is_skipped(over):
    assert poller.clean_state(state(over), 1002) is None


def test_null_callsign_is_empty_string_not_None():
    assert poller.clean_state(state({1: None}), 1002)["callsign"] == ""


def test_null_time_position_falls_back_to_last_contact():
    c = poller.clean_state(state({3: None, 4: 1234}), 1002)
    assert c["time_position"] == 1234
    assert c["time_position_from_last_contact"] is True


def test_non_extended_response_has_no_category():
    c = poller.clean_state(state()[:17], 1002)
    assert c["category"] is None and c["category_name"] is None


def test_unknown_category_code_is_labelled():
    assert poller.clean_state(state({17: 99}), 1)["category_name"] == "Unknown"


def test_malformed_input_is_skipped():
    assert poller.clean_state(None, 1) is None
    assert poller.clean_state([1, 2, 3], 1) is None


def test_clean_response_handles_null_states_and_counts_drops():
    assert poller.clean_response({"time": 1, "states": None}) == ([], 0)
    data = {"time": 5, "states": [state(), state({5: None}), state({0: "ffffff"})]}
    clean, dropped = poller.clean_response(data)
    assert len(clean) == 2 and dropped == 1


# ---- archive -----------------------------------------------------------
def test_archive_roundtrip(tmp_path):
    data = {"time": 7, "states": [state()]}
    p = poller.archive_raw(tmp_path, data, 1_700_000_000.5, {"extended": 1})
    assert p.suffixes == [".json", ".gz"]
    with gzip.open(p, "rt") as f:
        w = json.load(f)
    assert w["response"] == data
    assert w["fetched_at_epoch"] == 1_700_000_000.5
    assert not list(tmp_path.rglob("*.tmp"))


# ---- polling with a mocked OpenSky ------------------------------------
def make_cfg(tmp_path, with_creds=True):
    return poller.Config(
        client_id="id" if with_creds else None,
        client_secret="secret" if with_creds else None,
        archive_dir=tmp_path / "archive",
        clean_dir=tmp_path / "clean",
    )


def run_cycle(cfg, handler):
    async def go():
        stats = poller.Stats()
        tm = poller.TokenManager(cfg.client_id, cfg.client_secret, cfg.token_url)
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            clean = await poller.poll_cycle(client, tm, cfg, stats)
        return clean, stats
    return asyncio.run(go())


def test_poll_cycle_requests_extended_and_bbox_and_writes_everything(tmp_path):
    seen = {}

    def handler(req: httpx.Request):
        if req.url.host == "auth.opensky-network.org":
            return httpx.Response(200, json={"access_token": "tok", "expires_in": 1800})
        seen["params"] = dict(req.url.params)
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(
            200,
            json={"time": 9, "states": [state(), state({5: None})]},
            headers={"X-Rate-Limit-Remaining": "3999"},
        )

    cfg = make_cfg(tmp_path)
    clean, stats = run_cycle(cfg, handler)
    assert seen["params"]["extended"] == "1"
    assert seen["params"]["lamin"] == "40.2" and seen["params"]["lomax"] == "-73.2"
    assert seen["auth"] == "Bearer tok"
    assert len(clean) == 1 and stats.last_states_dropped == 1
    assert stats.credits_remaining == "3999"
    assert list((tmp_path / "archive").rglob("*.json.gz"))
    assert list((tmp_path / "clean").glob("states-*.ndjson"))
    assert stats.last_category_counts == {"Heavy": 1}


def test_401_triggers_one_token_refresh_then_succeeds(tmp_path):
    tokens = iter(["old", "new"])
    calls = {"api": 0}

    def handler(req):
        if req.url.host == "auth.opensky-network.org":
            return httpx.Response(200, json={"access_token": next(tokens), "expires_in": 1800})
        calls["api"] += 1
        if req.headers["authorization"] == "Bearer old":
            return httpx.Response(401)
        return httpx.Response(200, json={"time": 1, "states": []})

    clean, _ = run_cycle(make_cfg(tmp_path), handler)
    assert clean == [] and calls["api"] == 2


def test_429_raises_rate_limited_with_retry_after(tmp_path):
    def handler(req):
        if req.url.host == "auth.opensky-network.org":
            return httpx.Response(200, json={"access_token": "t", "expires_in": 1800})
        return httpx.Response(429, headers={"X-Rate-Limit-Retry-After-Seconds": "42"})

    with pytest.raises(poller.RateLimited) as e:
        run_cycle(make_cfg(tmp_path), handler)
    assert e.value.retry_after == 42


def test_anonymous_mode_sends_no_auth_header(tmp_path):
    seen = {}

    def handler(req):
        seen["auth"] = req.headers.get("authorization")
        return httpx.Response(200, json={"time": 1, "states": None})

    run_cycle(make_cfg(tmp_path, with_creds=False), handler)
    assert seen["auth"] is None
