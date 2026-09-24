"""Tests for the SSE streaming endpoint.

Note: FastAPI's TestClient and httpx's ASGITransport both block until the
whole ASGI response completes before returning anything to the caller, so
neither can incrementally read an endpoint that streams forever (like this
one, which only stops on client disconnect). Instead we drive the
`generate_price_events` async generator directly with a fake Request that
controls when `is_disconnected()` flips true — this covers the actual
streaming/version-change/disconnect/heartbeat logic without needing a live
socket.
"""

import json

from app.market.cache import PriceCache
from app.market.stream import create_stream_router, generate_price_events


class _FakeRequest:
    """Stand-in for fastapi.Request: disconnects after N is_disconnected() calls."""

    client = None

    def __init__(self, disconnect_after: int) -> None:
        self._calls = 0
        self._disconnect_after = disconnect_after

    async def is_disconnected(self) -> bool:
        self._calls += 1
        return self._calls > self._disconnect_after


async def _run(cache: PriceCache, disconnect_after: int, **kwargs) -> list[str]:
    request = _FakeRequest(disconnect_after)
    return [
        event async for event in generate_price_events(cache, request, interval=0.01, **kwargs)
    ]


class TestGeneratePriceEvents:
    """Unit tests for the SSE event generator."""

    async def test_first_event_is_retry_directive(self):
        events = await _run(PriceCache(), disconnect_after=0)
        assert events[0] == "retry: 1000\n\n"

    async def test_stops_once_client_disconnects(self):
        """No data is read from the cache once is_disconnected() is True."""
        cache = PriceCache()
        cache.update("AAPL", 100.0)
        events = await _run(cache, disconnect_after=0)
        assert events == ["retry: 1000\n\n"]

    async def test_emits_seeded_prices(self):
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        cache.update("GOOGL", 175.00)

        events = await _run(cache, disconnect_after=1)

        data_events = [e for e in events if e.startswith("data: ")]
        assert len(data_events) == 1
        payload = json.loads(data_events[0][len("data: "):].strip())
        assert payload["AAPL"]["price"] == 190.50
        assert payload["GOOGL"]["price"] == 175.00
        assert payload["AAPL"]["direction"] == "flat"

    async def test_empty_cache_sends_empty_data_event(self):
        """An empty {} still goes out — it's what clears the UI when the
        last tracked ticker is removed."""
        events = await _run(PriceCache(), disconnect_after=1)
        data_events = [e for e in events if e.startswith("data: ")]
        assert data_events == ["data: {}\n\n"]

    async def test_no_duplicate_event_while_version_unchanged(self):
        """Repeated iterations without a cache write must not re-emit data."""
        cache = PriceCache()
        cache.update("AAPL", 100.0)

        events = await _run(cache, disconnect_after=3)

        data_events = [e for e in events if e.startswith("data: ")]
        assert len(data_events) == 1

    async def test_removal_triggers_new_event_via_version_bump(self):
        cache = PriceCache()
        cache.update("AAPL", 100.0)
        cache.remove("AAPL")

        events = await _run(cache, disconnect_after=1)

        data_events = [e for e in events if e.startswith("data: ")]
        assert data_events == ["data: {}\n\n"]

    async def test_heartbeat_sent_when_idle_and_unchanged(self):
        cache = PriceCache()
        cache.update("AAPL", 100.0)

        events = await _run(cache, disconnect_after=3, heartbeat_every=0)

        assert ": keepalive\n\n" in events

    async def test_no_heartbeat_before_interval_elapses(self):
        cache = PriceCache()
        cache.update("AAPL", 100.0)

        events = await _run(cache, disconnect_after=3, heartbeat_every=999)

        assert ": keepalive\n\n" not in events


class TestCreateStreamRouter:
    """Tests for the router factory (regression coverage for a module-level
    router that could double-register routes if the factory ran twice)."""

    def test_registers_prices_route(self):
        router = create_stream_router(PriceCache())
        paths = [route.path for route in router.routes]
        assert "/api/stream/prices" in paths

    def test_callable_multiple_times_without_duplicate_routes(self):
        router_a = create_stream_router(PriceCache())
        router_b = create_stream_router(PriceCache())

        assert router_a is not router_b
        assert len(router_a.routes) == 1
        assert len(router_b.routes) == 1
