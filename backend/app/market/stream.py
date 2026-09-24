"""SSE streaming endpoint for live price updates."""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import AsyncGenerator

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse

from .cache import PriceCache

logger = logging.getLogger(__name__)

SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",  # Disable nginx / proxy buffering
}


def create_stream_router(price_cache: PriceCache, interval: float = 0.5) -> APIRouter:
    """Create the SSE streaming router with a reference to the price cache.

    This factory pattern lets us inject the PriceCache without globals.
    A fresh APIRouter is created per call so the function is safe to call
    more than once (e.g. from multiple tests) without double-registering
    the route.
    """
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
        """SSE endpoint for live price updates.

        Streams all tracked ticker prices every ~500ms. The client connects
        with EventSource and receives events in the format:

            data: {"AAPL": {"ticker": "AAPL", "price": 190.50, ...}, ...}

        Includes a retry directive so the browser auto-reconnects on
        disconnection (EventSource built-in behavior).
        """
        return StreamingResponse(
            generate_price_events(price_cache, request, interval),
            media_type="text/event-stream",
            headers=SSE_HEADERS,
        )

    return router


async def generate_price_events(
    price_cache: PriceCache,
    request: Request,
    interval: float = 0.5,
    heartbeat_every: float = 15.0,
) -> AsyncGenerator[str, None]:
    """Async generator that yields SSE-formatted price events.

    Sends a snapshot of all prices whenever the cache version changes, and an
    SSE comment (`: keepalive`) every `heartbeat_every` seconds of otherwise
    idle connection, so proxies don't close it during off-hours under Massive.
    Stops when the client disconnects (detected via request.is_disconnected()).
    """
    # Tell the client to retry after 1 second if the connection drops
    yield "retry: 1000\n\n"

    last_version = -1
    last_sent = time.monotonic()
    client_ip = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client_ip)

    try:
        while not await request.is_disconnected():
            current_version = price_cache.version
            if current_version != last_version:
                last_version = current_version
                # An empty {} is sent too: it clears the UI when the last
                # tracked ticker is removed.
                data = {ticker: update.to_dict() for ticker, update in price_cache.get_all().items()}
                yield f"data: {json.dumps(data)}\n\n"
                last_sent = time.monotonic()
            elif time.monotonic() - last_sent >= heartbeat_every:
                yield ": keepalive\n\n"
                last_sent = time.monotonic()

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client_ip)
