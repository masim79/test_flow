"""Massive (Polygon.io) API client for real market data."""

from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    """Fields pulled out of a Massive TickerSnapshot that we actually use."""

    ticker: str
    price: float
    timestamp: float | None  # Unix seconds; None -> cache uses now()
    reference_price: float | None  # previous close


def parse_snapshot(snap) -> ParsedQuote | None:
    """Pull (ticker, price, ts, prev close) from a massive TickerSnapshot.

    Returns None if the snapshot has no usable price. Never raises.

    `LastTrade` has no `.timestamp` attribute (it's `.sip_timestamp`, in
    nanoseconds) — parsing that field directly against the real SDK models
    is the whole point of this function; see planning/MARKET_DATA_DESIGN.md §9.3.
    """
    ticker = getattr(snap, "ticker", None)
    if not ticker:
        return None

    last_trade = getattr(snap, "last_trade", None)
    minute = getattr(snap, "min", None)
    price = getattr(last_trade, "price", None)
    if price is None:
        price = getattr(minute, "close", None)
    if not isinstance(price, (int, float)) or price <= 0:
        return None

    ts_ns = getattr(last_trade, "sip_timestamp", None)
    if ts_ns is None:
        ts_ns = getattr(snap, "updated", None)
    timestamp = ts_ns / 1e9 if isinstance(ts_ns, (int, float)) and ts_ns > 0 else None

    prev_day = getattr(snap, "prev_day", None)
    ref = getattr(prev_day, "close", None)
    reference_price = ref if isinstance(ref, (int, float)) and ref > 0 else None

    return ParsedQuote(ticker.upper(), float(price), timestamp, reference_price)


class MassiveDataSource(MarketDataSource):
    """MarketDataSource backed by the Massive (Polygon.io) REST API.

    Polls GET /v2/snapshot/locale/us/markets/stocks/tickers for all watched
    tickers in a single API call, then writes results to the PriceCache.

    Rate limits:
      - Free tier: 5 req/min → poll every 15s (default)
      - Paid tiers: higher limits → poll every 2-5s
    """

    MAX_BACKOFF_MULTIPLIER = 8  # 15s -> up to 120s while failing

    def __init__(
        self,
        api_key: str,
        price_cache: PriceCache,
        poll_interval: float = 15.0,
        min_poll_gap: float | None = None,
    ) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        # Never poll more often than this, even on an early wake from add_ticker.
        self._min_gap = poll_interval * 0.8 if min_poll_gap is None else min_poll_gap
        self._tickers: list[str] = []
        self._task: asyncio.Task | None = None
        self._client: RESTClient | None = None
        self._wake = asyncio.Event()  # set by add_ticker to poll sooner
        self._failures = 0
        self._last_poll = 0.0

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))

        # Do an immediate first poll so the cache has data right away
        await self._poll_once()

        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info(
            "Massive poller started: %d tickers, %.1fs interval",
            len(self._tickers),
            self._interval,
        )

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None
        logger.info("Massive poller stopped")

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            self._wake.set()  # poll early, rate-limit permitting
            logger.info("Massive: added ticker %s (will appear on next poll)", ticker)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)
        logger.info("Massive: removed ticker %s", ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- Internal ---

    async def _poll_loop(self) -> None:
        """Poll on interval, waking early (rate-limit permitting) on add_ticker."""
        while True:
            delay = self._interval * min(2**self._failures, self.MAX_BACKOFF_MULTIPLIER)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except TimeoutError:
                pass
            self._wake.clear()

            gap = self._min_gap - (time.monotonic() - self._last_poll)
            if gap > 0:
                await asyncio.sleep(gap)
            await self._poll_once()

    async def _poll_once(self) -> None:
        """Execute one poll cycle: fetch snapshots, update cache."""
        if not self._tickers or not self._client:
            return

        requested = list(self._tickers)
        self._last_poll = time.monotonic()
        try:
            # The Massive RESTClient is synchronous — run in a thread to
            # avoid blocking the event loop.
            snapshots = await asyncio.to_thread(self._fetch_snapshots, requested)
        except Exception as e:
            # Common failures: 401 (bad key), 429 (rate limit), network errors.
            self._failures += 1
            logger.error("Massive poll failed (%d in a row): %s", self._failures, e)
            return
        self._failures = 0

        # Only write tickers that are STILL tracked. A remove_ticker() that ran
        # while we were awaiting the thread must not be undone by this poll.
        tracked = set(self._tickers)
        written = 0
        for snap in snapshots:
            quote = parse_snapshot(snap)
            if quote is None or quote.ticker not in tracked:
                continue
            self._cache.update(
                ticker=quote.ticker,
                price=quote.price,
                timestamp=quote.timestamp,
                reference_price=quote.reference_price,
            )
            written += 1

        missing = set(requested) - {getattr(s, "ticker", None) for s in snapshots}
        if missing:
            logger.warning("Massive returned no data for: %s", ", ".join(sorted(missing)))
        logger.debug("Massive poll: updated %d/%d tickers", written, len(requested))

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Synchronous call to the Massive REST API. Runs in a thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS,
            tickers=tickers,
        )
