"""Tests for MassiveDataSource and parse_snapshot, using real massive SDK models.

Regression coverage for R1: the original tests here built snapshots with
MagicMock, which happily returns a value for *any* attribute access —
including `last_trade.timestamp`, which doesn't exist on the real SDK's
`LastTrade` model (it's `.sip_timestamp`). That masked a bug where every
snapshot was silently skipped in production. Building snapshots with the
real `TickerSnapshot.from_dict(...)` here means these tests fail if the
parsing code touches an attribute the SDK doesn't actually have.
"""

import asyncio
import os
from unittest.mock import patch

import pytest
from massive.rest.models import TickerSnapshot

from app.market.cache import PriceCache
from app.market.massive_client import MassiveDataSource, parse_snapshot


def make_snap(
    ticker: str = "AAPL",
    price: float | None = 191.23,
    t_ns: int = 1_675_190_399_000_000_000,
    prev_close: float | None = 189.5,
) -> TickerSnapshot:
    """Build a real TickerSnapshot the way the SDK would from raw JSON."""
    raw: dict = {"ticker": ticker}
    if prev_close is not None:
        raw["prevDay"] = {"c": prev_close}
    if price is not None:
        raw["lastTrade"] = {"T": ticker, "p": price, "s": 100, "t": t_ns}
    return TickerSnapshot.from_dict(raw)


class TestParseSnapshot:
    """Unit tests for parse_snapshot against real TickerSnapshot models."""

    def test_parses_price_timestamp_and_reference(self):
        q = parse_snapshot(make_snap())
        assert q.ticker == "AAPL"
        assert q.price == 191.23
        assert q.reference_price == 189.5
        assert q.timestamp == 1_675_190_399.0  # ns -> s

    def test_missing_trade_and_minute_returns_none(self):
        assert parse_snapshot(make_snap(price=None)) is None

    def test_falls_back_to_minute_close_when_no_last_trade(self):
        snap = TickerSnapshot.from_dict({"ticker": "AAPL", "min": {"c": 150.0}})
        q = parse_snapshot(snap)
        assert q is not None
        assert q.price == 150.0

    def test_missing_ticker_returns_none(self):
        snap = TickerSnapshot.from_dict({"lastTrade": {"p": 100.0}})
        assert parse_snapshot(snap) is None

    def test_zero_price_returns_none(self):
        snap = TickerSnapshot.from_dict({"ticker": "AAPL", "lastTrade": {"p": 0.0}})
        assert parse_snapshot(snap) is None

    def test_no_reference_price_when_prev_day_missing(self):
        q = parse_snapshot(make_snap(prev_close=None))
        assert q.reference_price is None

    def test_falls_back_to_updated_when_no_sip_timestamp(self):
        snap = TickerSnapshot.from_dict(
            {
                "ticker": "AAPL",
                "lastTrade": {"p": 100.0},
                "updated": 1_700_000_000_000_000_000,
            }
        )
        q = parse_snapshot(snap)
        assert q.timestamp == 1_700_000_000.0

    def test_ticker_is_uppercased(self):
        q = parse_snapshot(make_snap(ticker="aapl"))
        assert q.ticker == "AAPL"


@pytest.mark.asyncio
class TestMassiveDataSource:
    """Unit tests for MassiveDataSource with real snapshot models."""

    async def test_poll_writes_cache_with_real_models(self):
        """Regression test for R1: a real TickerSnapshot must price the ticker."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]
        source._client = object()  # Satisfy the _poll_once guard

        with patch.object(source, "_fetch_snapshots", return_value=[make_snap()]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 191.23
        update = cache.get("AAPL")
        assert update.timestamp == 1_675_190_399.0
        assert update.reference_price == 189.5

    async def test_malformed_snapshot_skipped(self):
        """Test that unparseable snapshots are skipped gracefully."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "BAD"]
        source._client = object()

        good_snap = make_snap("AAPL")
        bad_snap = TickerSnapshot.from_dict({"ticker": "BAD"})  # no price anywhere

        with patch.object(source, "_fetch_snapshots", return_value=[good_snap, bad_snap]):
            await source._poll_once()

        assert cache.get_price("AAPL") == 191.23
        assert cache.get_price("BAD") is None

    async def test_api_error_does_not_crash(self):
        """Test that API errors don't crash the poller."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]
        source._client = object()

        with patch.object(source, "_fetch_snapshots", side_effect=Exception("network error")):
            await source._poll_once()  # Should not raise

        assert cache.get_price("AAPL") is None
        assert source._failures == 1

    async def test_removed_during_poll_is_not_resurrected(self):
        """A ticker removed while a poll is in flight must not reappear."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL", "GOOGL"]
        source._client = object()

        def slow_fetch(tickers):
            source._tickers.remove("GOOGL")  # simulate a removal during the await
            return [make_snap("AAPL"), make_snap("GOOGL", price=170.0)]

        source._fetch_snapshots = slow_fetch
        await source._poll_once()

        assert "GOOGL" not in cache
        assert cache.get_price("AAPL") == 191.23

    async def test_failure_increments_backoff(self):
        """Consecutive poll failures increment the backoff counter."""
        source = MassiveDataSource(api_key="test-key", price_cache=PriceCache(), poll_interval=60.0)
        source._tickers = ["AAPL"]
        source._client = object()

        def boom(_tickers):
            raise RuntimeError("429")

        source._fetch_snapshots = boom
        await source._poll_once()
        await source._poll_once()

        assert source._failures == 2

    async def test_failure_count_resets_on_success(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)
        source._tickers = ["AAPL"]
        source._client = object()
        source._failures = 3

        with patch.object(source, "_fetch_snapshots", return_value=[make_snap()]):
            await source._poll_once()

        assert source._failures == 0

    async def test_add_ticker(self):
        """Test adding a ticker."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.add_ticker("AAPL")
        assert "AAPL" in source.get_tickers()

    async def test_add_ticker_uppercase_normalization(self):
        """Test that tickers are normalized to uppercase."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.add_ticker("aapl")
        assert "AAPL" in source.get_tickers()

    async def test_add_ticker_strips_whitespace(self):
        """Test that ticker whitespace is stripped."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.add_ticker("  AAPL  ")
        assert "AAPL" in source.get_tickers()

    async def test_add_ticker_wakes_the_poll_loop(self):
        """Adding a ticker should signal the loop to poll early."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        assert not source._wake.is_set()
        await source.add_ticker("AAPL")
        assert source._wake.is_set()

    async def test_remove_ticker(self):
        """Test removing a ticker."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)
        source._tickers = ["AAPL", "GOOGL"]
        cache.update("AAPL", 190.00)

        await source.remove_ticker("AAPL")
        assert "AAPL" not in source.get_tickers()
        assert cache.get("AAPL") is None

    async def test_get_tickers(self):
        """Test getting the list of active tickers."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)
        source._tickers = ["AAPL", "GOOGL"]

        tickers = source.get_tickers()
        assert tickers == ["AAPL", "GOOGL"]

    async def test_empty_tickers_skips_poll(self):
        """Test that polling is skipped when there are no tickers."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)
        source._tickers = []

        with patch.object(source, "_fetch_snapshots") as mock_fetch:
            await source._poll_once()
            mock_fetch.assert_not_called()

    async def test_stop_is_idempotent(self):
        """Test that stop() can be called multiple times."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache)

        await source.stop()
        await source.stop()  # Should not raise

    async def test_stop_cancels_task(self):
        """Test that stop() cancels the polling task."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=10.0)

        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=[]):
                await source.start(["AAPL"])

        assert source._task is not None
        assert not source._task.done()

        await source.stop()
        assert source._task is None

    async def test_start_immediate_poll(self):
        """Test that start() does an immediate poll before starting the loop."""
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)

        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=[make_snap()]):
                await source.start(["AAPL"])

        # Cache should have data immediately from the first poll
        assert cache.get_price("AAPL") == 191.23

        await source.stop()

    async def test_add_ticker_triggers_early_poll(self):
        """R8: adding a ticker should cause a poll well before the full interval elapses."""
        cache = PriceCache()
        source = MassiveDataSource(
            api_key="test-key", price_cache=cache, poll_interval=10.0, min_poll_gap=0.05
        )
        poll_count = 0

        def counting_fetch(tickers):
            nonlocal poll_count
            poll_count += 1
            return []

        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", side_effect=counting_fetch):
                await source.start([])  # no tickers yet: start()'s own poll is a no-op
                assert poll_count == 0

                await source.add_ticker("AAPL")
                await asyncio.sleep(0.2)  # well under the 10s interval

                await source.stop()

        assert poll_count >= 1

    async def test_start_normalizes_and_dedupes_tickers(self):
        cache = PriceCache()
        source = MassiveDataSource(api_key="test-key", price_cache=cache, poll_interval=60.0)

        with patch("app.market.massive_client.RESTClient"):
            with patch.object(source, "_fetch_snapshots", return_value=[]):
                await source.start(["aapl", "AAPL", " tsla "])

        assert source.get_tickers() == ["AAPL", "TSLA"]


@pytest.mark.skipif(not os.getenv("MASSIVE_API_KEY"), reason="needs a real Massive API key")
async def test_live_massive_smoke():
    """Optional smoke test against the real Massive API. Never runs in CI (no
    key there) — run it by hand before a demo that uses real data. This is
    the test that would have caught R1: a MagicMock-based test can't."""
    cache = PriceCache()
    source = MassiveDataSource(os.environ["MASSIVE_API_KEY"], cache)
    await source.start(["AAPL", "MSFT"])
    await source.stop()
    price = cache.get_price("AAPL")
    assert price is not None
    assert price > 0
