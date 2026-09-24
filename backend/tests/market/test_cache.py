"""Tests for PriceCache."""

from concurrent.futures import ThreadPoolExecutor

from app.market.cache import PriceCache


class TestPriceCache:
    """Unit tests for the PriceCache."""

    def test_update_and_get(self):
        """Test updating and getting a price."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.ticker == "AAPL"
        assert update.price == 190.50
        assert cache.get("AAPL") == update

    def test_first_update_is_flat(self):
        """Test that the first update has flat direction."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.50)
        assert update.direction == "flat"
        assert update.previous_price == 190.50

    def test_direction_up(self):
        """Test price update with upward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 191.00)
        assert update.direction == "up"
        assert update.change == 1.00

    def test_direction_down(self):
        """Test price update with downward direction."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        update = cache.update("AAPL", 189.00)
        assert update.direction == "down"
        assert update.change == -1.00

    def test_remove(self):
        """Test removing a ticker from cache."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.remove("AAPL")
        assert cache.get("AAPL") is None

    def test_remove_nonexistent(self):
        """Test removing a ticker that doesn't exist."""
        cache = PriceCache()
        cache.remove("AAPL")  # Should not raise

    def test_remove_bumps_version(self):
        """Removals must be visible to SSE clients via the version counter."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        v = cache.version
        cache.remove("AAPL")
        assert cache.version == v + 1

    def test_remove_nonexistent_does_not_bump_version(self):
        cache = PriceCache()
        v = cache.version
        cache.remove("NOPE")
        assert cache.version == v

    def test_get_all(self):
        """Test getting all prices."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        cache.update("GOOGL", 175.00)
        all_prices = cache.get_all()
        assert set(all_prices.keys()) == {"AAPL", "GOOGL"}

    def test_version_increments(self):
        """Test that version counter increments."""
        cache = PriceCache()
        v0 = cache.version
        cache.update("AAPL", 190.00)
        assert cache.version == v0 + 1
        cache.update("AAPL", 191.00)
        assert cache.version == v0 + 2

    def test_get_price_convenience(self):
        """Test the convenience get_price method."""
        cache = PriceCache()
        cache.update("AAPL", 190.50)
        assert cache.get_price("AAPL") == 190.50
        assert cache.get_price("NOPE") is None

    def test_len(self):
        """Test __len__ method."""
        cache = PriceCache()
        assert len(cache) == 0
        cache.update("AAPL", 190.00)
        assert len(cache) == 1
        cache.update("GOOGL", 175.00)
        assert len(cache) == 2

    def test_contains(self):
        """Test __contains__ method."""
        cache = PriceCache()
        cache.update("AAPL", 190.00)
        assert "AAPL" in cache
        assert "GOOGL" not in cache

    def test_custom_timestamp(self):
        """Test updating with a custom timestamp."""
        cache = PriceCache()
        custom_ts = 1234567890.0
        update = cache.update("AAPL", 190.50, timestamp=custom_ts)
        assert update.timestamp == custom_ts

    def test_zero_timestamp_is_respected(self):
        """0.0 is a valid explicit timestamp, not a falsy 'use now()' sentinel."""
        update = PriceCache().update("AAPL", 190.50, timestamp=0.0)
        assert update.timestamp == 0.0

    def test_reference_price_defaults_to_first_price_and_persists(self):
        cache = PriceCache()
        cache.update("AAPL", 100.0)
        update = cache.update("AAPL", 102.0)
        assert update.reference_price == 100.0
        assert update.day_change_percent == 2.0

    def test_reference_price_can_be_overridden(self):
        cache = PriceCache()
        cache.update("AAPL", 100.0)
        update = cache.update("AAPL", 102.0, reference_price=90.0)
        assert update.reference_price == 90.0

    def test_price_rounding(self):
        """Test that prices are rounded to 2 decimal places."""
        cache = PriceCache()
        update = cache.update("AAPL", 190.12345)
        assert update.price == 190.12

    def test_concurrent_updates_are_thread_safe(self):
        """Many threads updating many tickers concurrently shouldn't corrupt state."""
        cache = PriceCache()
        tickers = [f"T{i}" for i in range(20)]
        updates_per_ticker = 50

        def hammer(ticker: str) -> None:
            for i in range(updates_per_ticker):
                cache.update(ticker, 100.0 + i)

        with ThreadPoolExecutor(max_workers=20) as pool:
            pool.map(hammer, tickers)

        assert len(cache) == len(tickers)
        assert cache.version == len(tickers) * updates_per_ticker
        for ticker in tickers:
            update = cache.get(ticker)
            assert update is not None
            assert update.price == 100.0 + updates_per_ticker - 1
