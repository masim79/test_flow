"""Tests for market data source factory."""

import os
from unittest.mock import patch

from app.market.cache import PriceCache
from app.market.factory import create_market_data_source
from app.market.massive_client import MassiveDataSource
from app.market.simulator import SimulatorDataSource


class TestFactory:
    """Tests for create_market_data_source factory."""

    def test_creates_simulator_when_no_api_key(self):
        """Test that simulator is created when MASSIVE_API_KEY is not set."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_empty(self):
        """Test that simulator is created when MASSIVE_API_KEY is empty."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": ""}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_simulator_when_api_key_whitespace(self):
        """Test that simulator is created when MASSIVE_API_KEY is whitespace."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "   "}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)

    def test_creates_massive_when_api_key_set(self):
        """Test that Massive client is created when MASSIVE_API_KEY is set."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)

    def test_massive_receives_api_key(self):
        """Test that Massive client receives the API key."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key-123"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._api_key == "test-key-123"

    def test_simulator_receives_cache(self):
        """Test that simulator receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, SimulatorDataSource)
        assert source._cache is cache

    def test_massive_receives_cache(self):
        """Test that Massive client receives the cache reference."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert isinstance(source, MassiveDataSource)
        assert source._cache is cache

    def test_massive_default_poll_interval(self):
        """Default poll interval is 15s when MASSIVE_POLL_INTERVAL is unset."""
        cache = PriceCache()

        with patch.dict(os.environ, {"MASSIVE_API_KEY": "test-key"}, clear=True):
            source = create_market_data_source(cache)

        assert source._interval == 15.0

    def test_massive_custom_poll_interval(self):
        """MASSIVE_POLL_INTERVAL overrides the default poll interval."""
        cache = PriceCache()

        env = {"MASSIVE_API_KEY": "test-key", "MASSIVE_POLL_INTERVAL": "5"}
        with patch.dict(os.environ, env, clear=True):
            source = create_market_data_source(cache)

        assert source._interval == 5.0

    def test_massive_poll_interval_floor(self):
        """MASSIVE_POLL_INTERVAL is floored at 1 second."""
        cache = PriceCache()

        env = {"MASSIVE_API_KEY": "test-key", "MASSIVE_POLL_INTERVAL": "0.1"}
        with patch.dict(os.environ, env, clear=True):
            source = create_market_data_source(cache)

        assert source._interval == 1.0

    def test_massive_invalid_poll_interval_falls_back_to_default(self):
        """A non-numeric MASSIVE_POLL_INTERVAL logs a warning and uses the default."""
        cache = PriceCache()

        env = {"MASSIVE_API_KEY": "test-key", "MASSIVE_POLL_INTERVAL": "not-a-number"}
        with patch.dict(os.environ, env, clear=True):
            source = create_market_data_source(cache)

        assert source._interval == 15.0
