# Backend — Developer Guide

## Project Setup

```bash
cd backend
uv sync --extra dev   # Install all dependencies including test/lint tools
```

## Market Data API

The market data subsystem lives in `app/market/`. Use these imports:

```python
from app.market import (
    PriceCache, PriceUpdate, MarketDataSource,
    create_market_data_source, create_stream_router,
    normalize_ticker,
)
```

### Core Types

- **`PriceUpdate`** — Immutable dataclass: `ticker`, `price`, `previous_price`, `timestamp`, `reference_price` (previous close under Massive, first price under the simulator), plus properties `change`/`change_percent` (tick-to-tick), `day_change_percent` (vs. `reference_price` — this is what drives the watchlist's daily % column), `direction` ("up"/"down"/"flat"), and `to_dict()` for JSON serialization.

- **`normalize_ticker(raw) -> str`** — Upper-cases and validates a ticker symbol (`^[A-Z][A-Z0-9.\-]{0,9}$`), e.g. `"aapl"` → `"AAPL"`, `"brk.b"` → `"BRK.B"`. Raises `ValueError` on anything invalid (empty, lower-case-only-after-strip mismatches the pattern, too long, bad characters). Both `SimulatorDataSource` and `MassiveDataSource` normalize internally, so tickers never fork into separate case-sensitive series; use it directly in API routes too, so a bad ticker 422s before it reaches a data source.

- **`PriceCache`** — Thread-safe in-memory store. Key methods:
  - `update(ticker, price, timestamp=None, reference_price=None) -> PriceUpdate` — `reference_price` defaults to the first price seen for that ticker and persists across later updates unless explicitly overridden
  - `get(ticker) -> PriceUpdate | None`
  - `get_price(ticker) -> float | None`
  - `get_all() -> dict[str, PriceUpdate]`
  - `remove(ticker)` — also bumps `version`, so SSE clients see the removal promptly
  - `version` property — monotonic counter, increments on every update and removal (for SSE change detection)

- **`MarketDataSource`** — Abstract interface implemented by `SimulatorDataSource` and `MassiveDataSource`. Lifecycle: `start(tickers)` -> `add_ticker()` / `remove_ticker()` -> `stop()`.

- **`create_market_data_source(cache)`** — Factory. Returns `MassiveDataSource` if `MASSIVE_API_KEY` is set, otherwise `SimulatorDataSource`. `MASSIVE_POLL_INTERVAL` (seconds, default 15, floor 1) controls the Massive poll cadence.

### Massive client notes

- `app/market/massive_client.py` exposes `parse_snapshot(snap) -> ParsedQuote | None`, a pure function that pulls price/timestamp/reference-price out of a real `massive.rest.models.TickerSnapshot`. **Test it (and `MassiveDataSource`) with real `TickerSnapshot.from_dict(...)` objects, not `MagicMock`** — a `MagicMock` returns a value for any attribute access, which previously masked a bug where `last_trade.timestamp` (a real `LastTrade` has no such attribute; it's `.sip_timestamp`, in nanoseconds) raised and silently dropped every snapshot. See `tests/market/test_massive.py::make_snap`.
- Consecutive poll failures back off exponentially (up to 8x `poll_interval`) and reset on the next success.
- `add_ticker()` wakes the poll loop early (rate-limited by `min_poll_gap`, default `0.8 * poll_interval`) instead of waiting up to a full interval.
- A ticker removed while a poll is in flight is dropped from that poll's results rather than being written back into the cache.

### SSE Streaming

```python
from app.market import create_stream_router, generate_price_events

router = create_stream_router(price_cache)  # Returns FastAPI APIRouter; builds a fresh
                                             # router each call, safe to call more than once
# Endpoint: GET /api/stream/prices (text/event-stream)
```

`generate_price_events()` is the underlying async generator (exported so tests can drive it directly with a fake `Request`, since FastAPI's `TestClient` and `httpx.ASGITransport` both block until the whole response completes — neither can incrementally read an endpoint that streams forever). It sends a full snapshot whenever `price_cache.version` changes (an empty `{}` included — that's what clears the UI when the last tracked ticker is removed), and an SSE `: keepalive` comment every 15s of otherwise-idle connection.

### Seed Data

Default tickers: AAPL, GOOGL, MSFT, AMZN, TSLA, NVDA, META, JPM, V, NFLX. Seed prices and per-ticker volatility/drift params are in `app/market/seed_prices.py`.

## Running Tests

```bash
uv run --extra dev pytest -v              # All tests
uv run --extra dev pytest --cov=app       # With coverage
uv run --extra dev ruff check app/ tests/ # Lint
```

## Demo

```bash
uv run market_data_demo.py   # Live terminal dashboard with simulated prices
```
