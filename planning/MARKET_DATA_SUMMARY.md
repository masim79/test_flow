# Market Data Backend — Summary

**Status:** Complete, tested, reviewed, and revised against the real `massive` SDK. See `planning/MARKET_DATA_DESIGN.md` for the full design and revision rationale (this file replaces the earlier `planning/archive/*.md` drafts).

## What Was Built

A complete market data subsystem in `backend/app/market/` (8 modules) providing live price simulation and real market data via a unified interface.

### Architecture

```
MarketDataSource (ABC)
├── SimulatorDataSource  →  GBM simulator (default, no API key needed)
└── MassiveDataSource    →  Polygon.io REST poller (when MASSIVE_API_KEY set)
        │
        ▼
   PriceCache (thread-safe, in-memory)
        │
        ├──→ SSE stream endpoint (/api/stream/prices)
        ├──→ Portfolio valuation
        └──→ Trade execution
```

### Modules

| File | Purpose |
|------|---------|
| `models.py` | `PriceUpdate` — immutable frozen dataclass (ticker, price, previous_price, timestamp, reference_price, change, change_percent, day_change_percent, direction). `normalize_ticker()` — shared ticker validation/upper-casing |
| `interface.py` | `MarketDataSource` — abstract base class defining `start/stop/add_ticker/remove_ticker/get_tickers` |
| `cache.py` | `PriceCache` — thread-safe price store with version counter for SSE change detection; `remove()` also bumps the version |
| `seed_prices.py` | Realistic seed prices, per-ticker GBM params (drift/volatility), correlation groups |
| `simulator.py` | `GBMSimulator` (Geometric Brownian Motion with Cholesky-correlated moves) + `SimulatorDataSource` |
| `massive_client.py` | `MassiveDataSource` — REST polling client for Polygon.io via the `massive` package, with `parse_snapshot()` as a pure, independently-testable parsing function |
| `factory.py` | `create_market_data_source()` — selects simulator or Massive based on `MASSIVE_API_KEY`; `MASSIVE_POLL_INTERVAL` controls poll cadence |
| `stream.py` | `create_stream_router()` — FastAPI SSE endpoint factory; `generate_price_events()` is the (public, importable) event generator |

### Key Design Decisions

- **Strategy pattern** — both data sources implement the same ABC; downstream code is source-agnostic
- **PriceCache as single point of truth** — producers write, consumers read; no direct coupling
- **GBM with correlated moves** — Cholesky decomposition of sector-based correlation matrix; tech stocks correlate at 0.6, finance at 0.5, cross-sector at 0.3
- **Random shock events** — ~0.1% chance per tick per ticker of a 2-5% move for visual drama
- **SSE over WebSockets** — simpler, one-way push, universal browser support
- **`day_change_percent`** (vs. `reference_price`) drives the watchlist's daily % column; `change`/`change_percent` remain tick-to-tick

## Test Suite

**127 tests (126 passing, 1 skipped without a live API key).** 7 test modules in `backend/tests/market/`.

| Module | Tests | Coverage |
|--------|-------|----------|
| test_models.py | 25 | models.py: 100% |
| test_cache.py | 19 | cache.py: 100% |
| test_simulator.py | 22 | simulator.py: 98% |
| test_simulator_source.py | 13 | (integration tests) |
| test_factory.py | 11 | factory.py: 100% |
| test_massive.py | 27 (1 skipped without `MASSIVE_API_KEY`) | massive_client.py: 97% |
| test_stream.py | 10 | stream.py: 92% |

Overall coverage: 98%. Lint (`ruff check`): clean.

## Revision History

### Round 1 — initial code review (`planning/archive/MARKET_DATA_REVIEW.md`)

Fixed the `pyproject.toml` build config, lazy-import fragility (made `massive` a core dependency), the SSE generator's return-type annotation, unused test imports, and added a public `get_tickers()` on `GBMSimulator`.

### Round 2 — detailed design pass against the real `massive` SDK (`planning/MARKET_DATA_DESIGN.md`)

Round 1's Massive tests used `MagicMock` snapshots, which return a value for any attribute access — masking a real bug. Verifying against the installed SDK found and fixed:

1. **R1 (bug, was silently dropping every Massive price)** — the code read `last_trade.timestamp`, which doesn't exist on the real `LastTrade` model (it's `.sip_timestamp`, in nanoseconds). Every snapshot raised `AttributeError` and was skipped, so the cache never got Massive prices. Fixed by extracting a pure `parse_snapshot()` function tested against real `TickerSnapshot.from_dict(...)` objects.
2. **R2 (bug)** — a ticker removed while a poll's `asyncio.to_thread` was in flight could be written back into the cache afterward. Fixed by filtering poll results against the *currently* tracked set after the await.
3. **R3 (feature gap, PLAN §10)** — added `reference_price` and `day_change_percent` to `PriceUpdate`/`PriceCache` so the watchlist can show a real daily % change (Massive: previous close; simulator: first price since start/added).
4. **R4 (bug)** — the simulator didn't normalize ticker case, so `add_ticker("tsla")` created a separate series from `"TSLA"`. Fixed with a shared `normalize_ticker()` used by both sources.
5. **R5** — added an SSE `: keepalive` comment every 15s of an otherwise-idle connection, so proxies don't close it during off-hours under Massive.
6. **R6** — `PriceCache.version` reads now happen under the lock; `remove()` now bumps the version so SSE clients see removals promptly.
7. **R7** — `MassiveDataSource` now backs off exponentially (up to 8x the poll interval) on consecutive failures, resetting on success.
8. **R8** — `add_ticker()` now wakes the poll loop early (rate-limited by `min_poll_gap`) instead of waiting up to a full `poll_interval`.
9. **R9** — added the `MASSIVE_POLL_INTERVAL` env var (default 15s, floor 1s).
10. **R10** — renamed `_generate_events` to the public `generate_price_events` and confirmed `create_stream_router()` builds a fresh `APIRouter` per call (no shared module-level router).

## Demo

A Rich terminal demo is available at `backend/market_data_demo.py`:

```bash
cd backend
uv run market_data_demo.py
```

Displays a live-updating dashboard with all 10 tickers, sparklines, color-coded direction arrows, and an event log for notable price moves. Runs 60 seconds or until Ctrl+C.

## Usage for Downstream Code

```python
from app.market import PriceCache, create_market_data_source, normalize_ticker

# Startup
cache = PriceCache()
source = create_market_data_source(cache)  # Reads MASSIVE_API_KEY / MASSIVE_POLL_INTERVAL
await source.start(["AAPL", "GOOGL", "MSFT", ...])

# Read prices
update = cache.get("AAPL")          # PriceUpdate or None
price = cache.get_price("AAPL")     # float or None
all_prices = cache.get_all()        # dict[str, PriceUpdate]

# Dynamic watchlist (tickers are validated/normalized; raises ValueError if invalid)
await source.add_ticker("TSLA")
await source.remove_ticker("GOOGL")

# Shutdown
await source.stop()
```
