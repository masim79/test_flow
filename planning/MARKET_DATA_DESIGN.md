# Market Data Backend — Detailed Design

**Scope:** everything under `backend/app/market/`, which covers the unified data-source interface, the in-memory price cache, the GBM simulator, the Massive (Polygon.io) REST poller, the SSE price stream, and how the rest of the backend (watchlist, trades, portfolio, chat) plugs into it.

**Audience:** the Backend agent wiring up `app/main.py` and the portfolio/watchlist/chat routes, and anyone changing the market data code.

**Relationship to other docs**

| Doc | Role |
|---|---|
| `planning/PLAN.md` §6 | The requirements this design implements |
| `planning/MARKET_DATA_SUMMARY.md` | Status summary of what's already built (73 tests passing) |
| `planning/archive/*.md` | Earlier drafts, the API reference and the code review. Kept for history. **This document replaces them** |

**How to read this doc.** Most of the design already exists in `backend/app/market/`. Each section gives the current code as the baseline. Changes this design makes to that baseline are tagged **[R1]…[R10]** and collected in [§15 Revision Checklist](#15-revision-checklist). One of them, **[R1]**, is a real bug: with the real `massive` SDK, the Massive poller never writes a price. See §9.3.

---

## Table of Contents

1. [Requirements](#1-requirements)
2. [Architecture](#2-architecture)
3. [File Layout & Public API](#3-file-layout--public-api)
4. [Data Model — `models.py`](#4-data-model--modelspy)
5. [Price Cache — `cache.py`](#5-price-cache--cachepy)
6. [Unified Interface — `interface.py`](#6-unified-interface--interfacepy)
7. [Seed Data — `seed_prices.py`](#7-seed-data--seed_pricespy)
8. [GBM Simulator — `simulator.py`](#8-gbm-simulator--simulatorpy)
9. [Massive API Client — `massive_client.py`](#9-massive-api-client--massive_clientpy)
10. [Factory & Configuration — `factory.py`](#10-factory--configuration--factorypy)
11. [SSE Streaming — `stream.py`](#11-sse-streaming--streampy)
12. [Integration with the Rest of the Backend](#12-integration-with-the-rest-of-the-backend)
13. [Error Handling & Edge Cases](#13-error-handling--edge-cases)
14. [Testing](#14-testing)
15. [Revision Checklist](#15-revision-checklist)

---

## 1. Requirements

Traced from `PLAN.md`:

| # | Requirement | Where it's met |
|---|---|---|
| M1 | One interface, two implementations (simulator and Massive), chosen by env var | `interface.py`, `factory.py` |
| M2 | Simulator: GBM, ~500ms ticks, correlated moves, random 2–5% events, realistic seed prices, in-process | `simulator.py`, `seed_prices.py` |
| M3 | Massive: REST polling (no WebSocket), all watched tickers in one call, 15s on the free tier, 2–15s on paid tiers | `massive_client.py` |
| M4 | Shared in-memory cache holding latest price, previous price and timestamp per ticker | `cache.py` |
| M5 | `GET /api/stream/prices` SSE: pushes all known tickers about every 500ms; each event has ticker, price, previous price, timestamp and direction; the client auto-reconnects | `stream.py` |
| M6 | Watchlist add/remove changes the set of streamed tickers | `add_ticker` / `remove_ticker`, §12.3 |
| M7 | Trades fill instantly at the current price | `PriceCache.get_price`, §12.4 |
| M8 | Watchlist shows **daily change %** | **[R3]** reference price, §4 |
| M9 | Downstream code doesn't care which source is active | Everyone reads only from `PriceCache` |

Non-goals: order books, bid/ask spreads, historical bars, and more than one user. The cache is keyed by ticker, not by user, so a second user can be added later without changing it.

---

## 2. Architecture

```
                    ┌──────────────────────────────────────────┐
                    │         create_market_data_source()      │
                    │   MASSIVE_API_KEY set?  yes → Massive    │
                    │                         no  → Simulator  │
                    └──────────────────┬───────────────────────┘
                                       │ returns one of
             ┌─────────────────────────┴──────────────────────────┐
             ▼                                                    ▼
┌───────────────────────────┐                     ┌───────────────────────────────┐
│ SimulatorDataSource       │                     │ MassiveDataSource             │
│  asyncio task, every 0.5s │                     │  asyncio task, every 15s      │
│  GBMSimulator.step()      │                     │  to_thread(get_snapshot_all)  │
└─────────────┬─────────────┘                     └───────────────┬───────────────┘
              │ cache.update(ticker, price, ...)                  │
              └──────────────────────┬────────────────────────────┘
                                     ▼
                     ┌───────────────────────────────┐
                     │ PriceCache  (Lock, version++) │  ← single source of truth
                     └──┬─────────────┬───────────┬──┘
                        │             │           │
             get_all()  │  get_price()│           │ get_all()
                        ▼             ▼           ▼
             SSE /api/stream/   POST /api/     GET /api/portfolio,
             prices (per client) portfolio/    snapshots task,
                                 trade         chat context
```

### Design principles

1. **Producers write, consumers read, and neither knows about the other.** A data source never returns prices to a caller; it pushes them into the cache. Routes never call a data source for a price; they read the cache. That is what lets you swap the simulator for Massive without touching anything else.
2. **One writer at a time.** Exactly one `MarketDataSource` is live per process.
3. **Everything runs on the asyncio event loop**, with one exception: the synchronous Massive HTTP call runs in a worker thread (`asyncio.to_thread`). The cache is therefore guarded by a `threading.Lock`, not an `asyncio.Lock`.
4. **Background loops never die.** Every loop iteration catches and logs exceptions, then carries on. A dead price loop would freeze the whole UI with no error.
5. **Fail soft to the UI.** A missing price for a ticker means the UI shows no price for it. It never means a 500.

### Threading model

| Code | Runs on | Touches cache via |
|---|---|---|
| `SimulatorDataSource._run_loop` | event loop | `update()` |
| `MassiveDataSource._fetch_snapshots` | worker thread | nothing (returns data only) |
| `MassiveDataSource._poll_once` (after `await`) | event loop | `update()` |
| SSE generator(s) | event loop | `version`, `get_all()` |
| Trade and portfolio routes | event loop | `get_price()`, `get_all()` |

Only one thread ever calls `update()` in practice, but the lock costs nothing and keeps the cache safe if that changes. It also covers free-threaded Python (3.13t).

---

## 3. File Layout & Public API

```
backend/app/market/
  __init__.py         # Re-exports the public API below
  models.py           # PriceUpdate (+ normalize_ticker [R4])
  cache.py            # PriceCache
  interface.py        # MarketDataSource ABC
  seed_prices.py      # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, correlation constants
  simulator.py        # GBMSimulator (pure math) + SimulatorDataSource (async wrapper)
  massive_client.py   # MassiveDataSource
  factory.py          # create_market_data_source()
  stream.py           # create_stream_router() → GET /api/stream/prices
backend/tests/market/ # one test module per source module
backend/market_data_demo.py  # Rich terminal dashboard (uv run market_data_demo.py)
```

The rest of the backend imports **only** from the package root:

```python
from app.market import (
    PriceUpdate,                # immutable price snapshot
    PriceCache,                 # the shared store
    MarketDataSource,           # type for DI / annotations
    create_market_data_source,  # factory
    create_stream_router,       # SSE router factory
    normalize_ticker,           # [R4] validation shared with API routes
)
```

---

## 4. Data Model — `models.py`

`PriceUpdate` is the only type that leaves the market layer. It is frozen, so a snapshot handed to an SSE client can't be changed by a later tick. `slots=True` keeps it small because thousands are created per minute.

```python
from __future__ import annotations

import re
import time
from dataclasses import dataclass, field


@dataclass(frozen=True, slots=True)
class PriceUpdate:
    """Immutable snapshot of a single ticker's price at a point in time."""

    ticker: str
    price: float
    previous_price: float
    timestamp: float = field(default_factory=time.time)  # Unix seconds
    # [R3] Price the "daily change" is measured against:
    #   simulator → the ticker's first price this session (seed price)
    #   Massive   → previous trading day's close (snap.prev_day.close)
    reference_price: float | None = None

    @property
    def change(self) -> float:
        """Absolute change since the previous update (tick-to-tick)."""
        return round(self.price - self.previous_price, 4)

    @property
    def change_percent(self) -> float:
        """Percent change since the previous update (tick-to-tick)."""
        if self.previous_price == 0:
            return 0.0
        return round((self.price - self.previous_price) / self.previous_price * 100, 4)

    @property
    def day_change_percent(self) -> float:
        """[R3] Percent change vs. the reference price. Drives the watchlist 'daily %' column."""
        if not self.reference_price:
            return 0.0
        return round((self.price - self.reference_price) / self.reference_price * 100, 4)

    @property
    def direction(self) -> str:
        """'up', 'down', or 'flat' — drives the green/red price flash."""
        if self.price > self.previous_price:
            return "up"
        if self.price < self.previous_price:
            return "down"
        return "flat"

    def to_dict(self) -> dict:
        """Serialize for JSON / SSE."""
        return {
            "ticker": self.ticker,
            "price": self.price,
            "previous_price": self.previous_price,
            "timestamp": self.timestamp,
            "change": self.change,
            "change_percent": self.change_percent,
            "direction": self.direction,
            "reference_price": self.reference_price,        # [R3]
            "day_change_percent": self.day_change_percent,  # [R3]
        }
```

### Why [R3]?

`PLAN.md` §10 asks for "daily change %" in the watchlist. The current `change_percent` is tick-to-tick, so it's about 0.01% and meaningless in that column. The frontend can't compute a daily figure itself, because its SSE history resets on page reload. Adding one optional field fixes this and doesn't break existing callers: `reference_price` defaults to `None`, and `day_change_percent` then reads `0.0`.

### Ticker normalization [R4]

Right now `MassiveDataSource` upper-cases tickers but `SimulatorDataSource` doesn't. So `add_ticker("tsla")` on the simulator creates a separate `"tsla"` series with a random seed price. The fix is one shared validator, used by the API routes **and** defensively inside both sources:

```python
_TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.\-]{0,9}$")  # AAPL, BRK.B, BF-B


def normalize_ticker(raw: str) -> str:
    """Upper-case and validate a ticker symbol. Raises ValueError if invalid."""
    ticker = (raw or "").strip().upper()
    if not _TICKER_RE.fullmatch(ticker):
        raise ValueError(f"Invalid ticker symbol: {raw!r}")
    return ticker
```

---

## 5. Price Cache — `cache.py`

```python
from __future__ import annotations

import time
from threading import Lock

from .models import PriceUpdate


class PriceCache:
    """Thread-safe store of the latest PriceUpdate per ticker.

    Writers: the active MarketDataSource (exactly one).
    Readers: SSE streams, trade execution, portfolio valuation, chat context.
    """

    def __init__(self) -> None:
        self._prices: dict[str, PriceUpdate] = {}
        self._lock = Lock()
        self._version = 0  # bumped on every write; SSE uses it for change detection

    def update(
        self,
        ticker: str,
        price: float,
        timestamp: float | None = None,
        reference_price: float | None = None,  # [R3]
    ) -> PriceUpdate:
        """Record a new price. The first update for a ticker is 'flat' (previous == price)."""
        with self._lock:
            ts = time.time() if timestamp is None else timestamp  # [R6] 0.0 is a valid value
            prev = self._prices.get(ticker)
            previous_price = prev.price if prev else price
            # [R3] Keep the first reference we see unless the source gives a new one
            if reference_price is None:
                reference_price = prev.reference_price if prev else price

            update = PriceUpdate(
                ticker=ticker,
                price=round(price, 2),
                previous_price=round(previous_price, 2),
                timestamp=ts,
                reference_price=round(reference_price, 2),
            )
            self._prices[ticker] = update
            self._version += 1
            return update

    def get(self, ticker: str) -> PriceUpdate | None:
        with self._lock:
            return self._prices.get(ticker)

    def get_price(self, ticker: str) -> float | None:
        update = self.get(ticker)
        return update.price if update else None

    def get_all(self) -> dict[str, PriceUpdate]:
        """Shallow copy — safe to iterate while writers keep writing."""
        with self._lock:
            return dict(self._prices)

    def remove(self, ticker: str) -> None:
        with self._lock:
            if self._prices.pop(ticker, None) is not None:
                self._version += 1  # [R6] removals must reach SSE clients too

    @property
    def version(self) -> int:
        with self._lock:  # [R6] consistent with the rest of the class
            return self._version

    def __len__(self) -> int:
        with self._lock:
            return len(self._prices)

    def __contains__(self, ticker: str) -> bool:
        with self._lock:
            return ticker in self._prices
```

### Design notes

- **Rounding happens here**, to 2 decimals, so every consumer sees the same cent-precise number that trades fill at. The simulator keeps full precision internally, so rounding never builds up drift.
- **Version counter.** Each SSE client remembers the last `version` it sent and skips a tick if nothing changed. With Massive (15s polls) this avoids sending 30 identical payloads between polls. With the simulator every tick changes the version, so it has no effect.
- **Bumping `version` on `remove()`** [R6] fixes a subtle bug. At the moment, removing a ticker between two simulator ticks is covered by the next tick. Under Massive, though, the removed ticker stays in every client's view for up to 15s, because nothing bumps the version until the next poll.
- **Memory** is O(number of tickers). No history is kept. The frontend builds sparklines itself from the SSE stream, and portfolio history lives in SQLite.

---

## 6. Unified Interface — `interface.py`

```python
from __future__ import annotations

from abc import ABC, abstractmethod


class MarketDataSource(ABC):
    """Contract for market data providers.

    Implementations push PriceUpdates into a shared PriceCache on their own
    schedule. Nobody asks a source for a price; they read the cache.

    Lifecycle:
        source = create_market_data_source(cache)
        await source.start(["AAPL", "GOOGL", ...])
        await source.add_ticker("TSLA")
        await source.remove_ticker("GOOGL")
        await source.stop()
    """

    @abstractmethod
    async def start(self, tickers: list[str]) -> None:
        """Start the background task. Call once. Should leave the cache
        populated (or at least attempted) before returning, so the first
        SSE event isn't empty."""

    @abstractmethod
    async def stop(self) -> None:
        """Cancel the background task and await it. Idempotent. After this
        returns the source never writes to the cache again."""

    @abstractmethod
    async def add_ticker(self, ticker: str) -> None:
        """Start tracking a ticker. No-op if already tracked."""

    @abstractmethod
    async def remove_ticker(self, ticker: str) -> None:
        """Stop tracking a ticker AND evict it from the cache. No-op if absent."""

    @abstractmethod
    def get_tickers(self) -> list[str]:
        """Currently tracked tickers (a copy)."""
```

### Contract details both implementations must honour

| Behaviour | Simulator | Massive |
|---|---|---|
| `start()` fills the cache before returning | Yes, with seed prices | Yes, if the first poll succeeds |
| `add_ticker()` → price available | Immediately (seeded on add) | Next poll: up to `poll_interval`, or sooner with [R8] |
| `remove_ticker()` evicts from cache | Yes | Yes, and in-flight poll results for it are dropped [R2] |
| Ticker case | Normalized [R4] | Normalized |
| `stop()` twice | Safe | Safe |
| Exceptions in the loop | Logged, loop continues | Logged, loop continues with backoff [R7] |

---

## 7. Seed Data — `seed_prices.py`

Unchanged from the current code. Constants only, no logic.

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00, "GOOGL": 175.00, "MSFT": 420.00, "AMZN": 185.00, "TSLA": 250.00,
    "NVDA": 800.00, "META": 500.00, "JPM": 195.00, "V": 280.00, "NFLX": 600.00,
}

# sigma = annualized volatility, mu = annualized drift
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # high vol
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # high vol, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},   # low vol (bank)
    "V": {"sigma": 0.17, "mu": 0.04},     # low vol (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}  # for tickers added at runtime

CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}
INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3
TSLA_CORR = 0.3
```

Tickers that aren't in `SEED_PRICES` (for example PYPL added through chat) start at `random.uniform(50, 300)` with `DEFAULT_PARAMS`.

---

## 8. GBM Simulator — `simulator.py`

The module has two classes:

- **`GBMSimulator`** is pure, synchronous math with no asyncio and no cache. It's easy to unit test and to reuse, for example in the demo script.
- **`SimulatorDataSource`** is the async `MarketDataSource` wrapper that runs `step()` on a timer and writes to the cache.

### 8.1 The math

Each tick, every price moves by one step of geometric Brownian motion:

```
S(t+dt) = S(t) · exp( (μ − σ²/2)·dt  +  σ·√dt·Z )
```

- `dt` is the tick length as a fraction of a trading year: `0.5 / (252 × 6.5 × 3600) ≈ 8.48e-8`.
- With σ = 0.22 (AAPL), a single tick has a standard deviation of σ·√dt ≈ 0.0064%, about 1.2¢ on $190. That's enough to flash the price most ticks without random-walking off to silly values.
- `exp()` keeps prices positive. The `−σ²/2` term (the Itô correction) makes the expected price grow at μ rather than μ + σ²/2.

**Correlation.** Draw `Z_ind ~ N(0, I)`, then `Z = L · Z_ind`, where `L = cholesky(C)` and `C` is the pairwise correlation matrix built from `CORRELATION_GROUPS`. Tech pairs get 0.6, finance pairs 0.5, and everything else (including every pair that involves TSLA) 0.3. With a constant off-diagonal of at least 0.3 and blocks of at most 0.6, `C` is always positive definite, so the Cholesky decomposition can't fail for any ticker set.

**Shock events.** On each tick, each ticker has a probability `p = 0.001` of jumping 2–5% up or down. At 2 ticks per second across 10 tickers, that's about one event every 50 seconds somewhere on the board: enough drama for demos without wrecking the prices.

### 8.2 `GBMSimulator`

```python
import logging
import math
import random

import numpy as np

from .seed_prices import (
    CORRELATION_GROUPS, CROSS_GROUP_CORR, DEFAULT_PARAMS, INTRA_FINANCE_CORR,
    INTRA_TECH_CORR, SEED_PRICES, TICKER_PARAMS, TSLA_CORR,
)

logger = logging.getLogger(__name__)


class GBMSimulator:
    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600      # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR      # ≈ 8.48e-8

    def __init__(self, tickers: list[str], dt: float = DEFAULT_DT,
                 event_probability: float = 0.001) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None
        for t in tickers:
            self._add_ticker_internal(t)
        self._rebuild_cholesky()  # once, not once per ticker

    def step(self) -> dict[str, float]:
        """Advance every ticker one tick. Hot path: runs every 500ms."""
        n = len(self._tickers)
        if n == 0:
            return {}
        z = np.random.standard_normal(n)
        if self._cholesky is not None:
            z = self._cholesky @ z

        sqrt_dt = math.sqrt(self._dt)
        out: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            mu, sigma = self._params[ticker]["mu"], self._params[ticker]["sigma"]
            self._prices[ticker] *= math.exp((mu - 0.5 * sigma**2) * self._dt
                                             + sigma * sqrt_dt * z[i])
            if random.random() < self._event_prob:
                shock = random.uniform(0.02, 0.05) * random.choice((-1, 1))
                self._prices[ticker] *= 1 + shock
                logger.debug("Shock on %s: %+.1f%%", ticker, shock * 100)
            out[ticker] = round(self._prices[ticker], 2)
        return out

    def add_ticker(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker], self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- internals ---

    def _add_ticker_internal(self, ticker: str) -> None:
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = dict(TICKER_PARAMS.get(ticker, DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return
        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                corr[i, j] = corr[j, i] = self._pairwise_correlation(
                    self._tickers[i], self._tickers[j])
        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        if "TSLA" in (t1, t2):
            return TSLA_CORR  # TSLA is in no group on purpose: it does its own thing
        tech, fin = CORRELATION_GROUPS["tech"], CORRELATION_GROUPS["finance"]
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in fin and t2 in fin:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

Rebuilding the Cholesky factor is O(n³) in numpy, which is under 1ms for n ≤ 50. It only happens on add and remove, never on `step()`.

### 8.3 `SimulatorDataSource`

```python
import asyncio

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker


class SimulatorDataSource(MarketDataSource):
    def __init__(self, price_cache: PriceCache, update_interval: float = 0.5,
                 event_probability: float = 0.001) -> None:
        self._cache = price_cache
        self._interval = update_interval
        self._event_prob = event_probability
        self._sim: GBMSimulator | None = None
        self._task: asyncio.Task | None = None

    async def start(self, tickers: list[str]) -> None:
        tickers = [normalize_ticker(t) for t in tickers]                      # [R4]
        self._sim = GBMSimulator(tickers=tickers, event_probability=self._event_prob)
        for t in tickers:  # seed the cache so the first SSE event has data
            self._cache.update(t, self._sim.get_price(t))                     # reference = seed
        self._task = asyncio.create_task(self._run_loop(), name="simulator-loop")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)                                     # [R4]
        if self._sim and ticker not in self._sim.get_tickers():
            self._sim.add_ticker(ticker)
            self._cache.update(ticker, self._sim.get_price(ticker))           # price right away

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)                                     # [R4]
        if self._sim:
            self._sim.remove_ticker(ticker)
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return self._sim.get_tickers() if self._sim else []

    async def _run_loop(self) -> None:
        while True:
            try:
                if self._sim:
                    for ticker, price in self._sim.step().items():
                        self._cache.update(ticker, price)
            except Exception:
                logger.exception("Simulator step failed")  # never let the loop die
            await asyncio.sleep(self._interval)
```

**Reference price under the simulator.** The first `cache.update()` for a ticker has no `reference_price`, so the cache uses that first price, which is the seed. Later updates carry it forward. So "daily change %" means "change since the server started, or since the ticker was added". That's the right meaning for a simulated market that has no real session boundaries.

**Why `asyncio.sleep` and not a drift-corrected clock?** `step()` takes microseconds, so drift is negligible and the simple loop is easier to read.

---

## 9. Massive API Client — `massive_client.py`

### 9.1 Endpoint

One call per poll fetches every tracked ticker:

```
GET https://api.massive.com/v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,MSFT,...
Authorization: Bearer $MASSIVE_API_KEY
```

With the `massive` SDK (a core dependency, `massive>=1.0.0`):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient(api_key=api_key)
snaps = client.get_snapshot_all(market_type=SnapshotMarketType.STOCKS,
                                tickers=["AAPL", "MSFT"])   # -> list[TickerSnapshot]
```

The SDK is **synchronous** (it uses urllib3 under the hood) and retries 5xx errors itself. We call it through `asyncio.to_thread` so a slow response never stalls SSE or the API.

### 9.2 Field mapping (checked against the installed SDK)

The SDK deserializes the JSON into typed models. **The attribute names differ from the older docs in `planning/archive/`, and those docs are wrong on these points:**

| Raw JSON | SDK attribute | Type / unit | Our use |
|---|---|---|---|
| `ticker` | `snap.ticker` | `str` | cache key |
| `lastTrade.p` | `snap.last_trade.price` | `float` | **price** |
| `lastTrade.t` | `snap.last_trade.sip_timestamp` | `int`, **nanoseconds** | **timestamp** (÷ 1e9) |
| `prevDay.c` | `snap.prev_day.close` | `float` | **reference_price** [R3] |
| `todaysChangePerc` | `snap.todays_change_percent` | `float` | (cross-check only) |
| `updated` | `snap.updated` | `int`, nanoseconds | fallback timestamp |
| `min.c` | `snap.min.close` | `float` | fallback price if there's no last trade |

There is **no** `last_trade.timestamp` and **no** `day.previous_close` on these models.

### 9.3 [R1] The bug in the current code

`massive_client.py` does `snap.last_trade.timestamp / 1000.0`. `LastTrade` has no `timestamp` attribute, so this raises `AttributeError`, which the `except (AttributeError, TypeError)` block catches and logs as "Skipping snapshot". **Every snapshot is skipped and the cache stays empty.** The unit tests pass only because they use `MagicMock` snapshots, which will happily return a value for any attribute. Reproduction:

```python
raw = {"ticker": "AAPL", "lastTrade": {"p": 191.23, "t": 1675190399000000000}, "prevDay": {"c": 189.5}}
snap = TickerSnapshot.from_dict(raw)
# current code → log "Skipping snapshot for AAPL: 'LastTrade' object has no attribute 'timestamp'"
# cache.get_all() == {}
```

The fix is a single, defensive parse function (below), plus tests that build snapshots with `TickerSnapshot.from_dict` instead of `MagicMock` (§14.4).

### 9.4 Parsing: a pure function

Take parsing out of the poll loop so it can be tested without asyncio or an HTTP client:

```python
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ParsedQuote:
    ticker: str
    price: float
    timestamp: float | None         # Unix seconds; None → cache uses now()
    reference_price: float | None   # previous close


def parse_snapshot(snap) -> ParsedQuote | None:
    """Pull (ticker, price, ts, prev close) from a massive TickerSnapshot.

    Returns None if the snapshot has no usable price. Never raises.
    """
    ticker = getattr(snap, "ticker", None)
    if not ticker:
        return None

    last_trade = getattr(snap, "last_trade", None)
    minute = getattr(snap, "min", None)
    price = getattr(last_trade, "price", None) or getattr(minute, "close", None)
    if not isinstance(price, (int, float)) or price <= 0:
        return None

    ts_ns = getattr(last_trade, "sip_timestamp", None) or getattr(snap, "updated", None)
    timestamp = ts_ns / 1e9 if isinstance(ts_ns, (int, float)) and ts_ns > 0 else None

    prev_day = getattr(snap, "prev_day", None)
    ref = getattr(prev_day, "close", None)
    reference_price = ref if isinstance(ref, (int, float)) and ref > 0 else None

    return ParsedQuote(ticker.upper(), float(price), timestamp, reference_price)
```

> **Timestamp unit guard (optional).** Massive gives nanoseconds for snapshot `lastTrade.t` and `updated`. To survive a unit change on some plan, you can infer the unit from the value's size:
>
> ```python
> def to_seconds(ts: int | float) -> float:
>     if ts > 1e17: return ts / 1e9   # ns
>     if ts > 1e14: return ts / 1e6   # µs
>     if ts > 1e11: return ts / 1e3   # ms
>     return float(ts)                # s
> ```

### 9.5 `MassiveDataSource`

```python
import asyncio
import logging
import time

from massive import RESTClient
from massive.rest.models import SnapshotMarketType

from .cache import PriceCache
from .interface import MarketDataSource
from .models import normalize_ticker

logger = logging.getLogger(__name__)


class MassiveDataSource(MarketDataSource):
    """Polls the Massive snapshot endpoint for all tracked tickers in one call.

    Free tier: 5 req/min → 15s interval (default). Paid: 2–5s is fine.
    """

    MAX_BACKOFF_MULTIPLIER = 8   # [R7] 15s → up to 120s while failing

    def __init__(self, api_key: str, price_cache: PriceCache,
                 poll_interval: float = 15.0, min_poll_gap: float | None = None) -> None:
        self._api_key = api_key
        self._cache = price_cache
        self._interval = poll_interval
        # [R8] Never poll more often than this, even on an early wake.
        self._min_gap = poll_interval * 0.8 if min_poll_gap is None else min_poll_gap
        self._tickers: list[str] = []
        self._client: RESTClient | None = None
        self._task: asyncio.Task | None = None
        self._wake = asyncio.Event()          # [R8] set by add_ticker
        self._failures = 0                    # [R7]
        self._last_poll = 0.0

    # --- lifecycle ---

    async def start(self, tickers: list[str]) -> None:
        self._client = RESTClient(api_key=self._api_key)
        self._tickers = list(dict.fromkeys(normalize_ticker(t) for t in tickers))
        await self._poll_once()  # fill the cache before the first SSE client connects
        self._task = asyncio.create_task(self._poll_loop(), name="massive-poller")
        logger.info("Massive poller started: %d tickers, %.1fs interval",
                    len(self._tickers), self._interval)

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        self._client = None

    async def add_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        if ticker not in self._tickers:
            self._tickers.append(ticker)
            self._wake.set()                  # [R8] poll early (rate-limit permitting)

    async def remove_ticker(self, ticker: str) -> None:
        ticker = normalize_ticker(ticker)
        self._tickers = [t for t in self._tickers if t != ticker]
        self._cache.remove(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    # --- internals ---

    async def _poll_loop(self) -> None:
        while True:
            delay = self._interval * min(2 ** self._failures, self.MAX_BACKOFF_MULTIPLIER)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=delay)
            except TimeoutError:
                pass
            self._wake.clear()
            # [R8] respect the rate limit even when woken early
            gap = self._min_gap - (time.monotonic() - self._last_poll)
            if gap > 0:
                await asyncio.sleep(gap)
            await self._poll_once()

    async def _poll_once(self) -> None:
        if not self._tickers or not self._client:
            return
        requested = list(self._tickers)
        self._last_poll = time.monotonic()
        try:
            snapshots = await asyncio.to_thread(self._fetch_snapshots, requested)
        except Exception as e:  # 401 bad key, 403 plan, 429 rate limit, network
            self._failures += 1
            logger.error("Massive poll failed (%d in a row): %s", self._failures, e)
            return
        self._failures = 0

        # [R2] Only write tickers that are STILL tracked. A remove_ticker() that
        # ran while we were awaiting the thread must not be undone by this poll.
        tracked = set(self._tickers)
        written = 0
        for snap in snapshots:
            q = parse_snapshot(snap)
            if q is None or q.ticker not in tracked:
                continue
            self._cache.update(q.ticker, q.price, timestamp=q.timestamp,
                               reference_price=q.reference_price)
            written += 1

        missing = set(requested) - {getattr(s, "ticker", None) for s in snapshots}
        if missing:
            logger.warning("Massive returned no data for: %s", ", ".join(sorted(missing)))
        logger.debug("Massive poll: %d/%d tickers updated", written, len(requested))

    def _fetch_snapshots(self, tickers: list[str]) -> list:
        """Blocking HTTP call. Runs in a worker thread."""
        return self._client.get_snapshot_all(
            market_type=SnapshotMarketType.STOCKS, tickers=tickers)
```

### 9.6 Why these changes

- **[R2] Removal race.** A poll's `await asyncio.to_thread(...)` can take hundreds of milliseconds. If the user removes GOOGL during that time, the current code writes GOOGL back into the cache afterwards, and it reappears in the SSE stream until the next removal. Filtering against the *current* tracked set after the `await` closes that gap. The filter runs on the event loop, so no lock is needed.
- **[R2b] Pass `requested` explicitly to the thread** instead of reading `self._tickers` from another thread while the event loop mutates it.
- **[R7] Backoff.** With a bad key or a 429, polling every 15s forever only makes rate-limiting worse. Doubling up to 8× (2 minutes) and resetting on the first success keeps things simple.
- **[R8] Early poll on add.** Without it, a ticker added in the UI or by the AI shows "—" for up to 15s, and a trade on it can't fill. The `_wake` event triggers the next poll right away, and `_min_gap` keeps us under the free tier's 5 requests per minute. On free tier, adding a ticker 3s after a poll gets its price about 9s later; on paid tiers with 2s polls it's close to instant.

### 9.7 Market hours

The snapshot endpoint returns the last trade even when the market is closed, so prices simply stop moving outside market hours. The version counter stops changing, SSE stops sending data events, and heartbeats [R5] keep the connection alive. This is expected behaviour, not an error. Snapshot data is cleared at midnight ET and refills from about 4am ET. During that window `last_trade` can be missing, which is why `parse_snapshot` falls back to `min.close` and otherwise skips the ticker; the cache then keeps its last price.

### 9.8 Unknown tickers

If a user adds `ZZZZ`, Massive just leaves it out of the response. `parse_snapshot` never sees it and the cache never gets it. The poll logs a warning. The watchlist route should say so to the user (§12.3), and the trade route rejects it with "no price available" (§12.4).

---

## 10. Factory & Configuration — `factory.py`

```python
import logging
import os

from .cache import PriceCache
from .interface import MarketDataSource
from .massive_client import MassiveDataSource
from .simulator import SimulatorDataSource

logger = logging.getLogger(__name__)


def _float_env(name: str, default: float, minimum: float) -> float:
    raw = os.environ.get(name, "").strip()
    try:
        return max(float(raw), minimum) if raw else default
    except ValueError:
        logger.warning("Ignoring invalid %s=%r; using %s", name, raw, default)
        return default


def create_market_data_source(price_cache: PriceCache) -> MarketDataSource:
    """Return an UNSTARTED source; the caller awaits source.start(tickers)."""
    api_key = os.environ.get("MASSIVE_API_KEY", "").strip()
    if api_key:
        interval = _float_env("MASSIVE_POLL_INTERVAL", 15.0, minimum=1.0)   # [R9]
        logger.info("Market data source: Massive API (poll every %.1fs)", interval)
        return MassiveDataSource(api_key=api_key, price_cache=price_cache,
                                 poll_interval=interval)
    logger.info("Market data source: GBM simulator")
    return SimulatorDataSource(price_cache=price_cache)
```

### Environment variables

| Variable | Default | Effect |
|---|---|---|
| `MASSIVE_API_KEY` | empty | Non-empty after `.strip()` → Massive; otherwise the simulator |
| `MASSIVE_POLL_INTERVAL` [R9] | `15` | Seconds between polls. Use 15 on the free tier and 2–5 on paid tiers. Floor is 1s |

Keep `.env.example` in step with this table. The simulator's tick (0.5s) and event probability stay code-level constants on purpose, so students don't have to tune them.

---

## 11. SSE Streaming — `stream.py`

### 11.1 Wire format

```
retry: 1000

data: {"AAPL":{"ticker":"AAPL","price":190.52,"previous_price":190.48,"timestamp":1758640000.12,"change":0.04,"change_percent":0.021,"direction":"up","reference_price":190.0,"day_change_percent":0.2737},"GOOGL":{...}}

: keepalive

data: {...}
```

- **One `message` event per tick, holding a snapshot of all tickers** as an object keyed by ticker. It's idempotent: a client that reconnects or misses events is fully in sync again after the next event, so we don't need `Last-Event-ID` handling.
- **Tickers missing from the payload have been removed.** The frontend should replace its map rather than merge into it, so removed tickers disappear.
- `retry: 1000` tells `EventSource` to reconnect after 1 second.
- `: keepalive` is an SSE comment. `EventSource` ignores it, but it stops proxies and load balancers from closing an idle connection, which happens under Massive after hours [R5].

### 11.2 Implementation

```python
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
    "X-Accel-Buffering": "no",  # stop nginx / App Runner proxies from buffering
}


def create_stream_router(price_cache: PriceCache, interval: float = 0.5) -> APIRouter:
    # [R10] Build the router INSIDE the factory. The current code uses a
    # module-level router, so calling the factory twice (e.g. in tests)
    # registers /prices twice on one shared object.
    router = APIRouter(prefix="/api/stream", tags=["streaming"])

    @router.get("/prices")
    async def stream_prices(request: Request) -> StreamingResponse:
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
    heartbeat_every: float = 15.0,   # [R5]
) -> AsyncGenerator[str, None]:
    yield "retry: 1000\n\n"
    last_version = -1
    last_sent = time.monotonic()
    client = request.client.host if request.client else "unknown"
    logger.info("SSE client connected: %s", client)
    try:
        while not await request.is_disconnected():
            version = price_cache.version
            if version != last_version:
                last_version = version
                data = {t: u.to_dict() for t, u in price_cache.get_all().items()}
                yield f"data: {json.dumps(data)}\n\n"   # an empty {} is sent too: "no tickers"
                last_sent = time.monotonic()
            elif time.monotonic() - last_sent >= heartbeat_every:
                yield ": keepalive\n\n"
                last_sent = time.monotonic()
            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass
    finally:
        logger.info("SSE client disconnected: %s", client)
```

**Small behaviour change:** the current code skips the event when the cache is empty. Here an empty `{}` is sent, so removing the *last* watchlist ticker actually clears the UI.

### 11.3 Why poll the cache instead of pub/sub?

Each client polls `price_cache.version` every 500ms. The alternative is an `asyncio.Condition` or per-client queues fed by the writer. At our scale (one user, a few tabs, about 10–50 tickers) polling costs nothing, has no back-pressure or slow-consumer problems, and fixes the output rate at 2Hz whatever the source does. Consider pub/sub only if you ever need hundreds of concurrent clients.

### 11.4 Frontend contract (for the Frontend agent)

```ts
type PriceUpdate = {
  ticker: string; price: number; previous_price: number; timestamp: number; // unix s
  change: number; change_percent: number; direction: "up" | "down" | "flat";
  reference_price: number | null; day_change_percent: number;
};

const es = new EventSource("/api/stream/prices");
es.onopen    = () => setStatus("connected");                  // green dot
es.onerror   = () => setStatus(es.readyState === EventSource.CLOSED
                               ? "disconnected" : "reconnecting"); // red / yellow
es.onmessage = (e) => {
  const snapshot: Record<string, PriceUpdate> = JSON.parse(e.data);
  setPrices(snapshot);                       // replace, don't merge (removed tickers vanish)
  for (const u of Object.values(snapshot)) appendSparkline(u.ticker, u.timestamp, u.price);
};
```

Only flash the price when `u.timestamp` changes for that ticker. Under Massive, the same snapshot can be re-sent when some *other* ticker changes.

---

## 12. Integration with the Rest of the Backend

### 12.1 App lifecycle — `app/main.py`

`create_stream_router()` needs the cache when routers are mounted, which happens before `lifespan` runs. So the cache is created once at module level. The data source, which needs the event loop, is created and started inside `lifespan`:

```python
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles

from app.db import get_tracked_tickers, init_db  # DB layer: watchlist ∪ open positions
from app.market import PriceCache, create_market_data_source, create_stream_router

price_cache = PriceCache()  # one per process; shared by SSE and all routes


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()                                          # lazy schema + seed (PLAN §7)
    source = create_market_data_source(price_cache)
    await source.start(get_tracked_tickers())
    app.state.price_cache = price_cache
    app.state.market_source = source
    snapshot_task = start_portfolio_snapshot_task(price_cache)  # every 30s (PLAN §7)
    try:
        yield
    finally:
        snapshot_task.cancel()
        await source.stop()


app = FastAPI(title="FinAlly", lifespan=lifespan)
app.include_router(create_stream_router(price_cache))
# app.include_router(portfolio_router); watchlist_router; chat_router; health_router
app.mount("/", StaticFiles(directory="static", html=True), name="static")  # LAST
```

Mount the static files **after** every `/api` router, or the catch-all will shadow the API routes.

### 12.2 Dependency helpers — `app/deps.py`

```python
from fastapi import Request

from app.market import MarketDataSource, PriceCache


def get_price_cache(request: Request) -> PriceCache:
    return request.app.state.price_cache


def get_market_source(request: Request) -> MarketDataSource:
    return request.app.state.market_source
```

Routes declare `cache: PriceCache = Depends(get_price_cache)`. Tests override these with `app.dependency_overrides`.

### 12.3 Watchlist coordination

**What gets tracked** = watchlist tickers ∪ tickers with an open position. A held position must keep getting prices even after you un-watch it, otherwise portfolio valuation and selling break.

```python
# app/routes/watchlist.py
from fastapi import APIRouter, Depends, HTTPException

from app.market import normalize_ticker

router = APIRouter(prefix="/api/watchlist", tags=["watchlist"])


@router.post("", status_code=201)
async def add_to_watchlist(body: TickerIn, source=Depends(get_market_source),
                           cache=Depends(get_price_cache)):
    try:
        ticker = normalize_ticker(body.ticker)
    except ValueError as e:
        raise HTTPException(422, str(e))
    if not db.watchlist_add(ticker):                   # UNIQUE(user_id, ticker)
        raise HTTPException(409, f"{ticker} already in watchlist")
    await source.add_ticker(ticker)
    u = cache.get(ticker)                              # simulator: present; Massive: maybe not yet
    return {"ticker": ticker, "price": u.price if u else None}


@router.delete("/{ticker}", status_code=204)
async def remove_from_watchlist(ticker: str, source=Depends(get_market_source)):
    ticker = normalize_ticker(ticker)
    if not db.watchlist_remove(ticker):
        raise HTTPException(404, f"{ticker} not in watchlist")
    if not db.has_position(ticker):                    # keep pricing held tickers
        await source.remove_ticker(ticker)
```

Chat's `watchlist_changes` go through the **same** service functions as these routes, not a second copy of the code.

### 12.4 Trade execution: price lookup

A trade fills at `cache.get_price(ticker)`. If the ticker isn't tracked (for example the AI buys something off the watchlist), start tracking it and wait briefly for a price:

```python
# app/market/helpers.py (new, tiny)
import asyncio


async def ensure_price(source, cache, ticker: str, timeout: float = 3.0) -> float | None:
    """Make sure `ticker` is tracked and return its price, waiting up to `timeout`s."""
    if (p := cache.get_price(ticker)) is not None:
        return p
    await source.add_ticker(ticker)
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if (p := cache.get_price(ticker)) is not None:
            return p
        await asyncio.sleep(0.1)
    return None
```

```python
price = await ensure_price(source, cache, ticker)
if price is None:
    raise TradeError(f"No market price available for {ticker}")   # → 400 / chat error text
```

- Simulator: returns right away, because `add_ticker` seeds the cache.
- Massive: returns within about `min_poll_gap` thanks to [R8]. On the free tier that can exceed 3s, in which case the trade fails with a clear message and the ticker is now tracked, so a retry works. That trade-off is acceptable.

### 12.5 Portfolio valuation and snapshots

```python
prices = cache.get_all()   # take ONE snapshot and value everything against it
total = cash + sum(p.quantity * (prices[p.ticker].price if p.ticker in prices else p.avg_cost)
                   for p in positions)
```

Falling back to `avg_cost` when a price is missing (for example right after startup under Massive) avoids a phantom crash in total value. Use one `get_all()` snapshot per request so all positions are valued at the same moment.

### 12.6 Chat context

The LLM prompt builder reads `cache.get_all()` for the watchlist and position prices, and includes `day_change_percent` so the assistant can say things like "NVDA is up 2.1% today".

---

## 13. Error Handling & Edge Cases

| Situation | Behaviour |
|---|---|
| Empty watchlist at startup | `start([])` works. The simulator steps nothing and Massive skips polls. SSE sends `{}` |
| Invalid ticker (`"aa pl"`, `""`, 11+ characters) | `normalize_ticker` raises `ValueError` → 422 from the route |
| Lower-case ticker | Normalized to upper case everywhere [R4] |
| Unknown ticker under Massive | Tracked but never priced. Logged. Trades fail with "no price". The watchlist shows "—" |
| Unknown ticker under the simulator | Priced at a random $50–300 with default params (by design, for demos) |
| Invalid Massive key (401) or plan lacks the endpoint (403) | `start()` logs the error. The cache stays empty. Backoff slows retries. The UI shows no prices. **Log clearly at startup** so the user knows to fix `.env` |
| 429 rate limit | Backoff [R7]. Recovers on its own |
| Network drop | Same as 429 |
| Ticker removed mid-poll | Result dropped [R2] |
| Ticker removed from watchlist but position still held | Stays tracked (§12.3) |
| Snapshot with no `last_trade` (overnight) | Falls back to `min.close`, otherwise skipped. Last cached price stays |
| Exception in `GBMSimulator.step()` | Logged. Loop continues next tick |
| SSE client disconnects | `is_disconnected()` or `CancelledError` ends the generator. Nothing leaks |
| App shutdown | `lifespan` finally block → `source.stop()` cancels and awaits the task |
| Server restart | Simulator prices reset to seeds. Portfolio history in SQLite survives. This is acceptable for a simulation |

---

## 14. Testing

Current state: 73 tests in `backend/tests/market/`, all passing (`uv run --extra dev pytest`). The additions below cover the gaps found here and in the archived review.

### 14.1 Simulator: statistical sanity

```python
import numpy as np

from app.market.seed_prices import SEED_PRICES
from app.market.simulator import GBMSimulator


def test_full_default_set_builds_cholesky():
    sim = GBMSimulator(list(SEED_PRICES))
    assert sim._cholesky is not None and sim._cholesky.shape == (10, 10)


def test_prices_stay_positive_and_near_seed_over_an_hour():
    sim = GBMSimulator(["AAPL"], event_probability=0.0)
    for _ in range(7200):                     # 1 hour of 500ms ticks
        price = sim.step()["AAPL"]
        assert price > 0
    assert 0.9 * 190 < price < 1.1 * 190      # σ_hour ≈ 0.22·√(3600/5.9e6) ≈ 0.5%


def test_tech_pairs_are_correlated():
    np.random.seed(0)
    sim = GBMSimulator(["AAPL", "MSFT"], event_probability=0.0)
    prev = {"AAPL": 190.0, "MSFT": 420.0}
    ra, rm = [], []
    for _ in range(4000):
        sim.step()
        ra.append(np.log(sim.get_price("AAPL") / prev["AAPL"]))
        rm.append(np.log(sim.get_price("MSFT") / prev["MSFT"]))
        prev = {"AAPL": sim.get_price("AAPL"), "MSFT": sim.get_price("MSFT")}
    assert 0.5 < np.corrcoef(ra, rm)[0, 1] < 0.7
```

(Use `sim.get_price`, which is unrounded, for returns. The rounded `step()` output is too coarse for correlation tests.)

### 14.2 Cache

```python
def test_reference_price_defaults_to_first_price_and_persists():
    c = PriceCache()
    c.update("AAPL", 100.0)
    u = c.update("AAPL", 102.0)
    assert u.reference_price == 100.0 and u.day_change_percent == 2.0


def test_remove_bumps_version():
    c = PriceCache(); c.update("AAPL", 1.0)
    v = c.version; c.remove("AAPL")
    assert c.version == v + 1


def test_zero_timestamp_is_respected():
    assert PriceCache().update("X", 1.0, timestamp=0.0).timestamp == 0.0


def test_concurrent_writers():
    import threading
    c = PriceCache()
    def w(t): [c.update(t, float(i + 1)) for i in range(1000)]
    ts = [threading.Thread(target=w, args=(f"T{i}",)) for i in range(8)]
    [t.start() for t in ts]; [t.join() for t in ts]
    assert len(c) == 8 and c.version == 8000
```

### 14.3 Ticker normalization

```python
import pytest
from app.market import normalize_ticker

@pytest.mark.parametrize("raw,expected", [("aapl", "AAPL"), (" BRK.B ", "BRK.B"), ("bf-b", "BF-B")])
def test_normalize_ok(raw, expected):
    assert normalize_ticker(raw) == expected

@pytest.mark.parametrize("raw", ["", "  ", "1ABC", "AA PL", "TOOLONGTICKER", "$AAPL"])
def test_normalize_rejects(raw):
    with pytest.raises(ValueError):
        normalize_ticker(raw)


async def test_simulator_add_ticker_is_case_insensitive():
    cache = PriceCache(); src = SimulatorDataSource(cache)
    await src.start(["AAPL"])
    await src.add_ticker("aapl")
    assert src.get_tickers() == ["AAPL"]
    await src.stop()
```

### 14.4 Massive: use **real SDK models**, not `MagicMock` [R1]

```python
from massive.rest.models import TickerSnapshot

from app.market.massive_client import MassiveDataSource, parse_snapshot


def make_snap(ticker="AAPL", price=191.23, t_ns=1_675_190_399_000_000_000, prev_close=189.5):
    d = {"ticker": ticker, "prevDay": {"c": prev_close}}
    if price is not None:
        d["lastTrade"] = {"T": ticker, "p": price, "s": 100, "t": t_ns}
    return TickerSnapshot.from_dict(d)


def test_parse_real_snapshot():
    q = parse_snapshot(make_snap())
    assert (q.ticker, q.price, q.reference_price) == ("AAPL", 191.23, 189.5)
    assert q.timestamp == 1_675_190_399.0            # ns → s


def test_parse_missing_trade_returns_none():
    assert parse_snapshot(make_snap(price=None)) is None


async def test_poll_writes_cache_with_real_models():   # regression test for [R1]
    cache = PriceCache()
    src = MassiveDataSource("key", cache)
    src._client = object()
    src._tickers = ["AAPL"]
    src._fetch_snapshots = lambda tickers: [make_snap()]
    await src._poll_once()
    assert cache.get_price("AAPL") == 191.23


async def test_removed_during_poll_is_not_resurrected():  # [R2]
    cache = PriceCache()
    src = MassiveDataSource("key", cache)
    src._client = object(); src._tickers = ["AAPL", "GOOGL"]

    def slow_fetch(tickers):
        src._tickers.remove("GOOGL")                 # simulate remove during the await
        return [make_snap("AAPL"), make_snap("GOOGL", 170.0)]

    src._fetch_snapshots = slow_fetch
    await src._poll_once()
    assert "GOOGL" not in cache and "AAPL" in cache


async def test_failure_increments_backoff():          # [R7]
    src = MassiveDataSource("key", PriceCache())
    src._client = object(); src._tickers = ["AAPL"]
    def boom(_): raise RuntimeError("429")
    src._fetch_snapshots = boom
    await src._poll_once(); await src._poll_once()
    assert src._failures == 2
```

### 14.5 SSE generator (no server needed)

```python
import json

from app.market.cache import PriceCache
from app.market.stream import create_stream_router, generate_price_events


class FakeRequest:
    def __init__(self, polls: int):
        self._left = polls
        self.client = None

    async def is_disconnected(self) -> bool:
        self._left -= 1
        return self._left < 0


async def collect(gen):
    return [chunk async for chunk in gen]


async def test_stream_emits_retry_then_snapshot():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    out = await collect(generate_price_events(cache, FakeRequest(polls=1), interval=0))
    assert out[0] == "retry: 1000\n\n"
    payload = json.loads(out[1].removeprefix("data: ").strip())
    assert payload["AAPL"]["price"] == 190.0 and payload["AAPL"]["direction"] == "flat"


async def test_stream_skips_unchanged_version():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    out = await collect(generate_price_events(cache, FakeRequest(polls=3), interval=0))
    assert sum(c.startswith("data:") for c in out) == 1


async def test_stream_heartbeat():
    cache = PriceCache(); cache.update("AAPL", 190.0)
    out = await collect(generate_price_events(cache, FakeRequest(polls=3),
                                              interval=0, heartbeat_every=0))
    assert ": keepalive\n\n" in out


def test_router_factory_is_reentrant():               # [R10]
    r1, r2 = create_stream_router(PriceCache()), create_stream_router(PriceCache())
    assert r1 is not r2 and len(r1.routes) == 1
```

### 14.6 Optional live smoke test against Massive

```python
import os
import pytest

@pytest.mark.skipif(not os.getenv("MASSIVE_API_KEY"), reason="needs a real key")
async def test_live_massive_smoke():
    cache = PriceCache()
    src = MassiveDataSource(os.environ["MASSIVE_API_KEY"], cache)
    await src.start(["AAPL", "MSFT"])
    await src.stop()
    assert cache.get_price("AAPL") and cache.get_price("AAPL") > 0
```

This is the test that would have caught [R1]. Run it by hand before a demo that uses real data. Never run it in CI.

---

## 15. Revision Checklist

Changes this design makes to the current `backend/app/market/` code, in priority order:

| ID | Severity | File(s) | Change | Test |
|---|---|---|---|---|
| **R1** | **Bug: Massive never prices anything** | `massive_client.py` | Parse `last_trade.sip_timestamp` (ns ÷ 1e9) instead of the non-existent `last_trade.timestamp`. Move parsing into `parse_snapshot()` | §14.4 with real `TickerSnapshot` models, plus the §14.6 smoke test |
| R2 | Bug: removed tickers come back | `massive_client.py` | After the `await`, write only tickers that are still tracked. Pass `requested` to the thread | §14.4 |
| R3 | Feature gap (PLAN §10 daily %) | `models.py`, `cache.py`, `massive_client.py` | Add `reference_price` and `day_change_percent`. Massive supplies `prev_day.close`; the simulator uses the first price | §14.2 |
| R4 | Bug: simulator is case-sensitive | `models.py`, both sources, `__init__.py` | `normalize_ticker()` shared with the API routes | §14.3 |
| R5 | Robustness | `stream.py` | `: keepalive` comment every 15s while idle | §14.5 |
| R6 | Correctness nits | `cache.py` | `timestamp is None` check. `remove()` bumps `version`. `version` read under the lock | §14.2 |
| R7 | Robustness | `massive_client.py` | Exponential backoff (up to 8×) on consecutive failures | §14.4 |
| R8 | UX | `massive_client.py` | Early poll on `add_ticker`, rate-limited by `min_poll_gap` | Manual / §14.6 |
| R9 | Config | `factory.py`, `.env.example` | `MASSIVE_POLL_INTERVAL` env var | `test_factory.py` |
| R10 | Test hygiene | `stream.py` | Create `APIRouter` inside `create_stream_router()`. Rename `_generate_events` → `generate_price_events` (public, so tests can import it) | §14.5 |

Also update `test_massive.py`: replace its `MagicMock` snapshots with `TickerSnapshot.from_dict(...)` so the tests exercise the real attribute names. Update `planning/MARKET_DATA_SUMMARY.md` and `backend/CLAUDE.md` once these changes land.

**Definition of done for the market data layer:** all revisions merged; `uv run --extra dev pytest` and `ruff check` pass; the §14.6 smoke test passes with a real key; and `uv run market_data_demo.py` shows moving prices.
