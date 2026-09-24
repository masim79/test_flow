# Market Simulator Design

Approach and code structure for simulating realistic stock prices when no Massive API key is configured.

**Status**: Implemented in `backend/app/market/simulator.py` and `seed_prices.py`. This document is synced to the actual code as of the last refresh.

## Overview

The simulator uses **Geometric Brownian Motion (GBM)** to generate realistic stock price paths. GBM is the standard model underlying Black-Scholes option pricing — prices evolve continuously with random noise, can't go negative, and exhibit the lognormal distribution seen in real markets.

Updates run at ~500ms intervals, producing a continuous stream of price changes that feel alive.

## GBM Math

At each time step, a stock price evolves as:

```
S(t+dt) = S(t) * exp((mu - sigma^2/2) * dt + sigma * sqrt(dt) * Z)
```

Where:
- `S(t)` = current price
- `mu` = annualized drift (expected return), e.g. 0.05 (5%)
- `sigma` = annualized volatility, e.g. 0.20 (20%)
- `dt` = time step as fraction of a trading year
- `Z` = standard normal random variable (drawn from N(0,1))

The actual `dt` is computed, not hardcoded, from a trading-seconds-per-year constant:

```python
TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800 (252 days * 6.5h * 3600s)
DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8, for 500ms ticks
```

This tiny `dt` produces small, realistic per-tick moves.

## Correlated Moves

Real stocks don't move independently — tech stocks tend to move together, etc. We use a **Cholesky decomposition** of a correlation matrix to generate correlated random draws.

Given a correlation matrix `C`, compute `L = cholesky(C)`. Then for independent standard normals `Z_independent`:
```
Z_correlated = L @ Z_independent
```

Correlation groups (`backend/app/market/seed_prices.py`, `CORRELATION_GROUPS`):
- **Tech**: AAPL, GOOGL, MSFT, AMZN, META, NVDA, NFLX — `INTRA_TECH_CORR = 0.6`
- **Finance**: JPM, V — `INTRA_FINANCE_CORR = 0.5`
- **TSLA**: checked *before* the tech-group check even though it isn't in the `tech` set — `TSLA_CORR = 0.3` against everything, it does its own thing
- **Cross-group / unknown tickers**: `CROSS_GROUP_CORR = 0.3`

## Random Events

Every step, each ticker has a small probability (default `event_probability=0.001`) of a random event — a sudden 2-5% move. This adds drama and makes the dashboard visually interesting.

```python
if random.random() < event_probability:
    shock_magnitude = random.uniform(0.02, 0.05)
    shock_sign = random.choice([-1, 1])
    price *= 1 + shock_magnitude * shock_sign
```

With 10 tickers at ~2 ticks/sec, expect an event somewhere roughly every 50 seconds.

## Seed Prices & Per-Ticker Parameters

`backend/app/market/seed_prices.py`:

```python
SEED_PRICES: dict[str, float] = {
    "AAPL": 190.00,
    "GOOGL": 175.00,
    "MSFT": 420.00,
    "AMZN": 185.00,
    "TSLA": 250.00,
    "NVDA": 800.00,
    "META": 500.00,
    "JPM": 195.00,
    "V": 280.00,
    "NFLX": 600.00,
}

# sigma: annualized volatility (higher = more price movement)
# mu: annualized drift / expected return
TICKER_PARAMS: dict[str, dict[str, float]] = {
    "AAPL": {"sigma": 0.22, "mu": 0.05},
    "GOOGL": {"sigma": 0.25, "mu": 0.05},
    "MSFT": {"sigma": 0.20, "mu": 0.05},
    "AMZN": {"sigma": 0.28, "mu": 0.05},
    "TSLA": {"sigma": 0.50, "mu": 0.03},  # High volatility
    "NVDA": {"sigma": 0.40, "mu": 0.08},  # High volatility, strong drift
    "META": {"sigma": 0.30, "mu": 0.05},
    "JPM": {"sigma": 0.18, "mu": 0.04},  # Low volatility (bank)
    "V": {"sigma": 0.17, "mu": 0.04},  # Low volatility (payments)
    "NFLX": {"sigma": 0.35, "mu": 0.05},
}

# Default for tickers not in the list above (dynamically added)
DEFAULT_PARAMS: dict[str, float] = {"sigma": 0.25, "mu": 0.05}

CORRELATION_GROUPS: dict[str, set[str]] = {
    "tech": {"AAPL", "GOOGL", "MSFT", "AMZN", "META", "NVDA", "NFLX"},
    "finance": {"JPM", "V"},
}

INTRA_TECH_CORR = 0.6
INTRA_FINANCE_CORR = 0.5
CROSS_GROUP_CORR = 0.3
TSLA_CORR = 0.3
```

Tickers added dynamically (not in the seed list) start at a random price between $50-$300, using `DEFAULT_PARAMS` for volatility/drift.

## Implementation

`backend/app/market/simulator.py`, class `GBMSimulator`:

```python
class GBMSimulator:
    """Geometric Brownian Motion simulator for correlated stock prices."""

    TRADING_SECONDS_PER_YEAR = 252 * 6.5 * 3600  # 5,896,800
    DEFAULT_DT = 0.5 / TRADING_SECONDS_PER_YEAR   # ~8.48e-8

    def __init__(
        self,
        tickers: list[str],
        dt: float = DEFAULT_DT,
        event_probability: float = 0.001,
    ) -> None:
        self._dt = dt
        self._event_prob = event_probability
        self._tickers: list[str] = []
        self._prices: dict[str, float] = {}
        self._params: dict[str, dict[str, float]] = {}
        self._cholesky: np.ndarray | None = None

        for ticker in tickers:
            self._add_ticker_internal(ticker)   # no rebuild per-ticker during init
        self._rebuild_cholesky()                # single rebuild after all seeded

    def step(self) -> dict[str, float]:
        """Advance all tickers by one time step. Returns {ticker: new_price}.
        Hot path — called every 500ms."""
        n = len(self._tickers)
        if n == 0:
            return {}

        z_independent = np.random.standard_normal(n)
        z_correlated = self._cholesky @ z_independent if self._cholesky is not None else z_independent

        result: dict[str, float] = {}
        for i, ticker in enumerate(self._tickers):
            params = self._params[ticker]
            mu, sigma = params["mu"], params["sigma"]

            drift = (mu - 0.5 * sigma**2) * self._dt
            diffusion = sigma * math.sqrt(self._dt) * z_correlated[i]
            self._prices[ticker] *= math.exp(drift + diffusion)

            if random.random() < self._event_prob:
                shock_magnitude = random.uniform(0.02, 0.05)
                shock_sign = random.choice([-1, 1])
                self._prices[ticker] *= 1 + shock_magnitude * shock_sign

            result[ticker] = round(self._prices[ticker], 2)

        return result

    def add_ticker(self, ticker: str) -> None:
        """Add a ticker to the simulation. Rebuilds the correlation matrix."""
        if ticker in self._prices:
            return
        self._add_ticker_internal(ticker)
        self._rebuild_cholesky()

    def remove_ticker(self, ticker: str) -> None:
        """Remove a ticker from the simulation. Rebuilds the correlation matrix."""
        if ticker not in self._prices:
            return
        self._tickers.remove(ticker)
        del self._prices[ticker]
        del self._params[ticker]
        self._rebuild_cholesky()

    def get_price(self, ticker: str) -> float | None:
        return self._prices.get(ticker)

    def get_tickers(self) -> list[str]:
        return list(self._tickers)

    def _add_ticker_internal(self, ticker: str) -> None:
        """Add a ticker without rebuilding Cholesky (for batch initialization)."""
        if ticker in self._prices:
            return
        self._tickers.append(ticker)
        self._prices[ticker] = SEED_PRICES.get(ticker, random.uniform(50.0, 300.0))
        self._params[ticker] = TICKER_PARAMS.get(ticker, dict(DEFAULT_PARAMS))

    def _rebuild_cholesky(self) -> None:
        """O(n^2) but n < 50, called only when tickers are added/removed."""
        n = len(self._tickers)
        if n <= 1:
            self._cholesky = None
            return

        corr = np.eye(n)
        for i in range(n):
            for j in range(i + 1, n):
                rho = self._pairwise_correlation(self._tickers[i], self._tickers[j])
                corr[i, j] = rho
                corr[j, i] = rho

        self._cholesky = np.linalg.cholesky(corr)

    @staticmethod
    def _pairwise_correlation(t1: str, t2: str) -> float:
        """Same tech sector: 0.6. Same finance sector: 0.5.
        TSLA with anything: 0.3 (checked first — it's excluded from the tech set
        on purpose so it never gets the 0.6 intra-tech correlation).
        Cross-sector / unknown: 0.3."""
        tech = CORRELATION_GROUPS["tech"]
        finance = CORRELATION_GROUPS["finance"]

        if t1 == "TSLA" or t2 == "TSLA":
            return TSLA_CORR
        if t1 in tech and t2 in tech:
            return INTRA_TECH_CORR
        if t1 in finance and t2 in finance:
            return INTRA_FINANCE_CORR
        return CROSS_GROUP_CORR
```

`GBMSimulator` is pure computation — no asyncio, no cache access. It's wrapped by `SimulatorDataSource` (see `MARKET_INTERFACE.md`), which owns the asyncio loop and writes `step()`'s output into the shared `PriceCache` every `update_interval` seconds.

## File Structure

```
backend/
  app/
    market/
      simulator.py       # GBMSimulator class + SimulatorDataSource (asyncio wrapper)
      seed_prices.py       # SEED_PRICES, TICKER_PARAMS, DEFAULT_PARAMS, CORRELATION_GROUPS, correlation constants
```

`SimulatorDataSource` (in the same `simulator.py` file) additionally depends on `interface.py` (implements `MarketDataSource`) and `cache.py` (writes to `PriceCache`) — see `MARKET_INTERFACE.md` for those.

## Behavior Notes

- Prices never go negative (GBM is multiplicative — `exp()` is always positive)
- The tiny `dt` produces sub-cent moves per tick, which accumulate naturally over time
- With `sigma=0.50` (TSLA), a day of simulated trading produces roughly the right intraday range
- The correlation matrix must be positive semi-definite — Cholesky decomposition guarantees this for valid correlation matrices
- Random events happen ~0.1% of steps = roughly once every 500 seconds per ticker. With 10 tickers, expect an event somewhere roughly every 50 seconds — enough to keep it interesting
- When a new ticker is added mid-session, the Cholesky matrix is rebuilt. This is O(n^2) but n is small (<50 tickers)
- Prices are rounded to 2 decimals both inside `step()`'s return value and again by `PriceCache.update()` — redundant but harmless
