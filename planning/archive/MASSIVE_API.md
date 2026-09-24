# Massive API Reference (formerly Polygon.io)

Reference documentation for the Massive (formerly Polygon.io) REST API as used in FinAlly. Verified against the `massive` Python package actually installed in `backend/.venv` (not just the public README), so field names below match what the client returns at runtime.

## Overview

- **Base URL**: `https://api.massive.com` (legacy `https://api.polygon.io` still supported for an extended transition period; Polygon.io rebranded to Massive.com on 2025-10-30)
- **Python package**: `massive` (install via `pip install -U massive` / `uv add massive`)
- **Min Python version**: 3.9+
- **Auth**: API key via `MASSIVE_API_KEY` env var or passed to `RESTClient(api_key=...)`
- **Auth header**: `Authorization: Bearer <API_KEY>` (the client handles this automatically)

## Rate Limits

| Tier | Limit |
|------|-------|
| Free | 5 requests/minute |
| Paid (all tiers) | Unlimited (recommended: stay under 100 req/s) |

For FinAlly, we poll on a timer. Free tier: poll every 15s. Paid: poll every 2-5s. Confirm current limits against your plan at massive.com before relying on these numbers in production.

## Client Initialization

```python
from massive import RESTClient

# Reads MASSIVE_API_KEY from environment automatically
client = RESTClient()

# Or pass explicitly
client = RESTClient(api_key="your_key_here")
```

## Endpoints Used in FinAlly

### 1. Snapshot — All Tickers (Primary Endpoint)

Gets current prices for multiple tickers in a **single API call**. This is the main endpoint we use for polling.

**REST**: `GET /v2/snapshot/locale/us/markets/stocks/tickers?tickers=AAPL,GOOGL,MSFT`

**Python client** (verified signature — `market_type` and `tickers` both accepted as keyword args):

```python
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

client = RESTClient()

# Get snapshots for specific tickers (one API call)
snapshots = client.get_snapshot_all(
    market_type=SnapshotMarketType.STOCKS,   # or the plain string "stocks"
    tickers=["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"],
)

for snap in snapshots:
    print(f"{snap.ticker}: ${snap.last_trade.price}")
    print(f"  Day OHLC: O={snap.day.open} H={snap.day.high} L={snap.day.low} C={snap.day.close}")
    print(f"  Prev day close: {snap.prev_day.close}")
    print(f"  Today's change: {snap.todays_change} ({snap.todays_change_percent}%)")
    print(f"  Volume: {snap.day.volume}")
```

**Actual `TickerSnapshot` fields** (from `massive.rest.models.snapshot.TickerSnapshot`):

| Field | Type | Notes |
|---|---|---|
| `ticker` | `str` | |
| `day` | `Agg` | today's aggregate bar (see below) |
| `prev_day` | `Agg` | previous session's aggregate bar |
| `last_trade` | `LastTrade` | most recent trade |
| `last_quote` | `LastQuote` | most recent NBBO quote |
| `min` | `MinuteSnapshot` | latest minute bar |
| `todays_change` | `float` | absolute change vs. previous close |
| `todays_change_percent` | `float` | percent change vs. previous close |
| `updated` | `int` | Unix nanoseconds |
| `fair_market_value` | `float` | business-tier only |

**`Agg`** (used for both `day` and `prev_day`) has: `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`, `transactions`, `otc`.

> **There is no `previous_close`, `change`, or `change_percent` field on `Agg`.** Day-over-day change comes from `TickerSnapshot.todays_change` / `todays_change_percent` directly, or by computing `day.close - prev_day.close` yourself. An earlier draft of this doc (and the current `massive_client.py`) assumed `day.previous_close` / `day.change_percent` existed — they don't on the installed client version. See "Known Issue" below.

**`last_trade`** (snapshot-scoped `LastTrade`, `massive.rest.models.snapshot`) has: `ticker`, `price`, `size`, `exchange`, `sip_timestamp`, `participant_timestamp`, `trf_timestamp`, `sequence_number`, `conditions`, `correction`, `id`, `trf_id`, `tape`.

> **There is no plain `.timestamp` field on `last_trade`.** Use `sip_timestamp` (exchange-reported) or `participant_timestamp`. Both are `Optional[int]`.

**`last_quote`** has: `bid_price`, `ask_price`, `bid_size`, `ask_size`, `bid_exchange`, `ask_exchange`, `sip_timestamp`, `participant_timestamp`, plus `conditions`/`indicators`/`tape`.

### 2. Single Ticker Snapshot

For getting detailed data on one ticker (e.g., when user clicks a ticker for the detail view).

**Python client**:
```python
snapshot = client.get_snapshot_ticker(
    market_type=SnapshotMarketType.STOCKS,
    ticker="AAPL",
)

print(f"Price: ${snapshot.last_trade.price}")
print(f"Bid/Ask: ${snapshot.last_quote.bid_price} / ${snapshot.last_quote.ask_price}")
print(f"Day range: ${snapshot.day.low} - ${snapshot.day.high}")
```

Same `TickerSnapshot` shape as above — note `bid_price`/`ask_price`, not `bid`/`ask`.

### 3. Previous Close

Gets the previous day's OHLC for a ticker. Useful for seed prices.

**REST**: `GET /v2/aggs/ticker/{ticker}/prev`

**Python client** — returns a **single `PreviousCloseAgg` object, not a list**:

```python
prev = client.get_previous_close_agg(ticker="AAPL")

print(f"Previous close: ${prev.close}")
print(f"OHLC: O={prev.open} H={prev.high} L={prev.low} C={prev.close}")
print(f"Volume: {prev.volume}")
```

**`PreviousCloseAgg` fields**: `ticker`, `open`, `high`, `low`, `close`, `volume`, `vwap`, `timestamp`.

### 4. Aggregates (Bars)

Historical OHLCV bars over a date range. Not needed for live polling but useful if we add historical charts.

**REST**: `GET /v2/aggs/ticker/{ticker}/range/{multiplier}/{timespan}/{from}/{to}`

**Python client** (`list_aggs` returns an iterator of `Agg`):
```python
aggs = []
for a in client.list_aggs(
    ticker="AAPL",
    multiplier=1,
    timespan="day",
    from_="2024-01-01",
    to="2024-01-31",
    limit=50000,
):
    aggs.append(a)

for a in aggs:
    print(f"Timestamp: {a.timestamp}, O={a.open} H={a.high} L={a.low} C={a.close} V={a.volume}")
```

### 5. Last Trade / Last Quote

Individual endpoints for the most recent trade or NBBO quote. Note these use a **different, differently-named** `LastTrade`/`LastQuote` model than the one nested in a snapshot (`massive.rest.models.trades.LastTrade` / `massive.rest.models.quotes.LastQuote`) — field names happen to line up, but they're separate classes.

```python
# Last trade
trade = client.get_last_trade(ticker="AAPL")
print(f"Last trade: ${trade.price} x {trade.size}")

# Last NBBO quote
quote = client.get_last_quote(ticker="AAPL")
print(f"Bid: ${quote.bid_price} x {quote.bid_size}")
print(f"Ask: ${quote.ask_price} x {quote.ask_size}")
```

## How FinAlly Uses the API

The Massive poller (`backend/app/market/massive_client.py`, class `MassiveDataSource`) runs as a background `asyncio` task:

1. On `start()`, does an immediate synchronous poll so the cache has data right away, then schedules a recurring poll every `poll_interval` seconds (default 15s)
2. Each poll collects the current watched-ticker list and calls `get_snapshot_all(market_type=SnapshotMarketType.STOCKS, tickers=self._tickers)` via `asyncio.to_thread` (the Massive client is synchronous, so it's offloaded to a thread to avoid blocking the event loop)
3. For each returned snapshot, extracts `last_trade.price` and writes to the shared `PriceCache`
4. A failed poll (bad key, rate limit, network error) is logged and swallowed — the loop just retries on the next interval, it never crashes the app
5. `add_ticker()` / `remove_ticker()` mutate the in-memory ticker list; the change takes effect on the next poll cycle

```python
import asyncio
from massive import RESTClient
from massive.rest.models import SnapshotMarketType

async def poll_massive(api_key: str, get_tickers, price_cache, interval: float = 15.0):
    """Simplified sketch of the MassiveDataSource poll loop."""
    client = RESTClient(api_key=api_key)

    while True:
        tickers = get_tickers()
        if tickers:
            snapshots = await asyncio.to_thread(
                client.get_snapshot_all,
                market_type=SnapshotMarketType.STOCKS,
                tickers=tickers,
            )
            for snap in snapshots:
                price_cache.update(
                    ticker=snap.ticker,
                    price=snap.last_trade.price,
                    timestamp=snap.last_trade.sip_timestamp,  # nanoseconds — convert before use
                )

        await asyncio.sleep(interval)
```

### Known Issue — code/API drift found during this doc refresh (2026-09-23)

Inspecting the `massive` package actually installed in `backend/.venv` shows the current `massive_client.py` reads two fields that **do not exist** on the installed client's response models:

- `snap.last_trade.timestamp` — the snapshot-scoped `LastTrade` model has no `timestamp` attribute; the real fields are `sip_timestamp` and `participant_timestamp` (and units are almost certainly nanoseconds, not the milliseconds the code assumes — verify against a live response before trusting the conversion).
- Nothing in the shipped code reads `day.previous_close` today, but be aware `Agg` (`day`/`prev_day`) has no such field either — use `TickerSnapshot.todays_change_percent` or `day.close - prev_day.close` instead.

Because `backend/tests/market/test_massive.py` mocks the Massive client, these tests pass even though the field access would raise `AttributeError` against the real API — `massive_client.py` catches `(AttributeError, TypeError)` per-snapshot and logs a warning, so in production this would silently skip every ticker on every poll rather than crash. This is a real bug in `massive_client.py`, not just a doc issue — flagged here, not fixed, since fixing code was out of scope for this documentation pass.

## Error Handling

The client raises exceptions for HTTP errors:
- **401**: Invalid API key
- **403**: Insufficient permissions (plan doesn't include the endpoint)
- **429**: Rate limit exceeded (free tier: 5 req/min)
- **5xx**: Server errors (client has built-in retry with 3 retries by default)

## Notes

- The snapshot endpoint returns data for **all requested tickers in one call** — this is critical for staying within rate limits on the free tier
- Timestamps from the API are generally nanosecond- or millisecond-precision depending on the field (`sip_timestamp`/`participant_timestamp` are nanoseconds; `PreviousCloseAgg.timestamp` and `Agg.timestamp` are milliseconds) — check the specific field, don't assume
- During market closed hours, `last_trade.price` reflects the last traded price (may include after-hours)
- The `day` object resets at market open; during pre-market, values may be from the previous session
