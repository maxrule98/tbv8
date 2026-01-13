# TBV8 (NautilusTrader-based) - Dev README

TBV8 is a multi-venue trading system with:

- Live market data recording (Hyperliquid + MEXC)
- Historical backfill (Binance Spot as research/backtest feed)
- Local aggregation (1m -> 5m/15m/1h/4h/etc) into SQLite
- A simple backtester that reads bars from SQLite

This README focuses on **runnable commands**.

---

## Prereqs

- Python managed via `uv`
- Repo venv created (whatever you already did to get `uv run` working)
- Config files present:
  - `config/venues/*.yaml` (execution / live venues - e.g. hyperliquid, mexc)
  - `config/strategies/*.yaml` (strategy runtime config - symbol/timeframe/routing/risk)
  - `config/history.yaml` (backtest/backfill market-data venue + date range)
  - `config/data.yaml` (SQLite path)

---

## Quick start

### 1) Run the live recorder (writes trades/bbo and live bars_1m)

```bash
uv run python -m apps.recorder.main
```

Stop with Ctrl+C.

2. Backfill historical 1m data (Binance Spot -> ohlcv_1m)

```bash
uv run python -m apps.backfiller.main
```

This will populate SQLite with historical 1m candles (example: BTC/USDT from 2017-08-17 to now).

3. Aggregate 1m -> higher timeframes (writes bars_5m, bars_1h, etc.)

Example:

```bash
uv run python -m apps.aggregator.main --venue binance_spot --symbol BTC/USDT --timeframes 5m,15m,1h,4h --chunk-days 7
```

4. Run a backtest (reads bars\_{timeframe} from SQLite)

```bash
uv run python -m apps.backtester.main
```

---

## Common CLI commands

### Recorder

Run:

```bash
uv run python -m apps.recorder.main
```

Expected behavior:

- Connects to enabled WS data clients (e.g. hyperliquid + mexc)
- Writes:
- trades
- bbo
- bars_1m (live aggregated bars)

---

### Backfiller (historical 1m)

Run:

```bash
uv run python -m apps.backfiller.main
```

Expected behavior:

- Uses config/history.yaml to decide:
- market_data_venue (e.g. binance_spot)
- start_date / end_date (or end=NOW)
- Writes:
- ohlcv_1m
- history_coverage

---

### Aggregator (1m -> N timeframes)

Run:

```bash
uv run python -m apps.aggregator.main --venue <venue> --symbol <symbol> --timeframes <csv> --chunk-days <n>
```

Example:

```bash
uv run python -m apps.aggregator.main --venue binance_spot --symbol BTC/USDT --timeframes 5m,15m,1h,4h --chunk-days 7
```

Writes:

- bars_5m
- bars_15m
- bars_1h
- bars_4h
- (and any other requested TFs)

---

### Backtester

Run:

```bash
uv run python -m apps.backtester.main
```

Expected behavior:

- Loads strategy config (config/strategies/...)
- Loads backtest bars from SQLite using:
- venue = cfg.history.market_data_venue (e.g. binance_spot)
- symbol = cfg.strategy.symbol (e.g. BTC/USDT)
- timeframe = cfg.strategy.timeframe (e.g. 5m)
- start/end from cfg.history.start_date / cfg.history.end_date

---

### Database (SQLite) - inspection commands

List tables:

```bash
sqlite3 data/tbv8.sqlite ".tables"
```

Check row counts:

```bash
sqlite3 data/tbv8.sqlite "select count(_) from ohlcv_1m;"
sqlite3 data/tbv8.sqlite "select count(_) from bars*1m;"
sqlite3 data/tbv8.sqlite "select count(*) from bars*5m;"
sqlite3 data/tbv8.sqlite "select count(*) from bars_1h;"
```

Verify historical coverage for Binance spot backfill:

```bash
sqlite3 data/tbv8.sqlite "
select
min(datetime(ts_ms/1000,'unixepoch')),
max(datetime(ts_ms/1000,'unixepoch')),
count(\*)
from ohlcv_1m
where venue='binance_spot' and symbol='BTC/USDT';
"
```

Latest bar timestamps per venue:

```bash
sqlite3 data/tbv8.sqlite "
select venue, max(datetime(ts_ms/1000,'unixepoch')) as last_bar_time, count(\*)
from bars_1m
where symbol='BTC/USDT'
group by venue;
"
```

---

### Config reference (what matters for running commands)

config/data.yaml

db_path: data/tbv8.sqlite

config/history.yaml

history:
market_data_venue: binance_spot
start_date: "2017-08-17T00:00:00Z"
end_date: null

config/strategies/btc_usdt_perp_v1.yaml (example shape)

strategy_id: btc_usdt_perp_v1
symbol: BTC/USDT
timeframe: 5m

routing:
primary: hyperliquid
secondary: mexc
mode: PRIMARY_LIVE_SECONDARY_SHADOW

risk:
max_position_btc: 0.01
max_daily_loss_usd: 10.0
max_leverage: 2.0

Notes:

- routing.\* is for execution / live routing.
- Backtests use history.market_data_venue for data (e.g. binance_spot).

---

## Troubleshooting

Use:

```bash
uv run python
```

Always run python commands through uv:

```bash
uv run python -m <module>
```

Module import errors (No module named 'apps...' / packages...)

Make sure these exist:

```bash
touch apps/**init**.py
touch apps/backtester/**init**.py
touch packages/**init**.py
touch packages/backtester/**init**.py
```

---

## Typical workflows

Live-record a bit, then backtest on long history 1. Backfill 1m (once):

```bash
uv run python -m apps.backfiller.main
```

2. Aggregate timeframes:

```bash
uv run python -m apps.aggregator.main --venue binance_spot --symbol BTC/USDT --timeframes 5m,15m,1h,4h --chunk-days 7
```

3. Backtest:

```bash
uv run python -m apps.backtester.main
```

Live market data recording (perps venues)

```bash
uv run python -m apps.recorder.main
```

---

Repo map (high level)

- apps/recorder/ - live WS recorder -> SQLite
- apps/backfiller/ - historical fetcher -> ohlcv\*1m
- apps/aggregator/ - local aggregation -> bars\*{tf}
- apps/backtester/ - backtest runner
- packages/common/ - config + backfill / aggregation / store helpers
- packages/backtester/ - backtest engine, db loader, strategies
