# TBV8 - Multi-Venue Trading System

[![Python](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![NautilusTrader](https://img.shields.io/badge/NautilusTrader-1.222+-green.svg)](https://nautilustrader.io/)
[![uv](https://img.shields.io/badge/uv-package%20manager-purple.svg)](https://github.com/astral-sh/uv)

TBV8 is a multi-venue trading system built on NautilusTrader with comprehensive market data management and backtesting capabilities.

## Features

- **Live Market Data Recording** - Real-time data capture from Hyperliquid and MEXC
- **Historical Backfill** - Binance Spot data for research and backtesting
- **Local Aggregation** - Automatic timeframe conversion (1m → 5m/15m/1h/4h/etc) stored in SQLite
- **Backtesting Engine** - Replay historical bars from SQLite for strategy validation

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Commands](#commands)
  - [Recorder](#recorder)
  - [Backfiller](#backfiller)
  - [Aggregator](#aggregator)
  - [Backtester](#backtester)
  - [Database Inspection](#database-inspection)
- [Configuration](#configuration)
- [Typical Workflows](#typical-workflows)
- [Repository Structure](#repository-structure)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

Before running TBV8, ensure you have:

- **Python 3.12+** managed via [uv](https://github.com/astral-sh/uv)
- **Repository venv** created (should be ready if `uv run` works)
- **Configuration files** present:
  - `config/venues/*.yaml` - Execution/live venues (e.g., Hyperliquid, MEXC)
  - `config/strategies/*.yaml` - Strategy runtime config (symbol/timeframe/routing/risk)
  - `config/history.yaml` - Backtest/backfill market data venue and date range
  - `config/data.yaml` - SQLite database path

---

## Quick Start

### 1. Run the Live Recorder

Records trades, BBO (best bid/offer), and live 1-minute bars:

```bash
uv run python -m apps.recorder.main
```

Stop with `Ctrl+C`.

### 2. Backfill Historical Data

Populates SQLite with historical 1-minute candles (e.g., BTC/USDT from 2017-08-17 to present):

```bash
uv run python -m apps.backfiller.main
```

### 3. Aggregate Timeframes

Converts 1-minute data to higher timeframes (5m, 15m, 1h, 4h, etc.):

```bash
uv run python -m apps.aggregator.main \
  --venue binance_spot \
  --symbol BTC/USDT \
  --timeframes 5m,15m,1h,4h \
  --chunk-days 7
```

### 4. Run a Backtest

Reads bars from SQLite and executes your strategy:

```bash
uv run python -m apps.backtester.main
```

---

## Commands

### Recorder

Connects to enabled WebSocket data clients and records live market data.

**Command:**

```bash
uv run python -m apps.recorder.main
```

**What it does:**

- Connects to enabled WS data clients (e.g., Hyperliquid + MEXC)
- Writes to SQLite:
  - `trades` - Individual trade executions
  - `bbo` - Best bid/offer snapshots
  - `bars_1m` - Live aggregated 1-minute bars

---

### Backfiller

Fetches historical 1-minute OHLCV data for backtesting.

**Command:**

```bash
uv run python -m apps.backfiller.main
```

**What it does:**

- Reads configuration from `config/history.yaml`:
  - `market_data_venue` (e.g., `binance_spot`)
  - `start_date` / `end_date` (or end=NOW)
- Writes to SQLite:
  - `ohlcv_1m` - Historical 1-minute candles
  - `history_coverage` - Coverage tracking

---

### Aggregator

Aggregates 1-minute bars into higher timeframes.

**Command:**

```bash
uv run python -m apps.aggregator.main \
  --venue <venue> \
  --symbol <symbol> \
  --timeframes <csv> \
  --chunk-days <n>
```

**Example:**

```bash
uv run python -m apps.aggregator.main \
  --venue binance_spot \
  --symbol BTC/USDT \
  --timeframes 5m,15m,1h,4h \
  --chunk-days 7
```

**What it writes:**

- `bars_5m` - 5-minute bars
- `bars_15m` - 15-minute bars
- `bars_1h` - 1-hour bars
- `bars_4h` - 4-hour bars
- (Any other requested timeframes)

---

### Backtester

Runs strategy backtests using historical bar data.

**Command:**

```bash
uv run python -m apps.backtester.main
```

**What it does:**

- Loads strategy configuration from `config/strategies/...`
- Loads backtest bars from SQLite using:
  - `venue` = `config.history.market_data_venue` (e.g., `binance_spot`)
  - `symbol` = `config.strategy.symbol` (e.g., `BTC/USDT`)
  - `timeframe` = `config.strategy.timeframe` (e.g., `5m`)
  - `start`/`end` from `config.history.start_date` / `config.history.end_date`

---

### Database Inspection

Useful SQLite commands for inspecting your data.

**List all tables:**

```bash
sqlite3 data/tbv8.sqlite ".tables"
```

**Check row counts:**

```bash
sqlite3 data/tbv8.sqlite "SELECT COUNT(*) FROM ohlcv_1m;"
sqlite3 data/tbv8.sqlite "SELECT COUNT(*) FROM bars_1m;"
sqlite3 data/tbv8.sqlite "SELECT COUNT(*) FROM bars_5m;"
sqlite3 data/tbv8.sqlite "SELECT COUNT(*) FROM bars_1h;"
```

**Verify historical coverage:**

```bash
sqlite3 data/tbv8.sqlite "
SELECT
  MIN(datetime(ts_ms/1000, 'unixepoch')) as first_bar,
  MAX(datetime(ts_ms/1000, 'unixepoch')) as last_bar,
  COUNT(*) as total_bars
FROM ohlcv_1m
WHERE venue='binance_spot' AND symbol='BTC/USDT';
"
```

**Latest bar timestamps per venue:**

```bash
sqlite3 data/tbv8.sqlite "
SELECT
  venue,
  MAX(datetime(ts_ms/1000, 'unixepoch')) as last_bar_time,
  COUNT(*) as total_bars
FROM bars_1m
WHERE symbol='BTC/USDT'
GROUP BY venue;
"
```

---

## Configuration

### config/data.yaml

```yaml
db_path: data/tbv8.sqlite
```

### config/history.yaml

```yaml
history:
  market_data_venue: binance_spot
  start_date: "2017-08-17T00:00:00Z"
  end_date: null  # null = NOW
```

### config/strategies/btc_usdt_perp_v1.yaml

Example strategy configuration:

```yaml
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
```

**Notes:**

- `routing.*` settings are for execution/live routing
- Backtests use `history.market_data_venue` for data (e.g., `binance_spot`)

---

## Typical Workflows

### Workflow 1: Backtest on Historical Data

1. **Backfill 1-minute data** (one-time setup):

```bash
uv run python -m apps.backfiller.main
```

2. **Aggregate to desired timeframes**:

```bash
uv run python -m apps.aggregator.main \
  --venue binance_spot \
  --symbol BTC/USDT \
  --timeframes 5m,15m,1h,4h \
  --chunk-days 7
```

3. **Run backtest**:

```bash
uv run python -m apps.backtester.main
```

### Workflow 2: Live Market Data Recording

Record real-time data from perpetual futures venues:

```bash
uv run python -m apps.recorder.main
```

This continuously records trades, BBO, and 1-minute bars until stopped with `Ctrl+C`.

---

## Repository Structure

```
tbv8/
├── apps/
│   ├── recorder/       # Live WebSocket recorder → SQLite
│   ├── backfiller/     # Historical fetcher → ohlcv_1m
│   ├── aggregator/     # Local aggregation → bars_{tf}
│   └── backtester/     # Backtest runner
├── packages/
│   ├── common/         # Config, backfill, aggregation, storage helpers
│   ├── backtester/     # Backtest engine, DB loader, strategies
│   ├── adapters/       # Exchange adapters
│   ├── engine/         # Trading engine components
│   ├── market_data/    # Market data handling
│   └── runtime/        # Runtime utilities
├── config/
│   ├── data.yaml       # Database configuration
│   ├── history.yaml    # Historical data configuration
│   ├── venues/         # Exchange venue configurations
│   └── strategies/     # Strategy configurations
└── main.py             # Entry point
```

---

## Troubleshooting

### Running Python Commands

Always use `uv` to run Python commands:

```bash
uv run python -m <module>
```

For the Python REPL:

```bash
uv run python
```

### Module Import Errors

If you encounter errors like `No module named 'apps...'` or `No module named 'packages...'`, ensure all `__init__.py` files exist:

```bash
touch apps/__init__.py
touch apps/backfiller/__init__.py
touch apps/recorder/__init__.py
touch apps/aggregator/__init__.py
touch apps/backtester/__init__.py
touch packages/__init__.py
touch packages/common/__init__.py
touch packages/backtester/__init__.py
touch packages/adapters/__init__.py
touch packages/engine/__init__.py
touch packages/market_data/__init__.py
touch packages/runtime/__init__.py
```

### Database Issues

If you encounter database-related errors, verify:

1. The database path in `config/data.yaml` is correct
2. The `data/` directory exists and is writable
3. SQLite is installed and accessible

### Configuration Issues

Ensure all required configuration files exist:

- `config/data.yaml`
- `config/history.yaml`
- `config/venues/*.yaml` (at least one venue configuration)
- `config/strategies/*.yaml` (at least one strategy configuration)


## License

This project is for educational and research purposes.

## Contributing

This is a personal project. For questions or issues, please open a GitHub issue.
