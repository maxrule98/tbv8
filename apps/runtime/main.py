from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from packages.common.config import load_tbv8_config
from packages.common.timeframes import timeframe_to_ms
from packages.common.backfill.sqlite_store import get_coverage, ensure_schema

from packages.market_data.plant import MarketDataPlant
from packages.market_data.types import EnsureHistoryRequest

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter

from packages.backtester.db import LoadBarsQuery, load_bars

from packages.runtime.engine import RuntimeEngine, RuntimeEngineConfig
from packages.common.types import RoutingMode
from packages.execution.router import ExecutionRouter, RoutingPlan
from packages.execution.simfill import SimFillExecutor
from packages.common.execution.fill_model import FillModel

from packages.backtester.strategies.htf_trend_ltf_entry import (
    HTFTrendLTFEntryConfig,
    HTFTrendLTFEntryStrategy,
)


DB_PATH = Path("data/tbv8.sqlite")


def _iso_to_ms(iso: str) -> int:
    s = iso.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


def _maybe_ms(iso: Optional[str]) -> Optional[int]:
    if iso is None:
        return None
    if not iso.strip():
        return None
    return _iso_to_ms(iso)


def _fmt_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


async def _ensure_market_data(cfg) -> None:
    plant = MarketDataPlant(adapters=[BinanceSpotBackfillAdapter()])

    # Always ensure base 1m + the strategy tf
    timeframes = ["1m", "5m", "15m", "1h", cfg.strategy.timeframe]

    await plant.ensure_history(
        EnsureHistoryRequest(
            db_path=str(cfg.data.db_path),
            venue=cfg.history.market_data_venue,
            symbol=cfg.strategy.symbol,
            start_date=cfg.history.start_date,
            end_date=cfg.history.end_date,
            timeframes=timeframes,
            chunk_days=14,
        )
    )


def _resolve_bar_window_from_coverage(
    *,
    db_path: str,
    venue: str,
    symbol: str,
    timeframe: str,
    requested_start_ms: Optional[int],
    requested_end_ms: Optional[int],
) -> tuple[int, int]:
    """
    Returns [start_ms, end_ms_exclusive) for loading bars.
    We use coverage.end_ms as the last COMPLETE candle start.
    """
    tf_ms = timeframe_to_ms(timeframe)

    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)
        cov = get_coverage(conn, venue, symbol, timeframe)
        if cov is None:
            raise RuntimeError(f"No coverage for venue={venue} symbol={symbol} tf={timeframe}. Run backfiller first.")

        cov_start = cov.start_ms
        cov_end_inclusive = cov.end_ms
        cov_end_excl = cov_end_inclusive + tf_ms

        start_ms = max(cov_start, requested_start_ms) if requested_start_ms is not None else cov_start
        end_ms_excl = min(cov_end_excl, requested_end_ms) if requested_end_ms is not None else cov_end_excl

        if end_ms_excl <= start_ms:
            raise RuntimeError(
                f"Resolved empty bar window start={start_ms} end_excl={end_ms_excl} "
                f"(coverage=[{cov_start}..{cov_end_inclusive}])"
            )

        return start_ms, end_ms_excl
    finally:
        conn.close()


async def main_async() -> None:
    cfg = load_tbv8_config()
    venue = cfg.history.market_data_venue
    symbol = cfg.strategy.symbol
    timeframe = cfg.strategy.timeframe

    # 1) Ensure data exists + aggregates are correct/updated
    await _ensure_market_data(cfg)

    # 2) Resolve deterministic load window from coverage
    requested_start_ms = _maybe_ms(cfg.history.start_date)
    requested_end_ms = _maybe_ms(cfg.history.end_date)  # often None

    start_ms, end_ms_excl = _resolve_bar_window_from_coverage(
        db_path=str(cfg.data.db_path),
        venue=venue,
        symbol=symbol,
        timeframe=timeframe,
        requested_start_ms=requested_start_ms,
        requested_end_ms=requested_end_ms,
    )

    # 3) Load bars
    bars = load_bars(
        LoadBarsQuery(
            db_path=DB_PATH,
            venue=venue,
            symbol=symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms_excl,
        )
    )

    logger.info("Bars range: first={} last={}", _fmt_iso(bars[0].ts_ms), _fmt_iso(bars[-1].ts_ms))
    logger.info(
        "Loaded bars: {} (data_venue={} symbol={} tf={} start={} end={})",
        len(bars),
        venue,
        symbol,
        timeframe,
        cfg.history.start_date,
        cfg.history.end_date or "COVERAGE_END",
    )

    # 4) Strategy + runtime engine
    strat = HTFTrendLTFEntryStrategy(
        HTFTrendLTFEntryConfig(
            htf_minutes=60,
            htf_sma_period=50,
            ltf_ema_period=50,
            require_close_above_ema=True,
            allow_shorts=True,
            warmup_trend_mode="htf_candle",
            warmup_min_htf_bars=5,
            debug=False,
        )
    )

    engine = RuntimeEngine(
        cfg=RuntimeEngineConfig(
            data_venue=venue,
            symbol=symbol,
            timeframe=timeframe,
            exec_primary_venue=venue,
            exec_shadow_venue=None,
            starting_equity_usd=10_000.0,
            notional_per_trade_usd=1_000.0,
            max_position=1,
        ),
        router=ExecutionRouter(
            RoutingPlan(
                primary=venue,
                secondary=None,
                mode=RoutingMode.PRIMARY_ONLY,
            )
        ),
        executor=SimFillExecutor(
            venue=venue,
            fill=FillModel(
                taker_fee_rate=0.0006,
                slippage_bps=1.0,
            ),
        ),
    )

    res = engine.run(bars, strat)

    print("")
    print("===== TBV8 RUNTIME RESULT (SIMFILL) =====")
    print(f"Data venue:    {venue}")
    print(f"Symbol:        {symbol}")
    print(f"Timeframe:     {timeframe}")
    print(f"Start equity:  ${res.starting_equity_usd:,.2f}")
    print(f"End equity:    ${res.ending_equity_usd:,.2f}")
    print(f"PnL:           ${res.total_pnl_usd:,.2f}")
    print(f"Fees:          ${res.total_fees_usd:,.2f}")
    print(f"Trades:        {len(res.trades)}")
    print("")

    if res.trades:
        last = res.trades[-10:]
        print("Last 10 trades:")
        for t in last:
            side = "BUY" if t.side > 0 else "SELL"
            print(f"- {t.ts_ms} {side} px={t.price:.2f} qty={t.qty:.6f} fee=${t.fee_usd:.4f} {t.reason}")


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()
