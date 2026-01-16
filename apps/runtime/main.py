from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter
from packages.backtester.db import LoadBarsQuery, load_bars
from packages.common.config import load_tbv8_config
from packages.common.timeframes import timeframe_to_ms
from packages.common.backfill.sqlite_store import resolve_bar_window_from_coverage
from packages.market_data.plant import MarketDataPlant
from packages.market_data.synthetic_fill import fill_missing_bars
from packages.market_data.types import EnsureHistoryRequest
from packages.runtime.engine import RuntimeEngine, RuntimeEngineConfig
from packages.common.types import RoutingMode
from packages.execution.router import ExecutionRouter, RoutingPlan
from packages.execution.simfill import SimFillExecutor
from packages.common.execution.fill_model import FillModel

from packages.backtester.strategies.htf_trend_ltf_entry import (
    HTFTrendLTFEntryConfig,
    HTFTrendLTFEntryStrategy,
)


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


def _select_primary_timeframe(timeframes: list[str]) -> str:
    """
    Choose the run-loop timeframe. Deterministic rule:
    - pick the smallest timeframe by ms (e.g. 1m over 4h).
    """
    if not timeframes:
        raise ValueError("strategy.timeframes must be non-empty")
    return min(timeframes, key=timeframe_to_ms)


async def _ensure_market_data(cfg) -> None:
    plant = MarketDataPlant(adapters=[BinanceSpotBackfillAdapter()])

    timeframes = list(cfg.strategy.timeframes)
    if not timeframes:
        raise ValueError("strategy.timeframes must be non-empty")

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


async def run_simfill_backtest(cfg) -> None:
    venue = cfg.history.market_data_venue
    symbol = cfg.strategy.symbol

    timeframes = list(cfg.strategy.timeframes)
    timeframe = _select_primary_timeframe(timeframes)  # run-loop tf (typically LTF)

    # 1) Ensure history exists for all configured timeframes
    await _ensure_market_data(cfg)

    # 2) Resolve deterministic load window from coverage (for primary timeframe)
    requested_start_ms = _maybe_ms(cfg.history.start_date)
    requested_end_ms = _maybe_ms(cfg.history.end_date)  # often None

    start_ms, end_ms_excl = resolve_bar_window_from_coverage(
        db_path=str(cfg.data.db_path),
        venue=venue,
        symbol=symbol,
        timeframe=timeframe,
        requested_start_ms=requested_start_ms,
        requested_end_ms=requested_end_ms,
    )

    # 3) Load sparse bars from SQLite (may contain gaps)
    sparse_bars = load_bars(
        LoadBarsQuery(
            db_path=str(cfg.data.db_path),
            venue=venue,
            symbol=symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms_excl,
        )
    )

    tf_ms = timeframe_to_ms(timeframe)

    if not sparse_bars:
        raise RuntimeError("No bars loaded after resolving coverage window.")

    # 4) Runtime synthetic fill (do NOT write back to SQLite)
    filled = list(
        fill_missing_bars(
            bars=sparse_bars,
            start_ms=start_ms,
            end_ms_excl=end_ms_excl,
            tf_ms=tf_ms,
        )
    )

    if not filled:
        raise RuntimeError("Synthetic fill produced 0 bars (no anchor bar found).")

    # If your fill_missing_bars yields RuntimeBar(bar=..., synthetic=...), unwrap it.
    if hasattr(filled[0], "bar"):
        synthetic_ts = [x.bar.ts_ms for x in filled if getattr(x, "synthetic", False)]
        synthetic_count = len(synthetic_ts)
        bars = [x.bar for x in filled]

        if synthetic_count:
            synthetic_ts.sort()
            longest_run = 1
            run = 1
            for i in range(1, len(synthetic_ts)):
                if synthetic_ts[i] - synthetic_ts[i - 1] == tf_ms:
                    run += 1
                    longest_run = max(longest_run, run)
                else:
                    run = 1

            logger.warning(
                "Synthetic fill applied tf={} injected={} longest_run={} first_synth={} last_synth={}",
                timeframe,
                synthetic_count,
                longest_run,
                _fmt_iso(synthetic_ts[0]),
                _fmt_iso(synthetic_ts[-1]),
            )
    else:
        synthetic_count = 0
        bars = filled

    # If the first emitted bar doesn't match start_ms (no anchor until later),
    # move start_ms forward to the first available bar.
    if bars[0].ts_ms != start_ms:
        logger.warning(
            "Adjusted start due to missing leading data tf={} requested_start={} adjusted_start={}",
            timeframe,
            start_ms,
            bars[0].ts_ms,
        )
        start_ms = bars[0].ts_ms

    expected_last_open = end_ms_excl - tf_ms
    if bars[-1].ts_ms != expected_last_open:
        logger.warning(
            "Last bar ts mismatch after fill tf={} got={} expected={} (coverage_end_excl={})",
            timeframe,
            bars[-1].ts_ms,
            expected_last_open,
            end_ms_excl,
        )

    # Safety assert: contiguous after fill
    for i in range(1, len(bars)):
        dt = bars[i].ts_ms - bars[i - 1].ts_ms
        if dt != tf_ms:
            raise RuntimeError(
                f"Post-fill gap detected tf={timeframe}: prev={bars[i-1].ts_ms} curr={bars[i].ts_ms} "
                f"delta={dt} expected={tf_ms}"
            )

    if synthetic_count:
        logger.warning(
            "Synthetic fill applied tf={} injected={} total={}",
            timeframe,
            synthetic_count,
            len(bars),
        )

    logger.info("Bars range: first={} last={}", _fmt_iso(bars[0].ts_ms), _fmt_iso(bars[-1].ts_ms))
    logger.info(
        "Loaded bars: {} (data_venue={} symbol={} run_tf={} configured_tfs={} start={} end={})",
        len(bars),
        venue,
        symbol,
        timeframe,
        timeframes,
        cfg.history.start_date,
        cfg.history.end_date or "COVERAGE_END",
    )

    # Strategy + runtime engine
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
    print(f"Run TF:        {timeframe}")
    print(f"All TFs:       {timeframes}")
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


async def main_async() -> None:
    cfg = load_tbv8_config()
    await run_simfill_backtest(cfg)


def main() -> None:
    asyncio.run(main_async())


if __name__ == "__main__":
    main()