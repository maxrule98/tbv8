from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Optional, Dict, List

from loguru import logger

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter
from packages.backtester.db import LoadBarsQuery, load_bars, BacktestBar
from packages.backtester.types import Bar
from packages.common.config import load_tbv8_config
from packages.common.timeframes import timeframe_to_ms, floor_ts_to_tf
from packages.market_data.plant import MarketDataPlant
from packages.market_data.synthetic_fill import fill_missing_bars
from packages.market_data.types import EnsureHistoryRequest
from packages.market_data.bar_store import BarSeries, MultiTFBarStore
from packages.runtime.engine import RuntimeEngine, RuntimeEngineConfig, StrategyContext
from packages.common.types import RoutingMode
from packages.execution.router import ExecutionRouter, RoutingPlan
from packages.execution.simfill import SimFillExecutor
from packages.common.execution.fill_model import FillModel

from packages.common.backfill.sqlite_store import (
    resolve_bar_window_from_coverage,
    resolve_contiguous_window,
)

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


def _select_run_timeframe(timeframes: list[str]) -> str:
    """
    Deterministic rule: smallest tf by milliseconds.
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


def _load_series(
    *,
    db_path: str,
    venue: str,
    symbol: str,
    tf: str,
    start_ms: int,
    end_ms_excl: int,
) -> List[BacktestBar]:
    return load_bars(
        LoadBarsQuery(
            db_path=db_path,
            venue=venue,
            symbol=symbol,
            timeframe=tf,
            start_ms=start_ms,
            end_ms=end_ms_excl,
        )
    )


def _as_engine_bar(b: BacktestBar) -> Bar:
    return Bar(
        ts_ms=int(b.ts_ms),
        open=float(b.open),
        high=float(b.high),
        low=float(b.low),
        close=float(b.close),
        volume=float(b.volume),
    )


class LegacyStrategyAdapter:
    """
    Temporary adapter so existing strategies that implement:
        on_bar(bar: Bar) -> int
    can run under the new context-driven engine.

    Delete once strategies migrate to on_bar(ctx).
    """

    def __init__(self, legacy):
        self.legacy = legacy

    def on_bar(self, ctx: StrategyContext) -> int:
        return int(self.legacy.on_bar(ctx.bar))


async def run_simfill_backtest(cfg) -> None:
    data_venue = cfg.history.market_data_venue
    symbol = cfg.strategy.symbol

    tfs = list(cfg.strategy.timeframes)
    if not tfs:
        raise ValueError("strategy.timeframes must be non-empty")

    run_tf = _select_run_timeframe(tfs)

    # 1) Ensure history exists for all configured timeframes
    await _ensure_market_data(cfg)

    # 2) Resolve requested window from coverage for run_tf (deterministic)
    requested_start_ms = _maybe_ms(cfg.history.start_date)
    requested_end_ms = _maybe_ms(cfg.history.end_date)

    start_ms, end_ms_excl = resolve_bar_window_from_coverage(
        db_path=str(cfg.data.db_path),
        venue=data_venue,
        symbol=symbol,
        timeframe=run_tf,
        requested_start_ms=requested_start_ms,
        requested_end_ms=requested_end_ms,
    )

    # 2A) Try to pick a contiguous segment (skip known exchange holes)
    # Fallback safely to the coverage-resolved window if selection fails.
    try:
        contig_start, contig_end_excl = resolve_contiguous_window(
            db_path=str(cfg.data.db_path),
            venue=data_venue,
            symbol=symbol,
            timeframe=run_tf,
            start_ms=start_ms,
            end_ms_excl=end_ms_excl,
            min_window_candles=500,
        )

        if (contig_start, contig_end_excl) != (start_ms, end_ms_excl):
            logger.warning(
                "Contiguous window selected run_tf={} original=[{}..{}) chosen=[{}..{})",
                run_tf,
                _fmt_iso(start_ms),
                _fmt_iso(end_ms_excl),
                _fmt_iso(contig_start),
                _fmt_iso(contig_end_excl),
            )

        start_ms, end_ms_excl = contig_start, contig_end_excl

    except Exception as e:
        logger.warning(
            "resolve_contiguous_window failed (run_tf={}) - falling back to coverage window: {}",
            run_tf,
            str(e),
        )

    # 3) Load + fill run_tf (runtime-only synthetic)
    sparse_run = _load_series(
        db_path=str(cfg.data.db_path),
        venue=data_venue,
        symbol=symbol,
        tf=run_tf,
        start_ms=start_ms,
        end_ms_excl=end_ms_excl,
    )
    if not sparse_run:
        raise RuntimeError(f"No bars loaded for run_tf={run_tf} in resolved window")

    run_tf_ms = timeframe_to_ms(run_tf)

    filled = list(
        fill_missing_bars(
            bars=[_as_engine_bar(b) for b in sparse_run],
            start_ms=start_ms,
            end_ms_excl=end_ms_excl,
            tf_ms=run_tf_ms,
        )
    )
    if not filled:
        raise RuntimeError("Synthetic fill produced 0 bars (no anchor bar found).")

    # unwrap RuntimeBar(bar=..., synthetic=...)
    if hasattr(filled[0], "bar"):
        synth_ts = [x.bar.ts_ms for x in filled if getattr(x, "synthetic", False)]
        synthetic_count = len(synth_ts)
        run_bars: List[Bar] = [x.bar for x in filled]
    else:
        synthetic_count = 0
        run_bars = filled  # type: ignore[assignment]

    # Contiguity assert (after fill)
    for i in range(1, len(run_bars)):
        if (run_bars[i].ts_ms - run_bars[i - 1].ts_ms) != run_tf_ms:
            raise RuntimeError("run_tf bars not contiguous after synthetic fill")

    if synthetic_count:
        synth_ts = sorted([x.bar.ts_ms for x in filled if getattr(x, "synthetic", False)])  # type: ignore[attr-defined]
        longest_run = 1
        run = 1
        for i in range(1, len(synth_ts)):
            if synth_ts[i] - synth_ts[i - 1] == run_tf_ms:
                run += 1
                longest_run = max(longest_run, run)
            else:
                run = 1

        logger.warning(
            "Synthetic fill applied run_tf={} injected={} longest_run={} first_synth={} last_synth={}",
            run_tf,
            synthetic_count,
            longest_run,
            _fmt_iso(synth_ts[0]),
            _fmt_iso(synth_ts[-1]),
        )

    logger.info("Run bars range: first={} last={}", _fmt_iso(run_bars[0].ts_ms), _fmt_iso(run_bars[-1].ts_ms))
    logger.info(
        "Running backtest data_venue={} symbol={} run_tf={} all_tfs={} start={} end={}",
        data_venue,
        symbol,
        run_tf,
        tfs,
        _fmt_iso(start_ms),
        _fmt_iso(end_ms_excl),
    )

    # 4) Load all configured timeframes into the multi-TF store for the same overall window
    series: Dict[str, BarSeries] = {}

    for tf in tfs:
        tf_start = floor_ts_to_tf(start_ms, tf)
        tf_end_excl = floor_ts_to_tf(end_ms_excl, tf)

        bars_tf = _load_series(
            db_path=str(cfg.data.db_path),
            venue=data_venue,
            symbol=symbol,
            tf=tf,
            start_ms=tf_start,
            end_ms_excl=tf_end_excl,
        )

        series[tf] = BarSeries(timeframe=tf, bars=[_as_engine_bar(b) for b in bars_tf])

        logger.info(
            "Loaded tf={} bars={} window=[{}..{})",
            tf,
            len(bars_tf),
            _fmt_iso(tf_start),
            _fmt_iso(tf_end_excl),
        )

    store = MultiTFBarStore(series=series)

    # 5) Strategy (legacy scaffold for now)
    legacy_strat = HTFTrendLTFEntryStrategy(
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
    strat = LegacyStrategyAdapter(legacy_strat)

    # 6) Engine
    exec_primary = cfg.strategy.routing.primary
    exec_shadow = cfg.strategy.routing.secondary

    engine = RuntimeEngine(
        cfg=RuntimeEngineConfig(
            data_venue=data_venue,
            symbol=symbol,
            run_tf=run_tf,
            exec_primary_venue=exec_primary,
            exec_shadow_venue=exec_shadow,
            starting_equity_usd=10_000.0,
            notional_per_trade_usd=1_000.0,
            max_position=1,
        ),
        router=ExecutionRouter(
            RoutingPlan(
                primary=exec_primary,
                secondary=exec_shadow,
                mode=cfg.strategy.routing.mode if hasattr(cfg.strategy, "routing") else RoutingMode.PRIMARY_ONLY,
            )
        ),
        executor=SimFillExecutor(
            venue=exec_primary,
            fill=FillModel(
                taker_fee_rate=0.0006,
                slippage_bps=1.0,
            ),
        ),
        store=store,
    )

    res = engine.run(run_bars, strat)

    print("")
    print("===== TBV8 RUNTIME RESULT (SIMFILL) =====")
    print(f"Data venue:    {data_venue}")
    print(f"Symbol:        {symbol}")
    print(f"Run TF:        {run_tf}")
    print(f"All TFs:       {tfs}")
    print(f"Exec primary:  {exec_primary}")
    print(f"Exec shadow:   {exec_shadow}")
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