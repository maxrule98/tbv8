from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from packages.common.config import load_tbv8_config
from packages.backtester.db import LoadBarsQuery, load_bars
from packages.backtester.engine import FillModel
from packages.backtester.types import Bar
from packages.backtester.strategies.htf_trend_ltf_entry import (
    HTFTrendLTFEntryConfig,
    HTFTrendLTFEntryStrategy,
)

from packages.runtime.engine import RuntimeEngine, RuntimeEngineConfig
from packages.runtime.execution.router import ExecutionRouter, RoutingPlan
from packages.runtime.execution.simfill import SimFillExecutor


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


def _ms_to_iso(ts_ms: int) -> str:
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    cfg = load_tbv8_config()
    strat_cfg = cfg.strategy

    # ---------------------------
    # Data venue (research feed)
    # ---------------------------
    data_venue = cfg.history.market_data_venue
    symbol = strat_cfg.symbol
    timeframe = strat_cfg.timeframe

    start_ms = _maybe_ms(cfg.history.start_date)
    end_ms = _maybe_ms(cfg.history.end_date)  # None means "load until latest in DB"

    # Load bars from local SQLite (already backfilled + aggregated)
    bars_raw = load_bars(
        LoadBarsQuery(
            db_path=Path(cfg.data.db_path),
            venue=data_venue,
            symbol=symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
        )
    )

    # Convert OHLCV -> Bar for strategy/runtime usage
    bars: list[Bar] = [
        Bar(
            ts_ms=b.ts_ms,
            open=b.open,
            high=b.high,
            low=b.low,
            close=b.close,
            volume=b.volume,
        )
        for b in bars_raw
    ]

    if not bars:
        raise RuntimeError(f"No bars loaded for venue={data_venue} symbol={symbol} tf={timeframe}")

    logger.info("Bars range: first={} last={}", _ms_to_iso(bars[0].ts_ms), _ms_to_iso(bars[-1].ts_ms))
    logger.info(
        "Loaded bars: {} (data_venue={} symbol={} tf={} start={} end={})",
        len(bars),
        data_venue,
        symbol,
        timeframe,
        cfg.history.start_date,
        cfg.history.end_date or "NOW",
    )

    # ---------------------------
    # Execution venue(s)
    # ---------------------------
    exec_primary = str(strat_cfg.routing.primary)
    exec_secondary = str(strat_cfg.routing.secondary) if strat_cfg.routing.secondary else None

    router = ExecutionRouter(
        RoutingPlan(
            primary=exec_primary,
            secondary=exec_secondary,
            mode=strat_cfg.routing.mode,
        )
    )

    fill_model = FillModel(
        taker_fee_rate=0.0006,
        slippage_bps=1.0,
    )

    executor = SimFillExecutor(venue=exec_primary, fill=fill_model)

    # Demo strategy (smoke test)
    strat = HTFTrendLTFEntryStrategy(
        HTFTrendLTFEntryConfig(
            htf_minutes=60,
            htf_sma_period=50,
            ltf_ema_period=50,
            require_close_above_ema=True,
            allow_shorts=True,
            debug=False,
        )
    )

    engine = RuntimeEngine(
        cfg=RuntimeEngineConfig(
            data_venue=data_venue,
            symbol=symbol,
            timeframe=timeframe,
            exec_primary_venue=exec_primary,
            exec_shadow_venue=router.choose_shadow(),
            starting_equity_usd=10_000.0,
            notional_per_trade_usd=1_000.0,
            max_position=1,
        ),
        router=router,
        executor=executor,
    )

    res = engine.run(bars, strat)

    print("")
    print("===== TBV8 RUNTIME RESULT (SIMFILL) =====")
    print(f"Data venue:    {res.data_venue}")
    print(f"Exec primary:  {res.exec_primary_venue}")
    print(f"Exec shadow:   {res.exec_shadow_venue or '-'}")
    print(f"Symbol:        {res.symbol}")
    print(f"Timeframe:     {res.timeframe}")
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


if __name__ == "__main__":
    main()
