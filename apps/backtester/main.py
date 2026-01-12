from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from packages.common.config import load_tbv8_config
from packages.backtester.db import LoadBarsQuery, load_bars
from packages.backtester.engine import BacktestConfig, BacktestEngine, FillModel
from packages.backtester.strategies.htf_trend_ltf_entry import (
    HTFTrendLTFEntryConfig,
    HTFTrendLTFEntryStrategy,
)

DB_PATH = Path("data/tbv8.sqlite")


def _iso_to_ms(iso: str) -> int:
    """
    Accepts ISO strings like:
      - 2017-08-17T00:00:00Z
      - 2017-08-17T00:00:00+00:00
    Returns epoch ms (UTC).
    """
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

def _ms_to_utc_str(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat().replace("+00:00", "Z")


def main() -> None:
    cfg = load_tbv8_config()
    strat_cfg = cfg.strategy

    # ✅ Backtest data venue comes from history config (Binance spot, etc.)
    venue = cfg.history.market_data_venue
    symbol = strat_cfg.symbol
    timeframe = strat_cfg.timeframe  # e.g. "1m", "5m", "1h"

    start_ms = _maybe_ms(cfg.history.start_date)
    end_ms = _maybe_ms(cfg.history.end_date)  # None means "up to latest in DB"

    bars = load_bars(
        LoadBarsQuery(
            db_path=DB_PATH,
            venue=venue,
            symbol=symbol,
            timeframe=timeframe,
            start_ms=start_ms,
            end_ms=end_ms,
            # limit=200_000,  # optional cap for quick tests
        )
    )

    if bars:
        logger.info("Bars range: first={} last={}", _ms_to_utc_str(bars[0].ts_ms), _ms_to_utc_str(bars[-1].ts_ms))


    logger.info(
        "Loaded bars: {} (venue={} symbol={} tf={} start={} end={})",
        len(bars),
        venue,
        symbol,
        timeframe,
        cfg.history.start_date,
        cfg.history.end_date or "NOW",
    )

    if len(bars) < 300:
        logger.warning("You have <300 bars - choose a smaller timeframe or extend history.")

    # Demo strategy (still fine as a smoke test)
    strat = HTFTrendLTFEntryStrategy(
        HTFTrendLTFEntryConfig(
            # With LTF=5m, HTF=60m is a nice pairing
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

    engine = BacktestEngine(
        cfg=BacktestConfig(
            starting_equity_usd=10_000.0,
            notional_per_trade_usd=1_000.0,
            max_position=1,
        ),
        fill=FillModel(
            taker_fee_rate=0.0006,
            slippage_bps=1.0,
        ),
    )

    res = engine.run(bars, strat, venue=venue, symbol=symbol)

    if res.trades:
        logger.info("Last trade ts={} ({})", res.trades[-1].ts_ms, _ms_to_utc_str(res.trades[-1].ts_ms))


    print("")
    print("===== TBV8 BACKTEST RESULT =====")
    print(f"Data venue:   {venue}")
    print(f"Symbol:       {res.symbol}")
    print(f"Timeframe:    {timeframe}")
    print(f"Start equity: ${res.starting_equity_usd:,.2f}")
    print(f"End equity:   ${res.ending_equity_usd:,.2f}")
    print(f"PnL:          ${res.total_pnl_usd:,.2f}")
    print(f"Fees:         ${res.total_fees_usd:,.2f}")
    print(f"Trades:       {len(res.trades)}")
    print("")

    if res.trades:
        last = res.trades[-10:]
        print("Last 10 trades:")
        for t in last:
            side = "BUY" if t.side > 0 else "SELL"
            print(f"- {t.ts_ms} {side} px={t.price:.2f} qty={t.qty:.6f} fee=${t.fee_usd:.4f} {t.reason}")


if __name__ == "__main__":
    main()
