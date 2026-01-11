from __future__ import annotations

from pathlib import Path

from loguru import logger

from packages.backtester.db import LoadBarsQuery, load_bars_1m
from packages.backtester.engine import BacktestConfig, BacktestEngine, FillModel
from packages.backtester.strategies.htf_trend_ltf_entry import (
    HTFTrendLTFEntryConfig,
    HTFTrendLTFEntryStrategy,
)

DB_PATH = Path("data/tbv8.sqlite")


def main() -> None:
    # Start with one venue so results are interpretable.
    # You can run it twice (hyperliquid then mexc) and compare.
    venue = "hyperliquid"
    symbol = "BTC/USDT"

    bars = load_bars_1m(
        LoadBarsQuery(
            db_path=DB_PATH,
            venue=venue,
            symbol=symbol,
            # limit=50_000,  # optionally cap
        )
    )

    logger.info("Loaded bars_1m: {} ({} {})", len(bars), venue, symbol)
    if len(bars) < 300:
        logger.warning("You have <300 bars - run the recorder longer for meaningful backtests.")

        strat = HTFTrendLTFEntryStrategy(
            HTFTrendLTFEntryConfig(
                htf_minutes=5,
                htf_sma_period=10,
                ltf_ema_period=10,
                require_close_above_ema=True,
                allow_shorts=True,
                warmup_trend_mode="htf_candle",
                warmup_min_htf_bars=2,
                debug=True,
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

    res = engine.run(bars, strat)

    print("")
    print("===== TBV8 BACKTEST RESULT =====")
    print(f"Venue:        {res.venue}")
    print(f"Symbol:       {res.symbol}")
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