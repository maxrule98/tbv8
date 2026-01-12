from __future__ import annotations

import argparse
import asyncio
from typing import List

from loguru import logger

from packages.common.config import load_tbv8_config
from packages.market_data.plant import EnsureHistoryRequest, MarketDataPlant

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter


def _resolve_timeframes(strategy_timeframe: str) -> List[str]:
    """
    v1 rule (simple + correct):
    - Always backfill base 1m into ohlcv_1m
    - Ensure aggregates for the strategy timeframe as well (if not 1m)

    Later, when strategies declare multi-tf inputs, this becomes:
      ["1m", *strategy.required_timeframes]
    """
    tfs = ["1m"]
    tf = strategy_timeframe.strip()
    if tf and tf != "1m":
        tfs.append(tf)
    return tfs


async def _run() -> None:
    cfg = load_tbv8_config()

    timeframes = _resolve_timeframes(cfg.strategy.timeframe)

    plant = MarketDataPlant(
        cfg,
        backfill_adapters=[
            BinanceSpotBackfillAdapter(),  # venue="binance_spot"
        ],
    )

    req = EnsureHistoryRequest(
        venue=str(cfg.history.market_data_venue),
        symbol=cfg.strategy.symbol,
        start_date=cfg.history.start_date,
        end_date=cfg.history.end_date,
        timeframes=timeframes,
    )

    logger.info(
        "Backfiller starting venue={} symbol={} tfs={} start={} end={}",
        req.venue,
        req.symbol,
        list(req.timeframes),
        req.start_date,
        req.end_date or "NOW",
    )

    await plant.ensure_local_history(req)

    logger.info("Backfiller complete db={}", cfg.data.db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="TBV8 backfiller (bootstrap once, then incremental updates).")
    _ = parser.parse_args()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
