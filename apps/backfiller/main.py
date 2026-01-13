from __future__ import annotations

import argparse
import asyncio
from loguru import logger

from packages.common.config import load_tbv8_config
from packages.market_data.plant import MarketDataPlant
from packages.market_data.types import EnsureHistoryRequest

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter


async def _run() -> None:
    cfg = load_tbv8_config()

    # For now: always include 1m + the strategy tf (dedup handled by Plant)
    timeframes = ["1m", cfg.strategy.timeframe]

    logger.info(
        "Backfiller starting venue={} symbol={} tfs={} start={} end={}",
        cfg.history.market_data_venue,
        cfg.strategy.symbol,
        timeframes,
        cfg.history.start_date,
        cfg.history.end_date or "NOW",
    )

    plant = MarketDataPlant(adapters=[BinanceSpotBackfillAdapter()])

    await plant.ensure_history(
        EnsureHistoryRequest(
            db_path=cfg.data.db_path,
            venue=cfg.history.market_data_venue,
            symbol=cfg.strategy.symbol,
            start_date=cfg.history.start_date,
            end_date=cfg.history.end_date,
            timeframes=timeframes,
            chunk_days=14,
        )
    )

    logger.info("Backfiller complete db={}", cfg.data.db_path)


def main() -> None:
    parser = argparse.ArgumentParser(description="TBV8 backfiller (via MarketDataPlant).")
    _ = parser.parse_args()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
