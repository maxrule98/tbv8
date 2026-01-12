from __future__ import annotations

import argparse
import asyncio
from loguru import logger

from packages.common.config import load_tbv8_config

# NOTE: your backfill modules currently live under packages/common/backfill/
from packages.common.backfill.service import BackfillService, BackfillSpec

# If you already have a binance adapter, import it here.
# If not, comment this out until we add it.
from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter


async def _run() -> None:
    cfg = load_tbv8_config()

    # v0: we backfill 1m and aggregate locally
    # For now we just request whatever strategy timeframe is, and the service can
    # treat 1m as base and aggregate.
    timeframes = [cfg.strategy.timeframe]

    spec = BackfillSpec(
        db_path=cfg.data.db_path,
        venue=cfg.history.market_data_venue,
        symbol=cfg.strategy.symbol,
        start_date=cfg.history.start_date,
        end_date=cfg.history.end_date,
        timeframes=timeframes,
    )

    svc = BackfillService(
        adapters=[
            BinanceSpotBackfillAdapter(),  # venue="binance_spot"
        ]
    )

    await svc.ensure_history(spec)
    logger.info("Backfiller complete.")


def main() -> None:
    parser = argparse.ArgumentParser(description="TBV8 backfiller (bootstrap once, then incremental updates).")
    _ = parser.parse_args()
    asyncio.run(_run())


if __name__ == "__main__":
    main()
