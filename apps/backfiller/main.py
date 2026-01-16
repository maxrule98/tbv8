from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

from loguru import logger

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter
from packages.common.config import load_tbv8_config
from packages.market_data.plant import MarketDataPlant
from packages.market_data.types import EnsureHistoryRequest


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TBV8 backfiller (via MarketDataPlant, per-timeframe fetch).")
    p.add_argument("--venues-dir", default="config/venues")
    p.add_argument("--strategy-path", default="config/strategies/btc_usdt_perp_v1.yaml")
    p.add_argument("--history-path", default="config/history.yaml")
    p.add_argument("--data-path", default="config/data.yaml")
    return p.parse_args()


async def _run() -> None:
    args = _parse_args()

    cfg = load_tbv8_config(
        venues_dir=Path(args.venues_dir),
        strategy_path=Path(args.strategy_path),
        history_path=Path(args.history_path),
        data_path=Path(args.data_path),
    )

    # Config-driven
    venue = cfg.history.market_data_venue
    symbol = cfg.strategy.symbol
    timeframes = list(cfg.strategy.timeframes)
    db_path = cfg.data.db_path

    if not timeframes:
        raise ValueError("strategy.timeframes must be non-empty")

    logger.info(
        "Backfiller starting db={} venue={} symbol={} tfs={} start={} end={}",
        db_path,
        venue,
        symbol,
        timeframes,
        cfg.history.start_date,
        cfg.history.end_date or "NOW",
    )

    plant = MarketDataPlant(adapters=[BinanceSpotBackfillAdapter()])

    await plant.ensure_history(
        EnsureHistoryRequest(
            db_path=db_path,
            venue=venue,
            symbol=symbol,
            start_date=cfg.history.start_date,
            end_date=cfg.history.end_date,
            timeframes=timeframes,
            chunk_days=14,
        )
    )

    logger.info("Backfiller complete db={}", db_path)


def main() -> None:
    asyncio.run(_run())


if __name__ == "__main__":
    main()