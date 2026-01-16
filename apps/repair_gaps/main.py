# apps/repair_gaps/main.py

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path
from typing import List, Optional

from loguru import logger

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter
from packages.common.backfill.service import BackfillService
from packages.common.backfill.repair import GapRepairService, GapRepairConfig
from packages.common.config import load_tbv8_config
from packages.common.datetime_utils import parse_iso8601_to_ms
from packages.common.timeframes import ceil_ts_to_tf, floor_ts_to_tf


def _split_csv(v: Optional[str]) -> Optional[List[str]]:
    if v is None:
        return None
    parts = [p.strip() for p in v.split(",")]
    out = [p for p in parts if p]
    return out or None


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TBV8 Gap Repair Tool (per-timeframe, config-driven)")

    # Config sources
    p.add_argument("--venues-dir", default="config/venues", help="Directory of venue yamls")
    p.add_argument("--strategy-path", default="config/strategies/btc_usdt_perp_v1.yaml", help="Strategy yaml path")
    p.add_argument("--history-path", default="config/history.yaml", help="History yaml path")
    p.add_argument("--data-path", default="config/data.yaml", help="Data yaml path")

    # Optional overrides (otherwise come from config)
    p.add_argument("--venue", default=None, help="Market data venue override (e.g. binance_spot)")
    p.add_argument("--symbol", default=None, help="Symbol override (e.g. BTC/USDT)")
    p.add_argument("--timeframes", default=None, help="Comma-separated tf override, e.g. 1m,5m,4h")
    p.add_argument("--db-path", default=None, help="SQLite path override")

    # Optional scan bounds - if omitted, we scan whole table (can be slow)
    p.add_argument("--start", default=None, help="Scan start ISO (inclusive), e.g. 2020-01-01T00:00:00Z")
    p.add_argument("--end", default=None, help="Scan end ISO (exclusive), e.g. 2020-02-01T00:00:00Z")

    # Repair behaviour
    p.add_argument("--max-gap-minutes", type=int, default=60 * 24 * 14, help="Skip gaps larger than this")
    p.add_argument("--chunk-limit", type=int, default=1000, help="Max candles per fetch (Binance klines max is 1000)")
    p.add_argument("--max-ranges", type=int, default=200, help="Max number of distinct gaps to attempt")

    return p.parse_args()


async def main_async() -> None:
    args = _parse_args()

    cfg = load_tbv8_config(
        venues_dir=Path(args.venues_dir),
        strategy_path=Path(args.strategy_path),
        history_path=Path(args.history_path),
        data_path=Path(args.data_path),
    )

    venue = args.venue or cfg.history.market_data_venue
    symbol = args.symbol or cfg.strategy.symbol
    timeframes = _split_csv(args.timeframes) or list(cfg.strategy.timeframes)
    db_path = args.db_path or cfg.data.db_path

    if not timeframes:
        raise ValueError("No timeframes resolved (set strategy.timeframes or pass --timeframes)")

    # If you later add more adapters, route by venue here.
    if venue != "binance_spot":
        logger.warning(
            "This CLI currently wires BinanceSpotBackfillAdapter only. venue={} will likely fail unless you add routing.",
            venue,
        )

    scan_start_raw = parse_iso8601_to_ms(args.start) if args.start else None
    scan_end_raw_excl = parse_iso8601_to_ms(args.end) if args.end else None

    backfill = BackfillService(adapters=[BinanceSpotBackfillAdapter()])
    repair = GapRepairService(
        backfill=backfill,
        cfg=GapRepairConfig(
            max_gap_minutes=args.max_gap_minutes,
            chunk_limit=args.chunk_limit,
            max_ranges=args.max_ranges,
        ),
    )

    logger.info(
        "Gap repair starting db={} venue={} symbol={} tfs={} start={} end={}",
        db_path,
        venue,
        symbol,
        timeframes,
        args.start,
        args.end,
    )

    total_attempted = 0
    for tf in timeframes:
        # Align bounds to this tf (if provided)
        scan_start_ms = floor_ts_to_tf(scan_start_raw, tf) if scan_start_raw is not None else None
        scan_end_ms_excl = ceil_ts_to_tf(scan_end_raw_excl, tf) if scan_end_raw_excl is not None else None

        attempted = await repair.repair_gaps(
            db_path=db_path,
            venue=venue,
            symbol=symbol,
            timeframe=tf,
            scan_start_ms=scan_start_ms,
            scan_end_ms_excl=scan_end_ms_excl,
        )
        total_attempted += attempted

    logger.info("Gap repair complete. total_gap_ranges_attempted={}", total_attempted)


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Interrupted.")


if __name__ == "__main__":
    main()