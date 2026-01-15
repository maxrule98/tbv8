from __future__ import annotations

import argparse
import asyncio

from loguru import logger

from packages.adapters.binance_spot.backfill import BinanceSpotBackfillAdapter
from packages.common.backfill.service import BackfillService
from packages.common.backfill.repair import GapRepairService, GapRepairConfig
from packages.common.datetime_utils import parse_iso8601_to_ms
from packages.common.timeframes import floor_ts_to_tf
from packages.common.constants import BASE_TIMEFRAME


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TBV8 Gap Repair Tool (1m base)")
    p.add_argument("--venue", required=True, help="Market data venue (e.g. binance_spot)")
    p.add_argument("--symbol", required=True, help="Symbol (e.g. BTC/USDT)")
    p.add_argument("--db-path", default="data/tbv8.sqlite", help="Path to SQLite DB")

    # Optional scan bounds - if omitted, we scan whole table (can be slow)
    p.add_argument("--start", default=None, help="Scan start ISO (inclusive), e.g. 2020-01-01T00:00:00Z")
    p.add_argument("--end", default=None, help="Scan end ISO (exclusive), e.g. 2020-02-01T00:00:00Z")

    # Repair behavior
    p.add_argument("--max-gap-minutes", type=int, default=60 * 24 * 14, help="Skip gaps larger than this")
    p.add_argument("--chunk-minutes", type=int, default=1000, help="Repair chunk size (minutes)")
    p.add_argument("--max-ranges", type=int, default=200, help="Max number of distinct gaps to attempt")

    return p.parse_args()


async def main_async() -> None:
    args = _parse_args()

    if args.venue != "binance_spot":
        logger.warning(
            "This CLI currently wires BinanceSpotBackfillAdapter only. venue={} will still run but likely fail.",
            args.venue,
        )

    scan_start_ms = parse_iso8601_to_ms(args.start) if args.start else None
    scan_end_ms_excl = parse_iso8601_to_ms(args.end) if args.end else None

    # Optional: align scan bounds to 1m (nice to keep things clean)
    if scan_start_ms is not None:
        scan_start_ms = floor_ts_to_tf(scan_start_ms, BASE_TIMEFRAME)
    if scan_end_ms_excl is not None:
        scan_end_ms_excl = floor_ts_to_tf(scan_end_ms_excl, BASE_TIMEFRAME)

    backfill = BackfillService(adapters=[BinanceSpotBackfillAdapter()])
    repair = GapRepairService(
        backfill=backfill,
        cfg=GapRepairConfig(
            max_gap_minutes=args.max_gap_minutes,
            chunk_minutes=args.chunk_minutes,
            max_ranges=args.max_ranges,
        ),
    )

    attempted = await repair.repair_gaps(
        db_path=args.db_path,
        venue=args.venue,
        symbol=args.symbol,
        scan_start_ms=scan_start_ms,
        scan_end_ms_excl=scan_end_ms_excl,
    )

    logger.info("Gap repair complete. gap_ranges_attempted={}", attempted)


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Interrupted.")


if __name__ == "__main__":
    main()