from __future__ import annotations

import argparse
from pathlib import Path

from loguru import logger

from packages.common.backfill.aggregate import build_aggregates
from packages.common.datetime_utils import parse_iso8601_to_ms
from packages.common.timeframes import floor_ts_to_tf


DB_PATH = Path("data/tbv8.sqlite")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TBV8 - aggregate ohlcv_1m into bars_{tf}")
    p.add_argument("--db", default=str(DB_PATH))
    p.add_argument("--venue", default="binance_spot")
    p.add_argument("--symbol", default="BTC/USDT")
    p.add_argument("--start", default="2017-08-17T00:00:00Z")
    p.add_argument("--end", default=None)
    p.add_argument("--timeframes", default="5m,15m,1h,4h")
    p.add_argument("--chunk-days", type=int, default=7)
    return p.parse_args()


def main() -> None:
    args = parse_args()

    start_ms = floor_ts_to_tf(parse_iso8601_to_ms(args.start), "1m")
    end_ms = floor_ts_to_tf(parse_iso8601_to_ms(args.end), "1m") if args.end else None

    # If end not provided, we'll just use the max ts in ohlcv_1m for that venue/symbol
    import sqlite3

    conn = sqlite3.connect(args.db)
    try:
        if end_ms is None:
            row = conn.execute(
                "SELECT MAX(ts_ms) FROM ohlcv_1m WHERE venue=? AND symbol=?",
                (args.venue, args.symbol),
            ).fetchone()
            max_ts = int(row[0]) if row and row[0] is not None else start_ms
            end_ms = max_ts + 60_000  # exclusive
    finally:
        conn.close()

    tfs = [x.strip() for x in args.timeframes.split(",") if x.strip()]
    if not tfs:
        raise SystemExit("No timeframes specified")

    logger.info("Aggregator starting db={} venue={} symbol={}", args.db, args.venue, args.symbol)
    logger.info("Range [{}..{}) timeframes={} chunk_days={}", start_ms, end_ms, tfs, args.chunk_days)

    build_aggregates(
        db_path=args.db,
        venue=args.venue,
        symbol=args.symbol,
        start_ms=start_ms,
        end_ms=end_ms,
        timeframes=tfs,
        chunk_days=args.chunk_days,
    )


if __name__ == "__main__":
    main()
