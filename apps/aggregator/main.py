from __future__ import annotations

import argparse
import sqlite3
from dataclasses import dataclass
from typing import Sequence

from loguru import logger

from packages.common.config import load_tbv8_config
from packages.common.datetime_utils import now_ms, parse_iso8601_to_ms
from packages.common.timeframes import floor_ts_to_tf
from packages.common.backfill.sqlite_store import ensure_schema
from packages.common.backfill.service import get_max_ts  # if you have it here; otherwise import from sqlite_store
from packages.common.backfill.aggregate import build_aggregates


@dataclass(frozen=True)
class Args:
    db_path: str
    venue: str
    symbol: str
    timeframes: Sequence[str]
    chunk_days: int
    start_date: str | None
    end_date: str | None


def _parse_args() -> Args:
    p = argparse.ArgumentParser(description="TBV8 aggregator: derive bars_* from 1m OHLCV stored in sqlite.")
    p.add_argument("--db", dest="db_path", default=None, help="SQLite DB path (default: config/data.yaml or data/tbv8.sqlite)")
    p.add_argument("--venue", required=True)
    p.add_argument("--symbol", required=True)
    p.add_argument("--timeframes", required=True, help="Comma-separated, e.g. 5m,15m,1h,4h")
    p.add_argument("--chunk-days", type=int, default=7)
    p.add_argument("--start-date", default=None, help="ISO8601, e.g. 2017-08-17T00:00:00Z (default: earliest available 1m)")
    p.add_argument("--end-date", default=None, help="ISO8601 (default: now)")

    ns = p.parse_args()

    cfg = load_tbv8_config()
    db_path = ns.db_path or cfg.data.db_path

    tfs = [x.strip() for x in ns.timeframes.split(",") if x.strip()]
    if not tfs:
        raise ValueError("No timeframes provided")

    return Args(
        db_path=db_path,
        venue=ns.venue,
        symbol=ns.symbol,
        timeframes=tfs,
        chunk_days=ns.chunk_days,
        start_date=ns.start_date,
        end_date=ns.end_date,
    )


def main() -> None:
    a = _parse_args()

    conn = sqlite3.connect(a.db_path)
    try:
        ensure_schema(conn)

        # Determine range:
        # - start defaults to earliest required (either provided, or whatever exists in 1m)
        # - end defaults to now
        if a.start_date:
            start_ms = floor_ts_to_tf(parse_iso8601_to_ms(a.start_date), "1m")
        else:
            # If you don't have a "get_min_ts", just start from your known history start
            # (or add get_min_ts later). For now, use max_ts fallback logic.
            # Better: query MIN(ts_ms) from ohlcv_1m for this venue/symbol.
            row = conn.execute(
                "SELECT MIN(ts_ms) FROM ohlcv_1m WHERE venue=? AND symbol=?",
                (a.venue, a.symbol),
            ).fetchone()
            start_ms = int(row[0]) if row and row[0] is not None else None
            if start_ms is None:
                raise RuntimeError(f"No 1m data found in ohlcv_1m for {a.venue} {a.symbol}")

        end_ms = (
            floor_ts_to_tf(parse_iso8601_to_ms(a.end_date), "1m")
            if a.end_date
            else floor_ts_to_tf(now_ms(), "1m")
        )

        logger.info("Aggregator starting db={} venue={} symbol={}", a.db_path, a.venue, a.symbol)
        logger.info("Range [{}..{}) timeframes={} chunk_days={}", start_ms, end_ms, list(a.timeframes), a.chunk_days)

        build_aggregates(
            conn,
            venue=a.venue,
            symbol=a.symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            timeframes=a.timeframes,
            chunk_days=a.chunk_days,
        )

        conn.commit()
        logger.info("Aggregation complete.")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
