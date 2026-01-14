from __future__ import annotations

import argparse
import asyncio
import sqlite3

from loguru import logger

from packages.common.backfill.aggregate import build_aggregates
from packages.common.backfill.sqlite_store import get_coverage
from packages.common.constants import BASE_TIMEFRAME, BASE_TF_MS
from packages.common.datetime_utils import now_ms, parse_iso8601_to_ms


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="TBV8 Aggregator Tool")
    p.add_argument("--venue", required=True, help="Market data venue (e.g. binance_spot)")
    p.add_argument("--symbol", required=True, help="Symbol (e.g. BTC/USDT)")
    p.add_argument("--timeframe", required=True, help="Target timeframe (e.g. 1h, 15m, 4h)")
    p.add_argument("--start", default=None, help="Start ISO date (inclusive)")
    p.add_argument("--end", default=None, help="End ISO date (exclusive)")
    p.add_argument("--db-path", default="data/tbv8.sqlite", help="Path to SQLite DB")
    return p.parse_args()


def _get_min_max_ts_ms(conn: sqlite3.Connection, venue: str, symbol: str) -> tuple[int, int] | None:
    """
    Reads min/max open timestamps from bars_1m.
    Returns (min_ts_ms, max_ts_ms) or None if no rows.
    """
    row = conn.execute(
        """
        SELECT MIN(ts_ms) AS min_ts, MAX(ts_ms) AS max_ts
        FROM bars_1m
        WHERE venue = ? AND symbol = ?
        """,
        (venue, symbol),
    ).fetchone()
    if row is None or row[0] is None or row[1] is None:
        return None
    return int(row[0]), int(row[1])


def _resolve_range_ms(
    conn: sqlite3.Connection,
    *,
    venue: str,
    symbol: str,
    start_iso: str | None,
    end_iso: str | None,
) -> tuple[int, int]:
    """
    Resolve [start_ms, end_ms) for aggregation.

    Priority:
    - Use CLI start/end if provided
    - Else use BASE_TIMEFRAME coverage if present
    - Else fall back to min/max ts in bars_1m (end = max_open + 1m)
    """
    cov = get_coverage(conn, venue, symbol, BASE_TIMEFRAME)

    if start_iso is not None:
        start_ms = parse_iso8601_to_ms(start_iso)
    elif cov is not None:
        start_ms = int(cov.start_ms)
    else:
        mm = _get_min_max_ts_ms(conn, venue, symbol)
        if mm is None:
            raise RuntimeError(f"No base {BASE_TIMEFRAME} data found for venue={venue} symbol={symbol}")
        start_ms = mm[0]

    if end_iso is not None:
        end_ms = parse_iso8601_to_ms(end_iso)
    elif cov is not None:
        end_ms = int(cov.end_ms)  # end-exclusive coverage
    else:
        mm = _get_min_max_ts_ms(conn, venue, symbol)
        if mm is None:
            raise RuntimeError(f"No base {BASE_TIMEFRAME} data found for venue={venue} symbol={symbol}")
        end_ms = min(mm[1] + BASE_TF_MS, now_ms())

    if end_ms <= start_ms:
        raise ValueError(f"Invalid range [{start_ms}..{end_ms})")

    return start_ms, end_ms


async def main_async() -> None:
    args = _parse_args()

    if args.timeframe == BASE_TIMEFRAME:
        logger.info("Nothing to do - timeframe={} is base timeframe", BASE_TIMEFRAME)
        return

    conn = sqlite3.connect(args.db_path)
    try:
        # keep behaviour consistent with your DB usage elsewhere
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")

        start_ms, end_ms = _resolve_range_ms(
            conn,
            venue=args.venue,
            symbol=args.symbol,
            start_iso=args.start,
            end_iso=args.end,
        )

        logger.info(
            "Aggregating venue={} symbol={} base_tf={} -> tf={} range=[{}..{})",
            args.venue,
            args.symbol,
            BASE_TIMEFRAME,
            args.timeframe,
            start_ms,
            end_ms,
        )

        # Uses the SAME aggregator path as MarketDataPlant
        build_aggregates(
            conn,
            venue=args.venue,
            symbol=args.symbol,
            timeframes=[args.timeframe],
            start_ms=start_ms,
            end_ms=end_ms,
        )

        logger.info("Aggregation complete.")
    finally:
        conn.close()


def main() -> None:
    try:
        asyncio.run(main_async())
    except KeyboardInterrupt:
        logger.info("Aggregator interrupted by user.")


if __name__ == "__main__":
    main()