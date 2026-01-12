from __future__ import annotations

import sqlite3
from dataclasses import replace
from typing import List, Sequence

from loguru import logger

from packages.common.timeframes import floor_ts_to_tf
from packages.common.backfill.types import OHLCV
from packages.common.backfill.sqlite_store import upsert_agg


def load_1m_range(
    conn: sqlite3.Connection,
    venue: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
) -> List[OHLCV]:
    """
    Load 1m OHLCV bars from the canonical base table: ohlcv_1m.

    Range semantics: [start_ms, end_ms)
    """
    rows = conn.execute(
        """
        SELECT ts_ms, open, high, low, close, volume
        FROM ohlcv_1m
        WHERE venue = ? AND symbol = ? AND ts_ms >= ? AND ts_ms < ?
        ORDER BY ts_ms ASC
        """,
        (venue, symbol, int(start_ms), int(end_ms)),
    ).fetchall()

    return [
        OHLCV(
            ts_ms=int(r[0]),
            open=float(r[1]),
            high=float(r[2]),
            low=float(r[3]),
            close=float(r[4]),
            volume=float(r[5]),
        )
        for r in rows
    ]


def aggregate_from_1m(base: Sequence[OHLCV], timeframe: str) -> List[OHLCV]:
    """
    Aggregate sorted 1m bars into a higher timeframe.

    - Output ts_ms is floored to the timeframe bucket.
    - Safe with frozen dataclasses (uses dataclasses.replace).
    """
    if not base:
        return []

    out: List[OHLCV] = []
    cur: OHLCV | None = None

    for b in base:
        bucket = floor_ts_to_tf(b.ts_ms, timeframe)

        if cur is None or cur.ts_ms != bucket:
            cur = OHLCV(
                ts_ms=int(bucket),
                open=float(b.open),
                high=float(b.high),
                low=float(b.low),
                close=float(b.close),
                volume=float(b.volume),
            )
            out.append(cur)
            continue

        updated = replace(
            cur,
            high=max(cur.high, b.high),
            low=min(cur.low, b.low),
            close=b.close,
            volume=cur.volume + b.volume,
        )
        out[-1] = updated
        cur = updated

    return out


def build_aggregates(
    conn: sqlite3.Connection,
    venue: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    timeframes: Sequence[str],
    chunk_days: int = 7,
) -> None:
    """
    Chunked aggregation helper (used by apps.aggregator.main).
    Reads from ohlcv_1m and writes to bars_{tf}.
    """
    targets = [tf for tf in timeframes if tf != "1m"]
    if not targets:
        return

    chunk_ms = int(chunk_days) * 24 * 60 * 60 * 1000
    cursor = int(start_ms)

    logger.info(
        "Aggregating {} {} from {}..{} into {} (chunk_days={})",
        venue,
        symbol,
        start_ms,
        end_ms,
        targets,
        chunk_days,
    )

    while cursor < end_ms:
        chunk_end = min(cursor + chunk_ms, int(end_ms))

        base = load_1m_range(conn, venue, symbol, cursor, chunk_end)
        if base:
            for tf in targets:
                agg = aggregate_from_1m(base, timeframe=tf)
                if agg:
                    upserted = upsert_agg(conn, tf, venue, symbol, agg)
                    logger.info("Aggregated chunk {}..{} 1m -> {} upserted={}", cursor, chunk_end, tf, upserted)

            conn.commit()

        cursor = chunk_end

    logger.info("Aggregation complete.")
