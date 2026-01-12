from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Iterator, List

from loguru import logger

from packages.common.backfill.types import OHLCV
from packages.common.timeframes import floor_ts_to_tf
from packages.common.backfill.sqlite_store import upsert_agg


@dataclass(frozen=True)
class LoadRange:
    venue: str
    symbol: str
    start_ms: int
    end_ms: int


def load_1m_range(conn: sqlite3.Connection, q: LoadRange) -> List[OHLCV]:
    rows = conn.execute(
        """
        SELECT ts_ms, open, high, low, close, volume
        FROM ohlcv_1m
        WHERE venue=? AND symbol=? AND ts_ms >= ? AND ts_ms < ?
        ORDER BY ts_ms ASC
        """,
        (q.venue, q.symbol, q.start_ms, q.end_ms),
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


def aggregate_from_1m(bars_1m: Iterable[OHLCV], timeframe: str) -> List[OHLCV]:
    """
    Deterministic OHLCV aggregation from 1m -> timeframe.

    Important: OHLCV is immutable (frozen dataclass), so we aggregate using local
    variables and instantiate OHLCV when a bucket closes.
    """
    if timeframe == "1m":
        return list(bars_1m)

    out: List[OHLCV] = []

    cur_bucket: int | None = None
    o = h = l = c = v = None  # type: ignore[assignment]

    def flush(bucket_ts: int) -> None:
        nonlocal o, h, l, c, v
        if o is None:
            return
        out.append(
            OHLCV(
                ts_ms=bucket_ts,
                open=float(o),
                high=float(h),
                low=float(l),
                close=float(c),
                volume=float(v),
            )
        )
        o = h = l = c = v = None  # reset

    for b in bars_1m:
        bucket = floor_ts_to_tf(b.ts_ms, timeframe)

        if cur_bucket is None:
            # first bar
            cur_bucket = bucket
            o, h, l, c, v = b.open, b.high, b.low, b.close, b.volume
            continue

        if bucket != cur_bucket:
            # bucket rollover
            flush(cur_bucket)
            cur_bucket = bucket
            o, h, l, c, v = b.open, b.high, b.low, b.close, b.volume
            continue

        # same bucket - update accumulators
        h = max(h, b.high)
        l = min(l, b.low)
        c = b.close
        v = v + b.volume

    if cur_bucket is not None:
        flush(cur_bucket)

    return out


def iter_ranges(start_ms: int, end_ms: int, chunk_ms: int) -> Iterator[tuple[int, int]]:
    cur = start_ms
    while cur < end_ms:
        nxt = min(cur + chunk_ms, end_ms)
        yield cur, nxt
        cur = nxt


def build_aggregates(
    *,
    db_path: str,
    venue: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    timeframes: List[str],
    chunk_days: int = 7,
) -> None:
    """
    Aggregates ohlcv_1m -> bars_{tf} in time chunks to avoid loading the entire
    history into memory.
    """
    tfs = [tf for tf in timeframes if tf != "1m"]
    if not tfs:
        logger.info("No aggregate timeframes requested (only 1m). Nothing to do.")
        return

    chunk_ms = chunk_days * 24 * 60 * 60 * 1000

    conn = sqlite3.connect(db_path)
    try:
        logger.info(
            "Aggregating {} {} from {}..{} into {} (chunk_days={})",
            venue,
            symbol,
            start_ms,
            end_ms,
            tfs,
            chunk_days,
        )

        for i, (a, b) in enumerate(iter_ranges(start_ms, end_ms, chunk_ms), start=1):
            base = load_1m_range(conn, LoadRange(venue=venue, symbol=symbol, start_ms=a, end_ms=b))
            if not base:
                continue

            for tf in tfs:
                agg = aggregate_from_1m(base, tf)
                wrote = upsert_agg(conn, tf, venue, symbol, agg)
                logger.info(
                    "chunk={} tf={} range=[{}..{}) base={} wrote={}",
                    i,
                    tf,
                    a,
                    b,
                    len(base),
                    wrote,
                )

            conn.commit()

        logger.info("Aggregation complete.")
    finally:
        conn.close()
