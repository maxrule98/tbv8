from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import List, Sequence

from loguru import logger

from packages.common.timeframes import floor_ts_to_tf, timeframe_to_ms
from packages.common.backfill.types import OHLCV
from packages.common.backfill.sqlite_store import upsert_agg


def _validate_target_tf(tf: str) -> None:
    ms = timeframe_to_ms(tf)
    if ms <= 60_000:
        raise ValueError(f"timeframe must be > 1m for aggregation targets (got {tf})")


def load_1m_range(conn: sqlite3.Connection, venue: str, symbol: str, start_ms: int, end_ms: int) -> List[OHLCV]:
    rows = conn.execute(
        """
        SELECT ts_ms, open, high, low, close, volume
        FROM ohlcv_1m
        WHERE venue=? AND symbol=? AND ts_ms >= ? AND ts_ms < ?
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


@dataclass
class _AggState:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


def aggregate_from_1m(bars: Sequence[OHLCV], *, timeframe: str, complete_end_ms: int) -> List[OHLCV]:
    """
    Aggregate 1m OHLCV -> OHLCV(timeframe), emitting ONLY completed HTF candles.

    We only emit buckets where bucket_start < complete_end_ms,
    where complete_end_ms is typically floor_ts_to_tf(end_ms, timeframe).

    OHLCV is frozen/immutable, so we use an internal mutable state.
    """
    _validate_target_tf(timeframe)

    out: List[OHLCV] = []

    st: _AggState | None = None
    st_bucket: int | None = None

    for b in bars:
        buck = floor_ts_to_tf(b.ts_ms, timeframe)

        # Skip anything that belongs to an incomplete bucket.
        if buck >= complete_end_ms:
            break

        if st is None:
            st_bucket = buck
            st = _AggState(
                ts_ms=buck,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
            )
            continue

        if buck == st_bucket:
            st.high = max(st.high, b.high)
            st.low = min(st.low, b.low)
            st.close = b.close
            st.volume += b.volume
            continue

        # rollover -> emit completed candle
        out.append(
            OHLCV(
                ts_ms=st.ts_ms,
                open=st.open,
                high=st.high,
                low=st.low,
                close=st.close,
                volume=st.volume,
            )
        )

        # start new candle
        st_bucket = buck
        st = _AggState(
            ts_ms=buck,
            open=b.open,
            high=b.high,
            low=b.low,
            close=b.close,
            volume=b.volume,
        )

    # Emit the final bucket if it is complete.
    # A bucket starting at st_bucket is complete iff st_bucket + tf_ms <= complete_end_ms
    if st is not None and st_bucket is not None:
        tf_ms = timeframe_to_ms(timeframe)
        if (st_bucket + tf_ms) <= complete_end_ms:
            out.append(
                OHLCV(
                    ts_ms=st.ts_ms,
                    open=st.open,
                    high=st.high,
                    low=st.low,
                    close=st.close,
                    volume=st.volume,
                )
            )
    return out


def build_aggregates(
    conn: sqlite3.Connection,
    *,
    venue: str,
    symbol: str,
    start_ms: int,
    end_ms: int,
    timeframes: Sequence[str],
    chunk_days: int = 7,
) -> None:
    targets = [tf.strip() for tf in timeframes if tf.strip() and tf.strip() != "1m"]
    if not targets:
        logger.info("No target timeframes (or only 1m) requested - nothing to do.")
        return

    if chunk_days <= 0:
        raise ValueError("chunk_days must be > 0")

    # Defensive alignment to 1m
    start_ms = floor_ts_to_tf(int(start_ms), "1m")
    end_ms = floor_ts_to_tf(int(end_ms), "1m")

    chunk_ms = chunk_days * 24 * 60 * 60_000

    max_tf_ms = 0
    for tf in targets:
        _validate_target_tf(tf)
        max_tf_ms = max(max_tf_ms, timeframe_to_ms(tf))

    logger.info(
        "Aggregating {} {} from {}..{} into {} (chunk_days={})",
        venue,
        symbol,
        start_ms,
        end_ms,
        list(targets),
        chunk_days,
    )

    cursor = start_ms
    while cursor < end_ms:
        chunk_end = min(cursor + chunk_ms, end_ms)

        # Load a slightly earlier range so boundary candles are correct.
        load_start = max(start_ms, cursor - max_tf_ms)
        base = load_1m_range(conn, venue, symbol, load_start, chunk_end)
        if not base:
            logger.warning("No 1m data in chunk load [{}..{}) - skipping", load_start, chunk_end)
            cursor = chunk_end
            continue

        for tf in targets:
            # Only build completed candles up to this chunk_end
            complete_end = floor_ts_to_tf(chunk_end, tf)

            agg = aggregate_from_1m(base, timeframe=tf, complete_end_ms=complete_end)

            # Keep only candles that belong to this chunk’s "output window"
            # so repeated runs are stable and chunk boundaries don’t thrash.
            tf_cursor = floor_ts_to_tf(cursor, tf)
            filtered = [c for c in agg if tf_cursor <= c.ts_ms < complete_end]

            upserted = upsert_agg(conn, tf, venue, symbol, filtered)
            logger.info("Chunk [{}..{}) -> {} upserted={}", cursor, chunk_end, tf, upserted)

        conn.commit()
        cursor = chunk_end