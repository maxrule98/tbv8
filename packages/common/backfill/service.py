from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Sequence

from loguru import logger

from packages.common.datetime_utils import now_ms, parse_iso8601_to_ms
from packages.common.timeframes import floor_ts_to_tf, timeframe_to_ms

from packages.common.backfill.types import BackfillAdapter
from packages.common.backfill.sqlite_store import (
    CoverageRow,
    ensure_schema,
    get_max_ts,
    upsert_1m,
    upsert_coverage,
    upsert_agg,
)
from packages.common.backfill.aggregate import load_1m_range, aggregate_from_1m


@dataclass(frozen=True)
class BackfillSpec:
    db_path: str
    venue: str
    symbol: str
    start_date: str
    end_date: str | None
    timeframes: Sequence[str]


class BackfillService:
    def __init__(self, adapters: Sequence[BackfillAdapter]):
        self._adapters = {a.venue: a for a in adapters}

    def _adapter(self, venue: str) -> BackfillAdapter:
        a = self._adapters.get(venue)
        if not a:
            raise KeyError(f"No backfill adapter registered for venue={venue!r}")
        return a

    async def ensure_history(self, spec: BackfillSpec) -> None:
        adapter = self._adapter(spec.venue)

        start_ms = floor_ts_to_tf(parse_iso8601_to_ms(spec.start_date), "1m")
        end_ms = (
            floor_ts_to_tf(parse_iso8601_to_ms(spec.end_date), "1m")
            if spec.end_date
            else floor_ts_to_tf(now_ms(), "1m")
        )

        if end_ms <= start_ms:
            raise ValueError(f"end_ms <= start_ms (start_date={spec.start_date} end_date={spec.end_date})")

        conn = sqlite3.connect(spec.db_path)
        try:
            ensure_schema(conn)

            max_ts = get_max_ts(conn, adapter.venue, spec.symbol, "1m")

            # Determine what we fetch
            if max_ts is None:
                fetch_start = start_ms
                logger.info(
                    "History empty -> bootstrap 1m venue={} symbol={} [{}..{})",
                    adapter.venue,
                    spec.symbol,
                    start_ms,
                    end_ms,
                )
            else:
                fetch_start = max(max_ts + timeframe_to_ms("1m"), start_ms)
                if fetch_start >= end_ms:
                    logger.info(
                        "History already up-to-date venue={} symbol={} max_ts={} end_ms={}",
                        adapter.venue,
                        spec.symbol,
                        max_ts,
                        end_ms,
                    )
                    # Still ensure aggregates exist/are refreshed over requested window
                    self._aggregate_all(conn, adapter.venue, spec.symbol, start_ms, end_ms, spec.timeframes)
                    conn.commit()
                    return

                logger.info(
                    "History present -> tail update venue={} symbol={} fetch [{}..{}) (max_ts={})",
                    adapter.venue,
                    spec.symbol,
                    fetch_start,
                    end_ms,
                    max_ts,
                )

            await self._backfill_1m(conn, adapter, spec.symbol, fetch_start, end_ms)

            upsert_coverage(
                conn,
                CoverageRow(
                    venue=adapter.venue,
                    symbol=spec.symbol,
                    timeframe="1m",
                    start_ms=start_ms,
                    end_ms=end_ms,
                    updated_at_ms=now_ms(),
                ),
            )

            # Aggregate efficiently:
            # - bootstrap: full [start_ms..end_ms)
            # - tail update: only from (fetch_start - max_tf_ms) to end_ms to rebuild boundary buckets
            agg_start = start_ms
            if max_ts is not None:
                max_tf_ms = 0
                for tf in spec.timeframes:
                    if tf != "1m":
                        max_tf_ms = max(max_tf_ms, timeframe_to_ms(tf))
                if max_tf_ms > 0:
                    agg_start = max(start_ms, fetch_start - max_tf_ms)

            self._aggregate_all(conn, adapter.venue, spec.symbol, agg_start, end_ms, spec.timeframes)

            conn.commit()
            logger.info("ensure_history complete venue={} symbol={} [{}..{})", adapter.venue, spec.symbol, start_ms, end_ms)

        finally:
            conn.close()

    async def _backfill_1m(
        self,
        conn: sqlite3.Connection,
        adapter: BackfillAdapter,
        symbol: str,
        start_ms: int,
        end_ms: int,
    ) -> None:
        tf_ms = timeframe_to_ms("1m")
        cursor = int(start_ms)
        pages = 0
        total_rows = 0

        while cursor < end_ms:
            rows = await adapter.fetch_ohlcv(
                symbol=symbol,
                timeframe="1m",
                start_ms=cursor,
                end_ms=end_ms,
                limit=1000,
            )

            if not rows:
                logger.warning("Backfill got 0 rows at cursor={} - stopping", cursor)
                break

            wrote = upsert_1m(conn, adapter.venue, symbol, rows)
            conn.commit()

            pages += 1
            total_rows += wrote

            last_ts = floor_ts_to_tf(rows[-1].ts_ms, "1m")
            next_cursor = last_ts + tf_ms
            if next_cursor <= cursor:
                logger.warning("Backfill cursor did not advance (cursor={} last_ts={}) - stopping", cursor, last_ts)
                break

            cursor = next_cursor

            if pages % 10 == 0:
                logger.info("Backfill progress pages={} total_rows={} cursor={}", pages, total_rows, cursor)

        logger.info("Backfill done pages={} total_rows={}", pages, total_rows)

    def _aggregate_all(
        self,
        conn: sqlite3.Connection,
        venue: str,
        symbol: str,
        start_ms: int,
        end_ms: int,
        timeframes: Sequence[str],
    ) -> None:
        targets = [tf for tf in timeframes if tf != "1m"]
        if not targets:
            return

        base = load_1m_range(conn, venue, symbol, start_ms, end_ms)
        if not base:
            logger.warning("No 1m bars for venue={} symbol={} in [{}..{}) - cannot aggregate", venue, symbol, start_ms, end_ms)
            return

        for tf in targets:
            agg = aggregate_from_1m(base, timeframe=tf)
            if not agg:
                continue
            upserted = upsert_agg(conn, tf, venue, symbol, agg)
            logger.info("Aggregated 1m -> {} upserted={}", tf, upserted)
