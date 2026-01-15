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
    get_max_ts_tf,
    get_min_max_ts_tf,
    upsert_coverage,
    upsert_tf,
)


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

        conn = sqlite3.connect(spec.db_path)
        try:
            ensure_schema(conn)

            for tf in spec.timeframes:
                tf_ms = timeframe_to_ms(tf)

                start_ms = floor_ts_to_tf(parse_iso8601_to_ms(spec.start_date), tf)
                end_ms = (
                    floor_ts_to_tf(parse_iso8601_to_ms(spec.end_date), tf)
                    if spec.end_date
                    else floor_ts_to_tf(now_ms(), tf)
                )

                if end_ms <= start_ms:
                    raise ValueError(f"end_ms <= start_ms for tf={tf} ({spec.start_date}..{spec.end_date})")

                max_ts = get_max_ts_tf(conn, adapter.venue, spec.symbol, tf)

                if max_ts is None:
                    fetch_start = start_ms
                    logger.info(
                        "History empty -> bootstrap tf={} venue={} symbol={} [{}..{})",
                        tf,
                        adapter.venue,
                        spec.symbol,
                        start_ms,
                        end_ms,
                    )
                else:
                    fetch_start = max(max_ts + tf_ms, start_ms)
                    if fetch_start >= end_ms:
                        logger.info(
                            "History up-to-date tf={} venue={} symbol={} max_ts={} end_ms={}",
                            tf,
                            adapter.venue,
                            spec.symbol,
                            max_ts,
                            end_ms,
                        )
                        # Still ensure coverage is correct from materialized table
                        mm = get_min_max_ts_tf(conn, adapter.venue, spec.symbol, tf)
                        if mm is not None:
                            min_ts, max_ts2 = mm
                            upsert_coverage(
                                conn,
                                CoverageRow(
                                    venue=adapter.venue,
                                    symbol=spec.symbol,
                                    timeframe=tf,
                                    start_ms=min_ts,
                                    end_ms=max_ts2 + tf_ms,  # end-exclusive
                                    updated_at_ms=now_ms(),
                                ),
                            )
                            conn.commit()
                        continue

                    logger.info(
                        "History present -> tail update tf={} venue={} symbol={} fetch [{}..{}) (max_ts={})",
                        tf,
                        adapter.venue,
                        spec.symbol,
                        fetch_start,
                        end_ms,
                        max_ts,
                    )

                await self._backfill_tf(conn, adapter, spec.symbol, tf, fetch_start, end_ms)

                mm = get_min_max_ts_tf(conn, adapter.venue, spec.symbol, tf)
                if mm is None:
                    logger.warning("Backfill wrote no rows? tf={} venue={} symbol={}", tf, adapter.venue, spec.symbol)
                    continue

                min_ts, max_ts2 = mm
                upsert_coverage(
                    conn,
                    CoverageRow(
                        venue=adapter.venue,
                        symbol=spec.symbol,
                        timeframe=tf,
                        start_ms=min_ts,
                        end_ms=max_ts2 + tf_ms,  # end-exclusive
                        updated_at_ms=now_ms(),
                    ),
                )
                conn.commit()
                logger.info("ensure_history tf={} complete coverage=[{}..{})", tf, min_ts, max_ts2 + tf_ms)

        finally:
            conn.close()

    async def _backfill_tf(
        self,
        conn: sqlite3.Connection,
        adapter: BackfillAdapter,
        symbol: str,
        tf: str,
        start_ms: int,
        end_ms: int,
    ) -> None:
        tf_ms = timeframe_to_ms(tf)
        cursor = int(start_ms)
        pages = 0
        total_rows = 0

        while cursor < end_ms:
            rows = await adapter.fetch_ohlcv(
                symbol=symbol,
                timeframe=tf,
                start_ms=cursor,
                end_ms=end_ms,
                limit=1000,
            )

            if not rows:
                logger.warning("Backfill got 0 rows tf={} at cursor={} - stopping", tf, cursor)
                break

            wrote = upsert_tf(conn, tf, adapter.venue, symbol, rows)
            conn.commit()

            pages += 1
            total_rows += wrote

            last_ts = floor_ts_to_tf(rows[-1].ts_ms, tf)
            next_cursor = last_ts + tf_ms
            if next_cursor <= cursor:
                logger.warning(
                    "Backfill cursor did not advance tf={} (cursor={} last_ts={}) - stopping",
                    tf,
                    cursor,
                    last_ts,
                )
                break

            cursor = next_cursor

            if pages % 10 == 0:
                logger.info("Backfill progress tf={} pages={} total_rows={} cursor={}", tf, pages, total_rows, cursor)

        logger.info("Backfill done tf={} pages={} total_rows={}", tf, pages, total_rows)