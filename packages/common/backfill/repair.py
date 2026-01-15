from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from loguru import logger

from packages.common.constants import BASE_TIMEFRAME, BASE_TF_MS
from packages.common.timeframes import floor_ts_to_tf
from packages.common.backfill.service import BackfillService
from packages.common.backfill.sqlite_store import ensure_schema, find_gaps_1m, GapRange, upsert_1m


@dataclass(frozen=True)
class GapRepairConfig:
    max_gap_minutes: int = 60 * 24 * 14   # skip insane gaps by default
    chunk_minutes: int = 1000             # Binance max limit per request for 1m
    max_ranges: int = 200                 # safety cap


class GapRepairService:
    def __init__(self, backfill: BackfillService, cfg: GapRepairConfig | None = None):
        self._backfill = backfill
        self._cfg = cfg or GapRepairConfig()

    async def repair_1m_gaps(
        self,
        *,
        db_path: str,
        venue: str,
        symbol: str,
        scan_start_ms: int | None = None,
        scan_end_ms_excl: int | None = None,
    ) -> int:
        """
        Finds gaps in ohlcv_1m and tries to refill them via adapter.fetch_ohlcv.
        Returns number of gap-ranges attempted.
        """
        conn = sqlite3.connect(db_path)
        try:
            ensure_schema(conn)

            gaps = find_gaps_1m(
                conn,
                venue=venue,
                symbol=symbol,
                start_ms=scan_start_ms,
                end_ms_excl=scan_end_ms_excl,
                limit=self._cfg.max_ranges,
            )

            if not gaps:
                logger.info("No 1m gaps found venue={} symbol={}", venue, symbol)
                return 0

            logger.warning("Found {} gap ranges venue={} symbol={}", len(gaps), venue, symbol)

        finally:
            conn.close()

        # We do the repairs outside the first connection so we can re-open per loop safely.
        attempted = 0
        adapter = self._backfill._adapter(venue)  # internal, but fine for now in TBV8 single-dev

        max_gap_ms = self._cfg.max_gap_minutes * 60_000
        chunk_ms = self._cfg.chunk_minutes * 60_000

        for g in gaps:
            gap_len = g.end_ms_excl - g.start_ms
            if gap_len <= 0:
                continue

            if gap_len > max_gap_ms:
                logger.warning("Skipping huge gap [{}..{}) len_ms={}", g.start_ms, g.end_ms_excl, gap_len)
                continue

            attempted += 1
            logger.info("Repairing gap {}: [{}..{}) minutes={}", attempted, g.start_ms, g.end_ms_excl, gap_len // 60_000)

            cursor = floor_ts_to_tf(g.start_ms, BASE_TIMEFRAME)
            end_excl = floor_ts_to_tf(g.end_ms_excl, BASE_TIMEFRAME)

            conn2 = sqlite3.connect(db_path)
            try:
                ensure_schema(conn2)

                while cursor < end_excl:
                    window_end_excl = min(cursor + chunk_ms, end_excl)

                    # IMPORTANT: strict [start, end_excl) semantics
                    rows = await adapter.fetch_ohlcv(
                        symbol=symbol,
                        timeframe=BASE_TIMEFRAME,
                        start_ms=cursor,
                        end_ms=window_end_excl,
                        limit=1000,
                    )

                    if rows:
                        wrote = upsert_1m(conn2, venue, symbol, rows)
                        conn2.commit()
                        logger.info("Gap chunk [{}..{}) fetched={} upserted={}", cursor, window_end_excl, len(rows), wrote)
                    else:
                        logger.warning("Gap chunk [{}..{}) fetched 0 rows", cursor, window_end_excl)

                    cursor = window_end_excl

            finally:
                conn2.close()

        return attempted