from __future__ import annotations

import sqlite3
from dataclasses import dataclass

from loguru import logger

from packages.common.timeframes import floor_ts_to_tf, ceil_ts_to_tf, timeframe_to_ms
from packages.common.datetime_utils import now_ms
from packages.common.backfill.service import BackfillService
from packages.common.backfill.sqlite_store import (
    ensure_schema,
    find_gaps_tf,
    GapRange,
    upsert_tf,
    is_known_missing_range,      
    record_known_missing_range, 
)


@dataclass(frozen=True)
class GapRepairConfig:
    max_gap_minutes: int = 60 * 24 * 14   # skip insane gaps by default
    chunk_limit: int = 1000               # Binance max klines per request
    max_ranges: int = 200                 # safety cap

    def chunk_minutes_for_tf(self, tf: str) -> int:
        # How many minutes does 1000 klines represent?
        # 1m => 1000 minutes, 5m => 5000 minutes, etc.
        tf_ms = timeframe_to_ms(tf)
        return (self.chunk_limit * tf_ms) // 60_000


class GapRepairService:
    def __init__(self, backfill: BackfillService, cfg: GapRepairConfig | None = None):
        self._backfill = backfill
        self._cfg = cfg or GapRepairConfig()

    async def repair_gaps(
        self,
        *,
        db_path: str,
        venue: str,
        symbol: str,
        timeframe: str,
        scan_start_ms: int | None = None,
        scan_end_ms_excl: int | None = None,
    ) -> int:
        conn = sqlite3.connect(db_path)
        try:
            ensure_schema(conn)

            gaps = find_gaps_tf(
                conn,
                venue=venue,
                symbol=symbol,
                tf=timeframe,
                start_ms=scan_start_ms,
                end_ms_excl=scan_end_ms_excl,
                limit=self._cfg.max_ranges,
            )

            if not gaps:
                logger.info("No gaps found tf={} venue={} symbol={}", timeframe, venue, symbol)
                return 0

            logger.warning("Found {} gap ranges tf={} venue={} symbol={}", len(gaps), timeframe, venue, symbol)

        finally:
            conn.close()

        adapter = self._backfill._adapter(venue)

        tf_ms = timeframe_to_ms(timeframe)
        max_gap_ms = self._cfg.max_gap_minutes * 60_000

        # Request window size based on Binance limit
        chunk_ms = self._cfg.chunk_limit * tf_ms

        attempted = 0
        for g in gaps:
            gap_len = g.end_ms_excl - g.start_ms
            if gap_len <= 0:
                continue
            if gap_len > max_gap_ms:
                logger.warning(
                    "Skipping huge gap tf={} [{}..{}) len_ms={}",
                    timeframe,
                    g.start_ms,
                    g.end_ms_excl,
                    gap_len,
                )
                continue

            attempted += 1
            logger.info(
                "Repairing gap {} tf={}: [{}..{}) candles={}",
                attempted,
                timeframe,
                g.start_ms,
                g.end_ms_excl,
                gap_len // tf_ms,
            )

            cursor = floor_ts_to_tf(g.start_ms, timeframe)
            end_excl = ceil_ts_to_tf(g.end_ms_excl, timeframe)

            conn2 = sqlite3.connect(db_path)
            try:
                ensure_schema(conn2)

                while cursor < end_excl:
                    window_end_excl = min(cursor + chunk_ms, end_excl)

                    # Skip chunks we already proved are "no data exists"
                    if is_known_missing_range(
                        conn2,
                        venue=venue,
                        symbol=symbol,
                        tf=timeframe,
                        start_ms=cursor,
                        end_ms_excl=window_end_excl,
                    ):
                        logger.info(
                            "Skipping known-missing chunk tf={} [{}..{})",
                            timeframe,
                            cursor,
                            window_end_excl,
                        )
                        cursor = window_end_excl
                        continue

                    rows = await adapter.fetch_ohlcv(
                        symbol=symbol,
                        timeframe=timeframe,
                        start_ms=cursor,
                        end_ms=window_end_excl,
                        limit=self._cfg.chunk_limit,
                    )

                    if rows:
                        wrote = upsert_tf(conn2, timeframe, venue, symbol, rows)
                        conn2.commit()
                        logger.info(
                            "Gap chunk tf={} [{}..{}) fetched={} upserted={}",
                            timeframe,
                            cursor,
                            window_end_excl,
                            len(rows),
                            wrote,
                        )
                    else:
                        logger.warning(
                            "Gap chunk tf={} [{}..{}) fetched 0 rows",
                            timeframe,
                            cursor,
                            window_end_excl,
                        )

                        # PROBE: fetch the *next* available candle from cursor (limit=1)
                        # NOTE: best is omitting end_ms entirely, but if adapter requires it,
                        # use end_excl as a reasonable bound (or now_ms()).
                        probe = await adapter.fetch_ohlcv(
                            symbol=symbol,
                            timeframe=timeframe,
                            start_ms=cursor,
                            end_ms=end_excl,   # if you later support end_ms=None, make this None
                            limit=1,
                        )

                        # If the first candle we can see is AFTER this chunk, then nothing exists inside this chunk.
                        if probe and probe[0].ts_ms >= window_end_excl:
                            record_known_missing_range(
                                conn2,
                                venue=venue,
                                symbol=symbol,
                                tf=timeframe,
                                start_ms=cursor,
                                end_ms_excl=window_end_excl,
                                reason="binance_no_data",
                                updated_at_ms=now_ms(),
                            )
                            conn2.commit()
                            logger.info(
                                "Marked known-missing tf={} [{}..{}) (next candle at {})",
                                timeframe,
                                cursor,
                                window_end_excl,
                                probe[0].ts_ms,
                            )
                        elif not probe:
                            # If probe returns nothing at all, Binance is basically saying "no data after cursor".
                            # Still safe to mark this chunk as known-missing.
                            record_known_missing_range(
                                conn2,
                                venue=venue,
                                symbol=symbol,
                                tf=timeframe,
                                start_ms=cursor,
                                end_ms_excl=window_end_excl,
                                reason="binance_no_data_probe_empty",
                                updated_at_ms=now_ms(),
                            )
                            conn2.commit()
                            logger.info(
                                "Marked known-missing tf={} [{}..{}) (probe returned empty)",
                                timeframe,
                                cursor,
                                window_end_excl,
                            )

                    cursor = window_end_excl

            finally:
                conn2.close()

        return attempted