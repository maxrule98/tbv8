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
    upsert_coverage,
)
from packages.common.backfill.service import BackfillService, BackfillSpec
from packages.common.backfill.aggregate import build_aggregates
from packages.market_data.types import EnsureHistoryRequest


@dataclass(frozen=True)
class MarketDataPlantConfig:
    # Base timeframe is a design choice - we standardize on 1m everywhere.
    base_timeframe: str = "1m"


class MarketDataPlant:
    """
    Single owner of market data lifecycle:
    - Base backfill (1m)
    - Derived aggregation (5m, 15m, 1h, ...)
    - Coverage tracking for both base + derived
    """

    def __init__(self, *, adapters: Sequence[BackfillAdapter], cfg: MarketDataPlantConfig | None = None):
        self.cfg = cfg or MarketDataPlantConfig()
        self._backfill = BackfillService(adapters=list(adapters))

    async def ensure_history(self, req: EnsureHistoryRequest) -> None:
        base_tf = self.cfg.base_timeframe
        if base_tf != "1m":
            raise ValueError("This repo standardizes on base_timeframe='1m'")

        # Normalize timeframe list: always include 1m
        tfs = [tf.strip() for tf in req.timeframes if tf and tf.strip()]
        if base_tf not in tfs:
            tfs = [base_tf, *tfs]
        # Deduplicate preserving order
        seen: set[str] = set()
        timeframes: list[str] = []
        for tf in tfs:
            if tf not in seen:
                seen.add(tf)
                timeframes.append(tf)

        start_ms = floor_ts_to_tf(parse_iso8601_to_ms(req.start_date), base_tf)
        end_ms = (
            floor_ts_to_tf(parse_iso8601_to_ms(req.end_date), base_tf)
            if req.end_date
            else floor_ts_to_tf(now_ms(), base_tf)
        )
        if end_ms <= start_ms:
            raise ValueError(f"end_ms <= start_ms (start_date={req.start_date} end_date={req.end_date})")

        logger.info(
            "MarketDataPlant.ensure_history venue={} symbol={} tfs={} range=[{}..{})",
            req.venue,
            req.symbol,
            timeframes,
            start_ms,
            end_ms,
        )

        # 1) Ensure base 1m exists/up-to-date (delegates to BackfillService)
        await self._backfill.ensure_history(
            BackfillSpec(
                db_path=req.db_path,
                venue=req.venue,
                symbol=req.symbol,
                start_date=req.start_date,
                end_date=req.end_date,
                timeframes=[base_tf],  # service handles base only cleanly
            )
        )

        # 2) Ensure derived timeframes exist (aggregate from 1m)
        targets = [tf for tf in timeframes if tf != base_tf]
        if not targets:
            return

        conn = sqlite3.connect(req.db_path)
        try:
            ensure_schema(conn)

            # We always compute derived coverage as:
            # [first_complete_bucket..last_complete_bucket] derived from base coverage.
            base_max_ts = get_max_ts(conn, req.venue, req.symbol, base_tf)
            if base_max_ts is None:
                logger.warning("No base 1m data after ensure_history - cannot aggregate.")
                return

            # Derived bars can only be trusted up to the last *fully completed* bucket.
            # If base_max_ts is 22:31, 5m last complete bucket is 22:25.
            # Compute derived_end as floor(base_max_ts, tf) - tf_ms
            # BUT: if base_max_ts is exactly on boundary and represents a completed 1m bar,
            # floor gives bucket start; the last fully complete bucket is that bucket if we have all minutes.
            # We keep it simple + conservative: derived_end_exclusive = floor(base_max_ts + 60_000, tf)
            # and derived_end_inclusive_start = derived_end_exclusive - tf_ms.
            for tf in targets:
                tf_ms = timeframe_to_ms(tf)

                derived_start = floor_ts_to_tf(start_ms, tf)

                derived_end_excl = floor_ts_to_tf(base_max_ts + timeframe_to_ms("1m"), tf)
                derived_end = derived_end_excl - tf_ms

                if derived_end <= derived_start:
                    logger.warning("Derived window too small for tf={} start={} end={}", tf, derived_start, derived_end)
                    continue

                # Build aggregates from base in chunks.
                build_aggregates(
                    conn,
                    venue=req.venue,
                    symbol=req.symbol,
                    start_ms=derived_start,
                    end_ms=derived_end_excl,  # build_aggregates uses [start..end)
                    timeframes=[tf],
                    chunk_days=req.chunk_days,
                )

                # Update derived coverage row
                upsert_coverage(
                    conn,
                    CoverageRow(
                        venue=req.venue,
                        symbol=req.symbol,
                        timeframe=tf,
                        start_ms=derived_start,
                        end_ms=derived_end,
                        updated_at_ms=now_ms(),
                    ),
                )
                conn.commit()

                logger.info(
                    "Derived coverage updated tf={} coverage=[{}..{}]",
                    tf,
                    derived_start,
                    derived_end,
                )

        finally:
            conn.close()
