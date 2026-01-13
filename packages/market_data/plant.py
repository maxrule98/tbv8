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
    get_coverage,
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

    Coverage semantics (Option A):
    - history_coverage.end_ms is EXCLUSIVE: [start_ms .. end_ms)
    - For derived bars:
        last_bar_open = max(ts_ms in bars_<tf>)
        coverage_end_excl = last_bar_open + tf_ms
      so coverage_end_excl - last_bar_open == tf_ms
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

            base_max_ts = get_max_ts(conn, req.venue, req.symbol, base_tf)
            if base_max_ts is None:
                logger.warning("No base 1m data after ensure_history - cannot aggregate.")
                return

            for tf in targets:
                tf_ms = timeframe_to_ms(tf)
                overlap_ms = tf_ms * 2  # deterministic safety overlap (cheap)

                # We only claim derived coverage for fully complete buckets.
                #
                # base_max_ts is last 1m candle open time.
                # "latest complete" derived bucket ends at:
                #   derived_end_excl = floor((base_max_ts + 1m), tf)
                # and the last complete candle open is:
                #   last_complete_open = derived_end_excl - tf_ms
                derived_end_excl = floor_ts_to_tf(base_max_ts + timeframe_to_ms("1m"), tf)
                last_complete_open = derived_end_excl - tf_ms

                # Read previous derived coverage (source of truth) - end_ms is EXCLUSIVE.
                prev = get_coverage(conn, req.venue, req.symbol, tf)

                # If no new completed bucket -> skip entirely
                if prev and derived_end_excl <= prev.end_ms:
                    logger.info(
                        "No new completed buckets tf={} (prev_end_excl={} derived_end_excl={}) - skip",
                        tf,
                        prev.end_ms,
                        derived_end_excl,
                    )
                    continue

                # Incremental derived start:
                # - first run: request start aligned to tf
                # - later runs: start slightly before prev.end to be boundary-safe
                req_start_tf = floor_ts_to_tf(start_ms, tf)
                if prev:
                    derived_start = floor_ts_to_tf(max(req_start_tf, prev.end_ms - overlap_ms), tf)
                    cov_start = prev.start_ms  # never change established start
                else:
                    derived_start = req_start_tf
                    cov_start = req_start_tf

                # If window is too small to produce a completed candle, skip
                if derived_end_excl <= derived_start:
                    logger.info(
                        "Derived window too small tf={} (start={} end_excl={}) - skip",
                        tf,
                        derived_start,
                        derived_end_excl,
                    )
                    continue

                # Build aggregates from base in chunks (build_aggregates uses [start..end))
                build_aggregates(
                    conn,
                    venue=req.venue,
                    symbol=req.symbol,
                    start_ms=derived_start,
                    end_ms=derived_end_excl,
                    timeframes=[tf],
                    chunk_days=req.chunk_days,
                )

                # Coverage end is EXCLUSIVE, so it should be last_complete_open + tf_ms == derived_end_excl
                cov_end_excl = max(prev.end_ms, derived_end_excl) if prev else derived_end_excl

                upsert_coverage(
                    conn,
                    CoverageRow(
                        venue=req.venue,
                        symbol=req.symbol,
                        timeframe=tf,
                        start_ms=cov_start,
                        end_ms=cov_end_excl,
                        updated_at_ms=now_ms(),
                    ),
                )
                conn.commit()

                logger.info(
                    "Derived coverage updated tf={} coverage=[{}..{}) last_complete_open={}",
                    tf,
                    cov_start,
                    cov_end_excl,
                    last_complete_open,
                )

        finally:
            conn.close()