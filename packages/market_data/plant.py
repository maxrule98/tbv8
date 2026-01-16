from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from loguru import logger

from packages.common.backfill.types import BackfillAdapter
from packages.common.backfill.service import BackfillService, BackfillSpec
from packages.common.backfill.repair import GapRepairService, GapRepairConfig
from packages.market_data.types import EnsureHistoryRequest

from packages.common.datetime_utils import now_ms, parse_iso8601_to_ms
from packages.common.timeframes import floor_ts_to_tf, ceil_ts_to_tf, timeframe_to_ms


@dataclass(frozen=True)
class MarketDataPlantConfig:
    # Optional: gap repair behaviour. Keep conservative defaults.
    repair_gaps: bool = True
    max_gap_minutes: int = 60 * 24 * 14
    chunk_limit: int = 1000
    max_ranges: int = 500


class MarketDataPlant:
    """
    Owner of market data lifecycle.

    New rule: we do NOT aggregate. We fetch each requested timeframe directly,
    store in bars_{tf}, and track coverage per (venue, symbol, tf).
    """

    def __init__(self, *, adapters: Sequence[BackfillAdapter], cfg: MarketDataPlantConfig | None = None):
        self.cfg = cfg or MarketDataPlantConfig()
        self._backfill = BackfillService(adapters=list(adapters))

    async def ensure_history(self, req: EnsureHistoryRequest) -> None:
        # normalize + validate requested timeframes
        tfs = [tf.strip() for tf in (req.timeframes or []) if tf and tf.strip()]
        if not tfs:
            raise ValueError("EnsureHistoryRequest.timeframes must be non-empty")

        # dedupe preserving order
        seen: set[str] = set()
        timeframes: list[str] = []
        for tf in tfs:
            if tf not in seen:
                seen.add(tf)
                timeframes.append(tf)

        logger.info(
            "MarketDataPlant.ensure_history venue={} symbol={} tfs={} start={} end={}",
            req.venue,
            req.symbol,
            timeframes,
            req.start_date,
            req.end_date,
        )

        # 1) Backfill all requested timeframes directly into bars_{tf}
        await self._backfill.ensure_history(
            BackfillSpec(
                db_path=req.db_path,
                venue=req.venue,
                symbol=req.symbol,
                start_date=req.start_date,
                end_date=req.end_date,
                timeframes=timeframes,
            )
        )

        # 2) Optional: gap repair per timeframe (still useful because upstream can have holes)
        if not self.cfg.repair_gaps:
            return

        repair = GapRepairService(
            self._backfill,
            cfg=GapRepairConfig(
                max_gap_minutes=self.cfg.max_gap_minutes,
                chunk_limit=self.cfg.chunk_limit,
                max_ranges=self.cfg.max_ranges,
            ),
        )

        # We repair each tf independently within the requested date range
        for tf in timeframes:
            tf_ms = timeframe_to_ms(tf)

            scan_start_ms = floor_ts_to_tf(parse_iso8601_to_ms(req.start_date), tf)
            scan_end_ms_excl = (
                ceil_ts_to_tf(parse_iso8601_to_ms(req.end_date), tf)
                if req.end_date
                else ceil_ts_to_tf(now_ms(), tf)
            )

            await repair.repair_gaps(
                db_path=req.db_path,
                venue=req.venue,
                symbol=req.symbol,
                timeframe=tf,
                scan_start_ms=scan_start_ms,
                scan_end_ms_excl=scan_end_ms_excl,
            )