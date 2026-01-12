from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from packages.common.config import TBV8Config
from packages.common.backfill.service import BackfillService, BackfillSpec
from packages.common.backfill.types import BackfillAdapter


@dataclass(frozen=True)
class EnsureHistoryRequest:
    venue: str
    symbol: str
    start_date: str
    end_date: str | None
    timeframes: Sequence[str]


class MarketDataPlant:
    """
    MarketDataPlant owns:
    - history bootstrap + tail updates (REST backfill)
    - aggregation from 1m -> requested timeframes
    (later) - live WS ingestion + reconciliation + deterministic time authority
    """

    def __init__(self, cfg: TBV8Config, *, backfill_adapters: Sequence[BackfillAdapter]):
        self.cfg = cfg
        self._backfill = BackfillService(backfill_adapters)

    async def ensure_local_history(self, req: EnsureHistoryRequest) -> None:
        spec = BackfillSpec(
            db_path=self.cfg.data.db_path,
            venue=req.venue,
            symbol=req.symbol,
            start_date=req.start_date,
            end_date=req.end_date,
            timeframes=req.timeframes,
        )
        await self._backfill.ensure_history(spec)
