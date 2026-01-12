from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Optional

from packages.common.backfill.types import OHLCV
from packages.backtester.db import LoadBarsQuery, load_bars


@dataclass(frozen=True)
class SqliteBarsSource:
    db_path: Path
    venue: str
    symbol: str
    timeframe: str
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None
    limit: Optional[int] = None

    def stream_bars(self) -> Iterable[OHLCV]:
        bars = load_bars(
            LoadBarsQuery(
                db_path=self.db_path,
                venue=self.venue,
                symbol=self.symbol,
                timeframe=self.timeframe,
                start_ms=self.start_ms,
                end_ms=self.end_ms,
                limit=self.limit,
            )
        )
        # load_bars returns ordered ASC already, but keep it explicit:
        for b in bars:
            yield b
