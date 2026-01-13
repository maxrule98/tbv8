from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence


@dataclass(frozen=True)
class EnsureHistoryRequest:
    db_path: str
    venue: str
    symbol: str
    start_date: str
    end_date: str | None
    timeframes: Sequence[str]  # include "1m" + derived like "5m"
    chunk_days: int = 14       # used for bulk rebuilds / large windows
