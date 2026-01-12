from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class OHLCV:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


class BackfillAdapter(Protocol):
    venue: str

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[OHLCV]:
        """Return candles sorted ascending by ts_ms."""
        ...
