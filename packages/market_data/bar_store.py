from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Sequence

from packages.backtester.types import Bar


@dataclass(frozen=True)
class BarSeries:
    timeframe: str
    bars: List[Bar]  # sorted by ts_ms ASC


class MultiTFBarStore:
    """
    Read-only multi-timeframe store with "as-of" access.

    Strategy uses:
      - asof(tf, ts_ms) -> most recent closed bar at/before ts_ms
      - window(tf, ts_ms, n) -> last n bars ending at asof(tf, ts_ms)
    """

    def __init__(self, series: Dict[str, BarSeries]):
        self._series = dict(series)

        # Validate ordering
        for tf, s in self._series.items():
            if not s.bars:
                continue
            for i in range(1, len(s.bars)):
                if s.bars[i].ts_ms < s.bars[i - 1].ts_ms:
                    raise ValueError(f"Bars not sorted for tf={tf}")

    def timeframes(self) -> List[str]:
        return sorted(self._series.keys())

    def asof(self, tf: str, ts_ms: int) -> Optional[Bar]:
        s = self._series.get(tf)
        if not s or not s.bars:
            return None

        # Binary search for rightmost bar with ts_ms <= target
        lo, hi = 0, len(s.bars) - 1
        ans = None
        while lo <= hi:
            mid = (lo + hi) // 2
            b = s.bars[mid]
            if b.ts_ms <= ts_ms:
                ans = b
                lo = mid + 1
            else:
                hi = mid - 1
        return ans

    def window(self, tf: str, ts_ms: int, n: int) -> List[Bar]:
        if n <= 0:
            return []
        s = self._series.get(tf)
        if not s or not s.bars:
            return []

        # Find asof index, then slice last n
        lo, hi = 0, len(s.bars) - 1
        idx = None
        while lo <= hi:
            mid = (lo + hi) // 2
            b = s.bars[mid]
            if b.ts_ms <= ts_ms:
                idx = mid
                lo = mid + 1
            else:
                hi = mid - 1

        if idx is None:
            return []

        start = max(0, idx - (n - 1))
        return s.bars[start : idx + 1]