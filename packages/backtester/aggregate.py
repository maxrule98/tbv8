from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from packages.backtester.types import Bar

# -----------------------------
# Time bucketing
# -----------------------------

def bucket_ms(ts_ms: int, minutes: int) -> int:
    """
    Floor a timestamp into its HTF bucket.

    Example:
      ts = 12:07, minutes=5  -> bucket = 12:05
      ts = 12:10, minutes=5  -> bucket = 12:10
    """
    step = minutes * 60_000
    return (ts_ms // step) * step


# -----------------------------
# Internal aggregation state
# -----------------------------

@dataclass
class AggState:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


# -----------------------------
# Streaming aggregator
# -----------------------------

class Aggregator:
    """
    Online HTF aggregator.

    Feed it bars from a *single* instrument stream
    (1m, 5m, etc) and it emits completed HTF bars.

    Example:
        agg = Aggregator(minutes=60)   # build 1h candles from 1m or 5m feed
        for bar in bars:
            htf = agg.on_bar(bar)
            if htf:
                handle(htf)
    """

    def __init__(self, minutes: int):
        if minutes <= 1:
            raise ValueError("HTF minutes must be > 1")
        self.minutes = minutes
        self._state: Optional[AggState] = None

    def on_bar(self, b: Bar) -> Optional[Bar]:
        """
        Feed one LTF bar.
        Returns a completed HTF Bar when a bucket rolls over, else None.
        """
        buck = bucket_ms(b.ts_ms, self.minutes)
        st = self._state

        # First bar
        if st is None:
            self._state = AggState(
                ts_ms=buck,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                volume=b.volume,
            )
            return None

        # Still in same HTF candle
        if buck == st.ts_ms:
            st.high = max(st.high, b.high)
            st.low = min(st.low, b.low)
            st.close = b.close
            st.volume += b.volume
            return None

        # Bucket rollover -> emit completed HTF candle
        out = Bar(
            ts_ms=st.ts_ms,
            open=st.open,
            high=st.high,
            low=st.low,
            close=st.close,
            volume=st.volume,
        )

        # Start new HTF candle
        self._state = AggState(
            ts_ms=buck,
            open=b.open,
            high=b.high,
            low=b.low,
            close=b.close,
            volume=b.volume,
        )

        return out
