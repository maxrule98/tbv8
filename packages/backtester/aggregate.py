from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from packages.common.sqlite_store import Bar1m


def bucket_ms(ts_ms: int, minutes: int) -> int:
    step = minutes * 60_000
    return (ts_ms // step) * step


@dataclass
class AggState:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    count: int


class Aggregator:
    """
    Online aggregator: feed 1m bars, emits completed HTF bars on rollover.
    """

    def __init__(self, minutes: int):
        if minutes <= 1:
            raise ValueError("minutes must be > 1")
        self.minutes = minutes
        self._st: Dict[Tuple[str, str], AggState] = {}

    def on_bar_1m(self, b: Bar1m) -> Optional[Bar1m]:
        key = (b.venue, b.symbol)
        buck = bucket_ms(b.ts_ms, self.minutes)

        st = self._st.get(key)
        if st is None:
            self._st[key] = AggState(
                ts_ms=buck,
                open=b.open,
                high=b.high,
                low=b.low,
                close=b.close,
                count=1,
            )
            return None

        if buck == st.ts_ms:
            st.high = max(st.high, b.high)
            st.low = min(st.low, b.low)
            st.close = b.close
            st.count += 1
            return None

        # rollover -> emit completed HTF bar
        out = Bar1m(
            venue=b.venue,
            symbol=b.symbol,
            ts_ms=st.ts_ms,
            open=st.open,
            high=st.high,
            low=st.low,
            close=st.close,
            quote_count=st.count,
        )

        self._st[key] = AggState(
            ts_ms=buck,
            open=b.open,
            high=b.high,
            low=b.low,
            close=b.close,
            count=1,
        )
        return out
