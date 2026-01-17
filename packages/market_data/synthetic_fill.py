from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Iterator, Optional

from packages.backtester.types import Bar


@dataclass(frozen=True)
class RuntimeBar:
    """
    Runtime-only bar wrapper.
    - `bar` is the OHLCV payload
    - `synthetic` tells you if it was forward-filled for a missing timestamp
    """
    bar: Bar
    synthetic: bool


def make_synthetic_bar(*, ts_ms: int, prev_close: float) -> Bar:
    # volume=0, and OHLC all equal to last close
    return Bar(
        ts_ms=int(ts_ms),
        open=float(prev_close),
        high=float(prev_close),
        low=float(prev_close),
        close=float(prev_close),
        volume=0.0,
    )


def fill_missing_bars(
    *,
    bars: Iterable[Bar],
    start_ms: int,
    end_ms_excl: int,
    tf_ms: int,
) -> Iterator[RuntimeBar]:
    """
    Given sparse DB bars (sorted by ts_ms), yield a complete, gap-free stream in
    [start_ms, end_ms_excl) by injecting synthetic bars for missing timestamps.

    Rules:
    - never emit anything before the first real bar (we can't forward-fill without an anchor)
    - once we have an anchor, every missing ts gets a synthetic bar
    - synthetic bars are runtime-only; do not store them
    """
    it = iter(bars)

    next_real: Optional[Bar] = next(it, None)
    last_close: Optional[float] = None
    have_anchor = False

    cursor = int(start_ms)

    while cursor < end_ms_excl:
        # Consume real bars up to cursor
        while next_real is not None and int(next_real.ts_ms) < cursor:
            last_close = float(next_real.close)
            have_anchor = True
            next_real = next(it, None)

        # If we have a real bar exactly at cursor, emit it
        if next_real is not None and int(next_real.ts_ms) == cursor:
            last_close = float(next_real.close)
            have_anchor = True
            yield RuntimeBar(bar=next_real, synthetic=False)
            next_real = next(it, None)
            cursor += tf_ms
            continue

        # Otherwise, it's missing
        if have_anchor and last_close is not None:
            syn = make_synthetic_bar(ts_ms=cursor, prev_close=last_close)
            yield RuntimeBar(bar=syn, synthetic=True)

        # If no anchor yet, we skip (covers “market didn’t exist yet”)
        cursor += tf_ms