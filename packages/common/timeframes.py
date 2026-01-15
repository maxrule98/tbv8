from __future__ import annotations

import re

_TIMEFRAME_RE = re.compile(r"^(\d+)([smhdw])$")


def timeframe_to_ms(tf: str) -> int:
    m = _TIMEFRAME_RE.match(tf.strip())
    if not m:
        raise ValueError(f"Invalid timeframe: {tf!r} (expected e.g. '1m', '5m', '1h')")

    n = int(m.group(1))
    unit = m.group(2)

    mult = {
        "s": 1_000,
        "m": 60_000,
        "h": 3_600_000,
        "d": 86_400_000,
        "w": 604_800_000,
    }[unit]
    return n * mult


def floor_ts_to_tf(ts_ms: int, tf: str) -> int:
    ms = timeframe_to_ms(tf)
    return (ts_ms // ms) * ms


def ceil_ts_to_tf(ts_ms: int, tf: str) -> int:
    ms = timeframe_to_ms(tf)
    if ts_ms % ms == 0:
        return ts_ms
    return ((ts_ms // ms) + 1) * ms
