from __future__ import annotations

from datetime import datetime, timezone


def parse_iso8601_to_ms(s: str) -> int:
    """
    Accepts ISO8601 strings like:
      - 2017-08-17T00:00:00Z
      - 2017-08-17T00:00:00+00:00
      - 2017-08-17T00:00:00 (assumed UTC)
    Returns epoch ms.
    """
    ss = s.strip()
    if ss.endswith("Z"):
        ss = ss[:-1] + "+00:00"

    dt = datetime.fromisoformat(ss)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return int(dt.timestamp() * 1000)


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
