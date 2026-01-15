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


def ms_to_iso8601_z(ts_ms: int) -> str:
    """
    Epoch ms -> ISO8601 Zulu string, e.g. 1700000000000 -> "2023-11-14T22:13:20Z"
    """
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc)
    return dt.isoformat().replace("+00:00", "Z")


def now_ms() -> int:
    return int(datetime.now(tz=timezone.utc).timestamp() * 1000)
