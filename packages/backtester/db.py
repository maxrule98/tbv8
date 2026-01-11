from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import sqlite3

from packages.common.sqlite_store import Bar1m


@dataclass(frozen=True)
class LoadBarsQuery:
    db_path: Path
    venue: str
    symbol: str
    start_ts_ms: Optional[int] = None
    end_ts_ms: Optional[int] = None
    limit: Optional[int] = None


def load_bars_1m(q: LoadBarsQuery) -> List[Bar1m]:
    con = sqlite3.connect(str(q.db_path))
    try:
        sql = """
        SELECT venue, symbol, ts_ms, open, high, low, close, quote_count
        FROM bars_1m
        WHERE venue = ? AND symbol = ?
        """
        params = [q.venue, q.symbol]

        if q.start_ts_ms is not None:
            sql += " AND ts_ms >= ?"
            params.append(int(q.start_ts_ms))

        if q.end_ts_ms is not None:
            sql += " AND ts_ms < ?"
            params.append(int(q.end_ts_ms))

        sql += " ORDER BY ts_ms ASC"

        if q.limit is not None:
            sql += " LIMIT ?"
            params.append(int(q.limit))

        cur = con.execute(sql, params)
        rows = cur.fetchall()

        out: List[Bar1m] = []
        for venue, symbol, ts_ms, o, h, l, c, n in rows:
            out.append(
                Bar1m(
                    venue=str(venue),
                    symbol=str(symbol),
                    ts_ms=int(ts_ms),
                    open=float(o),
                    high=float(h),
                    low=float(l),
                    close=float(c),
                    quote_count=int(n),
                )
            )
        return out
    finally:
        con.close()
