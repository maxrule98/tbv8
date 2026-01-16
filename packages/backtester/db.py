from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional


@dataclass(frozen=True)
class BacktestBar:
    venue: str
    symbol: str
    timeframe: str
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float


@dataclass(frozen=True)
class LoadBarsQuery:
    db_path: Path
    venue: str
    symbol: str
    timeframe: str 
    start_ms: Optional[int] = None
    end_ms: Optional[int] = None
    limit: Optional[int] = None


def _table_for_timeframe(tf: str) -> str:
    # tf examples: 1m, 5m, 15m, 1h, 4h
    return f"bars_{tf}"


def load_bars(q: LoadBarsQuery) -> List[BacktestBar]:
    """
    Load bars from bars_{timeframe} and return BacktestBar objects
    (includes venue/symbol/timeframe for strategy + aggregator).
    """
    table = _table_for_timeframe(q.timeframe)

    where = ["venue=? AND symbol=?"]
    params: list[object] = [q.venue, q.symbol]

    if q.start_ms is not None:
        where.append("ts_ms >= ?")
        params.append(int(q.start_ms))

    if q.end_ms is not None:
        where.append("ts_ms < ?")
        params.append(int(q.end_ms))

    sql = f"""
    SELECT ts_ms, open, high, low, close, volume
    FROM {table}
    WHERE {" AND ".join(where)}
    ORDER BY ts_ms ASC
    """

    if q.limit is not None:
        sql += " LIMIT ?"
        params.append(int(q.limit))

    conn = sqlite3.connect(str(q.db_path))
    try:
        rows = conn.execute(sql, tuple(params)).fetchall()
    finally:
        conn.close()

    return [
        BacktestBar(
            venue=q.venue,
            symbol=q.symbol,
            timeframe=q.timeframe,
            ts_ms=int(r[0]),
            open=float(r[1]),
            high=float(r[2]),
            low=float(r[3]),
            close=float(r[4]),
            volume=float(r[5]),
        )
        for r in rows
    ]
