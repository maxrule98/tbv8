from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

from packages.common.backfill.types import OHLCV


@dataclass(frozen=True)
class CoverageRow:
    venue: str
    symbol: str
    timeframe: str
    start_ms: int
    end_ms: int
    updated_at_ms: int


def ensure_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ohlcv_1m (
          venue TEXT NOT NULL,
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL NOT NULL,
          PRIMARY KEY (venue, symbol, ts_ms)
        );
        """
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS history_coverage (
          venue TEXT NOT NULL,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          start_ms INTEGER NOT NULL,
          end_ms INTEGER NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          PRIMARY KEY (venue, symbol, timeframe)
        );
        """
    )
    conn.commit()


def get_min_max_ts(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Return (min_ts_ms, max_ts_ms) for a stored timeframe.

    We only guarantee this for the 1m base table today.
    Aggregates are derived and should be tracked via history_coverage, not by scanning.
    """
    if timeframe != "1m":
        return (None, None)

    row = conn.execute(
        "SELECT MIN(ts_ms), MAX(ts_ms) FROM ohlcv_1m WHERE venue=? AND symbol=?",
        (venue, symbol),
    ).fetchone()

    if not row:
        return (None, None)

    min_ts = row[0]
    max_ts = row[1]
    return (int(min_ts) if min_ts is not None else None, int(max_ts) if max_ts is not None else None)


def get_max_ts(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[int]:
    _min_ts, max_ts = get_min_max_ts(conn, venue, symbol, timeframe)
    return max_ts


def get_coverage(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[CoverageRow]:
    row = conn.execute(
        """
        SELECT venue, symbol, timeframe, start_ms, end_ms, updated_at_ms
        FROM history_coverage
        WHERE venue=? AND symbol=? AND timeframe=?
        """,
        (venue, symbol, timeframe),
    ).fetchone()

    if not row:
        return None

    return CoverageRow(
        venue=str(row[0]),
        symbol=str(row[1]),
        timeframe=str(row[2]),
        start_ms=int(row[3]),
        end_ms=int(row[4]),
        updated_at_ms=int(row[5]),
    )


def upsert_coverage(conn: sqlite3.Connection, row: CoverageRow) -> None:
    conn.execute(
        """
        INSERT INTO history_coverage (venue, symbol, timeframe, start_ms, end_ms, updated_at_ms)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(venue, symbol, timeframe) DO UPDATE SET
          start_ms=excluded.start_ms,
          end_ms=excluded.end_ms,
          updated_at_ms=excluded.updated_at_ms
        """,
        (row.venue, row.symbol, row.timeframe, row.start_ms, row.end_ms, row.updated_at_ms),
    )


def upsert_1m(conn: sqlite3.Connection, venue: str, symbol: str, rows: Iterable[OHLCV]) -> int:
    cur = conn.cursor()
    wrote = 0
    for r in rows:
        cur.execute(
            """
            INSERT INTO ohlcv_1m (venue, symbol, ts_ms, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, symbol, ts_ms) DO UPDATE SET
              open=excluded.open,
              high=excluded.high,
              low=excluded.low,
              close=excluded.close,
              volume=excluded.volume
            """,
            (venue, symbol, int(r.ts_ms), float(r.open), float(r.high), float(r.low), float(r.close), float(r.volume)),
        )
        wrote += 1
    return wrote


def ensure_agg_table(conn: sqlite3.Connection, timeframe: str) -> None:
    table = f"bars_{timeframe}"
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
          venue TEXT NOT NULL,
          symbol TEXT NOT NULL,
          ts_ms INTEGER NOT NULL,
          open REAL NOT NULL,
          high REAL NOT NULL,
          low REAL NOT NULL,
          close REAL NOT NULL,
          volume REAL NOT NULL,
          PRIMARY KEY (venue, symbol, ts_ms)
        );
        """
    )


def upsert_agg(conn: sqlite3.Connection, timeframe: str, venue: str, symbol: str, rows: Iterable[OHLCV]) -> int:
    ensure_agg_table(conn, timeframe)
    table = f"bars_{timeframe}"
    cur = conn.cursor()
    wrote = 0
    for r in rows:
        cur.execute(
            f"""
            INSERT INTO {table} (venue, symbol, ts_ms, open, high, low, close, volume)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(venue, symbol, ts_ms) DO UPDATE SET
              open=excluded.open,
              high=excluded.high,
              low=excluded.low,
              close=excluded.close,
              volume=excluded.volume
            """,
            (venue, symbol, int(r.ts_ms), float(r.open), float(r.high), float(r.low), float(r.close), float(r.volume)),
        )
        wrote += 1
    return wrote

def get_max_complete_ts(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[int]:
    """
    Return the max *completed* candle start ts for a timeframe.
    - For 1m: scan ohlcv_1m (base table).
    - For HTF: use history_coverage.end_ms (authoritative).
    """
    if timeframe == "1m":
        return get_max_ts(conn, venue, symbol, timeframe)

    cov = get_coverage(conn, venue, symbol, timeframe)
    return cov.end_ms if cov else None

def get_last_complete_open_ms(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[int]:
    cov = get_coverage(conn, venue, symbol, timeframe)
    if not cov:
        return None
    tf_ms = timeframe_to_ms(timeframe)
    # coverage end_ms is end-exclusive, so last complete candle open is end_ms - tf
    return cov.end_ms - tf_ms


def get_max_agg_open_ms(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[int]:
    # Used for diagnostics only - coverage is the source of truth.
    table = f"bars_{timeframe}"
    try:
        row = conn.execute(
            f"SELECT MAX(ts_ms) FROM {table} WHERE venue=? AND symbol=?",
            (venue, symbol),
        ).fetchone()
    except sqlite3.OperationalError:
        return None
    if not row or row[0] is None:
        return None
    return int(row[0])