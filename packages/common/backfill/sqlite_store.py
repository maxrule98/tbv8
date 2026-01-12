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

    conn.execute("CREATE INDEX IF NOT EXISTS idx_ohlcv_1m_vs ON ohlcv_1m(venue, symbol, ts_ms);")
    conn.commit()


def _table_for_tf(timeframe: str) -> str:
    return "ohlcv_1m" if timeframe == "1m" else f"bars_{timeframe}"


def _table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table,),
    ).fetchone()
    return bool(row)


def get_max_ts(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[int]:
    table = _table_for_tf(timeframe)
    if not _table_exists(conn, table):
        return None

    row = conn.execute(
        f"SELECT MAX(ts_ms) FROM {table} WHERE venue=? AND symbol=?",
        (venue, symbol),
    ).fetchone()
    return row[0] if row and row[0] is not None else None


def get_min_max_ts(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[Tuple[int, int]]:
    """
    Returns (min_ts_ms, max_ts_ms) for the given timeframe table.
    """
    table = _table_for_tf(timeframe)
    if not _table_exists(conn, table):
        return None

    row = conn.execute(
        f"SELECT MIN(ts_ms), MAX(ts_ms) FROM {table} WHERE venue=? AND symbol=?",
        (venue, symbol),
    ).fetchone()

    if not row or row[0] is None or row[1] is None:
        return None

    return (int(row[0]), int(row[1]))


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
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{table}_vs ON {table}(venue, symbol, ts_ms);")


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
