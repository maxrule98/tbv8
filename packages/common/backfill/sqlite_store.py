from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional

from loguru import logger

# IMPORTANT: keep imports within packages.common.backfill
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

    # Aggregated tables (we store by timeframe as separate tables)
    # We'll create them lazily when first used: bars_5m, bars_15m, etc.
    conn.commit()


def get_max_ts(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[int]:
    if timeframe != "1m":
        # For now we only track coverage/max for 1m base
        # (Aggregates are derived from 1m and can be re-generated)
        return None

    row = conn.execute(
        "SELECT MAX(ts_ms) FROM ohlcv_1m WHERE venue=? AND symbol=?",
        (venue, symbol),
    ).fetchone()
    if not row:
        return None
    return row[0]


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
