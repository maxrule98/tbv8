from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple

from packages.common.timeframes import ceil_ts_to_tf, floor_ts_to_tf, timeframe_to_ms
from packages.common.backfill.types import OHLCV


@dataclass(frozen=True)
class CoverageRow:
    venue: str
    symbol: str
    timeframe: str
    start_ms: int          # inclusive
    end_ms: int            # exclusive
    updated_at_ms: int


def ensure_schema(conn: sqlite3.Connection) -> None:
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

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS known_missing_ranges (
          venue TEXT NOT NULL,
          symbol TEXT NOT NULL,
          timeframe TEXT NOT NULL,
          start_ms INTEGER NOT NULL,       -- inclusive
          end_ms_excl INTEGER NOT NULL,    -- exclusive
          reason TEXT NOT NULL,
          updated_at_ms INTEGER NOT NULL,
          PRIMARY KEY (venue, symbol, timeframe, start_ms, end_ms_excl)
        );
        """
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_known_missing_lookup
          ON known_missing_ranges (venue, symbol, timeframe, start_ms, end_ms_excl);
        """
    )
    conn.commit()


def _bars_table(tf: str) -> str:
    return f"bars_{tf}"


def ensure_bars_table(conn: sqlite3.Connection, tf: str) -> None:
    t = _bars_table(tf)
    conn.execute(
        f"""
        CREATE TABLE IF NOT EXISTS {t} (
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
    conn.execute(f"CREATE INDEX IF NOT EXISTS idx_{t}_lookup ON {t} (venue, symbol, ts_ms);")
    conn.commit()

# =========================
# gap detection
# =========================

@dataclass(frozen=True)
class GapRange:
    start_ms: int        # inclusive
    end_ms_excl: int     # exclusive


def find_gaps_tf(
    conn: sqlite3.Connection,
    *,
    venue: str,
    symbol: str,
    tf: str,
    start_ms: Optional[int] = None,
    end_ms_excl: Optional[int] = None,
    limit: int = 10_000,
) -> list[GapRange]:
    """
    Find gaps in bars_{tf} where consecutive ts_ms differ != tf_ms.
    Returns ranges [gap_start, gap_end_excl) aligned to tf cadence.
    """
    ensure_bars_table(conn, tf)
    table = _bars_table(tf)
    tf_ms = timeframe_to_ms(tf)

    where = ["venue=? AND symbol=?"]
    params: list[object] = [venue, symbol]

    if start_ms is not None:
        where.append("ts_ms >= ?")
        params.append(int(start_ms))
    if end_ms_excl is not None:
        where.append("ts_ms < ?")
        params.append(int(end_ms_excl))

    sql = f"""
    WITH ordered AS (
      SELECT ts_ms,
             LAG(ts_ms) OVER (ORDER BY ts_ms) AS prev_ts
      FROM {table}
      WHERE {" AND ".join(where)}
    ),
    gaps AS (
      SELECT
        (prev_ts + {tf_ms}) AS gap_start,
        ts_ms AS gap_end
      FROM ordered
      WHERE prev_ts IS NOT NULL AND (ts_ms - prev_ts) != {tf_ms}
    )
    SELECT gap_start, gap_end
    FROM gaps
    ORDER BY gap_start
    LIMIT ?
    """
    rows = conn.execute(sql, (*params, int(limit))).fetchall()

    out: list[GapRange] = []
    for gs, ge in rows:
        gs_i = int(gs)
        ge_i = int(ge)
        if ge_i > gs_i:
            out.append(GapRange(start_ms=gs_i, end_ms_excl=ge_i))
    return out


def upsert_tf(conn: sqlite3.Connection, tf: str, venue: str, symbol: str, rows: list[OHLCV]) -> int:
    ensure_bars_table(conn, tf)
    t = _bars_table(tf)
    cur = conn.executemany(
        f"""
        INSERT INTO {t} (venue, symbol, ts_ms, open, high, low, close, volume)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(venue, symbol, ts_ms) DO UPDATE SET
          open=excluded.open,
          high=excluded.high,
          low=excluded.low,
          close=excluded.close,
          volume=excluded.volume
        """,
        [(venue, symbol, r.ts_ms, r.open, r.high, r.low, r.close, r.volume) for r in rows],
    )
    return cur.rowcount


def get_max_ts_tf(conn: sqlite3.Connection, venue: str, symbol: str, tf: str) -> int | None:
    ensure_bars_table(conn, tf)
    t = _bars_table(tf)
    row = conn.execute(
        f"SELECT MAX(ts_ms) FROM {t} WHERE venue=? AND symbol=?",
        (venue, symbol),
    ).fetchone()
    return None if not row or row[0] is None else int(row[0])


def get_min_max_ts_tf(conn: sqlite3.Connection, venue: str, symbol: str, tf: str) -> tuple[int, int] | None:
    ensure_bars_table(conn, tf)
    t = _bars_table(tf)
    row = conn.execute(
        f"SELECT MIN(ts_ms), MAX(ts_ms) FROM {t} WHERE venue=? AND symbol=?",
        (venue, symbol),
    ).fetchone()
    if not row or row[0] is None or row[1] is None:
        return None
    return int(row[0]), int(row[1])


def get_coverage(conn: sqlite3.Connection, venue: str, symbol: str, tf: str) -> Optional[CoverageRow]:
    row = conn.execute(
        """
        SELECT venue, symbol, timeframe, start_ms, end_ms, updated_at_ms
        FROM history_coverage
        WHERE venue=? AND symbol=? AND timeframe=?
        """,
        (venue, symbol, tf),
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


def record_known_missing_range(
    conn: sqlite3.Connection,
    *,
    venue: str,
    symbol: str,
    tf: str,
    start_ms: int,
    end_ms_excl: int,
    reason: str,
    updated_at_ms: int,
) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO known_missing_ranges
          (venue, symbol, timeframe, start_ms, end_ms_excl, reason, updated_at_ms)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (venue, symbol, tf, int(start_ms), int(end_ms_excl), reason, int(updated_at_ms)),
    )


def is_known_missing_range(
    conn: sqlite3.Connection,
    *,
    venue: str,
    symbol: str,
    tf: str,
    start_ms: int,
    end_ms_excl: int,
) -> bool:
    row = conn.execute(
        """
        SELECT 1
        FROM known_missing_ranges
        WHERE venue=? AND symbol=? AND timeframe=?
          AND start_ms<=?
          AND end_ms_excl>=?
        LIMIT 1
        """,
        (venue, symbol, tf, int(start_ms), int(end_ms_excl)),
    ).fetchone()
    return row is not None


def resolve_bar_window_from_coverage(
    *,
    db_path: str,
    venue: str,
    symbol: str,
    timeframe: str,
    requested_start_ms: Optional[int],
    requested_end_ms: Optional[int],
) -> Tuple[int, int]:
    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)
        cov = get_coverage(conn, venue, symbol, timeframe)
        if cov is None:
            raise RuntimeError(f"No coverage for venue={venue} symbol={symbol} tf={timeframe}")

        cov_start = cov.start_ms
        cov_end_excl = cov.end_ms

        start_raw = max(cov_start, requested_start_ms) if requested_start_ms is not None else cov_start
        end_raw_excl = min(cov_end_excl, requested_end_ms) if requested_end_ms is not None else cov_end_excl

        if end_raw_excl <= start_raw:
            raise RuntimeError(f"Resolved empty bar window start={start_raw} end_excl={end_raw_excl}")

        start_ms = ceil_ts_to_tf(start_raw, timeframe)
        end_ms_excl = floor_ts_to_tf(end_raw_excl, timeframe)

        if end_ms_excl <= start_ms:
            raise RuntimeError(f"Resolved empty aligned bar window start={start_ms} end_excl={end_ms_excl}")

        return start_ms, end_ms_excl
    finally:
        conn.close()