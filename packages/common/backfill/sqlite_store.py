from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Iterable, Optional, Tuple, List

from packages.common.timeframes import floor_ts_to_tf, ceil_ts_to_tf, timeframe_to_ms
from packages.common.constants import BASE_TIMEFRAME
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
    if timeframe != BASE_TIMEFRAME:
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

def get_min_ts(conn: sqlite3.Connection, venue: str, symbol: str, timeframe: str) -> Optional[int]:
    if timeframe == BASE_TIMEFRAME:
        row = conn.execute(
            "SELECT MIN(ts_ms) FROM ohlcv_1m WHERE venue=? AND symbol=?",
            (venue, symbol),
        ).fetchone()
        if not row or row[0] is None:
            return None
        return int(row[0])

    table = f"bars_{timeframe}"
    try:
        row = conn.execute(
            f"SELECT MIN(ts_ms) FROM {table} WHERE venue=? AND symbol=?",
            (venue, symbol),
        ).fetchone()
    except sqlite3.OperationalError:
        return None

    if not row or row[0] is None:
        return None
    return int(row[0])


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
    conn.execute(
        f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_{table}_vst
          ON {table} (venue, symbol, ts_ms);
        """
    )
    conn.commit()


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
    - For 1m: scan oBASE_TIMEFRAME_1m (base table).
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


def resolve_bar_window_from_coverage(
    *,
    db_path: str,
    venue: str,
    symbol: str,
    timeframe: str,
    requested_start_ms: Optional[int],
    requested_end_ms: Optional[int],
) -> Tuple[int, int]:
    """
    Returns [start_ms, end_ms_exclusive) for loading bars from coverage.

    Coverage semantics:
      coverage.start_ms is INCLUSIVE
      coverage.end_ms is EXCLUSIVE (== last_complete_bar_open + tf_ms)
    """
    conn = sqlite3.connect(db_path)
    try:
        ensure_schema(conn)
        cov = get_coverage(conn, venue, symbol, timeframe)
        if cov is None:
            raise RuntimeError(
                f"No coverage for venue={venue} symbol={symbol} tf={timeframe}. Run backfiller first."
            )

        cov_start = cov.start_ms
        cov_end_excl = cov.end_ms  # invariant: exclusive

        # 1) Clamp requested range to coverage
        start_raw = max(cov_start, requested_start_ms) if requested_start_ms is not None else cov_start
        end_raw_excl = min(cov_end_excl, requested_end_ms) if requested_end_ms is not None else cov_end_excl

        if end_raw_excl <= start_raw:
            raise RuntimeError(
                f"Resolved empty bar window start={start_raw} end_excl={end_raw_excl} "
                f"(coverage=[{cov_start}..{cov_end_excl}) requested=[{requested_start_ms}..{requested_end_ms}))"
            )

        # 2) Align to bar boundaries (bars live on exact opens)
        # - start must be a bar open at/after requested start => ceil
        # - end_excl must be on a boundary and exclusive => floor
        start_ms = ceil_ts_to_tf(start_raw, timeframe)
        end_ms_excl = floor_ts_to_tf(end_raw_excl, timeframe)

        # If requested_end is None, end_raw_excl == cov_end_excl which should already be aligned,
        # but flooring is still safe and keeps semantics tight.

        if end_ms_excl <= start_ms:
            tf_ms = timeframe_to_ms(timeframe)
            raise RuntimeError(
                f"Resolved empty *aligned* bar window start={start_ms} end_excl={end_ms_excl} tf_ms={tf_ms} "
                f"(raw=[{start_raw}..{end_raw_excl}) coverage=[{cov_start}..{cov_end_excl}) "
                f"requested=[{requested_start_ms}..{requested_end_ms}))"
            )

        return start_ms, end_ms_excl
    finally:
        conn.close()


from typing import List, Tuple

@dataclass(frozen=True)
class GapRange:
    start_ms: int        # inclusive
    end_ms_excl: int     # exclusive

def find_gaps_1m(
    conn: sqlite3.Connection,
    *,
    venue: str,
    symbol: str,
    start_ms: int | None = None,
    end_ms_excl: int | None = None,
    limit: int = 10_000,
) -> list[GapRange]:
    """
    Find gaps in ohlcv_1m where consecutive ts_ms differ != 60_000.
    Returns ranges [gap_start, gap_end_excl) measured in 1m boundaries.

    Note: This is deterministic and uses the DB contents as truth.
    """
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
      FROM ohlcv_1m
      WHERE {" AND ".join(where)}
    ),
    gaps AS (
      SELECT
        (prev_ts + 60000) AS gap_start,
        ts_ms AS gap_end,
        (ts_ms - prev_ts) AS delta
      FROM ordered
      WHERE prev_ts IS NOT NULL AND (ts_ms - prev_ts) != 60000
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


def resolve_contiguous_window(
    *,
    db_path: str,
    venue: str,
    symbol: str,
    timeframe: str,
    start_ms: int,
    end_ms_excl: int,
) -> tuple[int, int]:
    """
    Given an intended [start_ms, end_ms_excl), return a stricter window that contains
    no gaps for this venue/symbol/timeframe by moving start forward to after the last gap.

    This preserves Option A end-exclusive semantics and keeps determinism.
    """
    tf_ms = timeframe_to_ms(timeframe)
    table = f"bars_{timeframe}"

    conn = sqlite3.connect(db_path)
    try:
        # Find the *last* gap inside the requested window.
        row = conn.execute(
            f"""
            WITH ordered AS (
              SELECT ts_ms,
                     LAG(ts_ms) OVER (ORDER BY ts_ms) AS prev_ts
              FROM {table}
              WHERE venue=? AND symbol=? AND ts_ms >= ? AND ts_ms < ?
            ),
            gaps AS (
              SELECT prev_ts, ts_ms, (ts_ms - prev_ts) AS delta
              FROM ordered
              WHERE prev_ts IS NOT NULL AND (ts_ms - prev_ts) != ?
            )
            SELECT ts_ms
            FROM gaps
            ORDER BY ts_ms DESC
            LIMIT 1;
            """,
            (venue, symbol, int(start_ms), int(end_ms_excl), int(tf_ms)),
        ).fetchone()

        if row is None or row[0] is None:
            return start_ms, end_ms_excl

        # If there is a gap, we move start to the bar AFTER the gap (the current ts_ms).
        new_start = int(row[0])

        # Align just in case (should already be aligned)
        new_start = ceil_ts_to_tf(new_start, timeframe)

        if end_ms_excl <= new_start:
            raise RuntimeError(
                f"Contiguous window empty after trimming for gaps: start={new_start} end_excl={end_ms_excl}"
            )

        return new_start, end_ms_excl
    finally:
        conn.close()