# packages/market_data/tests/test_plant_invariants.py

from __future__ import annotations

import asyncio
import os
import sqlite3
import tempfile
from dataclasses import dataclass
from typing import List

from packages.common.backfill.types import OHLCV
from packages.common.backfill.sqlite_store import ensure_schema, get_coverage
from packages.market_data.plant import MarketDataPlant
from packages.market_data.types import EnsureHistoryRequest
from packages.common.timeframes import timeframe_to_ms


@dataclass
class DummyAdapter:
    venue: str = "test_venue"

    async def fetch_ohlcv(self, *, symbol: str, timeframe: str, start_ms: int, end_ms: int, limit: int = 1000):
        """
        Return a fully contiguous synthetic series aligned to timeframe boundaries:
        - emits ts_ms in [start_ms .. end_ms) stepping by tf_ms
        - each candle is constant OHLCV(100..100, vol=1)
        """
        tf_ms = timeframe_to_ms(timeframe)

        # align start to boundary (ceil) so we don't generate off-grid opens
        start = ((start_ms + tf_ms - 1) // tf_ms) * tf_ms
        out: List[OHLCV] = []
        ts = start

        while ts < end_ms and len(out) < limit:
            out.append(OHLCV(ts_ms=ts, open=100, high=100, low=100, close=100, volume=1))
            ts += tf_ms

        return out


def _rows(conn: sqlite3.Connection, tf: str) -> list[int]:
    return [int(r[0]) for r in conn.execute(f"SELECT ts_ms FROM bars_{tf} ORDER BY ts_ms").fetchall()]


def test_plant_fetch_per_timeframe_and_coverage_invariants():
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name

    try:
        # Make sure schema tables exist
        conn = sqlite3.connect(db_path)
        ensure_schema(conn)
        conn.close()

        plant = MarketDataPlant(adapters=[DummyAdapter()])

        # Request 10 minutes of 1m + 5m
        req = EnsureHistoryRequest(
            db_path=db_path,
            venue="test_venue",
            symbol="BTC/USDT",
            start_date="1970-01-01T00:00:00Z",
            end_date="1970-01-01T00:10:00Z",
            timeframes=["1m", "5m"],
            chunk_days=1,
        )

        asyncio.run(plant.ensure_history(req))

        conn = sqlite3.connect(db_path)

        # 1m bars: 0..9 minutes => 10 candles at 0,60k,...,540k
        rows_1m = _rows(conn, "1m")
        assert len(rows_1m) == 10
        assert rows_1m[0] == 0
        assert rows_1m[-1] == 9 * 60_000

        cov_1m = get_coverage(conn, "test_venue", "BTC/USDT", "1m")
        assert cov_1m is not None
        assert cov_1m.start_ms == 0
        assert cov_1m.end_ms == 10 * 60_000  # end-exclusive

        # 5m bars: 0 and 300k => 2 candles
        rows_5m = _rows(conn, "5m")
        assert rows_5m == [0, 300_000]

        cov_5m = get_coverage(conn, "test_venue", "BTC/USDT", "5m")
        assert cov_5m is not None
        assert cov_5m.start_ms == 0
        assert cov_5m.end_ms == 600_000  # end-exclusive (10 minutes)

        conn.close()

        # Idempotency: run again, nothing changes
        asyncio.run(plant.ensure_history(req))
        conn = sqlite3.connect(db_path)
        assert _rows(conn, "1m") == rows_1m
        assert _rows(conn, "5m") == rows_5m
        conn.close()

        # Incremental extension: extend end_date to 15 minutes
        req2 = EnsureHistoryRequest(
            db_path=db_path,
            venue="test_venue",
            symbol="BTC/USDT",
            start_date="1970-01-01T00:00:00Z",
            end_date="1970-01-01T00:15:00Z",
            timeframes=["1m", "5m"],
            chunk_days=1,
        )
        asyncio.run(plant.ensure_history(req2))

        conn = sqlite3.connect(db_path)

        rows_1m2 = _rows(conn, "1m")
        assert len(rows_1m2) == 15
        assert rows_1m2[-1] == 14 * 60_000

        cov_1m2 = get_coverage(conn, "test_venue", "BTC/USDT", "1m")
        assert cov_1m2 is not None
        assert cov_1m2.end_ms == 15 * 60_000

        rows_5m2 = _rows(conn, "5m")
        assert rows_5m2 == [0, 300_000, 600_000]

        cov_5m2 = get_coverage(conn, "test_venue", "BTC/USDT", "5m")
        assert cov_5m2 is not None
        assert cov_5m2.end_ms == 900_000  # 15 minutes end-exclusive

        conn.close()

    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)