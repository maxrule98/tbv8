import sqlite3
import os
import tempfile
from dataclasses import dataclass
from typing import List

from packages.common.constants import BASE_TIMEFRAME
from packages.common.backfill.types import OHLCV
from packages.common.backfill.sqlite_store import (
    ensure_schema,
    upsert_1m,
    get_coverage,
    upsert_coverage,
    CoverageRow,
)
from packages.common.backfill.aggregate import build_aggregates
from packages.market_data.plant import MarketDataPlant, MarketDataPlantConfig, EnsureHistoryRequest
from packages.market_data.types import EnsureHistoryRequest

@dataclass
class DummyAdapter:
    venue: str = "dummy"
    async def fetch_ohlcv(self, *args, **kwargs):
        # no-op, returns empty
        return []

def test_plant_aggregation_invariants():
    """
    Verify strict aggregation & coverage semantics:
    - Coverage end is EXCLUSIVE.
    - Derived bars are exclusively COMPLETED candles.
    """
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as tmp:
        db_path = tmp.name
    
    try:
        conn = sqlite3.connect(db_path)
        ensure_schema(conn)

        # 1) Setup: Insert 10 minutes of execution data [1000..11000)
        # 0 ms = 1970-01-01 00:00:00
        base_bars: List[OHLCV] = []
        for i in range(10):  # 0..9
            ts = i * 60_000
            base_bars.append(OHLCV(ts, 100, 100, 100, 100, 1))
        
        upsert_1m(conn, "test_venue", "BTC", base_bars)
        conn.commit()
        conn.close()
        
        # 2) Run ensure_history via Plant (or manually stimulate aggregation)
        # We'll use manual build_aggregates + manual coverage check to mimic what Plant does, 
        # OR we can instantiate Plant. Let's use Plant to verify the composition.
        
        plant = MarketDataPlant(adapters=[DummyAdapter(venue="test_venue")])
        
        # We want 5m bars.
        # 10m of data = two 5m candles: [0..5m) and [5m..10m).
        # 5m = 300_000 ms.
        # range 0..600_000 (10 mins).
        
        req = EnsureHistoryRequest(
            db_path=db_path,
            venue="test_venue",
            symbol="BTC",
            start_date="1970-01-01T00:00:00Z",
            end_date="1970-01-01T00:10:00Z",
            timeframes=["5m"],
            chunk_days=1,
        )
        
        import asyncio
        asyncio.run(plant.ensure_history(req))

        # 3) Assertions
        conn = sqlite3.connect(db_path)
        
        # Check 5m bar count
        # We expect candle at 0 and candle at 300,000.
        rows = conn.execute("SELECT ts_ms FROM bars_5m ORDER BY ts_ms").fetchall()
        assert len(rows) == 2, f"Expected 2 bars (0, 300000), got {len(rows)}: {rows}"
        assert rows[0][0] == 0
        assert rows[1][0] == 300_000
        
        # Check Coverage
        # Should cover [0 .. 600_000)
        # 600_000 is 10 mins.
        cov = get_coverage(conn, "test_venue", "BTC", "5m")
        assert cov is not None
        assert cov.start_ms == 0
        assert cov.end_ms == 600_000
        
        # 4) Idempotency Check
        # Run again. content should not change.
        conn.close()
        asyncio.run(plant.ensure_history(req))
        conn = sqlite3.connect(db_path)
        
        rows2 = conn.execute("SELECT ts_ms FROM bars_5m ORDER BY ts_ms").fetchall()
        assert len(rows2) == 2
        
        cov2 = get_coverage(conn, "test_venue", "BTC", "5m")
        assert cov2.end_ms == 600_000
        conn.close()
        
        # 5) Incremental Test
        # Add 5 more bars [10..15). 
        # This completes the [10..15) bucket.
        conn = sqlite3.connect(db_path)
        new_bars = []
        for i in range(10, 15):
            ts = i * 60_000
            new_bars.append(OHLCV(ts, 100, 100, 100, 100, 1))
        upsert_1m(conn, "test_venue", "BTC", new_bars)
        conn.commit()
        conn.close()
        
        # Update end_date to 15m
        req2 = EnsureHistoryRequest(
            db_path=db_path,
            venue="test_venue",
            symbol="BTC",
            start_date="1970-01-01T00:00:00Z",
            end_date="1970-01-01T00:15:00Z", # 900_000
            timeframes=["5m"],
            chunk_days=1,
        )
        asyncio.run(plant.ensure_history(req2))
        
        conn = sqlite3.connect(db_path)
        # Expect 3 bars now: 0, 5m, 10m
        rows3 = conn.execute("SELECT ts_ms FROM bars_5m ORDER BY ts_ms").fetchall()
        assert len(rows3) == 3
        assert rows3[2][0] == 600_000 # 10 mins
        
        cov3 = get_coverage(conn, "test_venue", "BTC", "5m")
        assert cov3.end_ms == 900_000  # 15 mins
        conn.close()
        
        print("\nTest passed: Invariants hold.")
        
    finally:
        if os.path.exists(db_path):
            os.unlink(db_path)

if __name__ == "__main__":
    test_plant_aggregation_invariants()

