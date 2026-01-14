from __future__ import annotations

# Canonical base timeframe for the entire TBV8 platform.
# We backfill 1m data and aggregate everything else from it.
BASE_TIMEFRAME: str = "1m"
BASE_TF_MS: int = 60_000
