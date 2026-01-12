from __future__ import annotations

from enum import Enum
from typing import Literal

# Execution venues (where we can trade)
VenueId = Literal["hyperliquid", "mexc"]

# Market-data venues (where we can backfill/research data)
# This can include sources we never execute on.
MarketDataVenueId = Literal["binance_spot", "hyperliquid", "mexc"]

class RoutingMode(str, Enum):
    PRIMARY_LIVE_SECONDARY_SHADOW = "PRIMARY_LIVE_SECONDARY_SHADOW"
    PRIMARY_ONLY = "PRIMARY_ONLY"
