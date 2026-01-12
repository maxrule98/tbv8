from __future__ import annotations

from enum import Enum
from typing import Literal

VenueId = Literal[
    "hyperliquid",
    "mexc",
    "binance_spot",
]

class RoutingMode(str, Enum):
    PRIMARY_LIVE_SECONDARY_SHADOW = "PRIMARY_LIVE_SECONDARY_SHADOW"
    PRIMARY_ONLY = "PRIMARY_ONLY"
