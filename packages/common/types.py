from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Literal


VenueId = Literal["mexc", "hyperliquid"]


@dataclass(frozen=True)
class Symbol:
    value: str  # e.g. "BTC/USDT"

    def __str__(self) -> str:
        return self.value


class RoutingMode(str, Enum):
    PRIMARY_ONLY = "PRIMARY_ONLY"
    PRIMARY_LIVE_SECONDARY_SHADOW = "PRIMARY_LIVE_SECONDARY_SHADOW"
    BEST_EXECUTION = "BEST_EXECUTION"
