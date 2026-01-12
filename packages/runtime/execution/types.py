from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Optional


class OrderSide(int, Enum):
    BUY = 1
    SELL = -1


@dataclass(frozen=True)
class OrderIntent:
    """
    What runtime wants to do (venue is chosen by router).
    """
    symbol: str
    side: OrderSide
    notional_usd: float
    ts_ms: int
    reason: str


@dataclass(frozen=True)
class Fill:
    """
    What execution returns.
    """
    venue: str
    symbol: str
    ts_ms: int
    side: int  # +1 buy, -1 sell
    price: float
    qty: float
    fee_usd: float
    reason: str


@dataclass(frozen=True)
class ExecutionRoute:
    """
    Routing decision (primary now, later can include shadow).
    """
    primary_venue: str
    shadow_venue: Optional[str] = None
