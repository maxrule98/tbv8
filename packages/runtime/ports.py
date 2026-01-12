from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol, runtime_checkable

from packages.common.backfill.types import OHLCV


@dataclass(frozen=True)
class StrategyContext:
    venue: str
    symbol: str
    timeframe: str  # the bar timeframe being driven (e.g. "5m")


@runtime_checkable
class Strategy(Protocol):
    def on_bar(self, bar: OHLCV) -> int:
        """
        Return target position:
          -1 short, 0 flat, +1 long
        """
        ...


@runtime_checkable
class MarketDataSource(Protocol):
    def stream_bars(self) -> Iterable[OHLCV]:
        """Yield bars in ascending time order."""
        ...


@dataclass(frozen=True)
class ExecutionEvent:
    ts_ms: int
    side: int           # +1 buy, -1 sell
    qty: float
    price: float
    fee_usd: float
    reason: str


@dataclass
class RuntimeResult:
    venue: str
    symbol: str
    starting_equity_usd: float
    ending_equity_usd: float
    total_pnl_usd: float
    total_fees_usd: float
    trades: list[ExecutionEvent]


@runtime_checkable
class ExecutionSink(Protocol):
    def on_target(self, ts_ms: int, target: int, price: float, reason: str) -> None:
        """Engine pushes target position changes here."""
        ...

    def finalize(self) -> RuntimeResult:
        """Return final result (backtest) or final snapshot (live)."""
        ...
