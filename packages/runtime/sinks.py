from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from packages.common.execution.fill_model import FillModel
from packages.runtime.ports import ExecutionEvent, RuntimeResult


@dataclass
class SimFillSink:
    """
    Simple position-target executor:
    - target is -1/0/+1
    - uses notional_per_trade_usd as fixed sizing
    - enters/exits at slipped close price
    """
    venue: str
    symbol: str
    starting_equity_usd: float = 10_000.0
    notional_per_trade_usd: float = 1_000.0
    max_position: int = 1
    fill: FillModel = FillModel()

    _equity: float = 0.0
    _pos: int = 0  # -1/0/+1
    _trades: list[ExecutionEvent] = None
    _fees: float = 0.0

    def __post_init__(self) -> None:
        self._equity = float(self.starting_equity_usd)
        self._trades = []

    def on_target(self, ts_ms: int, target: int, price: float, reason: str) -> None:
        target = int(max(-self.max_position, min(self.max_position, target)))
        if target == self._pos:
            return

        # Exit existing
        if self._pos != 0:
            side = -self._pos  # to flatten
            self._execute(ts_ms, side=side, price=price, reason="exit_to_change_target")

        # Enter new
        if target != 0:
            side = target
            self._execute(ts_ms, side=side, price=price, reason="enter_target")

        self._pos = target

    def _execute(self, ts_ms: int, side: int, price: float, reason: str) -> None:
        px = self.fill.apply_slippage(price=float(price), side=int(side))
        notional = float(self.notional_per_trade_usd)
        qty = notional / float(px)
        fee = self.fill.fee_usd(notional)

        self._equity -= fee
        self._fees += fee

        self._trades.append(
            ExecutionEvent(
                ts_ms=int(ts_ms),
                side=int(side),
                qty=float(qty),
                price=float(px),
                fee_usd=float(fee),
                reason=str(reason),
            )
        )

    def finalize(self) -> RuntimeResult:
        end_equity = float(self._equity)
        pnl = end_equity - float(self.starting_equity_usd)
        return RuntimeResult(
            venue=self.venue,
            symbol=self.symbol,
            starting_equity_usd=float(self.starting_equity_usd),
            ending_equity_usd=end_equity,
            total_pnl_usd=float(pnl),
            total_fees_usd=float(self._fees),
            trades=list(self._trades),
        )