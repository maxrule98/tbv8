from __future__ import annotations

from dataclasses import dataclass

from packages.backtester.engine import FillModel
from packages.runtime.execution.types import Fill, OrderIntent


@dataclass(frozen=True)
class SimFillExecutor:
    """
    Deterministic local fills.
    IMPORTANT:
    - Uses *execution venue* passed in (NOT data venue)
    - Uses FillModel for fee + slippage
    """
    venue: str
    fill: FillModel

    def execute(self, price: float, intent: OrderIntent) -> Fill:
        # Apply slippage in the direction of trade
        px = self.fill.apply_slippage(price=price, side=int(intent.side))
        qty = float(intent.notional_usd) / float(px)
        fee = float(intent.notional_usd) * float(self.fill.taker_fee_rate)

        return Fill(
            venue=self.venue,
            symbol=intent.symbol,
            ts_ms=intent.ts_ms,
            side=int(intent.side),
            price=float(px),
            qty=float(qty),
            fee_usd=float(fee),
            reason=intent.reason,
        )
