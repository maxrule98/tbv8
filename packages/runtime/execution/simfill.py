from __future__ import annotations

from dataclasses import dataclass

from packages.common.execution.fill_model import FillModel
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
        px = self.fill.apply_slippage(price=float(price), side=int(intent.side))
        qty = float(intent.notional_usd) / float(px)
        fee = self.fill.fee_usd(float(intent.notional_usd))

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