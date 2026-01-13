from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FillModel:
    taker_fee_rate: float = 0.0006
    slippage_bps: float = 0.0  # 1.0 = 1bp = 0.01%

    def apply_slippage(self, price: float, side: int) -> float:
        """
        Apply slippage in the direction of the trade.

        side: +1 buy, -1 sell
        slippage_bps: basis points (1 bp = 0.01% = 0.0001)
        """
        px = float(price)
        if self.slippage_bps <= 0:
            return px

        bps = float(self.slippage_bps) / 10_000.0

        if side > 0:  # buy pays up
            return px * (1.0 + bps)
        if side < 0:  # sell hits down
            return px * (1.0 - bps)

        return px

    def fee_usd(self, notional_usd: float) -> float:
        return abs(float(notional_usd)) * float(self.taker_fee_rate)