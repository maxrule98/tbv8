from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from packages.runtime.ports import ExecutionEvent, RuntimeResult


@dataclass(frozen=True)
class FillModel:
    taker_fee_rate: float = 0.0006     # 6 bps
    slippage_bps: float = 1.0          # 1 bp

    def slip_price(self, px: float, side: int) -> float:
        # side: +1 buy (pay up), -1 sell (hit down)
        slip = self.slippage_bps / 10_000.0
        return px * (1.0 + slip) if side > 0 else px * (1.0 - slip)

    def fee_usd(self, notional_usd: float) -> float:
        return notional_usd * self.taker_fee_rate


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

        # We always “close then open” at the same timestamp price for now.
        # This is fine for deterministic functionality; later we can model queueing/partial fills.
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
        px = self.fill.slip_price(price, side)
        notional = float(self.notional_per_trade_usd)
        qty = notional / px
        fee = self.fill.fee_usd(notional)

        # crude PnL accounting:
        # We treat each trade as consuming fee and the rest is mark-to-market handled implicitly by target model.
        # For now (functionality-first), equity just subtracts fees; PnL is computed at finalize using price deltas
        # is out of scope until we add proper position bookkeeping.
        self._equity -= fee
        self._fees += fee

        self._trades.append(
            ExecutionEvent(
                ts_ms=ts_ms,
                side=side,
                qty=qty,
                price=px,
                fee_usd=fee,
                reason=reason,
            )
        )

    def finalize(self) -> RuntimeResult:
        # For now, total_pnl is equity - starting (fees already applied)
        end_equity = self._equity
        pnl = end_equity - float(self.starting_equity_usd)
        return RuntimeResult(
            venue=self.venue,
            symbol=self.symbol,
            starting_equity_usd=float(self.starting_equity_usd),
            ending_equity_usd=end_equity,
            total_pnl_usd=pnl,
            total_fees_usd=self._fees,
            trades=list(self._trades),
        )
