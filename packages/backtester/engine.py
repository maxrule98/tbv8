from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional
from packages.common.execution.fill_model import FillModel

@dataclass
class BacktestConfig:
    starting_equity_usd: float = 10_000.0
    notional_per_trade_usd: float = 1_000.0
    # max absolute position in "units" (here we keep it as max 1x directional position)
    max_position: int = 1
    # optional: minimum hold minutes (0 = off)
    min_hold_minutes: int = 0


@dataclass
class TradeFill:
    ts_ms: int
    side: int  # +1 buy, -1 sell
    price: float
    qty: float
    fee_usd: float
    reason: str


@dataclass
class BacktestResult:
    venue: str
    symbol: str
    starting_equity_usd: float
    ending_equity_usd: float
    total_pnl_usd: float
    total_fees_usd: float
    trades: List[TradeFill]


class BacktestEngine:
    def __init__(self, cfg: BacktestConfig, fill: FillModel) -> None:
        self.cfg = cfg
        self.fill = fill

    def run(self, bars: List[Any], strat: Any, venue: Optional[str] = None, symbol: Optional[str] = None) -> BacktestResult:
        if not bars:
            v = venue or "unknown"
            s = symbol or "unknown"
            return BacktestResult(
                venue=v,
                symbol=s,
                starting_equity_usd=self.cfg.starting_equity_usd,
                ending_equity_usd=self.cfg.starting_equity_usd,
                total_pnl_usd=0.0,
                total_fees_usd=0.0,
                trades=[],
            )

        # Prefer explicit venue/symbol, otherwise try bar attributes, otherwise "unknown"
        v = venue or getattr(bars[0], "venue", None) or "unknown"
        s = symbol or getattr(bars[0], "symbol", None) or "unknown"

        equity = float(self.cfg.starting_equity_usd)
        total_fees = 0.0

        pos_side = 0  # -1 short, 0 flat, +1 long
        pos_qty = 0.0
        entry_price = 0.0
        last_trade_ts_ms: Optional[int] = None

        trades: List[TradeFill] = []

        def slippage_mult(side: int) -> float:
            # buy pays up, sell hits down
            bps = self.fill.slippage_bps / 10_000.0
            return 1.0 + (bps if side > 0 else -bps)

        def taker_fee_usd(notional_usd: float) -> float:
            return abs(notional_usd) * self.fill.taker_fee_rate

        for bar in bars:
            ts_ms = int(bar.ts_ms)
            close = float(bar.close)

            # Strategy emits desired target position: -1, 0, +1
            target = int(strat.on_bar(bar))

            # Clamp to engine limits
            if target > self.cfg.max_position:
                target = self.cfg.max_position
            if target < -self.cfg.max_position:
                target = -self.cfg.max_position

            # Optional min-hold logic
            if self.cfg.min_hold_minutes > 0 and last_trade_ts_ms is not None and target != pos_side:
                min_hold_ms = int(self.cfg.min_hold_minutes * 60_000)
                if ts_ms - last_trade_ts_ms < min_hold_ms:
                    # ignore target change until hold satisfied
                    continue

            if target == pos_side:
                continue

            # helper: close existing position at current bar
            def close_position(reason: str) -> None:
                nonlocal equity, total_fees, pos_side, pos_qty, entry_price, last_trade_ts_ms

                if pos_side == 0 or pos_qty == 0.0:
                    return

                # To close: if long -> sell, if short -> buy
                side = -1 if pos_side > 0 else +1
                fill_px = close * slippage_mult(side)
                notional = fill_px * pos_qty

                fee = taker_fee_usd(notional)
                total_fees += fee
                equity -= fee

                # PnL: long (sell - entry), short (entry - buy)
                if pos_side > 0:
                    pnl = (fill_px - entry_price) * pos_qty
                else:
                    pnl = (entry_price - fill_px) * pos_qty

                equity += pnl

                trades.append(
                    TradeFill(
                        ts_ms=ts_ms,
                        side=side,
                        price=fill_px,
                        qty=pos_qty,
                        fee_usd=fee,
                        reason=reason,
                    )
                )

                pos_side = 0
                pos_qty = 0.0
                entry_price = 0.0
                last_trade_ts_ms = ts_ms

            # helper: open new position at current bar
            def open_position(new_side: int, reason: str) -> None:
                nonlocal equity, total_fees, pos_side, pos_qty, entry_price, last_trade_ts_ms

                if new_side == 0:
                    return

                side = +1 if new_side > 0 else -1
                fill_px = close * slippage_mult(side)
                qty = self.cfg.notional_per_trade_usd / fill_px
                notional = fill_px * qty

                fee = taker_fee_usd(notional)
                total_fees += fee
                equity -= fee

                trades.append(
                    TradeFill(
                        ts_ms=ts_ms,
                        side=side,
                        price=fill_px,
                        qty=qty,
                        fee_usd=fee,
                        reason=reason,
                    )
                )

                pos_side = new_side
                pos_qty = qty
                entry_price = fill_px
                last_trade_ts_ms = ts_ms

            # Transition logic
            if pos_side != 0 and target == 0:
                close_position("exit_to_flat")
            elif pos_side == 0 and target != 0:
                open_position(target, "enter_target")
            else:
                # flip / change target
                close_position("exit_to_change_target")
                open_position(target, "enter_target")

        total_pnl = equity - self.cfg.starting_equity_usd
        return BacktestResult(
            venue=v,
            symbol=s,
            starting_equity_usd=self.cfg.starting_equity_usd,
            ending_equity_usd=equity,
            total_pnl_usd=total_pnl,
            total_fees_usd=total_fees,
            trades=trades,
        )
