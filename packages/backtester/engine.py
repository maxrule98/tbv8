from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List, Optional, Tuple

from loguru import logger

from packages.common.sqlite_store import Bar1m


@dataclass(frozen=True)
class FillModel:
    """
    Extremely simple execution model:
    - trades occur at close +/- slippage_bps
    - fees charged on notional
    """
    taker_fee_rate: float = 0.0006   # 6 bps default
    slippage_bps: float = 1.0        # 1 bp default

    def apply_fill_price(self, mid_price: float, side: int) -> float:
        # side: +1 buy, -1 sell
        slip = (self.slippage_bps / 10_000.0)
        if side > 0:
            return mid_price * (1.0 + slip)
        return mid_price * (1.0 - slip)


@dataclass
class BacktestConfig:
    starting_equity_usd: float = 10_000.0
    notional_per_trade_usd: float = 1_000.0
    max_position: int = 1  # -1,0,+1 (for now)


@dataclass
class BacktestTrade:
    ts_ms: int
    price: float
    side: int            # +1 buy, -1 sell
    qty: float           # base qty
    fee_usd: float
    venue: str
    symbol: str
    reason: str


@dataclass
class BacktestResult:
    venue: str
    symbol: str
    starting_equity_usd: float
    ending_equity_usd: float
    total_pnl_usd: float
    total_fees_usd: float
    trades: List[BacktestTrade]
    equity_curve: List[Tuple[int, float]]  # (ts_ms, equity)


class Strategy:
    """
    Minimal strategy interface:
    - on_bar returns target position: -1, 0, +1
    """
    def on_bar(self, bar_1m: Bar1m) -> int:
        raise NotImplementedError


class BacktestEngine:
    def __init__(self, cfg: BacktestConfig, fill: FillModel):
        self.cfg = cfg
        self.fill = fill

    def run(self, bars: List[Bar1m], strategy: Strategy) -> BacktestResult:
        if not bars:
            raise ValueError("No bars provided")

        venue = bars[0].venue
        symbol = bars[0].symbol

        equity = float(self.cfg.starting_equity_usd)
        starting_equity = equity

        pos = 0               # -1,0,+1
        entry_price = 0.0
        qty = 0.0

        total_fees = 0.0
        trades: List[BacktestTrade] = []
        equity_curve: List[Tuple[int, float]] = []

        def mark_to_market(price: float) -> float:
            nonlocal equity, pos, entry_price, qty
            if pos == 0:
                return equity
            # unrealized PnL
            pnl = (price - entry_price) * qty * pos
            return equity + pnl

        for bar in bars:
            price = float(bar.close)
            ts = int(bar.ts_ms)

            target = int(strategy.on_bar(bar))
            target = max(-self.cfg.max_position, min(self.cfg.max_position, target))

            # record equity snapshot using M2M
            equity_curve.append((ts, mark_to_market(price)))

            if target == pos:
                continue

            # Close existing position if needed
            if pos != 0:
                # exit at filled price
                exit_side = -pos
                exit_px = self.fill.apply_fill_price(price, exit_side)

                # realized pnl
                realized = (exit_px - entry_price) * qty * pos
                equity += realized

                # fee on exit
                notional = abs(qty * exit_px)
                fee = notional * self.fill.taker_fee_rate
                equity -= fee
                total_fees += fee

                trades.append(
                    BacktestTrade(
                        ts_ms=ts,
                        price=exit_px,
                        side=exit_side,
                        qty=qty,
                        fee_usd=fee,
                        venue=venue,
                        symbol=symbol,
                        reason="exit_to_change_target",
                    )
                )

                # flat now
                pos = 0
                entry_price = 0.0
                qty = 0.0

            # Open new position if target != 0
            if target != 0:
                side = target
                entry_px = self.fill.apply_fill_price(price, side)

                # position sizing: fixed USD notional -> base qty
                notional_usd = min(self.cfg.notional_per_trade_usd, equity)
                if notional_usd <= 0:
                    continue
                qty = notional_usd / entry_px

                # fee on entry
                fee = (qty * entry_px) * self.fill.taker_fee_rate
                equity -= fee
                total_fees += fee

                trades.append(
                    BacktestTrade(
                        ts_ms=ts,
                        price=entry_px,
                        side=side,
                        qty=qty,
                        fee_usd=fee,
                        venue=venue,
                        symbol=symbol,
                        reason="enter_target",
                    )
                )

                pos = target
                entry_price = entry_px

        # final mark
        last_price = float(bars[-1].close)
        final_equity = mark_to_market(last_price)

        return BacktestResult(
            venue=venue,
            symbol=symbol,
            starting_equity_usd=starting_equity,
            ending_equity_usd=final_equity,
            total_pnl_usd=final_equity - starting_equity,
            total_fees_usd=total_fees,
            trades=trades,
            equity_curve=equity_curve,
        )
