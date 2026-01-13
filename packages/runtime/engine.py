from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional, Protocol

from packages.backtester.types import Bar
from packages.execution.router import ExecutionRouter
from packages.execution.simfill import SimFillExecutor
from packages.execution.types import Fill, OrderIntent, OrderSide


class Strategy(Protocol):
    def on_bar(self, bar: Bar) -> int:
        """
        Return target position: -1, 0, +1
        """
        ...


@dataclass(frozen=True)
class RuntimeEngineConfig:
    # Data identity (what we're running on)
    data_venue: str
    symbol: str
    timeframe: str

    # Execution identity (where orders go)
    exec_primary_venue: str
    exec_shadow_venue: Optional[str]

    # Capital + sizing
    starting_equity_usd: float = 10_000.0
    notional_per_trade_usd: float = 1_000.0
    max_position: int = 1


@dataclass(frozen=True)
class RuntimeTrade:
    venue: str
    symbol: str
    ts_ms: int
    side: int  # +1 buy, -1 sell
    price: float
    qty: float
    fee_usd: float
    reason: str


@dataclass(frozen=True)
class RuntimeResult:
    data_venue: str
    exec_primary_venue: str
    exec_shadow_venue: Optional[str]
    symbol: str
    timeframe: str

    starting_equity_usd: float
    ending_equity_usd: float
    total_pnl_usd: float
    total_fees_usd: float
    trades: List[RuntimeTrade]


class RuntimeEngine:
    """
    Runtime (SIMFILL) engine.

    Key invariants:
    - Bars come from cfg.history.market_data_venue (data venue)
    - Orders are attributed to cfg.strategy.routing.primary (execution venue)
    - Shadow venue is wired but no side effects yet (we'll add shadow execution later)
    """

    def __init__(
        self,
        cfg: RuntimeEngineConfig,
        router: ExecutionRouter,
        executor: SimFillExecutor,
    ):
        self.cfg = cfg
        self.router = router
        self.executor = executor

    def run(self, bars: List[Bar], strat: Strategy) -> RuntimeResult:
        if not bars:
            return RuntimeResult(
                data_venue=self.cfg.data_venue,
                exec_primary_venue=self.cfg.exec_primary_venue,
                exec_shadow_venue=self.cfg.exec_shadow_venue,
                symbol=self.cfg.symbol,
                timeframe=self.cfg.timeframe,
                starting_equity_usd=self.cfg.starting_equity_usd,
                ending_equity_usd=self.cfg.starting_equity_usd,
                total_pnl_usd=0.0,
                total_fees_usd=0.0,
                trades=[],
            )

        equity = float(self.cfg.starting_equity_usd)
        position = 0  # -1, 0, +1

        trades: List[RuntimeTrade] = []
        total_fees = 0.0

        for bar in bars:
            target = int(strat.on_bar(bar))
            if target not in (-1, 0, 1):
                raise ValueError(f"Strategy returned invalid target={target}")

            if target == position:
                continue

            # Determine order(s) needed to move from position -> target
            # We execute at most one unit position, so transitions are:
            #  0 -> 1 (buy), 0 -> -1 (sell)
            #  1 -> 0 (sell), -1 -> 0 (buy)
            #  1 -> -1 (sell then sell? No - we just close then open: two intents)
            # -1 -> 1 (buy then buy: close then open)
            intents: List[OrderIntent] = []

            if position == 0 and target != 0:
                intents.append(self._intent_from_target(target, bar.ts_ms, reason="enter_target"))

            elif position != 0 and target == 0:
                intents.append(self._intent_to_flat(position, bar.ts_ms, reason="exit_to_flat"))

            elif position != 0 and target != 0 and target != position:
                # flip: close then open
                intents.append(self._intent_to_flat(position, bar.ts_ms, reason="exit_to_change_target"))
                intents.append(self._intent_from_target(target, bar.ts_ms, reason="enter_target"))

            # Execute
            for intent in intents:
                primary_venue = self.router.choose_primary()
                if primary_venue != self.cfg.exec_primary_venue:
                    # This should never diverge – if it does, we want to know immediately.
                    raise RuntimeError(
                        f"Router primary mismatch: router={primary_venue} cfg={self.cfg.exec_primary_venue}"
                    )

                fill = self.executor.execute(price=float(bar.close), intent=intent)

                # PnL accounting (simple): fees reduce equity; price movement is ignored in this SIMFILL scaffold.
                # In TBV8 we will replace this with proper PnL / inventory accounting.
                equity -= float(fill.fee_usd)
                total_fees += float(fill.fee_usd)

                trades.append(
                    RuntimeTrade(
                        venue=fill.venue,
                        symbol=fill.symbol,
                        ts_ms=fill.ts_ms,
                        side=fill.side,
                        price=fill.price,
                        qty=fill.qty,
                        fee_usd=fill.fee_usd,
                        reason=fill.reason,
                    )
                )

                # update position after each fill
                if fill.reason.startswith("exit"):
                    position = 0
                elif fill.reason.startswith("enter"):
                    position = 1 if fill.side > 0 else -1

        ending = float(equity)
        pnl = ending - float(self.cfg.starting_equity_usd)

        return RuntimeResult(
            data_venue=self.cfg.data_venue,
            exec_primary_venue=self.cfg.exec_primary_venue,
            exec_shadow_venue=self.cfg.exec_shadow_venue,
            symbol=self.cfg.symbol,
            timeframe=self.cfg.timeframe,
            starting_equity_usd=self.cfg.starting_equity_usd,
            ending_equity_usd=ending,
            total_pnl_usd=pnl,
            total_fees_usd=total_fees,
            trades=trades,
        )

    def _intent_from_target(self, target: int, ts_ms: int, reason: str) -> OrderIntent:
        side = OrderSide.BUY if target > 0 else OrderSide.SELL
        return OrderIntent(
            symbol=self.cfg.symbol,
            side=side,
            notional_usd=float(self.cfg.notional_per_trade_usd),
            ts_ms=int(ts_ms),
            reason=reason,
        )

    def _intent_to_flat(self, position: int, ts_ms: int, reason: str) -> OrderIntent:
        # If long, sell to close. If short, buy to close.
        side = OrderSide.SELL if position > 0 else OrderSide.BUY
        return OrderIntent(
            symbol=self.cfg.symbol,
            side=side,
            notional_usd=float(self.cfg.notional_per_trade_usd),
            ts_ms=int(ts_ms),
            reason=reason,
        )
