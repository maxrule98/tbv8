from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Sequence

from packages.backtester.types import Bar
from packages.market_data.bar_store import MultiTFBarStore
from packages.execution.router import ExecutionRouter
from packages.execution.simfill import SimFillExecutor
from packages.execution.types import OrderIntent, OrderSide


# =========================
# Strategy API (context-driven)
# =========================

@dataclass(frozen=True)
class StrategyContext:
    """
    Passed to strategies each run-loop step.

    - bar: the primary/run timeframe bar (the engine iterates on this stream)
    - store: access to all timeframes (as-of bar.ts_ms)
    - identity: data + execution venues, symbol, and run_tf
    - state: per-strategy scratchpad (persists across calls)
    """
    bar: Bar
    store: MultiTFBarStore

    data_venue: str
    exec_primary_venue: str
    exec_shadow_venue: Optional[str]

    symbol: str
    run_tf: str

    state: Dict[str, Any]


class Strategy(Protocol):
    def on_bar(self, ctx: StrategyContext) -> int:
        """
        Return target position: -1, 0, +1
        """
        ...


# =========================
# Runtime engine
# =========================

@dataclass(frozen=True)
class RuntimeEngineConfig:
    # Data identity (what we're running on)
    data_venue: str
    symbol: str
    run_tf: str

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
    run_tf: str

    starting_equity_usd: float
    ending_equity_usd: float
    total_pnl_usd: float
    total_fees_usd: float
    trades: List[RuntimeTrade]


class RuntimeEngine:
    """
    Runtime (SIMFILL) engine.

    Updated behaviour:
    - Strategy is context-driven (ctx.bar + ctx.store for multi-TF as-of access).
    - No hardcoded symbol/timeframes; identity is provided via config/context.

    NOTE: accounting is still fees-only in this scaffold (as per current TBV8 state).
    """

    def __init__(
        self,
        cfg: RuntimeEngineConfig,
        *,
        router: ExecutionRouter,
        executor: SimFillExecutor,
        store: MultiTFBarStore,
    ):
        self.cfg = cfg
        self.router = router
        self.executor = executor
        self.store = store

        # shared scratchpad for strategy runtime state
        self._state: Dict[str, Any] = {}

    def run(self, bars: Sequence[Bar], strat: Strategy) -> RuntimeResult:
        if not bars:
            return RuntimeResult(
                data_venue=self.cfg.data_venue,
                exec_primary_venue=self.cfg.exec_primary_venue,
                exec_shadow_venue=self.cfg.exec_shadow_venue,
                symbol=self.cfg.symbol,
                run_tf=self.cfg.run_tf,
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
            ctx = StrategyContext(
                bar=bar,
                store=self.store,
                data_venue=self.cfg.data_venue,
                exec_primary_venue=self.cfg.exec_primary_venue,
                exec_shadow_venue=self.cfg.exec_shadow_venue,
                symbol=self.cfg.symbol,
                run_tf=self.cfg.run_tf,
                state=self._state,
            )

            target = int(strat.on_bar(ctx))
            if target not in (-1, 0, 1):
                raise ValueError(f"Strategy returned invalid target={target}")

            if target == position:
                continue

            intents: List[OrderIntent] = []

            if position == 0 and target != 0:
                intents.append(self._intent_from_target(target, bar.ts_ms, reason="enter_target"))

            elif position != 0 and target == 0:
                intents.append(self._intent_to_flat(position, bar.ts_ms, reason="exit_to_flat"))

            elif position != 0 and target != 0 and target != position:
                intents.append(self._intent_to_flat(position, bar.ts_ms, reason="exit_to_change_target"))
                intents.append(self._intent_from_target(target, bar.ts_ms, reason="enter_target"))

            for intent in intents:
                primary_venue = self.router.choose_primary()
                if primary_venue != self.cfg.exec_primary_venue:
                    raise RuntimeError(
                        f"Router primary mismatch: router={primary_venue} cfg={self.cfg.exec_primary_venue}"
                    )

                fill = self.executor.execute(price=float(bar.close), intent=intent)

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
            run_tf=self.cfg.run_tf,
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
        side = OrderSide.SELL if position > 0 else OrderSide.BUY
        return OrderIntent(
            symbol=self.cfg.symbol,
            side=side,
            notional_usd=float(self.cfg.notional_per_trade_usd),
            ts_ms=int(ts_ms),
            reason=reason,
        )