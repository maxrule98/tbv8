from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from loguru import logger

from packages.common.sqlite_store import Bar1m
from packages.backtester.aggregate import Aggregator
from packages.backtester.indicators import SMA, EMA


@dataclass
class HTFTrendLTFEntryConfig:
    # Higher timeframe (HTF) trend filter
    htf_minutes: int = 15
    htf_sma_period: int = 50

    # Lower timeframe (LTF) entry filter
    ltf_ema_period: int = 20

    # Entry rules
    require_close_above_ema: bool = True
    allow_shorts: bool = True

    # Warmup behaviour (so we can trade before SMA is ready)
    warmup_trend_mode: str = "htf_candle"  # "htf_candle" | "htf_momentum"
    warmup_min_htf_bars: int = 2

    # Churn control (conceptually "risk/execution", lives here for now)
    min_hold_minutes: int = 10

    # Debug logs
    debug: bool = False


class HTFTrendLTFEntryStrategy:
    """
    HTF trend filter + LTF entry scaffold.

    Returns a target position each bar:
      -1 = short
       0 = flat
      +1 = long

    Notes:
    - We intentionally "stick" to the last trend when HTF candles are flat (open == close)
      to avoid churn.
    - We debounce rapid flips using min_hold_minutes (target can't flip too quickly).
    """

    def __init__(self, cfg: HTFTrendLTFEntryConfig):
        self.cfg = cfg

        self.agg = Aggregator(cfg.htf_minutes)
        self.htf_sma = SMA(cfg.htf_sma_period)

        self.ltf_ema = EMA(cfg.ltf_ema_period)

        # HTF state
        self._trend: int = 0  # -1, 0, +1
        self._htf_count: int = 0
        self._prev_htf_close: Optional[float] = None

        # LTF state
        self._last_ema: Optional[float] = None

        # Debounce state (min-hold for target flips)
        self._last_target: int = 0
        self._last_target_ts_ms: Optional[int] = None

    def _warmup_trend(self, htf_bar: Bar1m) -> int:
        """
        Determine trend before SMA is ready.

        Important: if the candle/momentum is flat, we KEEP the previous trend
        (stickiness) to avoid producing target=0 and churning.
        """
        if self.cfg.warmup_trend_mode == "htf_momentum":
            if self._prev_htf_close is None:
                return self._trend
            if htf_bar.close > self._prev_htf_close:
                return 1
            if htf_bar.close < self._prev_htf_close:
                return -1
            return self._trend

        # default: candle direction with stickiness
        if htf_bar.close > htf_bar.open:
            return 1
        if htf_bar.close < htf_bar.open:
            return -1
        return self._trend

    def _apply_min_hold(self, ts_ms: int, proposed_target: int) -> int:
        """
        Prevent rapid flips: if we already have a non-zero target, don't allow
        flipping to the opposite side until min_hold_minutes has elapsed.
        """
        if proposed_target == 0:
            return 0

        if self._last_target == 0:
            return proposed_target

        if proposed_target == self._last_target:
            return proposed_target

        # proposed flip
        if self._last_target_ts_ms is None:
            return self._last_target

        elapsed = ts_ms - self._last_target_ts_ms
        min_hold_ms = self.cfg.min_hold_minutes * 60_000

        if elapsed < min_hold_ms:
            return self._last_target

        return proposed_target

    def on_bar(self, bar_1m: Bar1m) -> int:
        # 1) Update HTF aggregation
        htf_bar = self.agg.on_bar_1m(bar_1m)
        if htf_bar is not None:
            self._htf_count += 1
            sma = self.htf_sma.update(float(htf_bar.close))

            if sma is not None:
                self._trend = 1 if htf_bar.close > sma else -1
                reason = f"sma_ready close={htf_bar.close:.2f} sma={sma:.2f}"
            else:
                if self._htf_count >= self.cfg.warmup_min_htf_bars:
                    prev = self._trend
                    self._trend = self._warmup_trend(htf_bar)
                    reason = (
                        f"warmup_{self.cfg.warmup_trend_mode} "
                        f"open={htf_bar.open:.2f} close={htf_bar.close:.2f} prev_trend={prev}"
                    )
                else:
                    reason = f"warmup_wait htf_count={self._htf_count}"

            if self.cfg.debug:
                logger.info(
                    "HTF emit t={} htf_count={} trend={} ({})",
                    htf_bar.ts_ms,
                    self._htf_count,
                    self._trend,
                    reason,
                )

            self._prev_htf_close = float(htf_bar.close)

        # 2) Update LTF EMA
        ltf_close = float(bar_1m.close)
        ema = self.ltf_ema.update(ltf_close)
        if ema is not None:
            self._last_ema = float(ema)

        # 3) Produce a proposed target
        proposed = 0

        if ema is not None and self._trend != 0:
            if self._trend > 0:
                if (not self.cfg.require_close_above_ema) or (ltf_close > ema):
                    proposed = 1
            elif self._trend < 0 and self.cfg.allow_shorts:
                if (not self.cfg.require_close_above_ema) or (ltf_close < ema):
                    proposed = -1

        # 4) Debounce flips (min-hold)
        target = self._apply_min_hold(bar_1m.ts_ms, proposed)

        # Track last non-zero target transitions (for min-hold)
        if target != 0 and target != self._last_target:
            self._last_target = target
            self._last_target_ts_ms = bar_1m.ts_ms

        if self.cfg.debug and (bar_1m.ts_ms // 60_000) % 5 == 0:  # occasional log
            logger.info(
                "LTF t={} close={:.2f} ema={} trend={} proposed={} -> target={}",
                bar_1m.ts_ms,
                ltf_close,
                f"{self._last_ema:.2f}" if self._last_ema is not None else None,
                self._trend,
                proposed,
                target,
            )

        return target
