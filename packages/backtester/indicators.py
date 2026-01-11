from __future__ import annotations

from collections import deque
from dataclasses import dataclass
from typing import Deque, Optional


@dataclass
class SMA:
    period: int
    _buf: Deque[float] = None
    _sum: float = 0.0

    def __post_init__(self):
        self._buf = deque(maxlen=self.period)

    def update(self, x: float) -> Optional[float]:
        if len(self._buf) == self.period:
            self._sum -= self._buf[0]
        self._buf.append(x)
        self._sum += x
        if len(self._buf) < self.period:
            return None
        return self._sum / self.period


@dataclass
class EMA:
    period: int
    _alpha: float = 0.0
    _value: Optional[float] = None

    def __post_init__(self):
        self._alpha = 2.0 / (self.period + 1.0)

    def update(self, x: float) -> Optional[float]:
        if self._value is None:
            self._value = x
            return None  # warmup
        self._value = (self._alpha * x) + ((1.0 - self._alpha) * self._value)
        return self._value
