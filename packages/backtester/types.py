from dataclasses import dataclass

@dataclass(frozen=True)
class Bar:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0
