from __future__ import annotations

import abc
from dataclasses import dataclass
from typing import AsyncIterator

from packages.common.types import VenueId


@dataclass(frozen=True)
class TradeTick:
    venue: VenueId
    symbol: str
    ts_ms: int
    price: float
    size: float


@dataclass(frozen=True)
class BBOQuote:
    venue: VenueId
    symbol: str
    ts_ms: int
    bid: float
    ask: float


class BaseDataClient(abc.ABC):
    @abc.abstractmethod
    async def connect(self) -> None: ...

    @abc.abstractmethod
    async def close(self) -> None: ...

    @abc.abstractmethod
    def trades(self) -> AsyncIterator[TradeTick]: ...

    @abc.abstractmethod
    def bbo(self) -> AsyncIterator[BBOQuote]: ...
