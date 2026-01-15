from __future__ import annotations

import random
import asyncio
from dataclasses import dataclass
from typing import Optional, Set

import aiohttp

from packages.common.backfill.types import OHLCV


BINANCE_SUPPORTED_TFS: Set[str] = {
    "1m", "3m", "5m", "15m", "30m",
    "1h", "2h", "4h", "6h", "8h", "12h",
    "1d", "3d", "1w", "1M",
}


@dataclass
class BinanceSpotBackfillAdapter:
    venue: str = "binance_spot"
    base_url: str = "https://api.binance.com"
    request_timeout_s: int = 15
    max_retries: int = 5

    def _symbol_to_binance(self, symbol: str) -> str:
        return symbol.replace("/", "").upper()

    def _validate_tf(self, timeframe: str) -> None:
        if timeframe not in BINANCE_SUPPORTED_TFS:
            raise ValueError(
                f"Unsupported Binance interval timeframe={timeframe!r}. "
                f"Supported: {sorted(BINANCE_SUPPORTED_TFS)}"
            )

    async def fetch_ohlcv(
        self,
        symbol: str,
        timeframe: str,
        start_ms: int,
        end_ms: int,
        limit: int = 1000,
    ) -> list[OHLCV]:
        self._validate_tf(timeframe)

        binance_symbol = self._symbol_to_binance(symbol)
        params = {
            "symbol": binance_symbol,
            "interval": timeframe,
            "startTime": str(start_ms),
            "limit": str(limit),
        }

        url = f"{self.base_url}/api/v3/klines"
        timeout = aiohttp.ClientTimeout(total=self.request_timeout_s)

        last_err: Optional[BaseException] = None
        for attempt in range(1, self.max_retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=timeout) as sess:
                    async with sess.get(url, params=params) as resp:
                        text = await resp.text()
                        if resp.status != 200:
                            raise RuntimeError(f"Binance klines HTTP {resp.status}: {text[:200]}")
                        data = await resp.json()

                out: list[OHLCV] = []
                for row in data:
                    ts_ms = int(row[0])
                    out.append(
                        OHLCV(
                            ts_ms=ts_ms,
                            open=float(row[1]),
                            high=float(row[2]),
                            low=float(row[3]),
                            close=float(row[4]),
                            volume=float(row[5]),
                        )
                    )
                out.sort(key=lambda x: x.ts_ms)
                out = [b for b in out if b.ts_ms < end_ms]
                return out

            except Exception as e:
                last_err = e
                base = min(2 ** (attempt - 1), 10)
                await asyncio.sleep(base + random.uniform(0, 0.25))

        raise RuntimeError(f"Binance backfill failed after {self.max_retries} attempts") from last_err