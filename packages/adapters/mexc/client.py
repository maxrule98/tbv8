from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator, Optional

import websockets
from loguru import logger

from packages.adapters.base import BBOQuote, BaseDataClient, TradeTick
from packages.common.types import VenueId


def _to_mexc_symbol(symbol: str) -> str:
    # "BTC/USDT" -> "BTC_USDT"
    return symbol.replace("/", "_").upper()


class MexcDataClient(BaseDataClient):
    def __init__(self, ws_url: str, symbols: list[str]):
        self.ws_url = ws_url
        self.symbols = symbols
        self.venue: VenueId = "mexc"

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None

        self._trade_q: "asyncio.Queue[TradeTick]" = asyncio.Queue(maxsize=50_000)
        self._bbo_q: "asyncio.Queue[BBOQuote]" = asyncio.Queue(maxsize=50_000)

        self._stop = asyncio.Event()

    async def connect(self) -> None:
        self._stop.clear()
        self._ws = await websockets.connect(self.ws_url, ping_interval=None)
        logger.info("MEXC WS connected: {}", self.ws_url)

        # Subscribe per symbol
        for s in self.symbols:
            ms = _to_mexc_symbol(s)

            # Trades
            await self._ws.send(json.dumps({"method": "sub.deal", "param": {"symbol": ms}, "gzip": False}))
            # Ticker (contains bid1/ask1/lastPrice/timestamp)
            await self._ws.send(json.dumps({"method": "sub.ticker", "param": {"symbol": ms}, "gzip": False}))

        # Reader loop
        self._task = asyncio.create_task(self._read_loop())

        # Start ping loop (MEXC says ping every 10–20s, disconnect if none within 1 min) :contentReference[oaicite:6]{index=6}
        asyncio.create_task(self._ping_loop())

    async def close(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
        if self._ws:
            await self._ws.close()
        logger.info("MEXC WS closed")

    async def _ping_loop(self) -> None:
        while not self._stop.is_set():
            try:
                if self._ws:
                    await self._ws.send(json.dumps({"method": "ping"}))
            except Exception as e:
                logger.warning("MEXC ping error: {}", e)
            await asyncio.sleep(15)

    async def _read_loop(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                ch = msg.get("channel")

                # Trades
                if ch == "push.deal":
                    symbol = msg.get("symbol", "")
                    data = msg.get("data", [])
                    # data is list of trades
                    for t in data:
                        tick = TradeTick(
                            venue=self.venue,
                            symbol=symbol.replace("_", "/"),
                            ts_ms=int(t["t"]),
                            price=float(t["p"]),
                            size=float(t["v"]),
                        )
                        if not self._trade_q.full():
                            self._trade_q.put_nowait(tick)

                # Ticker (BBO)
                elif ch == "push.ticker":
                    d = msg.get("data", {})
                    symbol = d.get("symbol") or msg.get("symbol") or ""
                    ts_ms = int(d.get("timestamp") or msg.get("ts") or 0)
                    bid = float(d.get("bid1") or 0)
                    ask = float(d.get("ask1") or 0)
                    if bid and ask:
                        q = BBOQuote(
                            venue=self.venue,
                            symbol=symbol.replace("_", "/"),
                            ts_ms=ts_ms,
                            bid=bid,
                            ask=ask,
                        )
                        if not self._bbo_q.full():
                            self._bbo_q.put_nowait(q)

        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.exception("MEXC read loop crashed: {}", e)

    async def _iter_q(self, q: "asyncio.Queue"):
        while True:
            item = await q.get()
            yield item

    def trades(self) -> AsyncIterator[TradeTick]:
        return self._iter_q(self._trade_q)

    def bbo(self) -> AsyncIterator[BBOQuote]:
        return self._iter_q(self._bbo_q)
