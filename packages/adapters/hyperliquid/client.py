from __future__ import annotations

import asyncio
import json
from typing import AsyncIterator, Optional

import websockets
from loguru import logger

from packages.adapters.base import BBOQuote, BaseDataClient, TradeTick
from packages.common.types import VenueId


def _to_hl_coin(symbol: str) -> str:
    # "BTC/USDT" -> "BTC"
    return symbol.split("/")[0].upper()


class HyperliquidDataClient(BaseDataClient):
    def __init__(self, ws_url: str, symbols: list[str]):
        self.ws_url = ws_url
        self.symbols = symbols
        self.venue: VenueId = "hyperliquid"

        self._ws: Optional[websockets.WebSocketClientProtocol] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

        self._trade_q: "asyncio.Queue[TradeTick]" = asyncio.Queue(maxsize=50_000)
        self._bbo_q: "asyncio.Queue[BBOQuote]" = asyncio.Queue(maxsize=50_000)

    async def connect(self) -> None:
        self._stop.clear()
        self._ws = await websockets.connect(self.ws_url, ping_interval=20)
        logger.info("Hyperliquid WS connected: {}", self.ws_url)

        # Subscribe per symbol
        for s in self.symbols:
            coin = _to_hl_coin(s)

            await self._ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "trades", "coin": coin}}))
            await self._ws.send(json.dumps({"method": "subscribe", "subscription": {"type": "bbo", "coin": coin}}))

            logger.info("Hyperliquid subscribed: trades + bbo for {}", coin)

        self._task = asyncio.create_task(self._read_loop())

    async def close(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()
        if self._ws:
            await self._ws.close()
        logger.info("Hyperliquid WS closed")

    async def _read_loop(self) -> None:
        assert self._ws is not None
        try:
            async for raw in self._ws:
                msg = json.loads(raw)
                ch = msg.get("channel")
                data = msg.get("data")

                # Subscription ACK / errors (super useful during bring-up)
                if ch == "subscriptionResponse":
                    logger.info("Hyperliquid subscriptionResponse: {}", data)
                    continue

                if ch == "error":
                    logger.error("Hyperliquid WS error: {}", msg)
                    continue

                # Trades: channel == "trades", data == WsTrade[]
                if ch == "trades" and isinstance(data, list):
                    for t in data:
                        coin = t.get("coin", "")
                        tick = TradeTick(
                            venue=self.venue,
                            symbol=f"{coin}/USDT",  # v1 assumption
                            ts_ms=int(t["time"]),
                            price=float(t["px"]),
                            size=float(t["sz"]),
                        )
                        if not self._trade_q.full():
                            self._trade_q.put_nowait(tick)
                    continue

                # BBO: channel == "bbo"
                # Docs: WsBbo = { coin: string; time: number; bbo: [WsLevel|null, WsLevel|null] }
                # WsLevel = { px: string; sz: string; n: number } 
                if ch == "bbo":
                    # Be liberal: usually payload is in `data`, but handle root payload too.
                    payload = data if isinstance(data, dict) else (msg if isinstance(msg, dict) else None)
                    if not isinstance(payload, dict):
                        continue

                    coin = payload.get("coin", "")
                    ts_ms = int(payload.get("time", 0))

                    bbo = payload.get("bbo")
                    if isinstance(bbo, list) and len(bbo) == 2:
                        bid_level = bbo[0]
                        ask_level = bbo[1]

                        bid = float(bid_level["px"]) if isinstance(bid_level, dict) and "px" in bid_level else 0.0
                        ask = float(ask_level["px"]) if isinstance(ask_level, dict) and "px" in ask_level else 0.0

                        if bid and ask:
                            q = BBOQuote(
                                venue=self.venue,
                                symbol=f"{coin}/USDT",
                                ts_ms=ts_ms,
                                bid=bid,
                                ask=ask,
                            )
                            if not self._bbo_q.full():
                                self._bbo_q.put_nowait(q)
                    continue

                # Anything else: log at debug while we’re still bringing it up
                logger.debug("Hyperliquid other message: {}", msg)

        except asyncio.CancelledError:
            return
        except Exception as e:
            logger.exception("Hyperliquid read loop crashed: {}", e)

    async def _iter_q(self, q: "asyncio.Queue"):
        while True:
            item = await q.get()
            yield item

    def trades(self) -> AsyncIterator[TradeTick]:
        return self._iter_q(self._trade_q)

    def bbo(self) -> AsyncIterator[BBOQuote]:
        return self._iter_q(self._bbo_q)
