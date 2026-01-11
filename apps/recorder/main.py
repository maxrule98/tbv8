from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Tuple, Optional, List

from loguru import logger

from packages.common.config import TBV8Config, load_tbv8_config
from packages.common.types import VenueId
from packages.common.sqlite_store import SQLiteStore, Bar1m
from packages.adapters.mexc.client import MexcDataClient
from packages.adapters.hyperliquid.client import HyperliquidDataClient
from packages.adapters.base import TradeTick, BBOQuote


# Tuning knobs
DB_PATH = Path("data/tbv8.sqlite")
BATCH_SIZE = 500
FLUSH_INTERVAL_S = 1.0

# Logging knobs
LOG_TRADES = False
LOG_BBO = False
LOG_BAR_ROLLOVER = True


@dataclass
class VenueClients:
    data: object  # BaseDataClient


def build_data_clients(cfg: TBV8Config) -> Dict[VenueId, VenueClients]:
    out: Dict[VenueId, VenueClients] = {}
    for v in cfg.venues:
        if v.venue == "mexc":
            out[v.venue] = VenueClients(data=MexcDataClient(ws_url=v.ws_url, symbols=v.symbols))
        elif v.venue == "hyperliquid":
            out[v.venue] = VenueClients(data=HyperliquidDataClient(ws_url=v.ws_url, symbols=v.symbols))
        else:
            raise ValueError(f"Unsupported venue: {v.venue}")
    return out


def minute_bucket_ms(ts_ms: int) -> int:
    return (ts_ms // 60_000) * 60_000


@dataclass
class BarState:
    ts_ms: int
    open: float
    high: float
    low: float
    close: float
    quote_count: int = 0


class BarBuilder1m:
    """
    Build 1m OHLC bars using BBO mid-price: mid = (bid+ask)/2.

    - on_quote() updates the in-progress bar for that minute.
    - returns a completed bar only on minute rollover (optional logging)
    - snapshot_bars() returns current in-progress bars for UPSERT.
    """

    def __init__(self):
        self._state: Dict[Tuple[str, str], BarState] = {}  # (venue, symbol) -> state

    def on_quote(self, q: BBOQuote) -> Optional[Bar1m]:
        if q.bid <= 0 or q.ask <= 0:
            return None

        mid = (q.bid + q.ask) / 2.0
        bucket = minute_bucket_ms(q.ts_ms)
        key = (q.venue, q.symbol)

        st = self._state.get(key)
        if st is None:
            self._state[key] = BarState(
                ts_ms=bucket,
                open=mid,
                high=mid,
                low=mid,
                close=mid,
                quote_count=1,
            )
            return None

        if bucket == st.ts_ms:
            st.high = max(st.high, mid)
            st.low = min(st.low, mid)
            st.close = mid
            st.quote_count += 1
            return None

        # rollover: finalize previous
        completed = Bar1m(
            venue=q.venue,
            symbol=q.symbol,
            ts_ms=st.ts_ms,
            open=st.open,
            high=st.high,
            low=st.low,
            close=st.close,
            quote_count=st.quote_count,
        )

        # start new
        self._state[key] = BarState(
            ts_ms=bucket,
            open=mid,
            high=mid,
            low=mid,
            close=mid,
            quote_count=1,
        )
        return completed

    def snapshot_bars(self) -> List[Bar1m]:
        out: List[Bar1m] = []
        for (venue, symbol), st in self._state.items():
            out.append(
                Bar1m(
                    venue=venue,
                    symbol=symbol,
                    ts_ms=st.ts_ms,
                    open=st.open,
                    high=st.high,
                    low=st.low,
                    close=st.close,
                    quote_count=st.quote_count,
                )
            )
        return out


async def pump_trades(client, venue: str, out_q: "asyncio.Queue[TradeTick]") -> None:
    async for t in client.trades():
        if LOG_TRADES:
            logger.info("[{}] TRADE {} px={} sz={} ts={}", venue, t.symbol, t.price, t.size, t.ts_ms)
        if not out_q.full():
            out_q.put_nowait(t)


async def pump_bbo(client, venue: str, out_q: "asyncio.Queue[BBOQuote]") -> None:
    async for q in client.bbo():
        if LOG_BBO:
            logger.info("[{}] BBO   {} bid={} ask={} ts={}", venue, q.symbol, q.bid, q.ask, q.ts_ms)
        if not out_q.full():
            out_q.put_nowait(q)


async def writer_loop_trades(store: SQLiteStore, in_q: "asyncio.Queue[TradeTick]", stop: asyncio.Event) -> None:
    batch: list[TradeTick] = []
    last_flush = asyncio.get_event_loop().time()

    while not stop.is_set():
        try:
            item = await asyncio.wait_for(in_q.get(), timeout=0.2)
            batch.append(item)
        except asyncio.TimeoutError:
            pass

        now = asyncio.get_event_loop().time()
        if batch and (len(batch) >= BATCH_SIZE or (now - last_flush) >= FLUSH_INTERVAL_S):
            n = await store.insert_trades(batch)
            await store.flush()
            logger.info("DB wrote trades: {}", n)
            batch.clear()
            last_flush = now

    # Drain remaining
    while not in_q.empty():
        batch.append(in_q.get_nowait())
        if len(batch) >= BATCH_SIZE:
            n = await store.insert_trades(batch)
            await store.flush()
            logger.info("DB wrote trades (drain): {}", n)
            batch.clear()

    if batch:
        n = await store.insert_trades(batch)
        await store.flush()
        logger.info("DB wrote trades (final): {}", n)


async def writer_loop_bbo_and_bars(
    store: SQLiteStore,
    in_q: "asyncio.Queue[BBOQuote]",
    stop: asyncio.Event,
) -> None:
    bbo_batch: list[BBOQuote] = []
    bar_builder = BarBuilder1m()
    last_flush = asyncio.get_event_loop().time()

    while not stop.is_set():
        try:
            item = await asyncio.wait_for(in_q.get(), timeout=0.2)
            bbo_batch.append(item)

            completed = bar_builder.on_quote(item)
            if completed and LOG_BAR_ROLLOVER:
                logger.info(
                    "BAR 1m FINAL [{}] {} t={} o={} h={} l={} c={} n={}",
                    completed.venue,
                    completed.symbol,
                    completed.ts_ms,
                    completed.open,
                    completed.high,
                    completed.low,
                    completed.close,
                    completed.quote_count,
                )

        except asyncio.TimeoutError:
            pass

        now = asyncio.get_event_loop().time()
        should_flush = (now - last_flush) >= FLUSH_INTERVAL_S
        too_big = len(bbo_batch) >= BATCH_SIZE

        if (bbo_batch or should_flush) and (should_flush or too_big):
            # Always upsert bar snapshots on every flush so bars appear immediately
            bar_snapshots = bar_builder.snapshot_bars()

            nb = await store.insert_bbo(bbo_batch)
            nbar = await store.upsert_bars_1m(bar_snapshots)
            await store.flush()

            if bbo_batch:
                logger.info("DB wrote bbo: {}", nb)
            logger.info("DB upserted bars_1m: {}", nbar)

            bbo_batch.clear()
            last_flush = now

    # Final flush of snapshots
    if bbo_batch:
        nb = await store.insert_bbo(bbo_batch)
        await store.flush()
        logger.info("DB wrote bbo (final): {}", nb)

    bar_snapshots = bar_builder.snapshot_bars()
    nbar = await store.upsert_bars_1m(bar_snapshots)
    await store.flush()
    logger.info("DB upserted bars_1m (final): {}", nbar)


async def main() -> None:
    logger.info("TBV8 recorder starting...")

    cfg = load_tbv8_config(
        venues_dir=Path("config/venues"),
        strategy_path=Path("config/strategies/btc_usdt_perp_v1.yaml"),
    )
    logger.info("Loaded venues: {}", [v.venue for v in cfg.venues])
    logger.info(
        "Strategy: {} symbol={} timeframe={}",
        cfg.strategy.strategy_id,
        cfg.strategy.symbol,
        cfg.strategy.timeframe,
    )

    store = await SQLiteStore.open(DB_PATH)
    clients = build_data_clients(cfg)

    for venue, vc in clients.items():
        logger.info("Connecting data client: {}", venue)
        await vc.data.connect()

    logger.info("All data clients connected. Starting pumps + writers...")

    trade_q: "asyncio.Queue[TradeTick]" = asyncio.Queue(maxsize=200_000)
    bbo_q: "asyncio.Queue[BBOQuote]" = asyncio.Queue(maxsize=200_000)

    stop = asyncio.Event()

    tasks = []
    for venue, vc in clients.items():
        tasks.append(asyncio.create_task(pump_trades(vc.data, venue, trade_q)))
        tasks.append(asyncio.create_task(pump_bbo(vc.data, venue, bbo_q)))

    tasks.append(asyncio.create_task(writer_loop_trades(store, trade_q, stop)))
    tasks.append(asyncio.create_task(writer_loop_bbo_and_bars(store, bbo_q, stop)))

    logger.info("Recording to {} (Ctrl+C to stop)", DB_PATH)

    try:
        await asyncio.gather(*tasks)
    except KeyboardInterrupt:
        logger.warning("Keyboard interrupt - shutting down...")
    finally:
        stop.set()
        for t in tasks:
            t.cancel()

        for venue, vc in clients.items():
            logger.info("Closing data client: {}", venue)
            await vc.data.close()

        await store.close()
        logger.info("Recorder stopped.")


if __name__ == "__main__":
    asyncio.run(main())
