from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import List

import aiosqlite
from loguru import logger

from packages.adapters.base import BBOQuote, TradeTick


SCHEMA_SQL = """
PRAGMA journal_mode=WAL;
PRAGMA synchronous=NORMAL;
PRAGMA temp_store=MEMORY;

CREATE TABLE IF NOT EXISTS trades (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  venue TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  price REAL NOT NULL,
  size REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_trades_vs_ts
  ON trades (venue, symbol, ts_ms);

CREATE TABLE IF NOT EXISTS bbo (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  venue TEXT NOT NULL,
  symbol TEXT NOT NULL,
  ts_ms INTEGER NOT NULL,
  bid REAL NOT NULL,
  ask REAL NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bbo_vs_ts
  ON bbo (venue, symbol, ts_ms);
"""


@dataclass
class SQLiteStore:
    db_path: Path
    conn: aiosqlite.Connection

    @classmethod
    async def open(cls, db_path: Path) -> "SQLiteStore":
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = await aiosqlite.connect(str(db_path))
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        logger.info("SQLiteStore ready: {}", db_path)
        return cls(db_path=db_path, conn=conn)

    async def close(self) -> None:
        await self.conn.close()
        logger.info("SQLiteStore closed")

    async def insert_trades(self, batch: List[TradeTick]) -> int:
        if not batch:
            return 0
        rows = [(t.venue, t.symbol, int(t.ts_ms), float(t.price), float(t.size)) for t in batch]
        await self.conn.executemany(
            "INSERT INTO trades (venue, symbol, ts_ms, price, size) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        return len(rows)

    async def insert_bbo(self, batch: List[BBOQuote]) -> int:
        if not batch:
            return 0
        rows = [(q.venue, q.symbol, int(q.ts_ms), float(q.bid), float(q.ask)) for q in batch]
        await self.conn.executemany(
            "INSERT INTO bbo (venue, symbol, ts_ms, bid, ask) VALUES (?, ?, ?, ?, ?)",
            rows,
        )
        return len(rows)

    async def flush(self) -> None:
        await self.conn.commit()