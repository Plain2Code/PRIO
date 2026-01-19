"""Shared SQLite helpers for trade logging and equity snapshots."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import aiosqlite

DB_PATH = Path("data/trade_history.db")


async def init_database() -> None:
    """Initialize SQLite trade log database."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS trades (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                units INTEGER NOT NULL,
                entry_price REAL NOT NULL,
                exit_price REAL,
                stop_loss REAL,
                take_profit REAL,
                pnl REAL,
                pnl_pct REAL,
                fees REAL DEFAULT 0.0,
                slippage REAL DEFAULT 0.0,
                strategy TEXT,
                regime TEXT,
                status TEXT DEFAULT 'open',
                closed_at TEXT,
                metadata TEXT
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS equity_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                balance REAL NOT NULL,
                equity REAL NOT NULL,
                unrealized_pnl REAL DEFAULT 0.0,
                drawdown_pct REAL DEFAULT 0.0
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_summary (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT NOT NULL UNIQUE,
                starting_balance REAL,
                ending_balance REAL,
                pnl REAL,
                trades_count INTEGER,
                win_count INTEGER,
                loss_count INTEGER,
                max_drawdown_pct REAL
            )
        """)
        await db.commit()


async def log_trade(trade: dict) -> None:
    """Log a trade to SQLite."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.execute(
            """INSERT INTO trades
               (timestamp, instrument, direction, units, entry_price, stop_loss,
                take_profit, strategy, regime, status)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                datetime.now(timezone.utc).isoformat(),
                trade["instrument"],
                trade["direction"],
                trade["units"],
                trade["entry_price"],
                trade.get("stop_loss"),
                trade.get("take_profit"),
                trade.get("strategy"),
                trade.get("regime"),
                "open",
            ),
        )
        await db.commit()


async def update_trade_close(
    instrument: str,
    exit_price: float,
    pnl: float,
    pnl_pct: float,
    fees: float,
) -> None:
    """Update trade record when position is closed."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.execute(
            """UPDATE trades SET exit_price=?, pnl=?, pnl_pct=?, fees=?, status='closed',
               closed_at=? WHERE id = (
                   SELECT id FROM trades WHERE instrument=? AND status='open'
                   ORDER BY id DESC LIMIT 1
               )""",
            (exit_price, pnl, pnl_pct, fees, datetime.now(timezone.utc).isoformat(), instrument),
        )
        await db.commit()


async def get_open_trade_instruments() -> set[str]:
    """Return set of instruments that have open trades in the DB."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT DISTINCT instrument FROM trades WHERE status = 'open'"
        )
        rows = await cursor.fetchall()
    return {row["instrument"] for row in rows}


async def get_open_trades() -> list[dict]:
    """Return all open trades with full details (for reconciliation)."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT * FROM trades WHERE status = 'open'"
        )
        rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def reconcile_closed_trades(broker_open_instruments: set[str]) -> list[str]:
    """Detect trades closed externally (manually via broker) and mark them.

    Compares DB open trades against the set of instruments currently open
    at the broker.  Any DB open trade whose instrument is NOT in
    *broker_open_instruments* was closed externally.

    Returns list of instruments that were reconciled.
    """
    db_open = await get_open_trade_instruments()
    externally_closed = db_open - broker_open_instruments
    if not externally_closed:
        return []

    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(str(DB_PATH)) as db:
        for instrument in externally_closed:
            await db.execute(
                """UPDATE trades SET status='closed', closed_at=?,
                   metadata='manual_close'
                   WHERE id = (
                       SELECT id FROM trades WHERE instrument=? AND status='open'
                       ORDER BY id DESC LIMIT 1
                   )""",
                (now, instrument),
            )
        await db.commit()
    return list(externally_closed)


async def enrich_reconciled_trade(
    instrument: str,
    exit_price: float,
    pnl: float,
    pnl_pct: float,
    close_reason: str,
) -> None:
    """Enrich a reconciled trade with actual exit data from broker activity.

    Called after reconcile_closed_trades() when we successfully fetch
    the close details from Capital.com's activity/transaction history.
    """
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.execute(
            """UPDATE trades SET exit_price=?, pnl=?, pnl_pct=?, metadata=?
               WHERE id = (
                   SELECT id FROM trades WHERE instrument=? AND status='closed'
                     AND metadata='manual_close'
                   ORDER BY id DESC LIMIT 1
               )""",
            (exit_price, pnl, pnl_pct, close_reason, instrument),
        )
        await db.commit()


async def get_known_deal_ids() -> set[str]:
    """Return set of dealIds already in the database (to avoid duplicates on sync)."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            "SELECT DISTINCT metadata FROM trades WHERE metadata LIKE 'dealId:%'"
        )
        rows = await cursor.fetchall()
    return {row["metadata"].split(":", 1)[1] for row in rows if row["metadata"]}


async def import_historical_trade(trade: dict) -> None:
    """Import a single historical trade from Capital.com activity into the DB.

    Used by the startup sync to backfill trades that happened before the bot
    was running or between restarts.
    """
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.execute(
            """INSERT INTO trades
               (timestamp, instrument, direction, units, entry_price,
                exit_price, stop_loss, take_profit, pnl, pnl_pct,
                status, closed_at, metadata, strategy)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'closed', ?, ?, 'imported')""",
            (
                trade.get("timestamp", ""),
                trade["instrument"],
                trade.get("direction", "unknown"),
                trade.get("units", 0),
                trade.get("entry_price", 0),
                trade.get("exit_price"),
                trade.get("stop_loss"),
                trade.get("take_profit"),
                trade.get("pnl"),
                trade.get("pnl_pct"),
                trade.get("closed_at", ""),
                trade.get("metadata", ""),
            ),
        )
        await db.commit()


async def get_null_pnl_trades() -> list[dict]:
    """Return closed trades that have NULL pnl (need repair from broker data)."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT * FROM trades
               WHERE status = 'closed' AND pnl IS NULL
               ORDER BY id"""
        )
        rows = await cursor.fetchall()
    return [dict(row) for row in rows]


async def repair_trade(
    trade_id: int,
    exit_price: float | None,
    pnl: float | None,
    pnl_pct: float | None,
    close_reason: str | None = None,
) -> None:
    """Repair a specific trade by ID — set exit_price, pnl, and optionally metadata."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        if close_reason is not None:
            await db.execute(
                "UPDATE trades SET exit_price=?, pnl=?, pnl_pct=?, metadata=? WHERE id=?",
                (exit_price, pnl, pnl_pct, close_reason, trade_id),
            )
        else:
            await db.execute(
                "UPDATE trades SET exit_price=?, pnl=?, pnl_pct=? WHERE id=?",
                (exit_price, pnl, pnl_pct, trade_id),
            )
        await db.commit()


async def get_trade_stats(last_n: int = 200) -> dict:
    """Query actual win_rate and avg_win_loss_ratio from last N closed trades.

    Used by adaptive Kelly position sizing to compute edge from real data
    rather than hardcoded assumptions.
    """
    async with aiosqlite.connect(str(DB_PATH)) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(
            """SELECT pnl FROM trades
               WHERE status = 'closed' AND pnl IS NOT NULL
               ORDER BY id DESC LIMIT ?""",
            (last_n,),
        )
        rows = await cursor.fetchall()

    if not rows:
        return {"count": 0, "win_rate": 0.0, "avg_win_loss_ratio": 0.0}

    pnls = [float(row["pnl"]) for row in rows]
    count = len(pnls)
    winners = [p for p in pnls if p > 0]
    losers = [p for p in pnls if p < 0]

    win_rate = len(winners) / count if count > 0 else 0.0
    avg_win = sum(winners) / len(winners) if winners else 0.0
    avg_loss = abs(sum(losers) / len(losers)) if losers else 0.0
    ratio = avg_win / avg_loss if avg_loss > 0 else 0.0

    return {"count": count, "win_rate": win_rate, "avg_win_loss_ratio": ratio}


async def snapshot_equity(
    balance: float,
    equity: float,
    unrealized_pnl: float,
    drawdown_pct: float,
) -> None:
    """Record equity snapshot."""
    async with aiosqlite.connect(str(DB_PATH)) as db:
        await db.execute(
            "INSERT INTO equity_snapshots (timestamp, balance, equity, unrealized_pnl, drawdown_pct) VALUES (?, ?, ?, ?, ?)",
            (datetime.now(timezone.utc).isoformat(), balance, equity, unrealized_pnl, drawdown_pct),
        )
        await db.commit()
