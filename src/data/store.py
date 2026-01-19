"""Trade and equity data store backed by SQLite.

STANDALONE: Knows nothing about trading logic.  Receives plain data
(dicts, floats, strings) and persists/queries it.

Usage:
    from src.data.store import TradeStore

    store = TradeStore()
    await store.init()
    await store.log_trade(instrument, signal_dict, result_dict)
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path

import aiosqlite
import structlog

logger = structlog.get_logger(__name__)

_DEFAULT_DB_PATH = Path("data/trade_history.db")


class TradeStore:
    """Async SQLite store for trades, equity snapshots, and daily summaries.

    Parameters
    ----------
    db_path : Path | str
        Path to the SQLite database file.  Parent directory is created
        automatically.
    """

    def __init__(self, db_path: Path | str = _DEFAULT_DB_PATH) -> None:
        self._db_path = Path(db_path)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def init(self) -> None:
        """Create tables if they don't exist."""
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        async with aiosqlite.connect(str(self._db_path)) as db:
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
            await db.commit()
        logger.info("trade_store_initialized", db_path=str(self._db_path))

    # ------------------------------------------------------------------
    # Trade logging
    # ------------------------------------------------------------------

    async def log_trade(
        self,
        instrument: str,
        signal: dict,
        result: dict,
    ) -> None:
        """Record a newly opened trade."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            await db.execute(
                """INSERT INTO trades
                   (timestamp, instrument, direction, units, entry_price,
                    stop_loss, take_profit, strategy, regime, status)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    instrument,
                    signal.get("direction", "unknown"),
                    result.get("units_filled", signal.get("units", 0)),
                    result.get("fill_price") or signal.get("entry_price") or 0,
                    signal.get("stop_loss"),
                    signal.get("take_profit"),
                    signal.get("strategy_name"),
                    signal.get("regime"),
                    "open",
                ),
            )
            await db.commit()

    async def close_trade(
        self,
        instrument: str,
        exit_price: float = 0.0,
        pnl: float = 0.0,
        pnl_pct: float = 0.0,
        fees: float = 0.0,
        close_reason: str = "",
    ) -> None:
        """Mark the most recent open trade for *instrument* as closed."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            await db.execute(
                """UPDATE trades SET exit_price=?, pnl=?, pnl_pct=?, fees=?,
                   status='closed', closed_at=?, metadata=?
                   WHERE id = (
                       SELECT id FROM trades WHERE instrument=? AND status='open'
                       ORDER BY id DESC LIMIT 1
                   )""",
                (
                    exit_price, pnl, pnl_pct, fees,
                    datetime.now(timezone.utc).isoformat(),
                    close_reason,
                    instrument,
                ),
            )
            await db.commit()

    # ------------------------------------------------------------------
    # Reconciliation
    # ------------------------------------------------------------------

    async def get_open_trade_instruments(self) -> set[str]:
        """Return set of instruments that have open trades in the DB."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT DISTINCT instrument FROM trades WHERE status = 'open'"
            )
            rows = await cursor.fetchall()
        return {row["instrument"] for row in rows}

    async def get_open_trades(self) -> list[dict]:
        """Return all open trades with full details."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM trades WHERE status = 'open'"
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def reconcile(self, broker_open_instruments: set[str]) -> list[str]:
        """Detect externally closed trades and mark them.

        Compares DB open trades against broker positions.  Returns list
        of instruments that were reconciled (closed externally).
        """
        db_open = await self.get_open_trade_instruments()
        externally_closed = db_open - broker_open_instruments
        if not externally_closed:
            return []

        now = datetime.now(timezone.utc).isoformat()
        async with aiosqlite.connect(str(self._db_path)) as db:
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

        logger.info("trades_reconciled", instruments=list(externally_closed))
        return list(externally_closed)

    async def enrich_reconciled_trade(
        self,
        instrument: str,
        exit_price: float,
        pnl: float,
        pnl_pct: float,
        close_reason: str,
    ) -> None:
        """Enrich a reconciled trade with actual exit data from broker."""
        async with aiosqlite.connect(str(self._db_path)) as db:
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

    # ------------------------------------------------------------------
    # Historical import
    # ------------------------------------------------------------------

    async def get_known_deal_ids(self) -> set[str]:
        """Return dealIds already in the database (to avoid duplicate import)."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT DISTINCT metadata FROM trades WHERE metadata LIKE 'dealId:%'"
            )
            rows = await cursor.fetchall()
        return {row["metadata"].split(":", 1)[1] for row in rows if row["metadata"]}

    async def import_historical_trade(self, trade: dict) -> None:
        """Import a single historical trade from broker activity."""
        async with aiosqlite.connect(str(self._db_path)) as db:
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

    # ------------------------------------------------------------------
    # Queries
    # ------------------------------------------------------------------

    async def get_trades(self, limit: int = 50) -> list[dict]:
        """Return recent trades (newest first)."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM trades ORDER BY id DESC LIMIT ?",
                (limit,),
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def get_trade_stats(self, last_n: int = 200) -> dict:
        """Compute win_rate and avg_win_loss_ratio from last N closed trades.

        Used by adaptive Kelly position sizing.
        """
        async with aiosqlite.connect(str(self._db_path)) as db:
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

    async def get_equity_curve(self) -> list[dict]:
        """Return equity snapshots for charting."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT * FROM equity_snapshots ORDER BY id"
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    # ------------------------------------------------------------------
    # Equity snapshots
    # ------------------------------------------------------------------

    async def snapshot_equity(self, account: dict) -> None:
        """Record an equity snapshot from account data."""
        balance = account.get("balance", 0.0)
        equity = account.get("equity", 0.0)
        unrealized_pnl = account.get("unrealized_pnl", 0.0)
        drawdown_pct = account.get("drawdown_pct", 0.0)

        async with aiosqlite.connect(str(self._db_path)) as db:
            await db.execute(
                """INSERT INTO equity_snapshots
                   (timestamp, balance, equity, unrealized_pnl, drawdown_pct)
                   VALUES (?, ?, ?, ?, ?)""",
                (
                    datetime.now(timezone.utc).isoformat(),
                    balance, equity, unrealized_pnl, drawdown_pct,
                ),
            )
            await db.commit()

    async def prune_equity_snapshots(self, keep_full_hours: int = 24) -> int:
        """Thin old equity snapshots to one per hour.

        Keeps full 15s resolution for the last *keep_full_hours* hours.
        Older snapshots are thinned to one per hour.

        Returns number of rows deleted.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=keep_full_hours)).isoformat()
        async with aiosqlite.connect(str(self._db_path)) as db:
            result = await db.execute(
                """DELETE FROM equity_snapshots
                   WHERE timestamp < ?
                     AND id NOT IN (
                         SELECT MIN(id) FROM equity_snapshots
                         WHERE timestamp < ?
                         GROUP BY strftime('%Y-%m-%d %H', timestamp)
                     )""",
                (cutoff, cutoff),
            )
            deleted = result.rowcount
            await db.commit()
        if deleted:
            logger.info("equity_snapshots_pruned", deleted=deleted)
        return deleted

    # ------------------------------------------------------------------
    # PnL reconstruction (for restart resilience)
    # ------------------------------------------------------------------

    async def get_period_pnl(self, since_iso: str) -> float:
        """Sum closed-trade PnL since a given ISO timestamp.

        Used to reconstruct daily/weekly PnL after a restart.
        """
        async with aiosqlite.connect(str(self._db_path)) as db:
            cursor = await db.execute(
                """SELECT COALESCE(SUM(pnl), 0.0) as total
                   FROM trades
                   WHERE status = 'closed' AND pnl IS NOT NULL
                     AND closed_at >= ?""",
                (since_iso,),
            )
            row = await cursor.fetchone()
        return float(row[0]) if row else 0.0

    async def get_daily_summary_stats(self, date_str: str) -> dict:
        """Compute summary stats for trades closed on a given UTC date.

        Parameters
        ----------
        date_str : str
            Date in ``YYYY-MM-DD`` format.

        Returns
        -------
        dict
            Keys: date, total_trades, winning_trades, losing_trades,
            win_rate, pnl, pnl_pct, profit_factor.
        """
        async with aiosqlite.connect(str(self._db_path)) as db:
            cursor = await db.execute(
                """SELECT pnl, pnl_pct FROM trades
                   WHERE status = 'closed' AND pnl IS NOT NULL
                     AND date(closed_at) = ?""",
                (date_str,),
            )
            rows = await cursor.fetchall()

        if not rows:
            return {"date": date_str, "total_trades": 0, "pnl": 0.0}

        pnls = [float(r[0]) for r in rows]
        winners = [p for p in pnls if p > 0]
        losers = [p for p in pnls if p < 0]
        total_pnl = sum(pnls)

        pnl_pcts = [float(r[1]) for r in rows if r[1] is not None]
        total_pnl_pct = sum(pnl_pcts)

        pf = (sum(winners) / abs(sum(losers))) if losers else (float("inf") if winners else 0.0)

        return {
            "date": date_str,
            "total_trades": len(pnls),
            "winning_trades": len(winners),
            "losing_trades": len(losers),
            "win_rate": len(winners) / len(pnls) * 100 if pnls else 0,
            "pnl": total_pnl,
            "pnl_pct": total_pnl_pct,
            "profit_factor": pf,
        }

    # ------------------------------------------------------------------
    # Repair helpers
    # ------------------------------------------------------------------

    async def get_null_pnl_trades(self) -> list[dict]:
        """Return closed trades with NULL pnl (need repair)."""
        async with aiosqlite.connect(str(self._db_path)) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                """SELECT * FROM trades
                   WHERE status = 'closed' AND pnl IS NULL
                   ORDER BY id"""
            )
            rows = await cursor.fetchall()
        return [dict(row) for row in rows]

    async def repair_trade(
        self,
        trade_id: int,
        exit_price: float | None,
        pnl: float | None,
        pnl_pct: float | None,
        close_reason: str | None = None,
    ) -> None:
        """Repair a specific trade by ID."""
        async with aiosqlite.connect(str(self._db_path)) as db:
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
