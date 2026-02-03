"""Dashboard data endpoints — thin layer over TradingBot + TradeStore.

All heavy logic lives in standalone modules.
These routes read from bot state and the SQLite store.
"""

from __future__ import annotations

import math
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import structlog
from fastapi import APIRouter, HTTPException, Query, Request

logger = structlog.get_logger(__name__)

router = APIRouter()

TRADE_LOG_DB = Path("data/trade_history.db")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _require_broker(bot) -> None:
    if bot.broker is None:
        raise HTTPException(
            status_code=503,
            detail="Broker not initialised. Start trading first.",
        )


async def _query_db(query: str, params: tuple = ()) -> list[dict]:
    if not TRADE_LOG_DB.exists():
        return []
    import aiosqlite

    async with aiosqlite.connect(str(TRADE_LOG_DB)) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(query, params) as cursor:
            rows = await cursor.fetchall()
            return [dict(row) for row in rows]


def _compute_performance(pnls: list[float]) -> dict:
    if not pnls:
        return {"total_pnl": 0, "total_trades": 0, "win_rate": 0, "wins": 0, "losses": 0}
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    return {
        "total_pnl": round(sum(pnls), 2),
        "total_trades": len(pnls),
        "wins": len(wins),
        "losses": len(losses),
        "win_rate": round(len(wins) / len(pnls) * 100, 1),
        "avg_win": round(sum(wins) / len(wins), 2) if wins else 0,
        "avg_loss": round(sum(losses) / len(losses), 2) if losses else 0,
        "best_trade": round(max(pnls), 2),
        "worst_trade": round(min(pnls), 2),
    }


def _instrument_from_name(name: str) -> str:
    if not name:
        return name
    if "_" in name:
        return name
    if "/" in name:
        return name.replace("/", "_")
    if len(name) == 6 and name.isalpha():
        return f"{name[:3]}_{name[3:]}"
    return name


def _parse_pnl(raw: Any) -> float | None:
    if raw is None:
        return None
    s = str(raw)
    cleaned = ""
    for ch in s:
        if ch in "0123456789.-":
            cleaned += ch
    try:
        return float(cleaned) if cleaned and cleaned != "-" else None
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Broker trade cache
# ---------------------------------------------------------------------------

_trade_cache: dict[str, Any] = {"trades": None, "ts": 0.0, "ttl": 60}


async def _fetch_broker_trades(broker, days: int = 14) -> list[dict]:
    """Fetch trade history from Capital.com: transactions + activities."""
    import time as _time
    from datetime import datetime as _dt
    from datetime import timedelta as _td

    now = _time.monotonic()
    if _trade_cache["trades"] is not None and (now - _trade_cache["ts"]) < _trade_cache["ttl"]:
        return _trade_cache["trades"]

    fmt = "%Y-%m-%dT%H:%M:%S"
    _now = _dt.now(timezone.utc).replace(tzinfo=None)
    date_from = (_now - _td(days=days)).strftime(fmt)
    date_to = _now.strftime(fmt)

    transactions = await broker.get_transactions(tx_type="ALL", date_from=date_from, date_to=date_to)
    activities = await broker.get_activity(date_from=date_from, date_to=date_to)

    _CLOSE_SOURCES = {"SL", "TP", "STOP_LOSS", "TAKE_PROFIT", "CLOSE_OUT", "SYSTEM", "DEALER"}
    activity_by_deal: dict[str, dict] = {}
    for act in activities:
        deal_id = act.get("dealId", "")
        if not deal_id:
            continue
        source = act.get("source") or ""
        instrument = act.get("instrument", "")
        existing = activity_by_deal.get(deal_id)
        if not existing or (source.upper() in _CLOSE_SOURCES and existing["source"].upper() not in _CLOSE_SOURCES):
            activity_by_deal[deal_id] = {"source": source, "instrument": instrument}

    source_map = {
        "SL": "stop_loss", "STOP_LOSS": "stop_loss",
        "TP": "take_profit", "TAKE_PROFIT": "take_profit",
        "CLOSE_OUT": "margin_closeout", "DEALER": "margin_closeout",
        "SYSTEM": "system", "USER": "manual_close", "API": "manual_close",
        "WEB": "manual_close", "MOBILE": "manual_close",
    }

    trades: list[dict] = []
    for tx in transactions:
        tx_type = (tx.get("transactionType") or "").upper()
        if tx_type != "TRADE":
            continue
        note = (tx.get("note") or "").lower()
        if "opened" in note:
            continue

        instrument_raw = tx.get("instrumentName") or ""
        instrument = _instrument_from_name(instrument_raw)
        deal_id = tx.get("dealId") or tx.get("reference") or ""
        act_info = activity_by_deal.get(deal_id, {})
        if act_info.get("instrument"):
            instrument = act_info["instrument"]
        if not instrument:
            continue

        pnl = _parse_pnl(tx.get("size"))
        closed_at = tx.get("dateUtc") or tx.get("date") or ""
        source = act_info.get("source", "")
        metadata = source_map.get(source.upper(), source.lower() if source else "")

        trades.append({
            "instrument": instrument,
            "pnl": round(pnl, 2) if pnl is not None else None,
            "status": "closed",
            "closed_at": closed_at,
            "metadata": metadata,
        })

    trades.sort(key=lambda t: t.get("closed_at", ""), reverse=True)
    _trade_cache["trades"] = trades
    _trade_cache["ts"] = now
    return trades


# ---------------------------------------------------------------------------
# Endpoints: Account & Positions
# ---------------------------------------------------------------------------

@router.get("/account")
async def get_account(request: Request):
    bot = request.app.state.bot
    _require_broker(bot)
    try:
        return await bot.broker.get_account()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Broker error: {e}")


@router.get("/positions")
async def get_positions(request: Request):
    bot = request.app.state.bot
    _require_broker(bot)
    try:
        positions = await bot.broker.get_positions()
        return {"positions": positions, "count": len(positions)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Broker error: {e}")


@router.get("/orders")
async def get_orders(request: Request):
    bot = request.app.state.bot
    _require_broker(bot)
    try:
        orders = await bot.broker.get_open_orders()
        return {"orders": orders, "count": len(orders)}
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Broker error: {e}")


# ---------------------------------------------------------------------------
# Endpoints: Equity & Drawdown
# ---------------------------------------------------------------------------

@router.get("/equity-curve")
async def get_equity_curve(request: Request):
    rows = await _query_db(
        "SELECT timestamp, equity FROM equity_snapshots ORDER BY timestamp ASC"
    )
    if not rows:
        rows = await _query_db(
            "SELECT closed_at AS timestamp, pnl AS equity FROM trades "
            "WHERE closed_at IS NOT NULL ORDER BY closed_at ASC"
        )
    return {"equity_curve": rows}


@router.get("/drawdown")
async def get_drawdown(request: Request):
    rows = await _query_db(
        "SELECT timestamp, equity FROM equity_snapshots ORDER BY timestamp ASC"
    )
    if not rows:
        return {"drawdown": []}

    series: list[dict] = []
    peak = 0.0
    for row in rows:
        equity = float(row.get("equity", 0))
        if equity > peak:
            peak = equity
        dd_pct = ((peak - equity) / peak * 100.0) if peak > 0 else 0.0
        series.append({"timestamp": row["timestamp"], "drawdown_pct": round(dd_pct, 4)})
    return {"drawdown": series}


# ---------------------------------------------------------------------------
# Endpoints: Trades
# ---------------------------------------------------------------------------

@router.get("/trades")
async def get_trades(
    request: Request,
    limit: int = Query(default=50, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
):
    """Recent trades + performance.  Capital.com API first, SQLite fallback."""
    bot = request.app.state.bot

    if bot.broker:
        try:
            all_trades = await _fetch_broker_trades(bot.broker)
            total = len(all_trades)
            page = all_trades[offset : offset + limit]
            all_pnls = [t["pnl"] for t in all_trades if t.get("pnl") is not None]
            return {
                "trades": page, "total": total, "limit": limit,
                "offset": offset, "source": "broker",
                "performance": _compute_performance(all_pnls),
            }
        except Exception as e:
            logger.warning("broker_trades_fetch_failed", error=str(e))

    rows = await _query_db(
        "SELECT * FROM trades ORDER BY closed_at DESC, timestamp DESC LIMIT ? OFFSET ?",
        (limit, offset),
    )
    count_rows = await _query_db("SELECT COUNT(*) AS total FROM trades")
    total = count_rows[0]["total"] if count_rows else 0
    all_rows = await _query_db("SELECT pnl FROM trades WHERE pnl IS NOT NULL AND status='closed'")
    all_pnls = [float(r["pnl"]) for r in all_rows if r.get("pnl") is not None]

    return {
        "trades": rows, "total": total, "limit": limit,
        "offset": offset, "source": "db",
        "performance": _compute_performance(all_pnls),
    }


# ---------------------------------------------------------------------------
# Endpoints: Performance Metrics
# ---------------------------------------------------------------------------

@router.get("/performance")
async def get_performance(request: Request):
    rows = await _query_db(
        "SELECT pnl, closed_at FROM trades "
        "WHERE closed_at IS NOT NULL AND pnl IS NOT NULL ORDER BY closed_at ASC"
    )
    if not rows:
        return {
            "total_trades": 0, "sharpe_ratio": None, "sortino_ratio": None,
            "max_drawdown_pct": None, "win_rate": None, "profit_factor": None,
            "expectancy": None, "total_pnl": 0.0,
        }

    pnls = [float(r["pnl"]) for r in rows]
    total_trades = len(pnls)
    wins = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p < 0]
    total_pnl = sum(pnls)
    win_rate = (len(wins) / total_trades) if total_trades > 0 else 0.0

    gross_profit = sum(wins) if wins else 0.0
    gross_loss = abs(sum(losses)) if losses else 0.0
    profit_factor = (gross_profit / gross_loss) if gross_loss > 0 else (float("inf") if gross_profit > 0 else 0.0)

    avg_win = (sum(wins) / len(wins)) if wins else 0.0
    avg_loss = (abs(sum(losses)) / len(losses)) if losses else 0.0
    expectancy = (win_rate * avg_win) - ((1 - win_rate) * avg_loss)

    mean_return = total_pnl / total_trades
    if total_trades > 1:
        variance = sum((p - mean_return) ** 2 for p in pnls) / (total_trades - 1)
        std_dev = math.sqrt(variance)
        sharpe_ratio = (mean_return / std_dev * math.sqrt(252)) if std_dev > 0 else 0.0
    else:
        sharpe_ratio = 0.0

    downside = [p for p in pnls if p < 0]
    if downside and total_trades > 1:
        downside_var = sum(p ** 2 for p in downside) / (total_trades - 1)
        sortino_ratio = (mean_return / math.sqrt(downside_var) * math.sqrt(252)) if downside_var > 0 else 0.0
    else:
        sortino_ratio = 0.0

    cumulative = peak = max_dd = 0.0
    for p in pnls:
        cumulative += p
        if cumulative > peak:
            peak = cumulative
        dd = peak - cumulative
        if dd > max_dd:
            max_dd = dd
    max_dd_pct = (max_dd / peak * 100.0) if peak > 0 else 0.0

    return {
        "total_trades": total_trades,
        "win_count": len(wins), "loss_count": len(losses),
        "total_pnl": round(total_pnl, 2),
        "win_rate": round(win_rate, 4),
        "profit_factor": round(profit_factor, 4) if profit_factor != float("inf") else None,
        "expectancy": round(expectancy, 4),
        "sharpe_ratio": round(sharpe_ratio, 4),
        "sortino_ratio": round(sortino_ratio, 4),
        "max_drawdown_pct": round(max_dd_pct, 4),
        "avg_win": round(avg_win, 2), "avg_loss": round(avg_loss, 2),
    }


# ---------------------------------------------------------------------------
# Endpoints: Regime
# ---------------------------------------------------------------------------

@router.get("/regime")
async def get_regime(request: Request):
    """Current market regime for all pairs.  Derived from ADX (no ML)."""
    bot = request.app.state.bot

    pairs_data: dict[str, dict] = {}
    for pair, diag in bot.diagnostics.items():
        pairs_data[pair] = {
            "regime": diag.regime or "unknown",
            "adx": round(diag.adx, 1) if diag.adx is not None else None,
            "scale": round(diag.regime_scale, 2),
        }

    if not pairs_data:
        return {"regime": "unknown", "pairs": {}, "method": "adx"}

    first_pair = next(iter(pairs_data))
    return {
        "regime": pairs_data[first_pair]["regime"],
        "pairs": pairs_data,
        "method": "adx",
    }


# ---------------------------------------------------------------------------
# Endpoints: Signal Matrix
# ---------------------------------------------------------------------------

@router.get("/signals")
async def get_signal_matrix(request: Request):
    """Per-pair signal diagnostics.  Reads from bot.diagnostics — no re-computation."""
    bot = request.app.state.bot

    # Live data for summary
    positions: list[dict] = []
    account: dict = {}
    if bot.broker and bot.is_running:
        try:
            positions = await bot.broker.get_positions()
            account = await bot.broker.get_account()
        except Exception:
            pass

    risk_cfg = bot.config.get("risk", {})
    recovery_phase = bot.recovery.phase if bot.recovery else 0
    dd_pct = bot.recovery.current_drawdown_pct if bot.recovery else 0.0
    balance = account.get("balance", 0.0)
    equity = account.get("equity", 0.0)
    margin_used = account.get("margin_used", 0.0)
    margin_pct = round((margin_used / balance * 100), 1) if balance > 0 else 0.0

    summary = {
        "open_positions": len(positions),
        "max_positions": risk_cfg.get("max_open_positions", 5),
        "recovery_phase": recovery_phase,
        "is_trading": bot.is_running,
        "drawdown_pct": round(dd_pct, 2),
        "margin_pct": margin_pct,
        "balance": round(balance, 2),
        "equity": round(equity, 2),
    }

    # Strategy thresholds for frontend display
    strat_cfg = bot.config.get("strategy", {}).get("trend_following", {})
    adx_threshold = strat_cfg.get("adx_threshold", 20)
    hurst_threshold = strat_cfg.get("hurst_threshold", 0.0)
    min_confidence = strat_cfg.get("min_confidence", 0.50)
    ms_cfg = strat_cfg.get("multi_speed", {})
    signal_threshold = ms_cfg.get("min_signal_strength", 0.3)

    # Instruments with open positions
    open_instruments = {p.get("instrument") for p in positions}

    # Convert PairDiagnostic to enriched PairRow for frontend.
    # All pass/fail values now come directly from diagnostics —
    # no re-computation needed.
    pairs_data: dict[str, dict] = {}
    for pair, diag in bot.diagnostics.items():
        adx = round(diag.adx, 1) if diag.adx is not None else None
        rsi = round(diag.rsi, 1) if diag.rsi is not None else None
        sig = round(diag.signal_strength, 2) if diag.signal_strength is not None else None
        conf_raw = diag.confidence
        if conf_raw is not None:
            conf_pct = round(conf_raw * 100, 1) if conf_raw <= 1.0 else round(conf_raw, 1)
        else:
            conf_pct = None
        regime_conf_raw = diag.regime_confidence or 0.0
        regime_conf_pct = round(regime_conf_raw * 100, 1) if regime_conf_raw <= 1.0 else round(regime_conf_raw, 1)

        # Override stale POSITION_OPEN verdict when broker confirms
        # the position is no longer open (e.g. manually closed between
        # trading loop iterations, or bot was restarted).
        has_pos = pair in open_instruments
        verdict = diag.verdict or "NO_DATA"
        if verdict == "POSITION_OPEN" and not has_pos:
            verdict = "NO_SIGNAL"

        pairs_data[pair] = {
            "regime": diag.regime,
            "regime_conf": regime_conf_pct,
            "regime_pass": diag.regime_pass,
            "regime_scale": round(diag.regime_scale, 2),
            "adx": adx,
            "adx_threshold": adx_threshold,
            "adx_pass": diag.adx_pass,
            "hurst": round(diag.hurst, 2) if diag.hurst is not None else None,
            "hurst_threshold": hurst_threshold,
            "hurst_pass": diag.hurst_pass,
            "htf_direction": diag.htf_direction,
            "htf_pass": diag.htf_pass,
            "direction": diag.signal_direction,
            "signal_strength": sig,
            "signal_threshold": signal_threshold,
            "signal_pass": diag.signal_pass,
            "confidence": conf_pct,
            "conf_threshold": round(min_confidence * 100, 1),
            "conf_pass": diag.confidence_pass,
            "rsi": rsi,
            "rsi_score": diag.rsi_score,
            "rsi_pass": diag.rsi_pass,
            "has_position": has_pos,
            "news_blackout": diag.news_blackout,
            "news_boost": diag.news_boost,
            "news_events": diag.news_events,
            "verdict": verdict,
            "block_reason": diag.detail,
            "loop_ts": diag.timestamp,
        }

    return {"pairs": pairs_data, "summary": summary}


# ---------------------------------------------------------------------------
# Endpoints: ML Status
# ---------------------------------------------------------------------------

@router.get("/ml-status")
async def get_ml_status(request: Request):
    """ML status — regime detection now uses ADX, no ML models."""
    return {
        "regime_models": {},
        "method": "adx",
        "message": "Regime detection uses ADX-based scaling (ML removed)",
    }


# ---------------------------------------------------------------------------
# Endpoints: Recovery Status
# ---------------------------------------------------------------------------

@router.get("/recovery")
async def get_recovery_status(request: Request):
    """Drawdown recovery phase — reads from bot.recovery module."""
    bot = request.app.state.bot
    recovery = bot.recovery

    if recovery is None:
        return {"phase": 0, "phase_label": "Nicht initialisiert", "enabled": False}

    phase = recovery.phase
    phase_labels = {
        0: "Normal", 1: "Cooloff (Pause)",
        2: "Reduzierte Groesse", 3: "HARD STOP",
    }

    result: dict[str, Any] = {
        "phase": phase,
        "phase_label": phase_labels.get(phase, "Unknown"),
        "enabled": recovery.enabled,
        "current_drawdown_pct": round(recovery.current_drawdown_pct, 2),
        "max_drawdown_pct": recovery.max_drawdown_pct,
        "hard_stop_drawdown_pct": recovery.hard_stop_drawdown_pct,
        "position_scale": recovery.position_scale,
    }

    if phase == 1:
        from datetime import datetime, timezone
        pause_start = recovery._pause_start_time
        if pause_start:
            elapsed = (datetime.now(timezone.utc) - pause_start).total_seconds() / 3600
            result["cooloff_elapsed_hours"] = round(elapsed, 1)
            result["cooloff_remaining_hours"] = round(max(0, recovery.cooloff_hours - elapsed), 1)
    elif phase == 2:
        profitable = recovery._recovery_profitable_trades
        target = recovery.recovery_trade_count
        result["recovery_profitable_trades"] = profitable
        result["recovery_target_trades"] = target
        result["recovery_progress_pct"] = round(profitable / target * 100 if target > 0 else 0, 1)

    return result


# ---------------------------------------------------------------------------
# Endpoints: Economic Calendar
# ---------------------------------------------------------------------------

@router.get("/calendar")
async def get_calendar(request: Request):
    """Upcoming economic calendar events and news filter status."""
    bot = request.app.state.bot
    calendar = getattr(bot, "calendar", None)

    if calendar is None or not calendar.enabled:
        return {"events": [], "status": {"enabled": False}}

    upcoming = calendar.get_upcoming_events(hours_ahead=48.0)

    events_data = []
    for event in upcoming:
        # Annotate which active pairs this event affects
        affected_pairs = []
        for pair in bot.active_pairs:
            currencies = calendar._get_currencies(pair)
            if event.country in currencies:
                affected_pairs.append(pair)

        events_data.append({
            "title": event.title,
            "country": event.country,
            "datetime_utc": event.datetime_utc.isoformat(),
            "impact": event.impact,
            "forecast": event.forecast,
            "previous": event.previous,
            "affected_pairs": affected_pairs,
        })

    return {
        "events": events_data,
        "status": calendar.get_status(),
    }
