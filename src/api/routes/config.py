"""Configuration endpoints — credentials, pairs, risk parameters, full config."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
import structlog
import os
import yaml
from dotenv import load_dotenv

logger = structlog.get_logger(__name__)

router = APIRouter()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ENV_FILE_PATH = Path(".env")
CONFIG_FILE_PATH = Path("config/default.yaml")

# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------

class CapitalComCredentials(BaseModel):
    api_key: str
    identifier: str
    password: str
    environment: str = "demo"  # "demo" | "live"

class TelegramCredentials(BaseModel):
    bot_token: str
    chat_id: str

class PairsUpdate(BaseModel):
    pairs: list[str]

class PairToggle(BaseModel):
    pair: str
    enabled: bool

class RiskUpdate(BaseModel):
    """Accepts any risk configuration keys."""
    max_position_size_pct: float | None = None
    max_open_positions: int | None = None
    max_correlated_positions: int | None = None
    correlation_threshold: float | None = None
    default_leverage: float | None = None
    position_sizing_method: str | None = None
    fixed_position_pct: float | None = None
    max_spread_pips: float | None = None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_env_file() -> dict[str, str]:
    """Read the .env file and return key-value pairs."""
    env_vars: dict[str, str] = {}
    if ENV_FILE_PATH.exists():
        with open(ENV_FILE_PATH) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "=" in line:
                    key, _, value = line.partition("=")
                    # Strip optional quotes
                    value = value.strip().strip("'\"")
                    env_vars[key.strip()] = value
    return env_vars


def _write_env_file(env_vars: dict[str, str]) -> None:
    """Write key-value pairs to the .env file, preserving comments."""
    lines: list[str] = []
    existing_keys: set[str] = set()

    # Read existing file to preserve comments and ordering
    if ENV_FILE_PATH.exists():
        with open(ENV_FILE_PATH) as f:
            for line in f:
                stripped = line.strip()
                if not stripped or stripped.startswith("#"):
                    lines.append(line.rstrip("\n"))
                    continue
                if "=" in stripped:
                    key = stripped.split("=", 1)[0].strip()
                    if key in env_vars:
                        lines.append(f"{key}={env_vars[key]}")
                        existing_keys.add(key)
                    else:
                        lines.append(line.rstrip("\n"))
                else:
                    lines.append(line.rstrip("\n"))

    # Append new keys that weren't in the existing file
    for key, value in env_vars.items():
        if key not in existing_keys:
            lines.append(f"{key}={value}")

    with open(ENV_FILE_PATH, "w") as f:
        f.write("\n".join(lines) + "\n")


def _save_config(config: dict) -> None:
    """Save the configuration dictionary back to the YAML file."""
    with open(CONFIG_FILE_PATH, "w") as f:
        yaml.dump(config, f, default_flow_style=False, sort_keys=False)


# ---------------------------------------------------------------------------
# Endpoints: Credentials
# ---------------------------------------------------------------------------

@router.get("/credentials")
async def get_credentials_status():
    """
    Return masked credentials status.
    Never exposes raw API keys — only reports whether they are set.
    """
    return {
        "has_capitalcom_key": bool(os.getenv("CAPITALCOM_API_KEY")),
        "has_capitalcom_identifier": bool(os.getenv("CAPITALCOM_IDENTIFIER")),
        "has_capitalcom_password": bool(os.getenv("CAPITALCOM_PASSWORD")),
        "capitalcom_env": os.getenv("CAPITALCOM_ENVIRONMENT", "demo"),
        "has_telegram_token": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "has_telegram_chat_id": bool(os.getenv("TELEGRAM_CHAT_ID")),
    }


@router.post("/credentials/capitalcom")
async def set_capitalcom_credentials(body: CapitalComCredentials):
    """
    Set Capital.com API credentials.
    Writes to the .env file and reloads into the process environment.
    """
    if body.environment not in ("demo", "live"):
        raise HTTPException(
            status_code=400,
            detail="Environment must be 'demo' or 'live'",
        )

    env_vars = _read_env_file()
    env_vars["CAPITALCOM_API_KEY"] = body.api_key
    env_vars["CAPITALCOM_IDENTIFIER"] = body.identifier
    env_vars["CAPITALCOM_PASSWORD"] = body.password
    env_vars["CAPITALCOM_ENVIRONMENT"] = body.environment

    _write_env_file(env_vars)

    # Reload into process
    os.environ["CAPITALCOM_API_KEY"] = body.api_key
    os.environ["CAPITALCOM_IDENTIFIER"] = body.identifier
    os.environ["CAPITALCOM_PASSWORD"] = body.password
    os.environ["CAPITALCOM_ENVIRONMENT"] = body.environment

    logger.info("capitalcom_credentials_updated", environment=body.environment)
    return {"status": "updated", "environment": body.environment}


@router.post("/credentials/telegram")
async def set_telegram_credentials(body: TelegramCredentials):
    """
    Set Telegram bot credentials.
    Writes to the .env file and reloads into the process environment.
    """
    env_vars = _read_env_file()
    env_vars["TELEGRAM_BOT_TOKEN"] = body.bot_token
    env_vars["TELEGRAM_CHAT_ID"] = body.chat_id

    _write_env_file(env_vars)

    # Reload into process
    os.environ["TELEGRAM_BOT_TOKEN"] = body.bot_token
    os.environ["TELEGRAM_CHAT_ID"] = body.chat_id

    logger.info("telegram_credentials_updated")
    return {"status": "updated"}


# ---------------------------------------------------------------------------
# Endpoints: Trading pairs
# ---------------------------------------------------------------------------

@router.get("/pairs")
async def get_pairs(request: Request):
    """Return all trading pairs with their enabled/disabled status."""
    bot = request.app.state.bot
    all_pairs = bot.all_pairs  # dict {pair: bool}

    return {
        "pairs": [
            {"pair": pair, "enabled": enabled}
            for pair, enabled in all_pairs.items()
        ],
        "enabled_pairs": [p for p, on in all_pairs.items() if on],
    }


@router.post("/pairs")
async def update_pairs(request: Request, body: PairsUpdate):
    """Update the list of enabled trading pairs (replaces all)."""
    state = request.app.state

    if state.bot.is_running:
        raise HTTPException(
            status_code=400,
            detail="Cannot update pairs while trading is active. Stop trading first.",
        )

    if not body.pairs:
        raise HTTPException(status_code=400, detail="At least one pair is required")

    # Build new dict: requested pairs enabled, rest disabled
    new_pairs = {p: (p in body.pairs) for p in state.bot.all_pairs}
    # Add any new pairs not in current universe
    for p in body.pairs:
        if p not in new_pairs:
            new_pairs[p] = True

    # Update in-memory
    if "trading" not in state.config:
        state.config["trading"] = {}
    state.config["trading"]["pairs"] = new_pairs
    state.bot.all_pairs = new_pairs
    state.bot.active_pairs = [p for p, on in new_pairs.items() if on]

    # Persist to YAML
    _save_config(state.config)

    logger.info("pairs_updated", pairs=state.bot.active_pairs)
    return {"status": "updated", "pairs": state.bot.active_pairs}


@router.patch("/pairs")
async def toggle_pair(request: Request, body: PairToggle):
    """Toggle a single pair on or off."""
    state = request.app.state

    if state.bot.is_running:
        raise HTTPException(
            status_code=400,
            detail="Cannot toggle pairs while trading is active. Stop trading first.",
        )

    if body.pair not in state.bot.all_pairs:
        raise HTTPException(status_code=404, detail=f"Unknown pair: {body.pair}")

    # Check at least one pair remains enabled
    if not body.enabled:
        active_after = [p for p, on in state.bot.all_pairs.items() if on and p != body.pair]
        if not active_after:
            raise HTTPException(status_code=400, detail="At least one pair must remain enabled")

    # Update in-memory
    state.bot.all_pairs[body.pair] = body.enabled
    state.bot.active_pairs = [p for p, on in state.bot.all_pairs.items() if on]

    # Update config and persist
    if "trading" not in state.config:
        state.config["trading"] = {}
    state.config["trading"]["pairs"] = dict(state.bot.all_pairs)
    _save_config(state.config)

    logger.info("pair_toggled", pair=body.pair, enabled=body.enabled)
    return {
        "status": "updated",
        "pair": body.pair,
        "enabled": body.enabled,
        "active_pairs": state.bot.active_pairs,
    }


# ---------------------------------------------------------------------------
# Endpoints: Risk configuration
# ---------------------------------------------------------------------------

@router.get("/risk")
async def get_risk(request: Request):
    """Return the current risk management parameters."""
    return request.app.state.config.get("risk", {})


@router.post("/risk")
async def update_risk(request: Request, body: RiskUpdate):
    """Update risk management parameters."""
    state = request.app.state

    if "risk" not in state.config:
        state.config["risk"] = {}

    # Only update fields that were explicitly provided (not None)
    updates = body.model_dump(exclude_none=True)

    if not updates:
        raise HTTPException(status_code=400, detail="No risk parameters provided")

    for key, value in updates.items():
        state.config["risk"][key] = value

    # Persist to YAML
    _save_config(state.config)

    # If risk modules are active, update their parameters
    bot = state.bot
    for module in [bot.risk_checks, bot.sizer, bot.recovery, bot.stops]:
        if module:
            for key, value in updates.items():
                if hasattr(module, key):
                    setattr(module, key, value)

    logger.info("risk_config_updated", updates=updates)
    return {"status": "updated", "risk": state.config["risk"]}


# ---------------------------------------------------------------------------
# Endpoints: Full config
# ---------------------------------------------------------------------------

@router.get("/")
async def get_config(request: Request):
    """Return the full current configuration (excluding secrets)."""
    cfg = request.app.state.config.copy()

    # Ensure no credentials leak into the config response
    # (credentials live in .env, not in the YAML config, but be safe)
    if "broker" in cfg:
        cfg["broker"] = {
            k: v for k, v in cfg["broker"].items()
            if k not in ("api_key", "secret", "token")
        }

    return cfg
