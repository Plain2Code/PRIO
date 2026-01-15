"""Centralized configuration manager.

Single source of truth for all configuration. Supports:
- Loading from YAML
- Dotted-path access: config.get("risk.max_drawdown_pct")
- Runtime updates with optional persistence
- Section access: config.get_section("risk")
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml


class ConfigManager:
    """Singleton configuration manager with persistence."""

    _instance: ConfigManager | None = None

    def __init__(self, data: dict, path: str | None = None) -> None:
        self._data = data
        self._path = path

    @classmethod
    def from_yaml(cls, path: str = "config/default.yaml") -> ConfigManager:
        """Load configuration from a YAML file."""
        config_path = Path(path)
        with open(config_path) as f:
            data = yaml.safe_load(f) or {}
        instance = cls(data, path=str(config_path))
        cls._instance = instance
        return instance

    @classmethod
    def from_dict(cls, data: dict) -> ConfigManager:
        """Create from a dict (useful for tests)."""
        instance = cls(data, path=None)
        cls._instance = instance
        return instance

    @classmethod
    def instance(cls) -> ConfigManager:
        """Get the current singleton instance."""
        if cls._instance is None:
            raise RuntimeError("ConfigManager not initialized. Call from_yaml() or from_dict() first.")
        return cls._instance

    @classmethod
    def reset(cls) -> None:
        """Reset singleton (for tests)."""
        cls._instance = None

    def get(self, dotted_key: str, default: Any = None) -> Any:
        """Get a value by dotted path.

        Example: config.get("risk.max_drawdown_pct") -> 10.0
        """
        keys = dotted_key.split(".")
        current = self._data
        for key in keys:
            if isinstance(current, dict) and key in current:
                current = current[key]
            else:
                return default
        return current

    def set(self, dotted_key: str, value: Any, persist: bool = True) -> None:
        """Set a value by dotted path.

        If persist=True and a file path is set, writes back to YAML.
        """
        keys = dotted_key.split(".")
        current = self._data
        for key in keys[:-1]:
            if key not in current or not isinstance(current[key], dict):
                current[key] = {}
            current = current[key]
        current[keys[-1]] = value

        if persist and self._path:
            self._save()

    def get_section(self, section: str) -> dict:
        """Return an entire config section as a dict."""
        return dict(self._data.get(section, {}))

    @property
    def raw(self) -> dict:
        """Full config dict (read-only copy)."""
        return dict(self._data)

    def _save(self) -> None:
        """Write current config back to YAML file."""
        if not self._path:
            return
        with open(self._path, "w") as f:
            yaml.dump(self._data, f, default_flow_style=False, allow_unicode=True, sort_keys=False)
