"""WebSocket hub for real-time frontend updates.

STANDALONE: Knows nothing about trading logic.  Receives events via
callback methods and broadcasts them to connected WebSocket clients.

The EventBus wiring happens in app.py lifespan.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import structlog
from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = structlog.get_logger(__name__)
ws_router = APIRouter()


class WebSocketHub:
    """Manages WebSocket clients and broadcasts event-driven messages."""

    def __init__(self) -> None:
        self.clients: set[WebSocket] = set()

    async def _broadcast(self, message: dict) -> None:
        """Send a JSON message to all connected clients."""
        disconnected: set[WebSocket] = set()
        for client in self.clients:
            try:
                await client.send_json(message)
            except Exception:
                disconnected.add(client)
        self.clients -= disconnected

    # ── Event handlers (subscribed via EventBus) ──────

    async def on_trade(self, **data: Any) -> None:
        await self._broadcast({
            "type": "trade",
            "data": {**data, "timestamp": datetime.now(timezone.utc).isoformat()},
        })

    async def on_status(self, **data: Any) -> None:
        await self._broadcast({
            "type": "status",
            "data": {**data, "timestamp": datetime.now(timezone.utc).isoformat()},
        })

    async def on_alert(self, **data: Any) -> None:
        await self._broadcast({
            "type": "alert",
            "data": {**data, "timestamp": datetime.now(timezone.utc).isoformat()},
        })

    async def on_equity(self, **data: Any) -> None:
        await self._broadcast({
            "type": "equity",
            "data": {**data, "timestamp": datetime.now(timezone.utc).isoformat()},
        })


# Keep the broadcast helper for backward compatibility
async def broadcast(clients: set, message: dict) -> None:
    disconnected: set = set()
    for client in clients:
        try:
            await client.send_json(message)
        except Exception:
            disconnected.add(client)
    clients -= disconnected


@ws_router.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    app = websocket.app

    # Register with the hub
    ws_hub: WebSocketHub | None = getattr(app.state, "ws_hub", None)
    if ws_hub:
        ws_hub.clients.add(websocket)
    else:
        app.state.ws_clients.add(websocket)

    logger.info("ws_client_connected", total=len(ws_hub.clients if ws_hub else app.state.ws_clients))

    try:
        while True:
            data = await websocket.receive_text()
            msg = json.loads(data)
            if msg.get("type") == "ping":
                await websocket.send_json({"type": "pong"})
    except WebSocketDisconnect:
        if ws_hub:
            ws_hub.clients.discard(websocket)
        else:
            app.state.ws_clients.discard(websocket)
        logger.info("ws_client_disconnected")
