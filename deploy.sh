#!/bin/bash
# ─────────────────────────────────────────────────────────
# Prio Trading Bot — Production Deployment Script
# For Ubuntu VPS (Oracle Cloud, Hetzner, etc.)
#
# Usage:
#   1. SSH into your server
#   2. Copy/clone this repo
#   3. Run: chmod +x deploy.sh && ./deploy.sh
# ─────────────────────────────────────────────────────────

set -e

echo "=== Prio Trading Bot — Deployment ==="
echo ""

# ── 1. Install Docker if not present ──
if ! command -v docker &> /dev/null; then
    echo "[1/5] Installing Docker..."
    curl -fsSL https://get.docker.com | sh
    sudo usermod -aG docker "$USER"
    echo "Docker installed. You may need to log out and back in for group changes."
else
    echo "[1/5] Docker already installed."
fi

# ── 2. Install Docker Compose plugin if not present ──
if ! docker compose version &> /dev/null; then
    echo "[2/5] Installing Docker Compose plugin..."
    sudo apt-get update && sudo apt-get install -y docker-compose-plugin
else
    echo "[2/5] Docker Compose already installed."
fi

# ── 3. Check .env file ──
if [ ! -f .env ]; then
    echo ""
    echo "[3/5] ERROR: .env file not found!"
    echo "  Copy .env.example to .env and fill in your credentials:"
    echo "    cp .env.example .env"
    echo "    nano .env"
    echo ""
    exit 1
else
    echo "[3/5] .env file found."
fi

# ── 4. Create data directories ──
echo "[4/5] Creating data directories..."
mkdir -p config logs data

# ── 5. Build and start ──
echo "[5/5] Building and starting Prio Bot..."
docker compose -f docker-compose.prod.yml up -d --build

echo ""
echo "=== Deployment complete! ==="
echo ""

# Get server IP
SERVER_IP=$(curl -s ifconfig.me 2>/dev/null || echo "your-server-ip")

echo "Dashboard: http://${SERVER_IP}:8000"
echo ""
echo "Useful commands:"
echo "  docker compose -f docker-compose.prod.yml logs -f     # View logs"
echo "  docker compose -f docker-compose.prod.yml restart     # Restart"
echo "  docker compose -f docker-compose.prod.yml down        # Stop"
echo ""
