FROM python:3.12-slim AS base

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY pyproject.toml ./
RUN pip install --no-cache-dir -e ".[dev]" 2>/dev/null || pip install --no-cache-dir .

# Copy application code
COPY config/ config/
COPY src/ src/

# Create required directories
RUN mkdir -p logs data

# ---------- Trader target (headless, no dashboard) ----------
FROM base AS trader
CMD ["python", "-m", "src.main"]

# ---------- Build React frontend ----------
FROM node:20-slim AS frontend
WORKDIR /app/dashboard
COPY dashboard/package.json dashboard/package-lock.json ./
RUN npm ci
COPY dashboard/ ./
RUN npm run build

# ---------- Production target (API + Bot + Dashboard) ----------
FROM base AS production
COPY --from=frontend /app/dashboard/dist /app/dashboard/dist
ENV STATIC_DIR=/app/dashboard/dist
EXPOSE 8000
CMD ["uvicorn", "src.api.app:app", "--host", "0.0.0.0", "--port", "8000"]
