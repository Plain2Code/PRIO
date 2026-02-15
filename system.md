# Trading Bot — Komponentenübersicht

---

## Strategie

- Multi-Speed Blending (Fast/Medium/Slow EMA)
- HTF Confirmation (H4)
- ADX Gate
- Hurst Filter
- Session Filter
- Candle Confirmation
- Confidence Score (Signal Strength + Hurst + RSI)
- Signal-Reversal Exit
- Trailing Stop
- Break-Even

---

## Risk Management

- Pre-Trade Gates: Spread, Margin, Cooldown (exponentiell), Loss Limits, Correlation Matrix, Max Positions
- Sizing: Regime Scaling, Confidence Scaling, Volatility Targeting, News Boost (deaktiviert)
- Drawdown Recovery (4 Phasen)

---

## Execution

- Order Engine (Queue, Retry, Latency)
- SL/TP Verification + Repair
- Broker-seitige Exits (SL/TP-Hit, Margin Call)
- Reconciliation (Broker ↔ DB)
- Kill Switch

---

## Daten

- Candle Fetch (Multi-Timeframe, parallel)
- Feature Computation (EMA, RSI, MACD, BB, ATR, ADX, Hurst, KAMA, Realized Vol)
- Correlation Prices
- Economic Calendar (Forex Factory, Blackout Filter, Post-Event Boost, Upcoming Alerts, Currency Mapping)

---

## Infrastruktur

- Broker: Session Management, Rate Limiting, Orders (Market/Limit/Stop), Modify Order, Close Position, Position Query, Spread Query, Deal Confirmation
- Persistenz: SQLite (Trades, Equity Snapshots, Daily Summary), Trade Stats
- Events: EventBus, Alle Event Types, Subscriber-Kette
- Config: YAML, Environment Variables, Docker

---

## Interface

- API: Trading, Config, Dashboard, System Routes, WebSocket
- Dashboard: Komponenten, Store, WebSocket Hook, Pages
- Telegram: Trade Alerts, Drawdown, Kill Switch, News, Daily Summary, Errors

---

## Orchestrator

- 4 Loops (Trading, Equity, Position, Calendar)
- Diagnostics
