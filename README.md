# OpenClaw Quant v2

OpenClaw Quant is an event-driven Binance USD-M Futures research and paper-trading runtime. It keeps a strict PAPER-only execution boundary: testnet, small-live, and production order submission are intentionally disabled.

The implementation follows the v2 requirements used by the OpenClaw deployment. Credentials are never stored in this repository; notification secrets are read from environment variables.

## Runtime path

```text
Binance WebSocket -> RealtimeMarketData -> EventDetector -> Strategy -> Candidate
-> OpenClaw advisory analysis -> RiskEngine -> Trade Plan -> PaperExecutor
-> PositionManager -> Journal / Review
```

Ticker updates maintain the local cache and positions. They do not run the full multi-timeframe strategy. Strategy evaluation is triggered by closed klines, breakouts, breakdowns, volume spikes, rank changes, regime changes or explicit diagnostics.

## Implemented phases

- Phase 1: versioned JSON configuration, JSON structured logs, SQLite audit database, Docker/Compose definitions. Redis/PostgreSQL definitions are included; this Windows host currently has no Docker runtime, so the active local deployment uses SQLite and in-memory realtime cache.
- Phase 2: exchange-driven Symbol Registry, REST bootstrap/recovery boundary, WebSocket ticker/trade/kline/mark/book streams and reconnect protection.
- Phase 3: CUSTOM/GAINERS/LOSERS pool, priority, configurable refresh and EventBus.
- Phase 4: EMA/SMA/RSI/MACD/ATR/VWAP/ADX/Bollinger/OBV/pivot/swing/support/resistance and regime events.
- Phase 5: default/custom strategy, safe condition tree, immutable versions and symbol/pool bindings.
- Phase 6: Candidate model, deterministic ID, persisted deduplication and 15-minute AI cooldown.
- Phase 7: StopLossManager modes, hard maximum loss, AUTO_RESIZE/REJECT, RiskEngine, TP1/TP2/TP3 and trailing rules.
- Phase 8: Paper orders, protected-position state, partial exits, local reconciliation and three risk actions.
- Phase 9: OpenClaw workspace skill, Candidate-only reports, natural-language preview plus explicit confirmation.
- Phase 10: local operational dashboard.
- Phase 11: closed-bar backtest, fees, funding, slippage, no-lookahead semantics, Walk Forward and performance metrics.

Testnet, Small Live and Production are not enabled. The isolated account-stream boundary contains no active signed-order implementation.

## Quick start

The service uses Python's standard library at runtime. Python 3.11+ is recommended.

```powershell
cd openclaw-quant
python -m unittest discover -s tests -v
python app.py
```

Open the dashboard at `http://127.0.0.1:8765/`.

On Windows, the helper scripts locate the bundled OpenClaw Python runtime when it is available:

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start_quant.ps1
powershell -NoProfile -ExecutionPolicy Bypass -File .\stop_quant.ps1
```

For a normal Python installation, run `python app.py` directly.

## Configuration and secrets

`config.json` contains paper-trading defaults and safe public endpoints. Copy `.env.example` to `.env` only when you need outbound notifications, and load those values through your process or container environment. Do not commit `.env`, exchange keys, database files, logs, or runtime PID files.

`docker-compose.yml` is an optional deployment definition. It starts the quant service with Redis and PostgreSQL adapters; the local Windows deployment does not require Docker and uses SQLite under `data/`.

## Important APIs

- `GET /api/status`, `/api/symbols`, `/api/events`, `/api/monitor/pool`
- `POST /api/monitor/custom`, `/api/market/refresh`, `/api/positions/reconcile`
- `POST /api/strategies/parse`, then `/api/strategies/confirm` with `confirmed:true`
- `POST /api/backtest`, `/api/walk-forward`
- `POST /api/risk/kill-switch` with `STOP_NEW_TRADES`, `STOP_ALL` or `EMERGENCY_EXIT`

Risk/execution configuration writes require `confirmed:true`. `TESTNET` and `LIVE` mode changes return HTTP 403.

## Test

```powershell
python -m unittest discover -s tests -v
```

## Repository layout

- `app.py`: HTTP API and dashboard server.
- `quant_engine.py`, `market_data.py`, `event_detection.py`: runtime, exchange boundary, and event pipeline.
- `strategy_manager.py`, `strategy_parser.py`, `backtest.py`: strategy lifecycle and research tools.
- `position_manager.py`, `stop_loss.py`: paper execution and risk controls.
- `storage.py`: SQLite audit and state store.
- `dashboard.html`: bilingual local operations dashboard.
- `tests/`: deterministic unit tests.
