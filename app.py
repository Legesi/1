"""Run the OpenClaw Quant PAPER service and its local dashboard.

Usage: python app.py [--once] [--port 8765]
"""
from __future__ import annotations

import argparse
import json
import logging
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from quant_engine import QuantService, write_json
from strategy_manager import StrategyDefinition
from logging_config import configure_logging

ROOT = Path(__file__).resolve().parent
configure_logging()
SERVICE = QuantService(ROOT)


def merge_dict(target: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    for key, value in updates.items():
        if isinstance(value, dict) and isinstance(target.get(key), dict):
            merge_dict(target[key], value)
        else:
            target[key] = value
    return target


class Handler(BaseHTTPRequestHandler):
    server_version = "OpenClawQuant/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:
        logging.getLogger("openclaw-quant.http").info(fmt, *args)

    def _send(self, payload: Any, status: int = 200, content_type: str = "application/json; charset=utf-8") -> None:
        if isinstance(payload, str):
            body = payload.encode("utf-8")
        else:
            body = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.end_headers()
        self.wfile.write(body)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", 0))
        if length <= 0:
            return {}
        try:
            return json.loads(self.rfile.read(length).decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            raise ValueError("request body must be JSON")

    def do_OPTIONS(self) -> None:
        self._send({}, 204)

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        path = parsed.path
        query = parse_qs(parsed.query)
        if path in ("/", "/index.html"):
            html = (ROOT / "dashboard.html").read_text(encoding="utf-8")
            self._send(html, content_type="text/html; charset=utf-8")
            return
        routes = {
            "/api/health": lambda: {"ok": True, "service": "openclaw-quant", "mode": SERVICE.store.account().get("mode")},
            "/api/status": SERVICE.status_json,
            "/api/config": lambda: SERVICE.config,
            "/api/symbols": SERVICE.symbol_registry.all,
            "/api/events": lambda: SERVICE.store.system_events(int(query.get("limit", [200])[0])),
            "/api/monitor/pool": SERVICE.pool.snapshot,
            "/api/gainers": lambda: SERVICE.pool.gainers,
            "/api/losers": lambda: SERVICE.pool.losers,
            "/api/signals": lambda: SERVICE.store.signals(100),
            "/api/positions": SERVICE.store.positions,
            "/api/orders": lambda: SERVICE.store.orders(100),
            "/api/trades": lambda: SERVICE.store.trades(100),
            "/api/strategies": lambda: {"strategies": SERVICE.store.strategies(), "bindings": SERVICE.store.strategy_bindings()},
            "/api/backtests": lambda: SERVICE.store.backtests(int(query.get("limit", [50])[0])),
            "/api/notifications": lambda: SERVICE.store.notifications(int(query.get("limit", [100])[0])),
            "/api/agents/reports": lambda: SERVICE.store.agent_reports(int(query.get("limit", [100])[0])),
            "/api/positions/events": lambda: SERVICE.store.position_events(int(query.get("limit", [200])[0])),
            "/api/audit": lambda: SERVICE.store.audit_events(int(query.get("limit", [200])[0])),
        }
        if path in routes:
            try:
                self._send(routes[path]())
            except Exception as exc:
                self._send({"error": str(exc)}, 500)
            return
        self._send({"error": "not found"}, 404)

    def do_POST(self) -> None:
        path = urlparse(self.path).path
        try:
            body = self._body()
            if path == "/api/scan":
                self._send(SERVICE.scan_once(body.get("symbols")))
                return
            if path == "/api/market/refresh":
                SERVICE.refresh_market()
                self._send(SERVICE.pool.snapshot())
                return
            if path == "/api/monitor/custom":
                symbol = str(body.get("symbol", "")).upper().strip()
                if not symbol.endswith("USDT") or not symbol.isalnum() or len(symbol) > 30:
                    self._send({"error": "symbol must be a Binance USDT perpetual symbol"}, 400)
                    return
                action = str(body.get("action", "add")).lower()
                if action == "add":
                    SERVICE.store.upsert_monitor(symbol, "CUSTOM", True, body.get("strategy", SERVICE.config["strategy"]["default"]), int(body.get("priority", 2)))
                elif action in ("remove", "delete"):
                    SERVICE.store.remove_monitor(symbol)
                elif action in ("pause", "disable"):
                    SERVICE.store.set_monitor_enabled(symbol, False)
                elif action in ("resume", "enable"):
                    SERVICE.store.set_monitor_enabled(symbol, True)
                elif action == "priority":
                    SERVICE.store.set_monitor_priority(symbol, int(body.get("priority", 2)))
                else:
                    self._send({"error": "action must be add, remove, pause or resume"}, 400)
                    return
                self._send(SERVICE.pool.snapshot())
                return
            if path == "/api/config":
                updates = body.get("updates", body)
                if not isinstance(updates, dict):
                    self._send({"error": "updates must be an object"}, 400)
                    return
                mode = updates.get("system", {}).get("mode") if isinstance(updates.get("system"), dict) else None
                if mode and mode.lower() != "paper":
                    self._send({"error": "TESTNET/LIVE execution is disabled in this deployment; PAPER is required"}, 403)
                    return
                risky_paths = any(key in updates for key in ("risk", "execution")) or (isinstance(updates.get("system"), dict) and "mode" in updates["system"])
                if risky_paths and body.get("confirmed") is not True:
                    self._send({"error": "explicit confirmed=true is required for risk, execution or mode changes"}, 409)
                    return
                merge_dict(SERVICE.config, updates)
                SERVICE.config["system"]["mode"] = "paper"
                version = SERVICE.store.save_config(SERVICE.config)
                write_json(SERVICE.config_path, SERVICE.config)
                self._send({"version": version, "config": SERVICE.config})
                return
            if path == "/api/mode":
                mode = str(body.get("mode", "paper")).lower()
                if mode != "paper":
                    self._send({"error": "Only PAPER mode is enabled. Configure Binance credentials and complete staged approvals before enabling live execution."}, 403)
                    return
                SERVICE.store.set_mode("paper")
                self._send(SERVICE.status_json())
                return
            if path == "/api/risk/kill-switch":
                enabled = bool(body.get("enabled", True))
                action = str(body.get("action", "STOP_NEW_TRADES" if enabled else "STOP_NEW_TRADES")).upper()
                if action == "KILL_SWITCH":
                    SERVICE.store.set_kill_switch(enabled)
                else:
                    SERVICE.store.set_risk_action(action, enabled)
                SERVICE.notifications.emit("KILL_SWITCH", f"PAPER risk action {action} {'enabled' if enabled else 'disabled'}", {"action": action, "enabled": enabled}, "CRITICAL" if enabled else "INFO")
                self._send(SERVICE.status_json())
                return
            if path == "/api/positions/reconcile":
                self._send(SERVICE.position_manager.reconcile())
                return
            if path == "/api/strategies":
                payload = body.get("payload", body)
                self._send({"strategy": SERVICE.strategy_manager.create(payload)})
                return
            if path == "/api/strategies/validate":
                definition = StrategyDefinition.model_validate(body.get("payload", body))
                self._send({"valid": True, "normalized": definition.model_dump(mode="json")})
                return
            if path == "/api/strategies/parse":
                preview = SERVICE.strategy_parser.parse(str(body.get("text", "")), body.get("name"))
                SERVICE.pending_strategy_previews[preview["preview_id"]] = preview
                self._send(preview)
                return
            if path == "/api/strategies/confirm":
                preview_id = str(body.get("preview_id", ""))
                preview = SERVICE.pending_strategy_previews.pop(preview_id, None)
                if not preview:
                    self._send({"error": "preview not found or already confirmed"}, 404)
                    return
                if body.get("confirmed") is not True:
                    self._send({"error": "explicit confirmed=true is required", "preview": preview}, 400)
                    return
                self._send({"strategy": SERVICE.strategy_manager.create(preview["normalized"])})
                return
            if path == "/api/strategies/revise":
                self._send({"strategy": SERVICE.strategy_manager.revise(str(body["name"]), str(body["base_version"]), body.get("changes", {}))})
                return
            if path == "/api/strategies/clone":
                self._send({"strategy": SERVICE.strategy_manager.clone(str(body["name"]), str(body["version"]), str(body["new_name"]))})
                return
            if path == "/api/strategies/toggle":
                SERVICE.strategy_manager.set_enabled(str(body["name"]), str(body["version"]), bool(body.get("enabled", True)))
                self._send({"ok": True})
                return
            if path == "/api/strategies/delete":
                if body.get("confirmed") is not True:
                    self._send({"error": "confirmed=true is required to delete a strategy version"}, 409)
                    return
                deleted = SERVICE.store.delete_strategy(str(body["name"]), str(body["version"]))
                self._send({"ok": deleted})
                return
            if path == "/api/strategies/bind":
                SERVICE.strategy_manager.bind(str(body["binding_type"]), str(body["binding_key"]), str(body["name"]), str(body["version"]))
                self._send({"ok": True, "bindings": SERVICE.store.strategy_bindings()})
                return
            if path in ("/api/backtest", "/api/walk-forward"):
                symbol = str(body.get("symbol", "")).upper()
                interval = str(body.get("interval", "5m"))
                name = str(body.get("strategy", "Default Trend Pullback"))
                version = str(body.get("strategy_version", "1.0"))
                record = SERVICE.store.strategy(name, version)
                if not record:
                    self._send({"error": "strategy version not found"}, 404)
                    return
                klines = body.get("klines")
                if not isinstance(klines, list):
                    klines = SERVICE.client.klines(symbol, interval, int(body.get("limit", 1000)), body.get("start_time"), body.get("end_time"))
                funding = body.get("funding", [])
                if body.get("include_funding") and not funding:
                    funding = SERVICE.client.funding_rates(symbol, body.get("start_time"), body.get("end_time"))
                if path == "/api/walk-forward":
                    result = SERVICE.backtest.walk_forward(symbol, klines, record["payload"], float(body.get("train_ratio", 0.6)), float(body.get("validation_ratio", 0.2)))
                    request = {"symbol": symbol, "strategy": name, "strategy_version": version, "interval": interval, "walk_forward": True}
                else:
                    result = SERVICE.backtest.run(symbol, klines, record["payload"], funding, body.get("initial_capital"))
                    request = {"symbol": symbol, "strategy": name, "strategy_version": version, "interval": interval, "bars": len(klines), "include_funding": bool(body.get("include_funding"))}
                SERVICE.store.save_backtest(request, result)
                self._send(result)
                return
            if path == "/api/agents/analyze":
                symbol = str(body.get("symbol", "BTCUSDT")).upper()
                frames = body.get("frames")
                if not isinstance(frames, dict):
                    frames = {timeframe: SERVICE.client.klines(symbol, timeframe, 250) for timeframe in ("4h", "1h", "15m", "5m")}
                report = SERVICE.agents.market_analysis(symbol, frames)
                self._send(report)
                return
            if path == "/api/agents/monitor":
                self._send({"trading": SERVICE.agents.trading_monitor(SERVICE.status_json()), "execution": SERVICE.agents.execution_monitor(), "performance": SERVICE.agents.performance(), "admin": SERVICE.agents.system_admin(SERVICE.status_json(), SERVICE.position_manager.reconcile())})
                return
            if path == "/api/notifications/test":
                self._send(SERVICE.notifications.emit("TEST", str(body.get("message", "OpenClaw Quant notification test")), {}, "INFO"))
                return
            self._send({"error": "not found"}, 404)
        except ValueError as exc:
            self._send({"error": str(exc)}, 400)
        except Exception as exc:
            logging.getLogger("openclaw-quant.http").exception("request failed")
            self._send({"error": str(exc)}, 500)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="refresh public data and exit")
    parser.add_argument("--host", default=None)
    parser.add_argument("--port", type=int, default=None)
    args = parser.parse_args()
    if args.once:
        SERVICE.start(background=False)
        print(json.dumps(SERVICE.status_json(), ensure_ascii=False, indent=2))
        SERVICE.close()
        return
    host = args.host or SERVICE.config["server"]["host"]
    port = args.port or int(SERVICE.config["server"]["port"])
    SERVICE.start(background=True)
    server = ThreadingHTTPServer((host, port), Handler)
    logging.info("Dashboard available at http://%s:%s", host, port)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        SERVICE.close()


if __name__ == "__main__":
    main()
