"""OpenClaw Quant core: public Binance data, monitor pool, strategy, risk and paper execution."""
from __future__ import annotations

import hashlib
import json
import logging
import os
import random
import secrets
import socket
import ssl
import threading
import time
import urllib.parse
import urllib.request
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from indicators import indicator_snapshot
from storage import Store, utc_now
from agents import AgentHub
from backtest import BacktestEngine, evaluate_condition
from notifications import NotificationCenter
from position_manager import PositionManager
from strategy_manager import StrategyManager
from events import Event, EventBus, make_event
from event_detection import EventDetector
from market_data import RealtimeMarketData, SymbolRegistry
from risk_events import EmergencyRiskEngine
from strategy_parser import StrategyParser
from stop_loss import StopLossManager
from trade_plan import TradePlan

LOG = logging.getLogger("openclaw-quant")


def read_json(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: str | Path, payload: dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


class BinancePublicClient:
    def __init__(self, rest_base: str, timeout: int = 12) -> None:
        self.rest_base = rest_base.rstrip("/")
        self.timeout = timeout

    def get(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urllib.parse.urlencode({k: v for k, v in (params or {}).items() if v is not None})
        request = urllib.request.Request(f"{self.rest_base}{path}?{query}" if query else f"{self.rest_base}{path}", headers={"User-Agent": "openclaw-quant/1.0"})
        with urllib.request.urlopen(request, timeout=self.timeout) as response:
            return json.loads(response.read().decode("utf-8"))

    def exchange_info(self) -> list[dict[str, Any]]:
        payload = self.get("/fapi/v1/exchangeInfo")
        return [s for s in payload.get("symbols", []) if s.get("status") == "TRADING" and s.get("quoteAsset") == "USDT" and s.get("contractType") == "PERPETUAL"]

    def tickers(self) -> list[dict[str, Any]]:
        return self.get("/fapi/v1/ticker/24hr")

    def klines(self, symbol: str, interval: str, limit: int = 250, start_time: int | None = None, end_time: int | None = None) -> list[list[Any]]:
        return self.get("/fapi/v1/klines", {"symbol": symbol, "interval": interval, "limit": min(limit, 1500), "startTime": start_time, "endTime": end_time})

    def funding_rates(self, symbol: str, start_time: int | None = None, end_time: int | None = None, limit: int = 1000) -> list[dict[str, Any]]:
        return self.get("/fapi/v1/fundingRate", {"symbol": symbol, "startTime": start_time, "endTime": end_time, "limit": min(limit, 1000)})


class WebSocketTicker(threading.Thread):
    """Minimal Binance stream client with reconnect, ping/pong and duplicate-event protection."""
    daemon = True

    def __init__(self, websocket_base: str, symbols: Callable[[], list[str]], on_ticker: Callable[[dict[str, Any]], None], on_kline: Callable[[dict[str, Any]], None], stop: threading.Event) -> None:
        super().__init__(name="binance-websocket")
        self.websocket_base = websocket_base
        self.symbols = symbols
        self.on_ticker = on_ticker
        self.on_kline = on_kline
        self.stop_event = stop
        self.last_event: dict[str, int] = {}
        self.connected = False
        self.last_error: str | None = None

    def _connect(self) -> tuple[socket.socket, str]:
        parsed = urllib.parse.urlparse(self.websocket_base)
        host = parsed.hostname or "fstream.binance.com"
        port = parsed.port or 443
        timeframes = ("1m", "5m", "15m", "1h", "4h", "1d")
        streams = "/".join(stream for symbol in self.symbols() for stream in (f"{symbol.lower()}@ticker", f"{symbol.lower()}@trade", f"{symbol.lower()}@markPrice", f"{symbol.lower()}@bookTicker", *(f"{symbol.lower()}@kline_{interval}" for interval in timeframes)))
        if not streams:
            streams = "btcusdt@ticker"
        path = (parsed.path or "/stream") + "?streams=" + urllib.parse.quote(streams, safe="/@")
        raw = socket.create_connection((host, port), timeout=10)
        sock = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
        key = secrets.token_urlsafe(16)
        request = f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
        sock.sendall(request.encode("ascii"))
        header = b""
        while b"\r\n\r\n" not in header and len(header) < 16384:
            header += sock.recv(4096)
        if b" 101 " not in header.split(b"\r\n", 1)[0]:
            raise RuntimeError(f"websocket handshake rejected: {header[:120]!r}")
        return sock, host

    @staticmethod
    def _frame(sock: socket.socket) -> tuple[int, bytes] | None:
        first = sock.recv(2)
        if not first:
            return None
        opcode = first[0] & 0x0F
        masked = bool(first[1] & 0x80)
        length = first[1] & 0x7F
        if length == 126:
            length = int.from_bytes(sock.recv(2), "big")
        elif length == 127:
            length = int.from_bytes(sock.recv(8), "big")
        mask = sock.recv(4) if masked else b""
        data = bytearray()
        while len(data) < length:
            chunk = sock.recv(min(65536, length - len(data)))
            if not chunk:
                return None
            data.extend(chunk)
        if masked:
            data = bytearray(value ^ mask[index % 4] for index, value in enumerate(data))
        return opcode, bytes(data)

    @staticmethod
    def _send_frame(sock: socket.socket, opcode: int, data: bytes = b"") -> None:
        length = len(data)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(length)
        elif length < 65536:
            header.append(126)
            header.extend(length.to_bytes(2, "big"))
        else:
            header.append(127)
            header.extend(length.to_bytes(8, "big"))
        sock.sendall(header + data)

    def run(self) -> None:
        while not self.stop_event.is_set():
            sock = None
            try:
                sock, _ = self._connect()
                sock.settimeout(25)
                self.connected = True
                self.last_error = None
                while not self.stop_event.is_set():
                    try:
                        frame = self._frame(sock)
                    except socket.timeout:
                        self._send_frame(sock, 0x9, b"ping")
                        continue
                    if frame is None:
                        raise ConnectionError("websocket closed")
                    opcode, payload = frame
                    if opcode == 0x8:
                        raise ConnectionError("websocket close frame")
                    if opcode == 0x9:
                        self._send_frame(sock, 0xA, payload)
                    elif opcode == 0x1:
                        outer = json.loads(payload.decode("utf-8"))
                        data = outer.get("data", outer)
                        event_type = data.get("e")
                        if event_type == "kline":
                            self.on_kline(data)
                            continue
                        symbol = data.get("s")
                        event_time = int(data.get("E", 0))
                        if symbol and event_time >= self.last_event.get(symbol, 0):
                            self.last_event[symbol] = event_time
                            self.on_ticker(data)
            except Exception as exc:  # reconnect is part of the runtime contract
                self.last_error = str(exc)
                self.connected = False
                LOG.warning("Binance WebSocket disconnected: %s", exc)
                self.stop_event.wait(3)
            finally:
                if sock:
                    try:
                        sock.close()
                    except OSError:
                        pass
        self.connected = False


class MonitorPool:
    def __init__(self, config: dict[str, Any], store: Store) -> None:
        self.config = config
        self.store = store
        self.lock = threading.RLock()
        for symbol in config["monitor"].get("custom_symbols", []):
            self.store.upsert_monitor(symbol.upper(), "CUSTOM", True, config["strategy"].get("default"))
        self.gainers: list[dict[str, Any]] = []
        self.losers: list[dict[str, Any]] = []
        self.last_refresh: str | None = None

    def refresh(self, tickers: list[dict[str, Any]]) -> None:
        monitor = self.config["monitor"]
        blacklist = {s.upper() for s in monitor.get("blacklist_symbols", [])}
        clean = []
        for ticker in tickers:
            try:
                symbol = str(ticker["symbol"]).upper()
                change = float(ticker.get("priceChangePercent", 0))
                quote_volume = float(ticker.get("quoteVolume", 0))
                volume = float(ticker.get("volume", 0))
                price = float(ticker.get("lastPrice", 0))
            except (KeyError, TypeError, ValueError):
                continue
            if symbol in blacklist or quote_volume < float(monitor.get("min_quote_volume", 0)) or volume < float(monitor.get("min_volume", 0)) or not (float(monitor.get("min_price", 0)) <= price <= float(monitor.get("max_price", 1e99))):
                continue
            clean.append({**ticker, "symbol": symbol, "change_percent": change, "quote_volume": quote_volume, "last_price": price})
        self.gainers = sorted([x for x in clean if x["change_percent"] >= float(monitor.get("min_change_percent", -1e99)) and x["change_percent"] <= float(monitor.get("max_change_percent", 1e99))], key=lambda x: x["change_percent"], reverse=True)[: int(monitor.get("gainers_top_n", 20))]
        self.losers = sorted(clean, key=lambda x: x["change_percent"])[: int(monitor.get("losers_top_n", 10))]
        for rank, item in enumerate(self.gainers, 1):
            item["rank"] = rank
        for rank, item in enumerate(self.losers, 1):
            item["rank"] = rank
        for item in self.gainers:
            self.store.upsert_monitor(item["symbol"], "GAINERS", True, self.config["strategy"].get("default"))
        for item in self.losers:
            self.store.upsert_monitor(item["symbol"], "LOSERS", True, self.config["strategy"].get("default"))
        dynamic_symbols = {item["symbol"] for item in self.gainers + self.losers}
        for row in self.store.monitors():
            if row["source"] in ("GAINERS", "LOSERS") and row["symbol"] not in dynamic_symbols:
                self.store.remove_monitor(row["symbol"])
        self.last_refresh = utc_now()

    def pool(self) -> list[str]:
        with self.lock:
            configured = self.store.monitors()
            symbols = {row["symbol"] for row in configured if row["enabled"] and row["symbol"] not in set(self.config["monitor"].get("blacklist_symbols", []))}
            return sorted(symbols)

    def snapshot(self) -> dict[str, Any]:
        return {"custom": [x for x in self.store.monitors() if x["source"] == "CUSTOM"], "gainers": self.gainers, "losers": self.losers, "final_symbols": self.pool(), "last_refresh": self.last_refresh}


class StrategyEngine:
    name = "Default Trend Pullback"
    version = "1.0"

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    def signal(self, symbol: str, frames: dict[str, list[list[Any]]], ticker: dict[str, Any] | None = None, definition: dict[str, Any] | None = None) -> dict[str, Any] | None:
        required = ["4h", "1h", "15m", "5m"]
        if any(not frames.get(key) or len(frames[key]) < 30 for key in required):
            return None
        snapshots = {key: indicator_snapshot(frames[key]) for key in required}
        price = float((ticker or {}).get("lastPrice") or frames["5m"][-1][4])
        trend_close = float(frames["4h"][-1][4])
        main_close = float(frames["1h"][-1][4])
        setup_close = float(frames["15m"][-1][4])
        confirm_close = float(frames["5m"][-1][4])
        up = trend_close > snapshots["4h"]["ema50"] > snapshots["4h"]["ema200"] and main_close > snapshots["1h"]["ema50"]
        down = trend_close < snapshots["4h"]["ema50"] < snapshots["4h"]["ema200"] and main_close < snapshots["1h"]["ema50"]
        confirmation_long = confirm_close > snapshots["5m"]["ema20"] and snapshots["5m"]["macd_hist"] >= 0
        confirmation_short = confirm_close < snapshots["5m"]["ema20"] and snapshots["5m"]["macd_hist"] <= 0
        volume_ok = snapshots["5m"]["volume_ratio"] >= float(self.config["strategy"].get("min_volume_ratio", 1.2))
        if definition and definition.get("name") != self.name:
            context = {key.upper(): value for key, value in snapshots["5m"].items()}
            context["PRICE"] = price
            context["VOLUME"] = float(frames["5m"][-1][5])
            previous_snapshot = indicator_snapshot(frames["5m"][:-1])
            previous = {key.upper(): value for key, value in previous_snapshot.items()}
            previous["PRICE"] = float(frames["5m"][-2][4])
            previous["VOLUME"] = float(frames["5m"][-2][5])
            matched = evaluate_condition(definition["conditions"], context, previous)
            direction = definition.get("direction", "LONG") if definition.get("direction") != "BOTH" else ("LONG" if matched else None)
            if not matched:
                return None
        else:
            direction = "LONG" if up and setup_close >= snapshots["15m"]["ema20"] and confirmation_long else "SHORT" if down and setup_close <= snapshots["15m"]["ema20"] and confirmation_short else None
            if not direction or not volume_ok:
                return None
        strategy_name = definition.get("name", self.name) if definition else self.name
        strategy_version = definition.get("version", self.version) if definition else self.version
        strategy_id = (definition.get("strategy_id") if definition else None) or strategy_name.lower().replace(" ", "_")
        risk_config = definition.get("risk", {}) if definition else {}
        atr_value = max(snapshots["5m"]["atr14"], price * 0.001)
        stop_distance = atr_value * float(risk_config.get("stop_atr", 1.5))
        stop_loss = price - stop_distance if direction == "LONG" else price + stop_distance
        tp1_r, tp2_r, tp3_r = (float(risk_config.get("tp1_r", 1)), float(risk_config.get("tp2_r", 2)), float(risk_config.get("tp3_r", 3)))
        tp1 = price + stop_distance * tp1_r if direction == "LONG" else price - stop_distance * tp1_r
        tp2 = price + stop_distance * tp2_r if direction == "LONG" else price - stop_distance * tp2_r
        tp3 = price + stop_distance * tp3_r if direction == "LONG" else price - stop_distance * tp3_r
        confidence = min(95, 60 + int(min(snapshots["5m"]["volume_ratio"], 3.0) * 8) + (8 if snapshots["1h"]["rsi14"] > 50 else 0))
        risk_reward = tp2_r
        signal_id = hashlib.sha256(f"{symbol}:{direction}:{strategy_name}:{strategy_version}:{frames['5m'][-1][0]}".encode()).hexdigest()[:24]
        return {"signal_id": signal_id, "symbol": symbol, "market_type": "CRYPTO", "direction": direction, "strategy_id": strategy_id, "strategy": strategy_name, "strategy_version": strategy_version, "entry": {"min": price - atr_value * 0.2 if direction == "LONG" else price, "max": price if direction == "LONG" else price + atr_value * 0.2, "ideal": price}, "stop_loss": stop_loss, "stop_loss_plan": {"mode": str(self.config.get("risk", {}).get("stop_loss_mode", "ATR")), "atr": atr_value, "distance": stop_distance}, "take_profit": {"tp1": tp1, "tp2": tp2, "tp3": tp3}, "position_size": {"method": "RISK_ENGINE", "quantity": None}, "risk": {"status": "PENDING", "risk_config_version": "runtime"}, "risk_reward": risk_reward, "min_rr": float(risk_config.get("min_rr", self.config["strategy"].get("min_rr", 1.8))), "confidence": confidence, "market_regime": "TREND_UP" if direction == "LONG" else "TREND_DOWN", "status": "CANDIDATE", "indicators": snapshots["5m"], "created_at": utc_now()}


class RiskEngine:
    def __init__(self, config: dict[str, Any], store: Store) -> None:
        self.config = config
        self.store = store
        self.stop_loss = StopLossManager(config.get("risk", {}))

    def check(self, signal: dict[str, Any]) -> dict[str, Any]:
        account = self.store.account()
        risk = self.config["risk"]
        failures: list[str] = []
        if bool(account.get("kill_switch")) or bool(account.get("stop_new_trades")) or bool(account.get("stop_all")) or bool(account.get("emergency_exit")) or bool(risk.get("kill_switch")):
            failures.append("KILL_SWITCH")
        if account.get("mode") != "paper":
            failures.append("NON_PAPER_MODE_DISABLED")
        if not bool(account.get("reconciliation_ok", True)):
            failures.append("RECONCILIATION_REQUIRED")
        if len(self.store.positions()) >= int(risk.get("max_positions", 5)):
            failures.append("MAX_POSITIONS")
        if float(signal.get("confidence", 0)) < float(self.config["strategy"].get("min_confidence", 60)):
            failures.append("LOW_CONFIDENCE")
        entry = float(signal["entry"]["ideal"])
        stop = float(signal["stop_loss"])
        stop_distance = abs(entry - stop)
        if stop_distance <= 0:
            failures.append("INVALID_STOP")
        min_rr = float(signal.get("min_rr", self.config["strategy"].get("min_rr", 1.8)))
        if float(signal.get("risk_reward", 0)) < min_rr:
            failures.append("LOW_RR")
        equity = float(account.get("equity", 0))
        risk_amount = equity * float(risk.get("risk_per_trade", 0.005))
        quantity = risk_amount / stop_distance if stop_distance else 0.0
        stop_result = self.stop_loss.calculate(direction=signal["direction"], entry=entry, quantity=quantity, equity=equity, mode="PERCENT_PRICE", stop_percent=stop_distance / entry if entry else 0, fee_rate=float(self.config.get("execution", {}).get("fee_rate", 0.0004)), slippage=float(self.config.get("execution", {}).get("paper_slippage", 0.0002)))
        if stop_result.rejected:
            failures.append("MAX_LOSS_HARD_LIMIT")
        quantity = stop_result.quantity
        return {"approved": not failures, "failures": failures, "risk_amount": risk_amount, "max_loss_usdt": stop_result.max_loss_usdt, "estimated_loss": stop_result.max_loss_usdt if stop_result.adjusted else stop_distance * quantity, "quantity": quantity, "entry": entry, "stop_distance": stop_distance, "stop_mode": stop_result.mode, "resized": stop_result.adjusted}


class PaperExecutor:
    def __init__(self, config: dict[str, Any], store: Store, position_manager: PositionManager | None = None, notifications: NotificationCenter | None = None) -> None:
        self.config = config
        self.store = store
        self.risk = RiskEngine(config, store)
        self.position_manager = position_manager
        self.notifications = notifications

    def execute(self, signal: dict[str, Any]) -> dict[str, Any]:
        check = self.risk.check(signal)
        if not check["approved"]:
            signal["status"] = "INVALIDATED"
            if not self.store.signal_exists(signal["signal_id"]):
                self.store.save_signal(signal)
            else:
                self.store.update_signal(signal)
            self.store.audit("RISK_REJECTED", {"signal_id": signal["signal_id"], "failures": check["failures"]})
            return {"approved": False, "check": check}
        if self.store.position(signal["symbol"]):
            return {"approved": False, "check": {"approved": False, "failures": ["DUPLICATE_POSITION"]}}
        quantity = round(check["quantity"], 8)
        plan = TradePlan(signal_id=signal["signal_id"], strategy_id=signal.get("strategy_id", signal["strategy"].lower().replace(" ", "_")), strategy_version=signal["strategy_version"], risk_config_version=signal.get("risk", {}).get("risk_config_version", "runtime"), symbol=signal["symbol"], direction=signal["direction"], entry=check["entry"], stop_loss=float(signal["stop_loss"]), tp1=float(signal["take_profit"]["tp1"]), tp2=float(signal["take_profit"]["tp2"]), tp3=float(signal["take_profit"]["tp3"]), quantity=quantity, risk_amount=float(check["risk_amount"]), max_loss_usdt=float(check["max_loss_usdt"]), risk_reward=float(signal["risk_reward"]), mode="paper")
        order_id = f"paper-{uuid.uuid4().hex[:16]}"
        order = {"order_id": order_id, "signal_id": signal["signal_id"], "strategy_run_id": signal["signal_id"], "client_order_id": order_id, "symbol": signal["symbol"], "side": "BUY" if signal["direction"] == "LONG" else "SELL", "order_type": "MARKET", "quantity": quantity, "price": check["entry"], "status": "FILLED", "mode": "paper", "strategy_id": plan.strategy_id, "strategy_version": plan.strategy_version, "risk_config_version": plan.risk_config_version, "trade_plan": plan.model_dump(mode="json"), "created_at": utc_now()}
        signal["status"] = "TRADED"
        if not self.store.signal_exists(signal["signal_id"]):
            self.store.save_signal(signal)
        else:
            self.store.update_signal(signal)
        self.store.save_order(order)
        now = utc_now()
        self.store.open_position({"symbol": signal["symbol"], "direction": signal["direction"], "quantity": quantity, "entry_price": check["entry"], "stop_loss": signal["stop_loss"], "take_profit": signal["take_profit"]["tp1"], "strategy": signal["strategy"], "strategy_version": signal["strategy_version"], "opened_at": now, "updated_at": now})
        self.store.save_trade({"trade_id": f"trade-{uuid.uuid4().hex[:16]}", "order_id": order_id, "symbol": signal["symbol"], "direction": signal["direction"], "quantity": quantity, "entry_price": check["entry"], "created_at": now})
        if self.position_manager:
            self.position_manager.register(signal, quantity)
        if self.notifications:
            self.notifications.emit("PAPER_ORDER_FILLED", f"{signal['symbol']} PAPER {signal['direction']} opened", order)
        self.store.audit("PAPER_ORDER_FILLED", order)
        return {"approved": True, "check": check, "order": order}


@dataclass
class RuntimeStatus:
    started_at: str = ""
    last_ticker_refresh: str | None = None
    last_scan: str | None = None
    last_error: str | None = None
    ws_connected: bool = False
    ws_error: str | None = None
    last_event: str | None = None


class QuantService:
    def __init__(self, root: str | Path) -> None:
        self.root = Path(root)
        self.config_path = self.root / "config.json"
        self.db_path = self.root / "data" / "quant.sqlite3"
        self.config = read_json(self.config_path)
        self.store = Store(self.db_path, self.config["system"].get("initial_equity", 10000.0))
        self.client = BinancePublicClient(self.config["binance"]["rest_base"], self.config["binance"].get("request_timeout", 12))
        self.pool = MonitorPool(self.config, self.store)
        self.strategy = StrategyEngine(self.config)
        self.strategy_manager = StrategyManager(self.store)
        self.notifications = NotificationCenter(self.config, self.store)
        self.position_manager = PositionManager(self.config, self.store, self.notifications)
        self.executor = PaperExecutor(self.config, self.store, self.position_manager, self.notifications)
        self.backtest = BacktestEngine(self.config)
        self.agents = AgentHub(self.store)
        self.strategy_parser = StrategyParser()
        self.pending_strategy_previews: dict[str, dict[str, Any]] = {}
        self.event_bus = EventBus()
        self.market_data = RealtimeMarketData()
        self.symbol_registry = SymbolRegistry(self.store)
        self.event_detector = EventDetector(self.market_data, self.event_bus)
        self.emergency_risk = EmergencyRiskEngine(self.config.get("risk", {}), self.store, self.event_bus)
        self.tickers: dict[str, dict[str, Any]] = {}
        self.status = RuntimeStatus(started_at=utc_now())
        self.stop_event = threading.Event()
        self.threads: list[threading.Thread] = []
        trigger_events = self.config.get("strategy", {}).get("trigger", {}).get("confirmation_events", [])
        for event_type in trigger_events:
            self.event_bus.subscribe(event_type, self._on_strategy_event)
        self.ws = WebSocketTicker(self.config["binance"]["websocket_base"], self.pool.pool, self._on_ws_ticker, self._on_ws_kline, self.stop_event)

    def _on_ws_ticker(self, ticker: dict[str, Any]) -> None:
        normalized = self.market_data.update_ticker(ticker)
        self.tickers[ticker["s"]] = {"symbol": ticker["s"], "lastPrice": ticker.get("c"), "priceChangePercent": ticker.get("P", 0), "volume": ticker.get("v", 0), "quoteVolume": ticker.get("q", 0), "event_time": ticker.get("E")}
        if ticker.get("e") in (None, "24hrTicker", "bookTicker", "markPriceUpdate"):
            self.event_detector.on_ticker(normalized)
            self.emergency_risk.evaluate_ticker(normalized)
        try:
            self.position_manager.on_price(ticker["s"], float(ticker.get("c", 0)))
            position = self.store.position(ticker["s"])
            if position:
                self.emergency_risk.evaluate_position(position, normalized)
        except Exception as exc:
            LOG.exception("Position update failed for %s: %s", ticker.get("s"), exc)

    def _on_ws_kline(self, data: dict[str, Any]) -> None:
        symbol, interval, row, closed = self.market_data.update_kline(data)
        self.event_detector.on_kline(symbol, interval, row, closed)

    def discover_symbols(self) -> list[str]:
        try:
            discovered = self.client.exchange_info()
            registry = self.symbol_registry.refresh(discovered)
            self.store.audit("SYMBOL_REGISTRY_REFRESHED", {"count": len(registry)})
            return [row["symbol"] for row in registry if row.get("status") == "TRADING"]
        except Exception as exc:
            self.status.last_error = f"symbol discovery: {exc}"
            LOG.warning("Symbol discovery failed: %s", exc)
            return []

    def refresh_market(self) -> None:
        try:
            tickers = self.client.tickers()
            self.pool.refresh(tickers)
            rank_by_symbol = {item["symbol"]: item["rank"] for item in self.pool.gainers + self.pool.losers}
            for ticker in tickers:
                if ticker.get("symbol"):
                    symbol = ticker["symbol"]
                    enriched = dict(ticker)
                    if symbol in rank_by_symbol:
                        enriched["rank"] = rank_by_symbol[symbol]
                    self.market_data.update_ticker(enriched)
                    self.tickers[symbol] = ticker
                    self.event_detector.on_ticker(self.market_data.ticker(symbol) or enriched)
                    self.position_manager.on_price(ticker["symbol"], float(ticker.get("lastPrice", 0)))
            self.status.last_ticker_refresh = utc_now()
            self.event_bus.publish(make_event("MARKET_REFRESH", payload={"gainers": len(self.pool.gainers), "losers": len(self.pool.losers)}))
        except Exception as exc:
            self.status.last_error = f"market refresh: {exc}"
            LOG.warning("Market refresh failed: %s", exc)

    def scan_once(self, symbols: list[str] | None = None, trigger_event: str = "MANUAL_SCAN") -> dict[str, Any]:
        symbols = symbols if symbols is not None else self.pool.pool()
        created: list[dict[str, Any]] = []
        skipped: list[dict[str, Any]] = []
        for symbol in symbols[: int(self.config["monitor"].get("gainers_top_n", 20))]:
            if self.store.position(symbol):
                skipped.append({"symbol": symbol, "reason": "POSITION_OPEN"})
                continue
            try:
                frames = {interval: self.client.klines(symbol, interval, 250) for interval in ("4h", "1h", "15m", "5m")}
                monitor_row = next((row for row in self.store.monitors() if row["symbol"] == symbol), {"source": "CUSTOM"})
                resolved = self.strategy_manager.resolve(symbol, monitor_row.get("source", "CUSTOM"))
                definition = resolved.get("payload") if resolved else None
                signal = self.strategy.signal(symbol, frames, self.tickers.get(symbol), definition)
                if not signal:
                    skipped.append({"symbol": symbol, "reason": "NO_TRADE"})
                    continue
                cooldown_key = f"{symbol}:{signal['direction']}:{signal['strategy']}:{trigger_event}"
                if self.store.cooldown_active(cooldown_key):
                    skipped.append({"symbol": symbol, "reason": "AI_CANDIDATE_COOLDOWN"})
                    continue
                if not self.store.save_signal(signal):
                    skipped.append({"symbol": symbol, "reason": "DUPLICATE_SIGNAL"})
                    continue
                created.append(signal)
                cooldown_seconds = int(self.config.get("strategy", {}).get("ai_cooldown_seconds", 900))
                expires_at = (datetime.now(timezone.utc) + timedelta(seconds=cooldown_seconds)).isoformat()
                self.store.save_cooldown(cooldown_key, symbol, signal["direction"], signal["strategy"], trigger_event, expires_at)
                # Candidate analysis is advisory and deterministic here; it never bypasses RiskEngine.
                self.agents.strategy_analysis(symbol, signal)
                result = self.executor.execute(signal)
                signal["execution"] = result
                self.agents.risk_analysis(signal, result.get("check", {}))
            except Exception as exc:
                skipped.append({"symbol": symbol, "reason": "ERROR", "error": str(exc)})
        self.status.last_scan = utc_now()
        return {"created": created, "skipped": skipped, "scanned": len(symbols)}

    def _refresh_loop(self) -> None:
        while not self.stop_event.is_set():
            self.refresh_market()
            self.stop_event.wait(float(self.config["monitor"].get("gainers_refresh_interval", 30)))

    def _event_loop(self) -> None:
        while not self.stop_event.is_set():
            event = self.event_bus.get(0.5)
            if event is None:
                continue
            self.store.save_event(asdict(event))
            self.status.last_event = event.type
            self.event_bus.dispatch(event)

    def _registry_loop(self) -> None:
        while not self.stop_event.is_set():
            self.discover_symbols()
            self.stop_event.wait(600)

    def _on_strategy_event(self, event: Event) -> None:
        if not event.symbol:
            return
        confirmation = self.config.get("strategy", {}).get("timeframes", {}).get("confirmation", "5m")
        if event.type == "KLINE_CLOSED" and event.payload.get("interval") != confirmation:
            return
        self.scan_once([event.symbol], trigger_event=event.type)

    def start(self, background: bool = True) -> None:
        self.store.audit("SERVICE_STARTED", {"mode": self.store.account().get("mode"), "paper_only": True})
        self.position_manager.reconcile()
        if not background:
            self.refresh_market()
            self.discover_symbols()
            return
        self.ws.start()
        self.threads = [threading.Thread(target=self._refresh_loop, name="market-refresh", daemon=True), threading.Thread(target=self._event_loop, name="event-dispatcher", daemon=True), threading.Thread(target=self._registry_loop, name="symbol-registry", daemon=True)]
        for thread in self.threads:
            thread.start()

    def stop(self) -> None:
        self.stop_event.set()
        self.store.audit("SERVICE_STOPPED")

    def status_json(self) -> dict[str, Any]:
        account = self.store.account()
        return {"service": "openclaw-quant", "mode": account.get("mode"), "paper_only": True, "event_driven": True, "started_at": self.status.started_at, "last_ticker_refresh": self.status.last_ticker_refresh, "last_scan": self.status.last_scan, "last_event": getattr(self.status, "last_event", None), "last_error": self.status.last_error, "websocket": {"connected": self.ws.connected, "last_error": self.ws.last_error}, "monitor_count": len(self.pool.pool()), "registry_count": len(self.symbol_registry.all()), "position_count": len(self.store.positions()), "equity": account.get("equity"), "kill_switch": bool(account.get("kill_switch")), "risk_actions": {"stop_new_trades": bool(account.get("stop_new_trades")), "stop_all": bool(account.get("stop_all")), "emergency_exit": bool(account.get("emergency_exit")), "reconciliation_ok": bool(account.get("reconciliation_ok", True))}}

    def close(self) -> None:
        self.stop()
        self.store.close()
