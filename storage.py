"""SQLite persistence. SQLite keeps the PAPER deployment portable and auditable."""
from __future__ import annotations

import json
import sqlite3
import threading
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path: str | Path, initial_equity: float = 10000.0) -> None:
        self.path = str(path)
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self._init_schema(initial_equity)

    def _init_schema(self, initial_equity: float) -> None:
        with self.db:
            self.db.executescript("""
            CREATE TABLE IF NOT EXISTS account (id INTEGER PRIMARY KEY CHECK (id = 1), equity REAL NOT NULL, mode TEXT NOT NULL, kill_switch INTEGER NOT NULL DEFAULT 0, stop_new_trades INTEGER NOT NULL DEFAULT 0, stop_all INTEGER NOT NULL DEFAULT 0, emergency_exit INTEGER NOT NULL DEFAULT 0, reconciliation_ok INTEGER NOT NULL DEFAULT 1, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS monitor_symbols (symbol TEXT PRIMARY KEY, source TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, priority INTEGER NOT NULL DEFAULT 2, strategy TEXT, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS config_versions (id INTEGER PRIMARY KEY AUTOINCREMENT, version TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS strategies (name TEXT NOT NULL, version TEXT NOT NULL, payload TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, created_at TEXT NOT NULL, PRIMARY KEY(name, version));
            CREATE TABLE IF NOT EXISTS strategy_bindings (binding_type TEXT NOT NULL, binding_key TEXT NOT NULL, strategy_name TEXT NOT NULL, strategy_version TEXT NOT NULL, updated_at TEXT NOT NULL, PRIMARY KEY(binding_type, binding_key));
            CREATE TABLE IF NOT EXISTS symbol_registry (symbol TEXT PRIMARY KEY, market_type TEXT NOT NULL, status TEXT NOT NULL, tick_size REAL, step_size REAL, min_qty REAL, min_notional REAL, leverage_rules TEXT, price_precision INTEGER, quantity_precision INTEGER, payload TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS signal_cooldowns (cooldown_key TEXT PRIMARY KEY, symbol TEXT NOT NULL, direction TEXT NOT NULL, strategy TEXT NOT NULL, setup TEXT NOT NULL, expires_at TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS signals (signal_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, direction TEXT NOT NULL, strategy TEXT NOT NULL, strategy_version TEXT NOT NULL, payload TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS orders (order_id TEXT PRIMARY KEY, signal_id TEXT NOT NULL, symbol TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL, quantity REAL NOT NULL, price REAL NOT NULL, status TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS positions (symbol TEXT PRIMARY KEY, direction TEXT NOT NULL, quantity REAL NOT NULL, entry_price REAL NOT NULL, stop_loss REAL, take_profit REAL, strategy TEXT NOT NULL, strategy_version TEXT NOT NULL, opened_at TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS trades (trade_id TEXT PRIMARY KEY, order_id TEXT NOT NULL, symbol TEXT NOT NULL, direction TEXT NOT NULL, quantity REAL NOT NULL, entry_price REAL NOT NULL, exit_price REAL, pnl REAL, status TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL, closed_at TEXT);
            CREATE TABLE IF NOT EXISTS audit_log (id INTEGER PRIMARY KEY AUTOINCREMENT, event TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS position_events (id INTEGER PRIMARY KEY AUTOINCREMENT, symbol TEXT NOT NULL, event TEXT NOT NULL, quantity REAL NOT NULL, price REAL NOT NULL, pnl REAL NOT NULL DEFAULT 0, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS position_state (symbol TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS backtest_runs (run_id TEXT PRIMARY KEY, symbol TEXT NOT NULL, strategy TEXT NOT NULL, strategy_version TEXT NOT NULL, request TEXT NOT NULL, result TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS notifications (id INTEGER PRIMARY KEY AUTOINCREMENT, level TEXT NOT NULL, event TEXT NOT NULL, message TEXT NOT NULL, payload TEXT NOT NULL, delivered INTEGER NOT NULL DEFAULT 0, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS agent_reports (report_id TEXT PRIMARY KEY, agent TEXT NOT NULL, report_type TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS system_events (event_id TEXT PRIMARY KEY, event_type TEXT NOT NULL, symbol TEXT, priority INTEGER NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL);
            """)
            if self.db.execute("SELECT 1 FROM account WHERE id = 1").fetchone() is None:
                self.db.execute("INSERT INTO account(id,equity,mode,updated_at) VALUES(1,?,?,?)", (initial_equity, "paper", utc_now()))
            self._ensure_account_columns()
            self._ensure_monitor_columns()

    def _ensure_account_columns(self) -> None:
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(account)").fetchall()}
        for name, definition in {
            "stop_new_trades": "INTEGER NOT NULL DEFAULT 0",
            "stop_all": "INTEGER NOT NULL DEFAULT 0",
            "emergency_exit": "INTEGER NOT NULL DEFAULT 0",
            "reconciliation_ok": "INTEGER NOT NULL DEFAULT 1",
        }.items():
            if name not in columns:
                self.db.execute(f"ALTER TABLE account ADD COLUMN {name} {definition}")

    def _ensure_monitor_columns(self) -> None:
        columns = {row[1] for row in self.db.execute("PRAGMA table_info(monitor_symbols)").fetchall()}
        if "priority" not in columns:
            self.db.execute("ALTER TABLE monitor_symbols ADD COLUMN priority INTEGER NOT NULL DEFAULT 2")

    def _row(self, row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row else None

    def audit(self, event: str, payload: dict[str, Any] | None = None) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO audit_log(event,payload,created_at) VALUES(?,?,?)", (event, json.dumps(payload or {}, ensure_ascii=False), utc_now()))

    def account(self) -> dict[str, Any]:
        return self._row(self.db.execute("SELECT * FROM account WHERE id=1").fetchone()) or {}

    def set_mode(self, mode: str) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE account SET mode=?, updated_at=? WHERE id=1", (mode, utc_now()))
        self.audit("MODE_CHANGED", {"mode": mode})

    def set_kill_switch(self, enabled: bool) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE account SET kill_switch=?, stop_new_trades=?, updated_at=? WHERE id=1", (int(enabled), int(enabled), utc_now()))
        self.audit("KILL_SWITCH_CHANGED", {"enabled": enabled})

    def set_risk_action(self, action: str, enabled: bool) -> None:
        action = action.upper()
        columns = {"STOP_NEW_TRADES": "stop_new_trades", "STOP_ALL": "stop_all", "EMERGENCY_EXIT": "emergency_exit"}
        if action not in columns:
            raise ValueError("risk action must be STOP_NEW_TRADES, STOP_ALL or EMERGENCY_EXIT")
        with self._lock, self.db:
            if action == "STOP_NEW_TRADES":
                self.db.execute("UPDATE account SET stop_new_trades=?,updated_at=? WHERE id=1", (int(enabled), utc_now()))
            elif action == "STOP_ALL":
                self.db.execute("UPDATE account SET stop_all=?,stop_new_trades=CASE WHEN ? THEN 1 ELSE stop_new_trades END,updated_at=? WHERE id=1", (int(enabled), int(enabled), utc_now()))
            else:
                self.db.execute("UPDATE account SET emergency_exit=?,stop_new_trades=CASE WHEN ? THEN 1 ELSE stop_new_trades END,updated_at=? WHERE id=1", (int(enabled), int(enabled), utc_now()))
        self.audit("RISK_ACTION_CHANGED", {"action": action, "enabled": enabled})

    def set_reconciliation(self, consistent: bool) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE account SET reconciliation_ok=?, stop_new_trades=CASE WHEN ? THEN stop_new_trades ELSE 1 END, updated_at=? WHERE id=1", (int(consistent), int(consistent), utc_now()))

    def save_config(self, payload: dict[str, Any]) -> str:
        version = f"config-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"
        with self._lock, self.db:
            self.db.execute("INSERT INTO config_versions(version,payload,created_at) VALUES(?,?,?)", (version, json.dumps(payload, ensure_ascii=False), utc_now()))
        self.audit("CONFIG_VERSION_CREATED", {"version": version})
        return version

    def upsert_monitor(self, symbol: str, source: str = "CUSTOM", enabled: bool = True, strategy: str | None = None, priority: int = 2) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO monitor_symbols(symbol,source,enabled,priority,strategy,updated_at) VALUES(?,?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET source=excluded.source,enabled=excluded.enabled,priority=excluded.priority,strategy=excluded.strategy,updated_at=excluded.updated_at", (symbol, source, int(enabled), max(0, min(4, int(priority))), strategy, utc_now()))
        self.audit("MONITOR_SYMBOL_UPDATED", {"symbol": symbol, "source": source, "enabled": enabled, "priority": priority})

    def remove_monitor(self, symbol: str) -> None:
        with self._lock, self.db:
            self.db.execute("DELETE FROM monitor_symbols WHERE symbol=?", (symbol,))
        self.audit("MONITOR_SYMBOL_REMOVED", {"symbol": symbol})

    def set_monitor_enabled(self, symbol: str, enabled: bool) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE monitor_symbols SET enabled=?,updated_at=? WHERE symbol=?", (int(enabled), utc_now(), symbol))
        self.audit("MONITOR_SYMBOL_TOGGLED", {"symbol": symbol, "enabled": enabled})

    def set_monitor_priority(self, symbol: str, priority: int) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE monitor_symbols SET priority=?,updated_at=? WHERE symbol=?", (max(0, min(4, int(priority))), utc_now(), symbol))
        self.audit("MONITOR_SYMBOL_PRIORITY_CHANGED", {"symbol": symbol, "priority": priority})

    def monitors(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.db.execute("SELECT * FROM monitor_symbols ORDER BY source,symbol").fetchall()]

    def active_symbols(self) -> list[str]:
        return [row[0] for row in self.db.execute("SELECT symbol FROM monitor_symbols WHERE enabled=1 ORDER BY priority,symbol").fetchall()]

    def upsert_symbol_registry(self, symbol: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO symbol_registry(symbol,market_type,status,tick_size,step_size,min_qty,min_notional,leverage_rules,price_precision,quantity_precision,payload,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(symbol) DO UPDATE SET market_type=excluded.market_type,status=excluded.status,tick_size=excluded.tick_size,step_size=excluded.step_size,min_qty=excluded.min_qty,min_notional=excluded.min_notional,leverage_rules=excluded.leverage_rules,price_precision=excluded.price_precision,quantity_precision=excluded.quantity_precision,payload=excluded.payload,updated_at=excluded.updated_at", (symbol["symbol"], symbol.get("market_type", "CRYPTO"), symbol.get("status", "UNKNOWN"), symbol.get("tick_size"), symbol.get("step_size"), symbol.get("min_qty"), symbol.get("min_notional"), json.dumps(symbol.get("leverage_rules"), ensure_ascii=False), symbol.get("price_precision"), symbol.get("quantity_precision"), json.dumps(symbol, ensure_ascii=False), symbol.get("updated_at", utc_now())))

    def symbol_registry(self, symbol: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM symbol_registry WHERE symbol=?", (symbol,)).fetchone()
        if not row:
            return None
        result = dict(row)
        result["leverage_rules"] = json.loads(result["leverage_rules"] or "null")
        result["payload"] = json.loads(result["payload"])
        return result

    def symbol_registry_all(self) -> list[dict[str, Any]]:
        return [self.symbol_registry(row[0]) for row in self.db.execute("SELECT symbol FROM symbol_registry ORDER BY symbol").fetchall()]

    def save_event(self, event: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT OR IGNORE INTO system_events(event_id,event_type,symbol,priority,payload,created_at) VALUES(?,?,?,?,?,?)", (event["event_id"], event["type"], event.get("symbol"), event.get("priority", 3), json.dumps(event.get("payload", {}), ensure_ascii=False), event.get("created_at", utc_now())))

    def system_events(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM system_events ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def cooldown_active(self, cooldown_key: str, now: str | None = None) -> bool:
        now = now or utc_now()
        row = self.db.execute("SELECT 1 FROM signal_cooldowns WHERE cooldown_key=? AND expires_at>?", (cooldown_key, now)).fetchone()
        return row is not None

    def save_cooldown(self, cooldown_key: str, symbol: str, direction: str, strategy: str, setup: str, expires_at: str) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO signal_cooldowns(cooldown_key,symbol,direction,strategy,setup,expires_at,created_at) VALUES(?,?,?,?,?,?,?)", (cooldown_key, symbol, direction, strategy, setup, expires_at, utc_now()))

    def save_strategy(self, name: str, version: str, payload: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO strategies(name,version,payload,enabled,created_at) VALUES(?,?,?,?,?)", (name, version, json.dumps(payload, ensure_ascii=False), 1, utc_now()))
        self.audit("STRATEGY_VERSION_CREATED", {"name": name, "version": version})

    def strategies(self) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM strategies ORDER BY name,created_at DESC").fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def strategy(self, name: str, version: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT * FROM strategies WHERE name=? AND version=?", (name, version)).fetchone()
        return {**dict(row), "payload": json.loads(row["payload"])} if row else None

    def set_strategy_enabled(self, name: str, version: str, enabled: bool) -> bool:
        with self._lock, self.db:
            cursor = self.db.execute("UPDATE strategies SET enabled=? WHERE name=? AND version=?", (int(enabled), name, version))
        if cursor.rowcount:
            self.audit("STRATEGY_TOGGLED", {"name": name, "version": version, "enabled": enabled})
        return bool(cursor.rowcount)

    def delete_strategy(self, name: str, version: str) -> bool:
        with self._lock, self.db:
            binding = self.db.execute("SELECT 1 FROM strategy_bindings WHERE strategy_name=? AND strategy_version=? LIMIT 1", (name, version)).fetchone()
            if binding:
                raise ValueError("strategy version is bound; unbind it before deletion")
            cursor = self.db.execute("DELETE FROM strategies WHERE name=? AND version=?", (name, version))
        if cursor.rowcount:
            self.audit("STRATEGY_DELETED", {"name": name, "version": version})
        return bool(cursor.rowcount)

    def bind_strategy(self, binding_type: str, binding_key: str, name: str, version: str) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO strategy_bindings(binding_type,binding_key,strategy_name,strategy_version,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(binding_type,binding_key) DO UPDATE SET strategy_name=excluded.strategy_name,strategy_version=excluded.strategy_version,updated_at=excluded.updated_at", (binding_type, binding_key, name, version, utc_now()))
        self.audit("STRATEGY_BOUND", {"binding_type": binding_type, "binding_key": binding_key, "name": name, "version": version})

    def strategy_bindings(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.db.execute("SELECT * FROM strategy_bindings ORDER BY binding_type,binding_key").fetchall()]

    def signal_exists(self, signal_id: str) -> bool:
        return self.db.execute("SELECT 1 FROM signals WHERE signal_id=?", (signal_id,)).fetchone() is not None

    def save_signal(self, signal: dict[str, Any]) -> bool:
        with self._lock, self.db:
            if self.signal_exists(signal["signal_id"]):
                return False
            self.db.execute("INSERT INTO signals(signal_id,symbol,direction,strategy,strategy_version,payload,status,created_at) VALUES(?,?,?,?,?,?,?,?)", (signal["signal_id"], signal["symbol"], signal["direction"], signal["strategy"], signal["strategy_version"], json.dumps(signal, ensure_ascii=False), signal.get("status", "CANDIDATE"), signal["created_at"]))
            return True

    def update_signal(self, signal: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("UPDATE signals SET payload=?,status=? WHERE signal_id=?", (json.dumps(signal, ensure_ascii=False), signal.get("status", "CANDIDATE"), signal["signal_id"]))

    def signals(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT payload FROM signals ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def positions(self) -> list[dict[str, Any]]:
        return [dict(row) for row in self.db.execute("SELECT * FROM positions ORDER BY opened_at").fetchall()]

    def position(self, symbol: str) -> dict[str, Any] | None:
        return self._row(self.db.execute("SELECT * FROM positions WHERE symbol=?", (symbol,)).fetchone())

    def save_order(self, order: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO orders(order_id,signal_id,symbol,side,order_type,quantity,price,status,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)", (order["order_id"], order["signal_id"], order["symbol"], order["side"], order["order_type"], order["quantity"], order["price"], order["status"], json.dumps(order, ensure_ascii=False), order["created_at"]))

    def orders(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT payload FROM orders ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [json.loads(row[0]) for row in rows]

    def open_position(self, position: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT OR REPLACE INTO positions(symbol,direction,quantity,entry_price,stop_loss,take_profit,strategy,strategy_version,opened_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?)", tuple(position[k] for k in ("symbol", "direction", "quantity", "entry_price", "stop_loss", "take_profit", "strategy", "strategy_version", "opened_at", "updated_at")))

    def save_position_state(self, symbol: str, payload: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO position_state(symbol,payload,updated_at) VALUES(?,?,?) ON CONFLICT(symbol) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at", (symbol, json.dumps(payload, ensure_ascii=False), utc_now()))

    def position_state(self, symbol: str) -> dict[str, Any] | None:
        row = self.db.execute("SELECT payload FROM position_state WHERE symbol=?", (symbol,)).fetchone()
        return json.loads(row[0]) if row else None

    def update_position(self, symbol: str, quantity: float, stop_loss: float | None = None, take_profit: float | None = None) -> None:
        with self._lock, self.db:
            current = self.position(symbol)
            if not current:
                return
            self.db.execute("UPDATE positions SET quantity=?,stop_loss=?,take_profit=?,updated_at=? WHERE symbol=?", (quantity, stop_loss if stop_loss is not None else current["stop_loss"], take_profit if take_profit is not None else current["take_profit"], utc_now(), symbol))

    def position_event(self, symbol: str, event: str, quantity: float, price: float, pnl: float = 0.0, payload: dict[str, Any] | None = None) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO position_events(symbol,event,quantity,price,pnl,payload,created_at) VALUES(?,?,?,?,?,?,?)", (symbol, event, quantity, price, pnl, json.dumps(payload or {}, ensure_ascii=False), utc_now()))
            if pnl:
                self.db.execute("UPDATE account SET equity=equity+?,updated_at=? WHERE id=1", (pnl, utc_now()))
                self.db.execute("UPDATE trades SET pnl=COALESCE(pnl,0)+? WHERE symbol=? AND status='OPEN'", (pnl, symbol))
        self.audit("POSITION_" + event, {"symbol": symbol, "quantity": quantity, "price": price, "pnl": pnl})

    def position_events(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM position_events ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def close_position(self, symbol: str, exit_price: float, pnl: float) -> None:
        with self._lock, self.db:
            pos = self.position(symbol)
            if pos:
                self.db.execute("DELETE FROM positions WHERE symbol=?", (symbol,))
                self.db.execute("DELETE FROM position_state WHERE symbol=?", (symbol,))
                self.db.execute("UPDATE trades SET exit_price=?,pnl=COALESCE(pnl,0)+?,status='CLOSED',closed_at=? WHERE symbol=? AND status='OPEN'", (exit_price, pnl, utc_now(), symbol))
                self.db.execute("UPDATE account SET equity=equity+?,updated_at=? WHERE id=1", (pnl, utc_now()))
        self.audit("POSITION_CLOSED", {"symbol": symbol, "exit_price": exit_price, "pnl": pnl})

    def save_trade(self, trade: dict[str, Any]) -> None:
        with self._lock, self.db:
            self.db.execute("INSERT INTO trades(trade_id,order_id,symbol,direction,quantity,entry_price,pnl,status,payload,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)", (trade["trade_id"], trade["order_id"], trade["symbol"], trade["direction"], trade["quantity"], trade["entry_price"], 0.0, "OPEN", json.dumps(trade, ensure_ascii=False), trade["created_at"]))

    def trades(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM trades ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [dict(row) for row in rows]

    def save_backtest(self, request: dict[str, Any], result: dict[str, Any], status: str = "COMPLETED") -> str:
        run_id = result.get("run_id") or f"bt-{uuid.uuid4().hex[:16]}"
        result["run_id"] = run_id
        with self._lock, self.db:
            self.db.execute("INSERT INTO backtest_runs(run_id,symbol,strategy,strategy_version,request,result,status,created_at) VALUES(?,?,?,?,?,?,?,?)", (run_id, request["symbol"], request["strategy"], request["strategy_version"], json.dumps(request, ensure_ascii=False), json.dumps(result, ensure_ascii=False), status, utc_now()))
        self.audit("BACKTEST_COMPLETED", {"run_id": run_id, "symbol": request["symbol"], "status": status})
        return run_id

    def backtests(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM backtest_runs ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "request": json.loads(row["request"]), "result": json.loads(row["result"])} for row in rows]

    def notify(self, level: str, event: str, message: str, payload: dict[str, Any] | None = None, delivered: bool = False) -> int:
        with self._lock, self.db:
            cursor = self.db.execute("INSERT INTO notifications(level,event,message,payload,delivered,created_at) VALUES(?,?,?,?,?,?)", (level, event, message, json.dumps(payload or {}, ensure_ascii=False), int(delivered), utc_now()))
            return int(cursor.lastrowid)

    def notifications(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM notifications ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def save_agent_report(self, agent: str, report_type: str, payload: dict[str, Any]) -> str:
        report_id = f"report-{uuid.uuid4().hex[:16]}"
        with self._lock, self.db:
            self.db.execute("INSERT INTO agent_reports(report_id,agent,report_type,payload,created_at) VALUES(?,?,?,?,?)", (report_id, agent, report_type, json.dumps(payload, ensure_ascii=False), utc_now()))
        return report_id

    def agent_reports(self, limit: int = 100) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM agent_reports ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def audit_events(self, limit: int = 200) -> list[dict[str, Any]]:
        rows = self.db.execute("SELECT * FROM audit_log ORDER BY created_at DESC LIMIT ?", (limit,)).fetchall()
        return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def close(self) -> None:
        self.db.close()
