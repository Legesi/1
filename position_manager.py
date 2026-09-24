"""PAPER position lifecycle with protection, partial exits and trailing stops."""
from __future__ import annotations

from typing import Any

from notifications import NotificationCenter
from storage import Store


class PositionManager:
    def __init__(self, config: dict[str, Any], store: Store, notifications: NotificationCenter) -> None:
        self.config = config
        self.store = store
        self.notifications = notifications

    def register(self, signal: dict[str, Any], quantity: float) -> None:
        entry = float(signal["entry"]["ideal"])
        stop = float(signal["stop_loss"])
        targets = signal["take_profit"]
        self.store.save_position_state(signal["symbol"], {
            "original_quantity": quantity,
            "remaining_quantity": quantity,
            "entry_price": entry,
            "initial_stop": stop,
            "stop_loss": stop,
            "tp1": float(targets["tp1"]), "tp2": float(targets["tp2"]), "tp3": float(targets["tp3"]),
            "tp1_done": False, "tp2_done": False,
            "highest_price": entry, "lowest_price": entry,
            "risk_distance": abs(entry - stop),
            "protected": stop > 0,
        })
        self.store.position_event(signal["symbol"], "OPENED", quantity, entry, 0.0, {"stop_loss": stop, "targets": targets})
        if stop > 0 and all(float(targets.get(key, 0)) > 0 for key in ("tp1", "tp2", "tp3")):
            self.store.position_event(signal["symbol"], "PROTECTED", quantity, entry, 0.0, {"stop_loss": stop, "tp": targets, "status": "POSITION_PROTECTED"})
        else:
            self.store.set_risk_action("STOP_NEW_TRADES", True)

    @staticmethod
    def _pnl(direction: str, entry: float, exit_price: float, quantity: float) -> float:
        return (exit_price - entry) * quantity * (1 if direction == "LONG" else -1)

    def on_price(self, symbol: str, price: float) -> list[dict[str, Any]]:
        position = self.store.position(symbol)
        state = self.store.position_state(symbol)
        if not position or not state or price <= 0:
            return []
        events: list[dict[str, Any]] = []
        direction = position["direction"]
        entry = float(position["entry_price"])
        remaining = float(position["quantity"])
        state["highest_price"] = max(float(state.get("highest_price", entry)), price)
        state["lowest_price"] = min(float(state.get("lowest_price", entry)), price)
        stop = float(state.get("stop_loss", position["stop_loss"]))
        hit_stop = price <= stop if direction == "LONG" else price >= stop
        if hit_stop:
            pnl = self._pnl(direction, entry, price, remaining)
            self.store.position_event(symbol, "STOP_FILLED", remaining, price, 0.0, {"stop_loss": stop})
            self.store.close_position(symbol, price, pnl)
            self.notifications.emit("STOP_LOSS", f"{symbol} PAPER stop filled at {price}", {"symbol": symbol, "pnl": pnl}, "HIGH")
            return [{"event": "STOP_FILLED", "quantity": remaining, "price": price, "pnl": pnl}]
        for label, fraction in (("tp1", float(self.config["execution"]["partial_take_profit"].get("tp1_percent", 0.3))), ("tp2", float(self.config["execution"]["partial_take_profit"].get("tp2_percent", 0.3)))):
            if state.get(f"{label}_done"):
                continue
            target = float(state[label])
            hit = price >= target if direction == "LONG" else price <= target
            if hit:
                quantity = min(float(state["original_quantity"]) * fraction, remaining)
                pnl = self._pnl(direction, entry, price, quantity)
                remaining -= quantity
                state[f"{label}_done"] = True
                if label == "tp1":
                    state["stop_loss"] = entry
                self.store.position_event(symbol, f"{label.upper()}_FILLED", quantity, price, pnl)
                self.store.update_position(symbol, remaining, float(state["stop_loss"]))
                self.notifications.emit(label.upper(), f"{symbol} PAPER {label.upper()} filled at {price}", {"symbol": symbol, "pnl": pnl})
                events.append({"event": f"{label.upper()}_FILLED", "quantity": quantity, "price": price, "pnl": pnl})
        tp3 = float(state["tp3"])
        hit_tp3 = price >= tp3 if direction == "LONG" else price <= tp3
        if hit_tp3 and remaining > 0:
            pnl = self._pnl(direction, entry, price, remaining)
            self.store.position_event(symbol, "TP3_FILLED", remaining, price, 0.0)
            self.store.close_position(symbol, price, pnl)
            self.notifications.emit("POSITION_CLOSED", f"{symbol} PAPER TP3 closed at {price}", {"symbol": symbol, "pnl": pnl})
            events.append({"event": "TP3_FILLED", "quantity": remaining, "price": price, "pnl": pnl})
            return events
        trailing = self.config["execution"].get("trailing_stop", {})
        if trailing.get("enabled") and state.get("tp1_done"):
            distance = float(state.get("risk_distance", 0)) * float(trailing.get("distance_r", 1.0))
            candidate = state["highest_price"] - distance if direction == "LONG" else state["lowest_price"] + distance
            old_stop = float(state["stop_loss"])
            new_stop = max(old_stop, candidate) if direction == "LONG" else min(old_stop, candidate)
            if new_stop != old_stop:
                state["stop_loss"] = new_stop
                self.store.update_position(symbol, remaining, new_stop)
                self.store.position_event(symbol, "TRAILING_STOP_MOVED", 0, new_stop, 0.0)
                events.append({"event": "TRAILING_STOP_MOVED", "price": new_stop})
        state["remaining_quantity"] = remaining
        self.store.save_position_state(symbol, state)
        return events

    def reconcile(self) -> dict[str, Any]:
        orphaned = []
        unprotected = []
        for position in self.store.positions():
            state = self.store.position_state(position["symbol"])
            if not state:
                orphaned.append(position["symbol"])
            elif not state.get("protected") or not position.get("stop_loss"):
                unprotected.append(position["symbol"])
        if orphaned or unprotected:
            self.store.set_risk_action("STOP_NEW_TRADES", True)
            self.notifications.emit("RECONCILIATION_FAILED", "PAPER reconciliation found unsafe positions", {"orphaned": orphaned, "unprotected": unprotected}, "CRITICAL")
        consistent = not orphaned and not unprotected
        self.store.set_reconciliation(consistent)
        return {"consistent": consistent, "orphaned": orphaned, "unprotected": unprotected, "source": "LOCAL_PAPER_LEDGER"}
