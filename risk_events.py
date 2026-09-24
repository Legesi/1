"""Deterministic emergency risk checks; AI is only an observer."""
from __future__ import annotations

from typing import Any

from events import EventBus, make_event
from storage import Store


class EmergencyRiskEngine:
    def __init__(self, config: dict[str, Any], store: Store, bus: EventBus) -> None:
        self.config = config
        self.store = store
        self.bus = bus

    def evaluate_ticker(self, ticker: dict[str, Any]) -> list[dict[str, Any]]:
        events = []
        symbol = ticker.get("symbol")
        spread = float(ticker.get("spread", 0))
        price = float(ticker.get("last_price", 0))
        bid = float(ticker.get("bid", 0))
        ask = float(ticker.get("ask", 0))
        spread_limit = float(self.config.get("max_spread_percent", 0.01))
        if price and spread / price > spread_limit:
            event = make_event("RISK_EVENT", symbol, {"kind": "SPREAD_EXPANDED", "spread": spread, "spread_percent": spread / price})
            self.bus.publish(event)
            events.append({"kind": "SPREAD_EXPANDED", "symbol": symbol})
        if bid and ask and (ask <= 0 or bid <= 0):
            self.bus.publish(make_event("RISK_EVENT", symbol, {"kind": "LIQUIDITY_DROPPED"}))
        return events

    def evaluate_position(self, position: dict[str, Any], ticker: dict[str, Any] | None) -> list[dict[str, Any]]:
        events = []
        if not position.get("stop_loss"):
            event = make_event("RISK_EVENT", position["symbol"], {"kind": "UNPROTECTED_POSITION", "action": "STOP_NEW_TRADES"})
            self.bus.publish(event)
            self.store.set_kill_switch(True)
            events.append({"kind": "UNPROTECTED_POSITION", "symbol": position["symbol"]})
        if ticker:
            entry = float(position["entry_price"])
            mark = float(ticker.get("mark_price") or ticker.get("last_price") or 0)
            if entry and mark and abs(mark - entry) / entry > float(self.config.get("emergency_move_percent", 0.15)):
                self.bus.publish(make_event("RISK_EVENT", position["symbol"], {"kind": "EXTREME_MOVE", "entry": entry, "mark": mark}))
                events.append({"kind": "EXTREME_MOVE", "symbol": position["symbol"]})
        return events
