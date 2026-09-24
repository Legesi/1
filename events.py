"""Low-latency in-process event bus for market and risk events."""
from __future__ import annotations

import queue
import threading
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Callable


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True)
class Event:
    type: str
    symbol: str | None = None
    payload: dict[str, Any] = field(default_factory=dict)
    priority: int = 3
    created_at: str = field(default_factory=now_iso)
    event_id: str = ""


class EventBus:
    """Thread-safe queue with priority ordering and observable history."""

    def __init__(self, history_size: int = 500) -> None:
        self._queue: queue.PriorityQueue[tuple[int, int, Event]] = queue.PriorityQueue()
        self._handlers: dict[str, list[Callable[[Event], None]]] = {}
        self._history: list[Event] = []
        self._history_size = history_size
        self._sequence = 0
        self._lock = threading.RLock()

    def subscribe(self, event_type: str, handler: Callable[[Event], None]) -> None:
        with self._lock:
            self._handlers.setdefault(event_type, []).append(handler)

    def publish(self, event: Event) -> None:
        with self._lock:
            self._sequence += 1
            self._history.append(event)
            self._history = self._history[-self._history_size:]
            self._queue.put((event.priority, self._sequence, event))

    def get(self, timeout: float = 0.5) -> Event | None:
        try:
            return self._queue.get(timeout=timeout)[2]
        except queue.Empty:
            return None

    def dispatch(self, event: Event) -> None:
        with self._lock:
            handlers = list(self._handlers.get(event.type, [])) + list(self._handlers.get("*", []))
        for handler in handlers:
            try:
                handler(event)
            except Exception:
                # A broken observer cannot stop safety-critical consumers.
                continue

    def history(self, limit: int = 100) -> list[Event]:
        with self._lock:
            return list(self._history[-limit:])


EVENT_PRIORITY = {
    "RISK_EVENT": 0, "POSITION_EVENT": 0, "KLINE_CLOSED": 1, "PRICE_BREAKOUT": 1,
    "PRICE_BREAKDOWN": 1, "VOLUME_SPIKE": 1, "NEW_GAINER": 2, "NEW_LOSER": 2,
    "MARKET_REGIME_CHANGED": 2, "SETUP_FORMED": 2, "TICKER": 3, "MARKET_REFRESH": 3,
}


def make_event(event_type: str, symbol: str | None = None, payload: dict[str, Any] | None = None) -> Event:
    return Event(type=event_type, symbol=symbol, payload=payload or {}, priority=EVENT_PRIORITY.get(event_type, 3), event_id=f"{event_type}:{symbol or '-'}:{now_iso()}")
