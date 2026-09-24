"""Binance user-data stream boundary.

The adapter is deliberately disabled for PAPER mode. When credentials and an approved
execution mode are supplied, this module is the single place allowed to consume
ORDER_TRADE_UPDATE, ACCOUNT_UPDATE and MARGIN_CALL events; it never exposes secrets to
the strategy or Agent layers.
"""
from __future__ import annotations

import os
import threading
from typing import Any, Callable


class AccountStreamDisabled(RuntimeError):
    pass


class BinanceUserDataStream(threading.Thread):
    daemon = True

    def __init__(self, mode: str, on_event: Callable[[dict[str, Any]], None], stop: threading.Event) -> None:
        super().__init__(name="binance-account-stream")
        self.mode = mode.lower()
        self.on_event = on_event
        self.stop_event = stop
        self.connected = False
        self.last_error: str | None = None

    def run(self) -> None:
        if self.mode == "paper":
            self.last_error = "disabled in PAPER mode"
            return
        if not os.environ.get("BINANCE_API_KEY") or not os.environ.get("BINANCE_API_SECRET"):
            self.last_error = "BINANCE_API_KEY and BINANCE_API_SECRET are required via environment only"
            return
        # The signed listen-key and private stream implementation is intentionally isolated
        # here for the Testnet phase; no strategy code can call it directly.
        self.last_error = "private user-data adapter is not enabled in this staged PAPER deployment"

    def require_enabled(self) -> None:
        if self.mode == "paper":
            raise AccountStreamDisabled("user-data stream is disabled in PAPER mode")
