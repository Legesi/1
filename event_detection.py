"""Deterministic market event detection. No LLM calls happen here."""
from __future__ import annotations

from typing import Any

from events import EventBus, make_event
from indicators import indicator_snapshot
from market_data import RealtimeMarketData


class EventDetector:
    def __init__(self, cache: RealtimeMarketData, bus: EventBus) -> None:
        self.cache = cache
        self.bus = bus
        self._last_regime: dict[str, str] = {}
        self._last_rank: dict[str, int] = {}

    def on_ticker(self, ticker: dict[str, Any]) -> None:
        symbol = ticker["symbol"]
        self.bus.publish(make_event("TICKER", symbol, ticker))
        previous_rank = self._last_rank.get(symbol)
        rank = ticker.get("rank")
        if rank is not None and previous_rank and previous_rank - int(rank) >= 20 and float(ticker.get("change_percent", 0)) > 0:
            self.bus.publish(make_event("NEW_GAINER", symbol, {"old_rank": previous_rank, "rank": rank, "ticker": ticker}))
        if rank is not None:
            self._last_rank[symbol] = int(rank)

    def on_kline(self, symbol: str, interval: str, row: list[Any], closed: bool) -> None:
        if not closed:
            return
        bars = self.cache.bars(symbol, interval)
        payload = {"interval": interval, "open_time": row[0], "close": float(row[4]), "bars": len(bars)}
        self.bus.publish(make_event("KLINE_CLOSED", symbol, payload))
        if len(bars) < 25:
            return
        snapshot = indicator_snapshot(bars)
        close = float(row[4])
        previous = float(bars[-2][4])
        if close > snapshot["bb_upper"] and previous <= snapshot["bb_upper"]:
            self.bus.publish(make_event("PRICE_BREAKOUT", symbol, {"interval": interval, "price": close, "level": snapshot["bb_upper"]}))
        if close < snapshot["bb_lower"] and previous >= snapshot["bb_lower"]:
            self.bus.publish(make_event("PRICE_BREAKDOWN", symbol, {"interval": interval, "price": close, "level": snapshot["bb_lower"]}))
        if snapshot["volume_ratio"] >= 2.0:
            self.bus.publish(make_event("VOLUME_SPIKE", symbol, {"interval": interval, "ratio": snapshot["volume_ratio"]}))
        regime = "TREND_UP" if snapshot["ema20"] > snapshot["ema50"] > snapshot["ema200"] else "TREND_DOWN" if snapshot["ema20"] < snapshot["ema50"] < snapshot["ema200"] else "RANGE"
        if self._last_regime.get(f"{symbol}:{interval}") != regime:
            self._last_regime[f"{symbol}:{interval}"] = regime
            self.bus.publish(make_event("MARKET_REGIME_CHANGED", symbol, {"interval": interval, "regime": regime}))
