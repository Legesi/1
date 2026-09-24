"""Realtime cache and exchange-driven Symbol Registry."""
from __future__ import annotations

import threading
from typing import Any

from storage import Store, utc_now


class SymbolRegistry:
    def __init__(self, store: Store) -> None:
        self.store = store
        self._symbols: dict[str, dict[str, Any]] = {}
        self._lock = threading.RLock()

    @staticmethod
    def _filters(symbol: dict[str, Any]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for item in symbol.get("filters", []):
            filter_type = item.get("filterType")
            if filter_type == "PRICE_FILTER":
                result["tick_size"] = float(item.get("tickSize", 0))
            elif filter_type == "LOT_SIZE":
                result["step_size"] = float(item.get("stepSize", 0))
                result["min_qty"] = float(item.get("minQty", 0))
            elif filter_type in {"MIN_NOTIONAL", "NOTIONAL"}:
                result["min_notional"] = float(item.get("notional", item.get("minNotional", 0)))
        return result

    def refresh(self, symbols: list[dict[str, Any]]) -> list[dict[str, Any]]:
        with self._lock:
            self._symbols = {}
            for item in symbols:
                symbol = str(item.get("symbol", "")).upper()
                if not symbol:
                    continue
                market_type = "EQUITY" if any(token in symbol for token in ("TSLA", "NVDA", "MSTR", "COIN", "AMZN", "PLTR")) else "CRYPTO"
                parsed = {"symbol": symbol, "market_type": market_type, "status": item.get("status"), "base_asset": item.get("baseAsset"), "quote_asset": item.get("quoteAsset"), "contract_type": item.get("contractType"), "price_precision": item.get("pricePrecision"), "quantity_precision": item.get("quantityPrecision"), "leverage_rules": item.get("maintMarginPercent"), **self._filters(item), "updated_at": utc_now()}
                self._symbols[symbol] = parsed
                self.store.upsert_symbol_registry(parsed)
            return list(self._symbols.values())

    def get(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            return self._symbols.get(symbol.upper()) or self.store.symbol_registry(symbol.upper())

    def all(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._symbols.values()) or self.store.symbol_registry_all()

    def active(self) -> list[str]:
        return [item["symbol"] for item in self.all() if item.get("status") == "TRADING"]


class RealtimeMarketData:
    def __init__(self, max_klines: int = 500) -> None:
        self.max_klines = max_klines
        self.tickers: dict[str, dict[str, Any]] = {}
        self.klines: dict[str, dict[str, list[list[Any]]]] = {}
        self._lock = threading.RLock()

    def update_ticker(self, data: dict[str, Any]) -> dict[str, Any]:
        symbol = str(data.get("s", data.get("symbol", ""))).upper()
        event_type = data.get("e")
        last_price = data.get("c", data.get("lastPrice", data.get("p", data.get("price", 0))))
        mark_price = data.get("p", data.get("markPrice", last_price)) if event_type in ("markPriceUpdate", "trade") else data.get("markPrice", last_price)
        ticker = {"symbol": symbol, "last_price": float(last_price or 0), "mark_price": float(mark_price or 0), "bid": float(data.get("b", data.get("bidPrice", 0)) or 0), "ask": float(data.get("a", data.get("askPrice", 0)) or 0), "volume": float(data.get("v", data.get("volume", data.get("q", 0))) or 0), "quote_volume": float(data.get("q", data.get("quoteVolume", 0)) or 0), "change_percent": float(data.get("P", data.get("priceChangePercent", 0)) or 0), "high": float(data.get("h", data.get("highPrice", 0)) or 0), "low": float(data.get("l", data.get("lowPrice", 0)) or 0), "rank": data.get("rank"), "event_type": event_type, "event_time": int(data.get("E", data.get("event_time", 0)) or 0), "updated_at": utc_now()}
        ticker["spread"] = max(0.0, ticker["ask"] - ticker["bid"]) if ticker["ask"] and ticker["bid"] else 0.0
        with self._lock:
            self.tickers[symbol] = ticker
        return ticker

    def update_kline(self, data: dict[str, Any]) -> tuple[str, str, list[Any], bool]:
        kline = data.get("k", data)
        symbol = str(data.get("s", kline.get("s", ""))).upper()
        interval = str(kline.get("i", data.get("interval", "1m")))
        row = [kline.get("t", 0), kline.get("o", 0), kline.get("h", 0), kline.get("l", 0), kline.get("c", 0), kline.get("v", 0), kline.get("T", 0), kline.get("q", 0), kline.get("n", 0), kline.get("V", 0), kline.get("Q", 0), kline.get("B", 0)]
        with self._lock:
            bars = self.klines.setdefault(symbol, {}).setdefault(interval, [])
            if bars and bars[-1][0] == row[0]:
                bars[-1] = row
            else:
                bars.append(row)
                del bars[:-self.max_klines]
        return symbol, interval, row, bool(kline.get("x", data.get("closed", False)))

    def ticker(self, symbol: str) -> dict[str, Any] | None:
        with self._lock:
            return self.tickers.get(symbol.upper())

    def bars(self, symbol: str, interval: str) -> list[list[Any]]:
        with self._lock:
            return list(self.klines.get(symbol.upper(), {}).get(interval, []))
