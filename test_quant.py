import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from indicators import indicator_snapshot, rsi
from quant_engine import MonitorPool, PaperExecutor, RiskEngine, StrategyEngine
from backtest import BacktestEngine
from notifications import NotificationCenter
from position_manager import PositionManager
from storage import Store
from strategy_manager import StrategyManager
from events import EventBus
from event_detection import EventDetector
from market_data import RealtimeMarketData, SymbolRegistry
from stop_loss import StopLossManager


def frame(length=250, start=100.0, step=0.2, volume=100.0):
    rows = []
    for i in range(length):
        close = start + i * step
        rows.append([i * 60_000, close - 0.05, close + 0.4, close - 0.4, close, volume * (2 if i == length - 1 else 1)])
    return rows


class QuantTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.config = {
            "monitor": {"custom_symbols": [], "blacklist_symbols": [], "gainers_top_n": 20, "losers_top_n": 10, "min_change_percent": 5, "max_change_percent": 100, "min_quote_volume": 0, "min_volume": 0, "min_price": 0, "max_price": 1e9},
            "strategy": {"default": "default_trend_v1", "min_volume_ratio": 1.2, "min_rr": 1.8, "min_confidence": 60},
            "risk": {"risk_per_trade": 0.005, "max_positions": 5, "kill_switch": False},
            "execution": {"partial_take_profit": {"tp1_percent": 0.3, "tp2_percent": 0.3, "tp3_percent": 0.4}, "trailing_stop": {"enabled": True, "distance_r": 1.0}},
            "backtest": {"initial_capital": 10000, "fee_rate": 0.0004, "slippage": 0.0002, "max_holding_bars": 12, "warmup_bars": 30},
            "notifications": {"in_app": True, "outbound_enabled": False},
        }
        self.store = Store(Path(self.tmp.name) / "test.sqlite3", 10000)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_indicators(self):
        candles = frame()
        snap = indicator_snapshot(candles)
        self.assertGreater(snap["ema20"], snap["ema50"])
        self.assertGreater(rsi([float(i) for i in range(40)]), 90)

    def test_pool_blacklist_and_ranking(self):
        pool = MonitorPool(self.config, self.store)
        pool.refresh([
            {"symbol": "AAAUSDT", "priceChangePercent": "12", "quoteVolume": "100", "volume": "10", "lastPrice": "2"},
            {"symbol": "BBBUSDT", "priceChangePercent": "20", "quoteVolume": "100", "volume": "10", "lastPrice": "2"},
        ])
        self.assertEqual(pool.gainers[0]["symbol"], "BBBUSDT")
        self.assertIn("BBBUSDT", pool.pool())

    def test_default_strategy_and_paper_risk(self):
        strategy = StrategyEngine(self.config)
        frames = {key: frame() for key in ("4h", "1h", "15m", "5m")}
        signal = strategy.signal("BTCUSDT", frames, {"lastPrice": "150"})
        self.assertIsNotNone(signal)
        check = RiskEngine(self.config, self.store).check(signal)
        self.assertTrue(check["approved"])
        result = PaperExecutor(self.config, self.store).execute(signal)
        self.assertTrue(result["approved"])
        self.assertEqual(len(self.store.positions()), 1)
        self.assertEqual(self.store.orders()[0]["status"], "FILLED")

    def test_strategy_versions_and_binding(self):
        manager = StrategyManager(self.store)
        revised = manager.revise("Default Trend Pullback", "1.0", {"description": "revised"})
        self.assertEqual(revised["version"], "1.1")
        manager.bind("SYMBOL", "BTCUSDT", revised["name"], revised["version"])
        self.assertEqual(manager.resolve("BTCUSDT")["version"], "1.1")
        with self.assertRaises(ValueError):
            manager.create({**revised, "name": "unsafe", "version": "1.0", "conditions": {"indicator": "PYTHON", "operator": ">", "value": 0}})

    def test_backtest_no_lookahead_and_metrics(self):
        strategy = {
            "name": "Always Long Test", "version": "1.0", "description": "test", "direction": "LONG",
            "timeframes": {"trend": "4h", "main": "1h", "setup": "15m", "confirmation": "5m", "entry": "1m"},
            "conditions": {"indicator": "RSI14", "operator": ">", "value": 0},
            "risk": {"min_rr": 1.8, "risk_per_trade": 0.005, "stop_atr": 1.5, "tp1_r": 1, "tp2_r": 2, "tp3_r": 3}, "enabled": True,
        }
        result = BacktestEngine(self.config).run("BTCUSDT", frame(160, step=0.1), strategy)
        self.assertGreater(result["metrics"]["total_trades"], 0)
        self.assertEqual(result["assumptions"]["fill_timing"], "next bar open")
        self.assertIn("max_drawdown", result["metrics"])

    def test_partial_take_profit_and_trailing(self):
        notifications = NotificationCenter(self.config, self.store)
        manager = PositionManager(self.config, self.store, notifications)
        signal = {"signal_id": "signal-1", "symbol": "BTCUSDT", "direction": "LONG", "strategy": "Test", "strategy_version": "1.0", "entry": {"min": 100, "max": 100, "ideal": 100}, "stop_loss": 95, "take_profit": {"tp1": 105, "tp2": 110, "tp3": 115}, "risk_reward": 2, "confidence": 80, "status": "CANDIDATE", "created_at": "2026-01-01T00:00:00+00:00"}
        executor = PaperExecutor(self.config, self.store, manager, notifications)
        self.assertTrue(executor.execute(signal)["approved"])
        events = manager.on_price("BTCUSDT", 106)
        self.assertEqual(events[0]["event"], "TP1_FILLED")
        self.assertLess(self.store.position("BTCUSDT")["quantity"], self.store.position_state("BTCUSDT")["original_quantity"])
        self.assertTrue(self.store.notifications())

    def test_event_driven_kline_detection_and_no_tick_strategy(self):
        cache = RealtimeMarketData()
        bus = EventBus()
        detector = EventDetector(cache, bus)
        seen = []
        bus.subscribe("KLINE_CLOSED", lambda event: seen.append(event))
        for index in range(40):
            row = [index * 60_000, 100 + index, 101 + index, 99 + index, 100 + index, 100]
            symbol, interval, _, closed = cache.update_kline({"s": "BTCUSDT", "k": {"i": "5m", "t": row[0], "o": row[1], "h": row[2], "l": row[3], "c": row[4], "v": row[5], "x": True}})
            detector.on_kline(symbol, interval, row, closed)
        event = bus.get(0.1)
        self.assertIsNotNone(event)
        self.assertIn(event.type, {"KLINE_CLOSED", "MARKET_REGIME_CHANGED", "PRICE_BREAKOUT", "VOLUME_SPIKE"})
        bus.dispatch(event)
        self.assertTrue(seen)

    def test_stop_loss_hard_limit_and_resize(self):
        manager = StopLossManager({"risk_per_trade": 0.005, "max_loss_usdt": 50, "max_loss_percent_of_equity": 0.02, "over_risk_behavior": "AUTO_RESIZE"})
        result = manager.calculate(direction="LONG", entry=100, quantity=10, equity=10000, mode="PERCENT_PRICE", stop_percent=0.1)
        self.assertTrue(result.adjusted)
        self.assertLessEqual(result.quantity * result.distance, 50)


if __name__ == "__main__":
    unittest.main()
