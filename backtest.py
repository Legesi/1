"""Deterministic bar-by-bar backtesting with conservative fill semantics."""
from __future__ import annotations

import math
import statistics
import uuid
from datetime import datetime, timezone
from typing import Any

from indicators import indicator_snapshot
from strategy_manager import DEFAULT_STRATEGY, StrategyDefinition


def _context(klines: list[list[Any]]) -> dict[str, float]:
    snap = {key.upper(): value for key, value in indicator_snapshot(klines).items()}
    snap["PRICE"] = float(klines[-1][4])
    snap["VOLUME"] = float(klines[-1][5])
    return snap


def evaluate_condition(node: dict[str, Any], current: dict[str, float], previous: dict[str, float] | None = None) -> bool:
    logic = str(node.get("logic", "")).upper()
    if logic == "AND":
        return all(evaluate_condition(child, current, previous) for child in node["conditions"])
    if logic == "OR":
        return any(evaluate_condition(child, current, previous) for child in node["conditions"])
    if logic == "NOT":
        return not evaluate_condition(node["condition"], current, previous)
    left_name = str(node["indicator"]).upper()
    right_raw = node.get("value", node.get("compare_to"))
    left = float(current[left_name])
    right = float(current[str(right_raw).upper()]) if isinstance(right_raw, str) else float(right_raw)
    operator = str(node["operator"]).upper()
    if operator == ">": return left > right
    if operator == ">=": return left >= right
    if operator == "<": return left < right
    if operator == "<=": return left <= right
    if operator == "==": return left == right
    if operator == "!=": return left != right
    if previous is None:
        return False
    previous_left = float(previous[left_name])
    previous_right = float(previous[str(right_raw).upper()]) if isinstance(right_raw, str) else float(right_raw)
    if operator == "CROSSES_ABOVE": return previous_left <= previous_right and left > right
    if operator == "CROSSES_BELOW": return previous_left >= previous_right and left < right
    return False


class BacktestEngine:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config

    @staticmethod
    def _closed_bars(klines: list[list[Any]]) -> list[list[Any]]:
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        return [bar for bar in klines if len(bar) < 7 or int(bar[6]) <= now_ms]

    def run(self, symbol: str, klines: list[list[Any]], strategy: dict[str, Any] | None = None, funding: list[dict[str, Any]] | None = None, initial_capital: float | None = None) -> dict[str, Any]:
        strategy = StrategyDefinition.model_validate(strategy or DEFAULT_STRATEGY).model_dump(mode="json")
        bars = self._closed_bars(klines)
        cfg = self.config.get("backtest", {})
        warmup = max(30, int(cfg.get("warmup_bars", 60)))
        if len(bars) < warmup + 2:
            raise ValueError(f"at least {warmup + 2} closed bars are required")
        capital = float(initial_capital or cfg.get("initial_capital", 10000))
        starting_capital = capital
        fee_rate = float(cfg.get("fee_rate", 0.0004))
        slippage = float(cfg.get("slippage", 0.0002))
        max_holding = int(cfg.get("max_holding_bars", 48))
        risk = strategy["risk"]
        trades: list[dict[str, Any]] = []
        equity_curve = [capital]
        previous_context: dict[str, float] | None = None
        index = warmup
        funding = funding or []
        while index < len(bars) - 1:
            current_context = _context(bars[: index + 1])
            long_condition = evaluate_condition(strategy["conditions"], current_context, previous_context)
            direction: str | None = None
            if strategy["direction"] in ("LONG", "BOTH") and long_condition:
                direction = "LONG"
            elif strategy["direction"] == "SHORT" and long_condition:
                direction = "SHORT"
            elif strategy["direction"] == "BOTH":
                # Built-in BOTH strategy uses a deterministic mirror for bearish alignment.
                direction = "SHORT" if current_context["EMA20"] < current_context["EMA50"] and current_context["PRICE"] < current_context["EMA20"] and current_context["VOLUME_RATIO"] >= 1.2 else None
            previous_context = current_context
            if not direction:
                index += 1
                continue
            entry_bar = bars[index + 1]  # signal is known only after current close
            raw_entry = float(entry_bar[1])
            entry = raw_entry * (1 + slippage if direction == "LONG" else 1 - slippage)
            atr = max(float(current_context["ATR14"]), entry * 0.0005)
            stop_distance = atr * float(risk["stop_atr"])
            stop = entry - stop_distance if direction == "LONG" else entry + stop_distance
            target = entry + stop_distance * float(risk["tp2_r"]) if direction == "LONG" else entry - stop_distance * float(risk["tp2_r"])
            risk_amount = capital * float(risk["risk_per_trade"])
            quantity = min(risk_amount / stop_distance, capital / entry) if stop_distance else 0
            if quantity <= 0:
                index += 1
                continue
            exit_index = min(index + 1 + max_holding, len(bars) - 1)
            exit_price = float(bars[exit_index][4])
            exit_reason = "TIME"
            for cursor in range(index + 1, exit_index + 1):
                high, low = float(bars[cursor][2]), float(bars[cursor][3])
                # If both stop and target occur inside one candle, take the stop: conservative and deterministic.
                if direction == "LONG" and low <= stop:
                    exit_price, exit_index, exit_reason = stop * (1 - slippage), cursor, "SL"
                    break
                if direction == "SHORT" and high >= stop:
                    exit_price, exit_index, exit_reason = stop * (1 + slippage), cursor, "SL"
                    break
                if direction == "LONG" and high >= target:
                    exit_price, exit_index, exit_reason = target * (1 - slippage), cursor, "TP"
                    break
                if direction == "SHORT" and low <= target:
                    exit_price, exit_index, exit_reason = target * (1 + slippage), cursor, "TP"
                    break
            gross = (exit_price - entry) * quantity * (1 if direction == "LONG" else -1)
            fees = (entry + exit_price) * quantity * fee_rate
            funding_cost = self._funding_cost(funding, int(entry_bar[0]), int(bars[exit_index][0]), entry * quantity, direction)
            pnl = gross - fees - funding_cost
            capital += pnl
            r_multiple = pnl / risk_amount if risk_amount else 0.0
            trades.append({"direction": direction, "entry_time": int(entry_bar[0]), "exit_time": int(bars[exit_index][0]), "entry": entry, "exit": exit_price, "quantity": quantity, "stop": stop, "target": target, "gross_pnl": gross, "fees": fees, "funding": funding_cost, "pnl": pnl, "r": r_multiple, "reason": exit_reason})
            equity_curve.append(capital)
            index = exit_index + 1
        metrics = self.metrics(starting_capital, capital, trades, equity_curve)
        return {"run_id": f"bt-{uuid.uuid4().hex[:16]}", "symbol": symbol, "strategy": strategy["name"], "strategy_version": strategy["version"], "bars": len(bars), "start_time": int(bars[0][0]), "end_time": int(bars[-1][0]), "metrics": metrics, "trades": trades, "equity_curve": equity_curve, "assumptions": {"signal_timing": "closed bar", "fill_timing": "next bar open", "same_bar_conflict": "stop first", "fee_rate": fee_rate, "slippage": slippage}}

    @staticmethod
    def _funding_cost(funding: list[dict[str, Any]], start: int, end: int, notional: float, direction: str) -> float:
        total = 0.0
        for item in funding:
            timestamp = int(item.get("fundingTime", 0))
            if start <= timestamp <= end:
                rate = float(item.get("fundingRate", 0))
                total += notional * rate * (1 if direction == "LONG" else -1)
        return total

    @staticmethod
    def metrics(starting_capital: float, ending_capital: float, trades: list[dict[str, Any]], equity_curve: list[float]) -> dict[str, Any]:
        pnls = [float(t["pnl"]) for t in trades]
        wins = [p for p in pnls if p > 0]
        losses = [p for p in pnls if p < 0]
        returns = [pnls[i] / max(equity_curve[i], 1e-12) for i in range(len(pnls))]
        peak, max_drawdown = equity_curve[0], 0.0
        for equity in equity_curve:
            peak = max(peak, equity)
            max_drawdown = max(max_drawdown, (peak - equity) / peak if peak else 0.0)
        mean_return = statistics.fmean(returns) if returns else 0.0
        stdev = statistics.stdev(returns) if len(returns) > 1 else 0.0
        downside = [r for r in returns if r < 0]
        downside_dev = statistics.stdev(downside) if len(downside) > 1 else 0.0
        longest, current = 0, 0
        for pnl in pnls:
            current = current + 1 if pnl < 0 else 0
            longest = max(longest, current)
        gross_profit, gross_loss = sum(wins), abs(sum(losses))
        return {"total_return": (ending_capital / starting_capital - 1) if starting_capital else 0.0, "ending_capital": ending_capital, "max_drawdown": max_drawdown, "win_rate": len(wins) / len(pnls) if pnls else 0.0, "profit_factor": gross_profit / gross_loss if gross_loss else (gross_profit if gross_profit else 0.0), "sharpe": mean_return / stdev * math.sqrt(len(returns)) if stdev else 0.0, "sortino": mean_return / downside_dev * math.sqrt(len(returns)) if downside_dev else 0.0, "average_r": statistics.fmean([t["r"] for t in trades]) if trades else 0.0, "average_trade": statistics.fmean(pnls) if pnls else 0.0, "average_win": statistics.fmean(wins) if wins else 0.0, "average_loss": statistics.fmean(losses) if losses else 0.0, "expectancy": statistics.fmean(pnls) if pnls else 0.0, "total_trades": len(trades), "longest_losing_streak": longest}

    def walk_forward(self, symbol: str, klines: list[list[Any]], strategy: dict[str, Any], train_ratio: float = 0.6, validation_ratio: float = 0.2) -> dict[str, Any]:
        if train_ratio <= 0 or validation_ratio <= 0 or train_ratio + validation_ratio >= 1:
            raise ValueError("ratios must leave a positive out-of-sample segment")
        bars = self._closed_bars(klines)
        first = int(len(bars) * train_ratio)
        second = int(len(bars) * (train_ratio + validation_ratio))
        warmup = int(self.config.get("backtest", {}).get("warmup_bars", 60))
        segments = {
            "train": bars[:first],
            "validation": bars[max(0, first - warmup):second],
            "out_of_sample": bars[max(0, second - warmup):],
        }
        results = {name: self.run(symbol, segment, strategy) for name, segment in segments.items()}
        return {"symbol": symbol, "strategy": strategy["name"], "strategy_version": strategy["version"], "ratios": {"train": train_ratio, "validation": validation_ratio, "out_of_sample": 1 - train_ratio - validation_ratio}, "segments": results}
