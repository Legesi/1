"""Structured deterministic agents. They analyze/orchestrate but never execute orders."""
from __future__ import annotations

import statistics
from typing import Any

from indicators import indicator_snapshot
from storage import Store, utc_now


class AgentHub:
    AGENTS = ("Market Analyst Agent", "Strategy Agent", "Risk Analyst Agent", "Trading Monitor Agent", "Execution Monitor Agent", "Trade Journal Agent", "Performance Analyst Agent", "System Admin Agent")

    def __init__(self, store: Store) -> None:
        self.store = store

    def _save(self, agent: str, report_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        report = {"agent": agent, "report_type": report_type, "created_at": utc_now(), **payload}
        report["report_id"] = self.store.save_agent_report(agent, report_type, report)
        return report

    def market_analysis(self, symbol: str, frames: dict[str, list[list[Any]]]) -> dict[str, Any]:
        snapshots = {timeframe: indicator_snapshot(bars) for timeframe, bars in frames.items() if bars}
        regimes = {}
        for timeframe, snapshot in snapshots.items():
            if snapshot["ema20"] > snapshot["ema50"] > snapshot["ema200"]:
                regime = "TREND_UP"
            elif snapshot["ema20"] < snapshot["ema50"] < snapshot["ema200"]:
                regime = "TREND_DOWN"
            elif snapshot["atr14"] / max(snapshot["ema20"], 1e-12) > 0.03:
                regime = "HIGH_VOLATILITY"
            else:
                regime = "RANGE"
            regimes[timeframe] = regime
        trend_votes = list(regimes.values())
        overall = "TREND_UP" if trend_votes.count("TREND_UP") >= 2 else "TREND_DOWN" if trend_votes.count("TREND_DOWN") >= 2 else "UNCERTAIN"
        return self._save("Market Analyst Agent", "MARKET_ANALYSIS", {"symbol": symbol, "market_regime": overall, "timeframes": snapshots, "regimes": regimes})

    def strategy_analysis(self, symbol: str, signal: dict[str, Any] | None) -> dict[str, Any]:
        return self._save("Strategy Agent", "STRATEGY_ANALYSIS", {"symbol": symbol, "decision": "CANDIDATE" if signal else "NO_TRADE", "signal": signal, "reason": "deterministic strategy conditions satisfied" if signal else "strategy alignment, volume or confirmation conditions not satisfied"})

    def risk_analysis(self, signal: dict[str, Any], check: dict[str, Any]) -> dict[str, Any]:
        return self._save("Risk Analyst Agent", "RISK_ANALYSIS", {"signal_id": signal.get("signal_id"), "recommendation": "APPROVE" if check.get("approved") else "REJECT", "deterministic_risk_result": check, "authority": "ADVISORY_ONLY"})

    def trading_monitor(self, status: dict[str, Any]) -> dict[str, Any]:
        warnings = []
        if not status.get("websocket", {}).get("connected"):
            warnings.append("WEBSOCKET_DISCONNECTED")
        if status.get("last_error"):
            warnings.append("MARKET_DATA_ERROR")
        if status.get("kill_switch"):
            warnings.append("KILL_SWITCH_ACTIVE")
        return self._save("Trading Monitor Agent", "SYSTEM_MONITOR", {"health": "OK" if not warnings else "DEGRADED", "warnings": warnings, "status": status})

    def execution_monitor(self) -> dict[str, Any]:
        positions = self.store.positions()
        unsafe = [position["symbol"] for position in positions if not position.get("stop_loss")]
        return self._save("Execution Monitor Agent", "EXECUTION_MONITOR", {"open_positions": len(positions), "unprotected_positions": unsafe, "health": "OK" if not unsafe else "CRITICAL"})

    def journal_review(self, trade: dict[str, Any]) -> dict[str, Any]:
        pnl = float(trade.get("pnl") or 0)
        return self._save("Trade Journal Agent", "TRADE_REVIEW", {"trade_id": trade.get("trade_id"), "symbol": trade.get("symbol"), "result": "WIN" if pnl > 0 else "LOSS" if pnl < 0 else "FLAT", "pnl": pnl, "strategy_compliance": "RECORDED", "review": "Evaluate entry timing, stop placement, partial exits, fees and slippage against the immutable strategy version."})

    def performance(self) -> dict[str, Any]:
        trades = [trade for trade in self.store.trades(10000) if trade.get("status") == "CLOSED"]
        pnls = [float(trade.get("pnl") or 0) for trade in trades]
        wins = [value for value in pnls if value > 0]
        losses = [value for value in pnls if value < 0]
        gross_loss = abs(sum(losses))
        return self._save("Performance Analyst Agent", "PERFORMANCE_ANALYSIS", {"total_trades": len(trades), "win_rate": len(wins) / len(trades) if trades else 0, "average_trade": statistics.fmean(pnls) if pnls else 0, "average_win": statistics.fmean(wins) if wins else 0, "average_loss": statistics.fmean(losses) if losses else 0, "profit_factor": sum(wins) / gross_loss if gross_loss else 0, "net_pnl": sum(pnls)})

    def system_admin(self, status: dict[str, Any], reconciliation: dict[str, Any]) -> dict[str, Any]:
        return self._save("System Admin Agent", "ADMIN_HEALTH", {"service_status": status, "reconciliation": reconciliation, "recommendation": "PAUSE_AND_RECONCILE" if not reconciliation.get("consistent") else "CONTINUE_PAPER"})
