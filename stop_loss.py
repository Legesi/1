"""Stop-loss calculation and hard maximum-loss enforcement."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class StopResult:
    mode: str
    entry: float
    stop: float
    distance: float
    max_loss_usdt: float
    risk_percent: float
    quantity: float
    adjusted: bool
    rejected: bool
    reason: str = ""


class StopLossManager:
    MODES = {"STRUCTURE", "PERCENT_PRICE", "FIXED_USDT", "EQUITY_PERCENT", "ATR", "STRUCTURE_WITH_MAX_LOSS"}

    def __init__(self, risk_config: dict[str, Any]) -> None:
        self.config = risk_config

    def calculate(self, *, direction: str, entry: float, quantity: float, equity: float, mode: str = "ATR", atr: float = 0.0, atr_multiplier: float = 1.5, swing_low: float | None = None, swing_high: float | None = None, stop_percent: float = 0.01, fixed_loss_usdt: float | None = None, structure_buffer: float = 0.2, fee_rate: float = 0.0004, slippage: float = 0.0002) -> StopResult:
        mode = mode.upper()
        if mode not in self.MODES:
            raise ValueError(f"unsupported stop mode: {mode}")
        direction = direction.upper()
        if direction not in {"LONG", "SHORT"} or entry <= 0 or quantity <= 0:
            raise ValueError("direction, entry and quantity are invalid")
        if mode == "PERCENT_PRICE":
            distance = entry * stop_percent
        elif mode == "FIXED_USDT":
            distance = (fixed_loss_usdt or 0) / quantity
        elif mode == "EQUITY_PERCENT":
            distance = (equity * float(self.config.get("risk_per_trade", 0.005))) / quantity
        elif mode == "ATR":
            distance = atr * atr_multiplier
        elif mode in {"STRUCTURE", "STRUCTURE_WITH_MAX_LOSS"}:
            anchor = swing_low if direction == "LONG" else swing_high
            if anchor is None:
                raise ValueError("structure stop requires swing_low or swing_high")
            distance = (entry - anchor + atr * structure_buffer) if direction == "LONG" else (anchor - entry + atr * structure_buffer)
        else:
            distance = 0
        if distance <= 0:
            raise ValueError("stop distance must be positive")
        stop = entry - distance if direction == "LONG" else entry + distance
        estimated_cost = (entry * fee_rate) + (entry * slippage)
        theoretical_loss = distance * quantity + estimated_cost * quantity
        max_loss_usdt = float(self.config.get("max_loss_usdt", equity * float(self.config.get("max_daily_loss", 0.02))))
        max_loss_percent = float(self.config.get("max_loss_percent_of_equity", self.config.get("max_daily_loss", 0.02)))
        max_loss_usdt = min(max_loss_usdt, equity * max_loss_percent) if max_loss_percent > 0 else max_loss_usdt
        risk_percent = theoretical_loss / equity if equity else 1.0
        adjusted = False
        rejected = False
        reason = ""
        if theoretical_loss > max_loss_usdt:
            behavior = str(self.config.get("over_risk_behavior", "AUTO_RESIZE")).upper()
            if behavior == "REJECT_TRADE":
                rejected = True
                reason = "MAX_LOSS_HARD_LIMIT"
            else:
                quantity = max(0.0, (max_loss_usdt / max(distance + estimated_cost, 1e-12)))
                adjusted = True
                risk_percent = max_loss_usdt / equity if equity else 1.0
                reason = "POSITION_RESIZED_TO_MAX_LOSS"
        return StopResult(mode, entry, stop, distance, max_loss_usdt, risk_percent, quantity, adjusted, rejected, reason)
