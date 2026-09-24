"""Validated pre-execution trade plan. No order can be created without this object."""
from __future__ import annotations

from pydantic import BaseModel, ConfigDict, Field


class TradePlan(BaseModel):
    model_config = ConfigDict(extra="forbid")
    signal_id: str = Field(min_length=8)
    strategy_id: str
    strategy_version: str
    risk_config_version: str
    symbol: str
    direction: str
    entry: float = Field(gt=0)
    stop_loss: float = Field(gt=0)
    tp1: float = Field(gt=0)
    tp2: float = Field(gt=0)
    tp3: float = Field(gt=0)
    quantity: float = Field(gt=0)
    risk_amount: float = Field(ge=0)
    max_loss_usdt: float = Field(ge=0)
    risk_reward: float = Field(gt=0)
    leverage: float = Field(gt=0, default=1)
    mode: str = "paper"
