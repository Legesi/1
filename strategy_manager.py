"""Validated, immutable strategy definitions and bindings."""
from __future__ import annotations

import copy
import re
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from storage import Store


ALLOWED_INDICATORS = {"PRICE", "EMA20", "EMA50", "EMA200", "RSI14", "MACD", "MACD_SIGNAL", "MACD_HIST", "ATR14", "VWAP20", "VOLUME", "VOLUME_MA20", "VOLUME_RATIO", "BB_UPPER", "BB_MIDDLE", "BB_LOWER", "ADX", "OBV", "PIVOT", "SUPPORT1", "RESISTANCE1", "SWING_HIGH", "SWING_LOW"}
ALLOWED_OPERATORS = {">", ">=", "<", "<=", "==", "!=", "CROSSES_ABOVE", "CROSSES_BELOW"}


class Timeframes(BaseModel):
    trend: str = "4h"
    main: str = "1h"
    setup: str = "15m"
    confirmation: str = "5m"
    entry: str = "1m"


class StrategyRisk(BaseModel):
    min_rr: float = Field(default=1.8, ge=1.0, le=20.0)
    risk_per_trade: float = Field(default=0.005, gt=0, le=0.05)
    stop_atr: float = Field(default=1.5, gt=0, le=10)
    tp1_r: float = Field(default=1.0, gt=0)
    tp2_r: float = Field(default=2.0, gt=0)
    tp3_r: float = Field(default=3.0, gt=0)


class StrategyDefinition(BaseModel):
    model_config = ConfigDict(extra="forbid")
    name: str = Field(min_length=2, max_length=100)
    version: str = Field(pattern=r"^\d+\.\d+(?:\.\d+)?$")
    description: str = Field(default="", max_length=1000)
    direction: Literal["LONG", "SHORT", "BOTH"] = "BOTH"
    timeframes: Timeframes = Field(default_factory=Timeframes)
    conditions: dict[str, Any]
    risk: StrategyRisk = Field(default_factory=StrategyRisk)
    enabled: bool = True

    @field_validator("conditions")
    @classmethod
    def validate_conditions(cls, value: dict[str, Any]) -> dict[str, Any]:
        validate_condition_tree(value)
        return value

    @model_validator(mode="after")
    def validate_targets(self) -> "StrategyDefinition":
        if not (self.risk.tp1_r <= self.risk.tp2_r <= self.risk.tp3_r):
            raise ValueError("take-profit R targets must be ascending")
        return self


def validate_condition_tree(node: Any, depth: int = 0) -> None:
    if depth > 10:
        raise ValueError("condition nesting exceeds 10 levels")
    if not isinstance(node, dict):
        raise ValueError("condition node must be an object")
    logic = str(node.get("logic", "")).upper()
    if logic in {"AND", "OR"}:
        children = node.get("conditions")
        if not isinstance(children, list) or not children:
            raise ValueError(f"{logic} requires a non-empty conditions array")
        for child in children:
            validate_condition_tree(child, depth + 1)
        return
    if logic == "NOT":
        validate_condition_tree(node.get("condition"), depth + 1)
        return
    indicator = str(node.get("indicator", "")).upper()
    operator = str(node.get("operator", "")).upper()
    if indicator not in ALLOWED_INDICATORS:
        raise ValueError(f"unsupported indicator: {indicator}")
    if operator not in ALLOWED_OPERATORS:
        raise ValueError(f"unsupported operator: {operator}")
    right = node.get("value", node.get("compare_to"))
    if not isinstance(right, (int, float, str)):
        raise ValueError("condition requires numeric value or indicator compare_to")
    if isinstance(right, str) and right.upper() not in ALLOWED_INDICATORS:
        raise ValueError(f"unsupported comparison indicator: {right}")


def _version_tuple(version: str) -> tuple[int, ...]:
    return tuple(int(piece) for piece in version.split("."))


def next_version(version: str) -> str:
    pieces = [int(piece) for piece in version.split(".")]
    while len(pieces) < 2:
        pieces.append(0)
    pieces[1] += 1
    if len(pieces) > 2:
        pieces[2] = 0
    return ".".join(str(piece) for piece in pieces)


DEFAULT_STRATEGY = {
    "name": "Default Trend Pullback",
    "version": "1.0",
    "description": "Multi-timeframe trend alignment with volume confirmation.",
    "direction": "BOTH",
    "timeframes": {"trend": "4h", "main": "1h", "setup": "15m", "confirmation": "5m", "entry": "1m"},
    "conditions": {"logic": "AND", "conditions": [
        {"indicator": "EMA20", "operator": ">", "compare_to": "EMA50"},
        {"indicator": "PRICE", "operator": ">", "compare_to": "EMA20"},
        {"indicator": "VOLUME_RATIO", "operator": ">=", "value": 1.2},
    ]},
    "risk": {"min_rr": 1.8, "risk_per_trade": 0.005, "stop_atr": 1.5, "tp1_r": 1, "tp2_r": 2, "tp3_r": 3},
    "enabled": True,
}


class StrategyManager:
    def __init__(self, store: Store) -> None:
        self.store = store
        if not self.store.strategy(DEFAULT_STRATEGY["name"], DEFAULT_STRATEGY["version"]):
            self.create(DEFAULT_STRATEGY)

    def create(self, payload: dict[str, Any]) -> dict[str, Any]:
        definition = StrategyDefinition.model_validate(payload)
        if self.store.strategy(definition.name, definition.version):
            raise ValueError("strategy version already exists; create a new version")
        data = definition.model_dump(mode="json")
        self.store.save_strategy(definition.name, definition.version, data)
        if not definition.enabled:
            self.store.set_strategy_enabled(definition.name, definition.version, False)
        return data

    def revise(self, name: str, base_version: str, changes: dict[str, Any]) -> dict[str, Any]:
        current = self.store.strategy(name, base_version)
        if not current:
            raise ValueError("base strategy version not found")
        payload = copy.deepcopy(current["payload"])
        payload.update(changes)
        payload["name"] = name
        payload["version"] = changes.get("version") or next_version(base_version)
        return self.create(payload)

    def clone(self, name: str, version: str, new_name: str) -> dict[str, Any]:
        current = self.store.strategy(name, version)
        if not current:
            raise ValueError("strategy version not found")
        payload = copy.deepcopy(current["payload"])
        payload["name"] = new_name
        payload["version"] = "1.0"
        return self.create(payload)

    def set_enabled(self, name: str, version: str, enabled: bool) -> None:
        if not self.store.set_strategy_enabled(name, version, enabled):
            raise ValueError("strategy version not found")

    def bind(self, binding_type: str, binding_key: str, name: str, version: str) -> None:
        binding_type = binding_type.upper()
        if binding_type not in {"SYMBOL", "POOL"}:
            raise ValueError("binding_type must be SYMBOL or POOL")
        strategy = self.store.strategy(name, version)
        if not strategy or not strategy["enabled"]:
            raise ValueError("enabled strategy version not found")
        if binding_type == "POOL" and binding_key.upper() not in {"CUSTOM", "GAINERS", "LOSERS"}:
            raise ValueError("pool binding key must be CUSTOM, GAINERS or LOSERS")
        self.store.bind_strategy(binding_type, binding_key.upper(), name, version)

    def resolve(self, symbol: str, source: str = "CUSTOM") -> dict[str, Any]:
        bindings = self.store.strategy_bindings()
        for item in bindings:
            if item["binding_type"] == "SYMBOL" and item["binding_key"] == symbol.upper():
                return self.store.strategy(item["strategy_name"], item["strategy_version"]) or {}
        for item in bindings:
            if item["binding_type"] == "POOL" and item["binding_key"] == source.upper():
                return self.store.strategy(item["strategy_name"], item["strategy_version"]) or {}
        return self.store.strategy(DEFAULT_STRATEGY["name"], DEFAULT_STRATEGY["version"]) or {}
