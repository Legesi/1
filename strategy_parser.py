"""Conservative natural-language strategy parser.

It creates a preview only. Saving a strategy requires an explicit confirmation token.
"""
from __future__ import annotations

import re
import uuid
from typing import Any

from strategy_manager import StrategyDefinition


class StrategyParser:
    def parse(self, text: str, name: str | None = None) -> dict[str, Any]:
        if not text or len(text) > 2000:
            raise ValueError("strategy text must be between 1 and 2000 characters")
        lowered = text.lower()
        minutes = [int(value) for value in re.findall(r"(\d+)\s*(?:分钟|min|m)", lowered)]
        volume_match = re.search(r"(?:成交量|volume).*?(?:大于|高于|>|above)\s*(?:20\s*(?:周期|period)?\s*)?(?:均量|ma)?\s*(\d+(?:\.\d+)?)\s*倍?", lowered)
        rr_match = re.search(r"(?:r\s*/?\s*r|盈亏比|风险回报).*?(\d+(?:\.\d+)?)", lowered)
        timeframe_setup = f"{minutes[0]}m" if minutes else "15m"
        timeframe_trend = "1h" if any(token in lowered for token in ("小时", "1h", "hour")) else "4h"
        volume_ratio = float(volume_match.group(1)) if volume_match else 1.5
        min_rr = float(rr_match.group(1)) if rr_match else 2.0
        direction = "SHORT" if any(token in lowered for token in ("做空", "short", "下破", "跌破")) and not any(token in lowered for token in ("做多", "long", "突破前高")) else "LONG"
        strategy_name = name or ("Natural Language Breakout" if any(token in lowered for token in ("突破", "breakout")) else "Natural Language Strategy")
        payload = {"name": strategy_name, "version": "1.0", "description": text.strip(), "direction": direction, "timeframes": {"trend": timeframe_trend, "main": timeframe_trend, "setup": timeframe_setup, "confirmation": "5m", "entry": "1m"}, "conditions": {"logic": "AND", "conditions": [{"indicator": "PRICE", "operator": ">" if direction == "LONG" else "<", "compare_to": "EMA20"}, {"indicator": "VOLUME_RATIO", "operator": ">=", "value": volume_ratio}]}, "risk": {"min_rr": min_rr, "risk_per_trade": 0.005, "stop_atr": 1.5, "tp1_r": 1, "tp2_r": 2, "tp3_r": 3}, "enabled": False}
        normalized = StrategyDefinition.model_validate(payload).model_dump(mode="json")
        return {"preview_id": f"preview-{uuid.uuid4().hex[:12]}", "source_text": text, "normalized": normalized, "requires_confirmation": True}
