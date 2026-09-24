"""Small, dependency-free indicator library used by the strategy engine."""
from __future__ import annotations

from math import sqrt
from typing import Iterable, Sequence


def _values(values: Iterable[float]) -> list[float]:
    return [float(v) for v in values]


def sma(values: Iterable[float], period: int) -> float:
    xs = _values(values)
    if not xs:
        return 0.0
    return sum(xs[-period:]) / min(period, len(xs))


def ema(values: Iterable[float], period: int) -> float:
    xs = _values(values)
    if not xs:
        return 0.0
    alpha = 2.0 / (period + 1)
    result = xs[0]
    for value in xs[1:]:
        result = alpha * value + (1.0 - alpha) * result
    return result


def rsi(values: Iterable[float], period: int = 14) -> float:
    xs = _values(values)
    if len(xs) <= period:
        return 50.0
    gains: list[float] = []
    losses: list[float] = []
    for before, after in zip(xs[-period - 1:-1], xs[-period:]):
        change = after - before
        gains.append(max(change, 0.0))
        losses.append(max(-change, 0.0))
    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period
    if avg_loss == 0:
        return 100.0 if avg_gain else 50.0
    return 100.0 - 100.0 / (1.0 + avg_gain / avg_loss)


def atr(klines: Sequence[Sequence[float]], period: int = 14) -> float:
    if len(klines) < 2:
        return 0.0
    true_ranges = []
    for previous, current in zip(klines[:-1], klines[1:]):
        high, low, previous_close = float(current[2]), float(current[3]), float(previous[4])
        true_ranges.append(max(high - low, abs(high - previous_close), abs(low - previous_close)))
    return sum(true_ranges[-period:]) / min(period, len(true_ranges))


def macd(values: Iterable[float], fast: int = 12, slow: int = 26, signal: int = 9) -> tuple[float, float, float]:
    xs = _values(values)
    if not xs:
        return 0.0, 0.0, 0.0
    # Build the MACD series so the signal line reflects recent momentum.
    fast_alpha, slow_alpha, signal_alpha = 2 / (fast + 1), 2 / (slow + 1), 2 / (signal + 1)
    fast_value = slow_value = xs[0]
    macd_series = []
    for value in xs:
        fast_value = fast_alpha * value + (1 - fast_alpha) * fast_value
        slow_value = slow_alpha * value + (1 - slow_alpha) * slow_value
        macd_series.append(fast_value - slow_value)
    signal_value = macd_series[0]
    for value in macd_series[1:]:
        signal_value = signal_alpha * value + (1 - signal_alpha) * signal_value
    line = macd_series[-1]
    return line, signal_value, line - signal_value


def bollinger(values: Iterable[float], period: int = 20, deviations: float = 2.0) -> tuple[float, float, float]:
    xs = _values(values)
    window = xs[-period:] or [0.0]
    middle = sum(window) / len(window)
    standard_deviation = sqrt(sum((x - middle) ** 2 for x in window) / len(window))
    return middle + deviations * standard_deviation, middle, middle - deviations * standard_deviation


def vwap(klines: Sequence[Sequence[float]], period: int = 20) -> float:
    window = klines[-period:]
    if not window:
        return 0.0
    total_volume = sum(float(k[5]) for k in window)
    if total_volume == 0:
        return float(window[-1][4])
    return sum(((float(k[2]) + float(k[3]) + float(k[4])) / 3.0) * float(k[5]) for k in window) / total_volume


def adx(klines: Sequence[Sequence[float]], period: int = 14) -> float:
    if len(klines) <= period + 1:
        return 0.0
    trs: list[float] = []
    plus_dm: list[float] = []
    minus_dm: list[float] = []
    for previous, current in zip(klines[:-1], klines[1:]):
        high, low, prev_high, prev_low, prev_close = map(float, (current[2], current[3], previous[2], previous[3], previous[4]))
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
        up, down = high - prev_high, prev_low - low
        plus_dm.append(up if up > down and up > 0 else 0.0)
        minus_dm.append(down if down > up and down > 0 else 0.0)
    tr = sum(trs[-period:]) / period
    plus = sum(plus_dm[-period:]) / period
    minus = sum(minus_dm[-period:]) / period
    if tr <= 0:
        return 0.0
    plus_di, minus_di = 100 * plus / tr, 100 * minus / tr
    denominator = plus_di + minus_di
    return 100 * abs(plus_di - minus_di) / denominator if denominator else 0.0


def obv(klines: Sequence[Sequence[float]]) -> float:
    value = 0.0
    for previous, current in zip(klines[:-1], klines[1:]):
        close, previous_close, volume = float(current[4]), float(previous[4]), float(current[5])
        value += volume if close > previous_close else -volume if close < previous_close else 0.0
    return value


def pivot_levels(klines: Sequence[Sequence[float]]) -> dict[str, float]:
    if not klines:
        return {"pivot": 0.0, "support1": 0.0, "resistance1": 0.0}
    high, low, close = map(float, (klines[-1][2], klines[-1][3], klines[-1][4]))
    pivot = (high + low + close) / 3
    return {"pivot": pivot, "support1": 2 * pivot - high, "resistance1": 2 * pivot - low}


def swing_levels(klines: Sequence[Sequence[float]], lookback: int = 5) -> dict[str, float]:
    window = klines[-max(lookback, 2):] or [[0, 0, 0, 0, 0, 0]]
    return {"swing_high": max(float(item[2]) for item in window), "swing_low": min(float(item[3]) for item in window)}


def indicator_snapshot(klines: Sequence[Sequence[float]]) -> dict[str, float]:
    closes = [float(k[4]) for k in klines]
    volumes = [float(k[5]) for k in klines]
    macd_line, macd_signal, macd_hist = macd(closes)
    bb_upper, bb_middle, bb_lower = bollinger(closes)
    pivots = pivot_levels(klines)
    swings = swing_levels(klines)
    return {
        "ema20": ema(closes, 20), "ema50": ema(closes, 50), "ema200": ema(closes, 200),
        "rsi14": rsi(closes, 14), "macd": macd_line, "macd_signal": macd_signal, "macd_hist": macd_hist,
        "atr14": atr(klines, 14), "vwap20": vwap(klines), "volume_ma20": sma(volumes, 20),
        "volume_ratio": volumes[-1] / max(sma(volumes[:-1], 20), 1e-12) if volumes else 0.0,
        "bb_upper": bb_upper, "bb_middle": bb_middle, "bb_lower": bb_lower,
        "adx": adx(klines, 14), "obv": obv(klines), "pivot": pivots["pivot"], "support1": pivots["support1"], "resistance1": pivots["resistance1"], "swing_high": swings["swing_high"], "swing_low": swings["swing_low"],
    }
