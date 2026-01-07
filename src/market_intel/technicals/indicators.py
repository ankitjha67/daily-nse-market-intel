from __future__ import annotations

from typing import Dict

import numpy as np
import pandas as pd


def _sma(s: pd.Series, n: int) -> pd.Series:
    return s.rolling(n).mean()


def _rsi(close: pd.Series, period: int = 14) -> pd.Series:
    delta = close.diff()
    up = delta.clip(lower=0)
    down = (-delta).clip(lower=0)
    ma_up = up.ewm(alpha=1/period, adjust=False).mean()
    ma_down = down.ewm(alpha=1/period, adjust=False).mean()
    rs = ma_up / ma_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def compute_technicals(df: pd.DataFrame, sma_fast: int = 20, sma_slow: int = 50, rsi_period: int = 14) -> Dict[str, float]:
    if df is None or df.empty or "close" not in df.columns:
        return {}

    close = pd.Series(df["close"].astype(float).values)
    sma_f = _sma(close, sma_fast)
    sma_s = _sma(close, sma_slow)
    rsi = _rsi(close, rsi_period)

    last_close = float(close.iloc[-1])
    last_sma_f = float(sma_f.iloc[-1]) if not np.isnan(sma_f.iloc[-1]) else last_close
    last_sma_s = float(sma_s.iloc[-1]) if not np.isnan(sma_s.iloc[-1]) else last_close
    last_rsi = float(rsi.iloc[-1]) if not np.isnan(rsi.iloc[-1]) else 50.0

    trend = 1.0 if last_sma_f > last_sma_s else (-1.0 if last_sma_f < last_sma_s else 0.0)
    momentum = (last_close / last_sma_s - 1.0) if last_sma_s else 0.0

    score = 0.5 + 0.25 * trend + 0.25 * np.tanh(5 * momentum)
    score = float(max(0.0, min(1.0, score)))

    bias = 1.0 if score >= 0.62 else (-1.0 if score <= 0.38 else 0.0)

    return {
        "last_close": last_close,
        "sma_fast": last_sma_f,
        "sma_slow": last_sma_s,
        "rsi": last_rsi,
        "technical_score": score,
        "technical_bias": float(bias),
    }
