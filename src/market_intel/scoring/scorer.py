from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


def _clip01(x: float) -> float:
    return max(0.0, min(1.0, float(x)))


@dataclass
class Scorer:
    weights: Dict[str, float]
    thresholds: Dict[str, float]

    def score_one(self, *, sentiment: float | None, fundamentals: Dict[str, Any], technicals: Dict[str, Any]) -> Dict[str, Any]:
        s = 0.5
        if sentiment is not None:
            s = _clip01(0.5 + 0.5 * float(sentiment))

        value_gap = float(fundamentals.get("value_gap", 0.0))
        f = _clip01(0.5 + 0.5 * max(-1.0, min(1.0, value_gap)))
        q = _clip01(float(fundamentals.get("quality_score", 0.5)))
        t = _clip01(float(technicals.get("technical_score", 0.5)))

        w = self.weights or {}
        total_w = sum(w.values()) if w else 1.0
        final = (
            w.get("sentiment", 0.25) * s
            + w.get("fundamentals", 0.4) * f
            + w.get("quality", 0.2) * q
            + w.get("technical", 0.15) * t
        ) / total_w

        conf = _clip01(
            0.35
            + 0.35 * abs((sentiment or 0.0))
            + 0.15 * (1.0 if fundamentals.get("has_fundamentals") else 0.0)
            + 0.15 * (1.0 if technicals else 0.0)
        )

        rec = self.bucket(final)

        return {
            "sentiment_norm": s,
            "fundamentals_norm": f,
            "quality_norm": q,
            "technical_norm": t,
            "score": float(final),
            "confidence": float(conf),
            "recommendation": rec,
            "value_gap": value_gap,
        }

    def bucket(self, score: float) -> str:
        th = self.thresholds or {}
        if score >= float(th.get("strong_buy", 0.78)):
            return "Strong Buy"
        if score >= float(th.get("buy", 0.62)):
            return "Buy"
        if score >= float(th.get("hold", 0.48)):
            return "Hold / Neutral"
        if score >= float(th.get("sell", 0.35)):
            return "Sell"
        return "Strong Sell"
