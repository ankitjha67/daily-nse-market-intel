from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer


@dataclass
class VaderSentiment:
    analyzer: SentimentIntensityAnalyzer = SentimentIntensityAnalyzer()

    def score(self, text: str) -> Tuple[float, float]:
        vs = self.analyzer.polarity_scores(text or "")
        compound = float(vs.get("compound", 0.0))
        conf = min(1.0, 0.5 + abs(compound) * 0.5)
        return compound, conf
