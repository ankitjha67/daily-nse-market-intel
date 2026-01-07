from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List


def compute_sector_boom(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    acc: Dict[str, List[float]] = defaultdict(list)
    for r in rows:
        sec = (r.get("sector") or "Unknown").strip() or "Unknown"
        s = r.get("article_sentiment")
        if s is None:
            continue
        try:
            acc[sec].append(float(s))
        except Exception:
            continue
    return {k: (sum(v) / max(1, len(v))) for k, v in acc.items()}
