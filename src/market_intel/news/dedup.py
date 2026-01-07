from __future__ import annotations

from typing import Any, Dict, Iterable, List, Set


def dedup_articles(articles: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    out: List[Dict[str, Any]] = []
    for a in articles:
        url = str(a.get("url") or "").strip().lower()
        title = str(a.get("title") or "").strip().lower()
        source = str(a.get("source") or "").strip().lower()
        sig = f"{source}|{title}|{url}"
        if sig in seen:
            continue
        seen.add(sig)
        out.append(a)
    return out
