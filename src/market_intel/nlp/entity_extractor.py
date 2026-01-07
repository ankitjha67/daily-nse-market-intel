from __future__ import annotations

import re
from typing import Any, Dict, List, Set

try:
    import spacy
except Exception:  # pragma: no cover
    spacy = None  # type: ignore

TICKER_PATTERNS = [
    re.compile(r"\bNSE\s*[:\-]\s*([A-Z]{2,15})\b"),
    re.compile(r"\bBSE\s*[:\-]\s*([A-Z]{2,15})\b"),
    re.compile(r"\b([A-Z]{2,15})\.NS\b"),
]
UPPER_TOKEN = re.compile(r"\b[A-Z]{2,12}\b")


def extract_entities(articles: List[Dict[str, Any]], use_spacy: bool = True) -> List[str]:
    texts: List[str] = []
    for a in articles:
        title = str(a.get("title") or "")
        summary = str(a.get("summary") or "")
        texts.append(f"{title}\n{summary}".strip())

    blob = "\n".join([t for t in texts if t])
    cands: Set[str] = set()

    for pat in TICKER_PATTERNS:
        for m in pat.findall(blob):
            cands.add(str(m).strip())

    for tok in UPPER_TOKEN.findall(blob):
        if tok in {"NSE", "BSE", "RBI", "GDP", "USD", "FII", "DII"}:
            continue
        cands.add(tok)

    if use_spacy and spacy is not None:
        try:
            nlp = spacy.load("en_core_web_sm")  # optional model
        except Exception:  # pragma: no cover
            nlp = spacy.blank("en")
        doc = nlp(blob[:250000])
        for ent in doc.ents:
            if ent.label_ in {"ORG", "PRODUCT"}:
                t = ent.text.strip()
                if 3 <= len(t) <= 60:
                    cands.add(t)

    return sorted(cands)
