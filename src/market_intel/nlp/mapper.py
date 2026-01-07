from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Sequence

from rapidfuzz import fuzz, process

from market_intel.universe.loader import SymbolRow


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _unique(xs: Iterable[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for x in xs:
        x = (x or "").strip()
        if not x:
            continue
        k = x.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def _load_manual_aliases(path: str) -> Dict[str, List[str]]:
    out: Dict[str, List[str]] = {}
    if not path or not os.path.exists(path):
        return out
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sym = (r.get("symbol") or "").strip().upper()
            alias = (r.get("alias") or "").strip()
            if not sym or not alias:
                continue
            out.setdefault(sym, []).append(alias)
    for k, v in list(out.items()):
        out[k] = _unique(v)
    return out


@dataclass(frozen=True)
class MappedSymbol:
    symbol: str
    score: float
    matched: str


class SymbolMapper:
    def __init__(
        self,
        master: Sequence[SymbolRow],
        manual_aliases_path: Optional[str] = None,
        min_score: float = 80.0,
    ) -> None:
        self.master = list(master)
        self.min_score = float(min_score)
        manual = _load_manual_aliases(manual_aliases_path or "")

        self._candidates: List[str] = []
        self._cand_to_symbol: Dict[str, str] = {}

        for row in self.master:
            sym = row.symbol.upper().strip()
            strings: List[str] = [sym, row.name, row.yahoo.replace(".NS", "").replace(".BO", "")]
            strings.extend(row.aliases or [])
            strings.extend(manual.get(sym, []))
            strings = _unique(strings)

            for s in strings:
                key = _norm(s)
                if not key or key in self._cand_to_symbol:
                    continue
                self._cand_to_symbol[key] = sym
                self._candidates.append(key)

    def map_one(self, entity: str) -> Optional[MappedSymbol]:
        q = _norm(entity)
        if not q:
            return None
        if q in self._cand_to_symbol:
            return MappedSymbol(symbol=self._cand_to_symbol[q], score=100.0, matched=entity)

        m = process.extractOne(q, self._candidates, scorer=fuzz.WRatio)
        if not m:
            return None
        best_key, best_score, _ = m
        if float(best_score) < self.min_score:
            return None
        sym = self._cand_to_symbol.get(best_key)
        if not sym:
            return None
        return MappedSymbol(symbol=sym, score=float(best_score), matched=best_key)

    def map_entities(self, entities: Iterable[str]) -> List[MappedSymbol]:
        out: List[MappedSymbol] = []
        for e in entities:
            r = self.map_one(str(e))
            if r:
                out.append(r)
        best: Dict[str, MappedSymbol] = {}
        for r in out:
            if r.symbol not in best or r.score > best[r.symbol].score:
                best[r.symbol] = r
        return sorted(best.values(), key=lambda x: (-x.score, x.symbol))
