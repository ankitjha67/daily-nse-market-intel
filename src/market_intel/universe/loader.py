from __future__ import annotations

import csv
from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class SymbolRow:
    symbol: str
    name: str
    sector: str
    yahoo: str
    aliases: List[str]


def _split_aliases(s: str) -> List[str]:
    s = (s or "").strip()
    if not s:
        return []
    out: List[str] = []
    for chunk in s.split(";"):
        out.extend([p.strip() for p in chunk.split(",") if p.strip()])
    return out


def load_symbol_master(symbol_master_path: str) -> List[SymbolRow]:
    rows: List[SymbolRow] = []
    with open(symbol_master_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            sym = (r.get("symbol") or "").strip().upper()
            if not sym:
                continue
            rows.append(
                SymbolRow(
                    symbol=sym,
                    name=(r.get("name") or "").strip(),
                    sector=(r.get("sector") or "").strip(),
                    yahoo=(r.get("yahoo") or f"{sym}.NS").strip(),
                    aliases=_split_aliases(r.get("aliases") or ""),
                )
            )
    return rows


def load_baseline_symbols(path: str) -> List[str]:
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            s = (r.get("symbol") or "").strip().upper()
            if s:
                out.append(s)
    return out


def load_watchlist(path: str) -> List[str]:
    out: List[str] = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip().upper()
            if s and not s.startswith("#"):
                out.append(s)
    return out
