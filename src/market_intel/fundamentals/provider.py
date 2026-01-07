from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Protocol

import yfinance as yf


class FundamentalsProvider(Protocol):
    def get(self, ticker: str) -> Dict[str, Any]:
        ...


@dataclass
class YFinanceFundamentalsProvider:
    def get(self, ticker: str) -> Dict[str, Any]:
        t = yf.Ticker(ticker)
        info: Dict[str, Any] = {}
        try:
            info = t.info or {}
        except Exception:
            info = {}

        out = {"info": info}
        # Keep statements optional; yfinance may return empty frames.
        try:
            out["financials"] = t.financials.to_dict() if getattr(t, "financials", None) is not None else {}
        except Exception:
            out["financials"] = {}
        try:
            out["balance_sheet"] = t.balance_sheet.to_dict() if getattr(t, "balance_sheet", None) is not None else {}
        except Exception:
            out["balance_sheet"] = {}
        try:
            out["cashflow"] = t.cashflow.to_dict() if getattr(t, "cashflow", None) is not None else {}
        except Exception:
            out["cashflow"] = {}
        return out
