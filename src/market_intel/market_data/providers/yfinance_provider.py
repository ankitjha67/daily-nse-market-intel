from __future__ import annotations

import pandas as pd
import yfinance as yf


def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = ["_".join([str(x) for x in tup if x is not None]).strip() for tup in df.columns]
    else:
        df.columns = [str(c) for c in df.columns]
    return df


class YFinanceProvider:
    def history(self, ticker: str, days: int = 550, interval: str = "1d") -> pd.DataFrame:
        t = yf.Ticker(ticker)
        df = t.history(period=f"{int(days)}d", interval=interval, auto_adjust=False)
        if df is None or df.empty:
            return pd.DataFrame()

        df = _flatten_columns(df)
        df = df.rename(columns={c: c.lower().replace(" ", "_") for c in df.columns})

        df = df.reset_index()
        if "date" not in df.columns and "datetime" in df.columns:
            df = df.rename(columns={"datetime": "date"})
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date

        keep = [c for c in ["date", "open", "high", "low", "close", "adj_close", "volume"] if c in df.columns]
        return df[keep].copy()
