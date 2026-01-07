from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


@dataclass
class SQLiteStore:
    path: str = ".cache/state.db"

    def __post_init__(self) -> None:
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def close(self) -> None:
        try:
            self.conn.close()
        except Exception:
            pass

    def _init_schema(self) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS articles (
                digest TEXT PRIMARY KEY,
                url TEXT,
                title TEXT,
                source TEXT,
                published_at TEXT,
                fetched_at TEXT,
                raw_json TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS article_sentiment (
                digest TEXT PRIMARY KEY,
                sentiment REAL,
                confidence REAL,
                model TEXT,
                updated_at TEXT
            )
            """
        )
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS article_symbols (
                digest TEXT,
                symbol TEXT,
                confidence REAL,
                PRIMARY KEY (digest, symbol)
            )
            """
        )
        self.conn.commit()

    def upsert_articles(self, articles: Iterable[Dict[str, Any]]) -> None:
        cur = self.conn.cursor()
        for a in articles:
            digest = str(a.get("digest") or a.get("id") or "").strip()
            url = str(a.get("url") or "").strip()
            title = str(a.get("title") or "").strip()
            source = str(a.get("source") or "").strip()
            published_at = str(a.get("published_at") or a.get("published") or "").strip()

            if not digest:
                base = f"{source.lower()}|{title.lower()}|{url}"
                digest = str(abs(hash(base)))

            raw_json = None
            try:
                raw_json = json.dumps(a, ensure_ascii=False)
            except Exception:
                raw_json = None

            cur.execute(
                """
                INSERT INTO articles (digest, url, title, source, published_at, fetched_at, raw_json)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(digest) DO UPDATE SET
                    url=excluded.url,
                    title=excluded.title,
                    source=excluded.source,
                    published_at=excluded.published_at,
                    fetched_at=excluded.fetched_at,
                    raw_json=excluded.raw_json
                """,
                (digest, url, title, source, published_at, _utcnow_iso(), raw_json),
            )
        self.conn.commit()

    def save_article_sentiment(self, digest: str, sentiment: float, confidence: float, model: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            """
            INSERT INTO article_sentiment (digest, sentiment, confidence, model, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(digest) DO UPDATE SET
                sentiment=excluded.sentiment,
                confidence=excluded.confidence,
                model=excluded.model,
                updated_at=excluded.updated_at
            """,
            (digest, float(sentiment), float(confidence), model, _utcnow_iso()),
        )
        self.conn.commit()

    def save_article_symbols(
        self,
        digest: str,
        symbols: Iterable[Tuple[str, float]] | Iterable[str],
        default_confidence: float = 0.7,
    ) -> None:
        cur = self.conn.cursor()
        for item in symbols:
            if isinstance(item, tuple) or isinstance(item, list):
                sym = str(item[0]).strip().upper()
                conf = _safe_float(item[1], default_confidence)
            else:
                sym = str(item).strip().upper()
                conf = default_confidence
            if not sym:
                continue
            cur.execute(
                """
                INSERT INTO article_symbols (digest, symbol, confidence)
                VALUES (?, ?, ?)
                ON CONFLICT(digest, symbol) DO UPDATE SET
                    confidence=excluded.confidence
                """,
                (digest, sym, float(conf)),
            )
        self.conn.commit()

    def aggregate_symbol_sentiment(self, *, since_iso: str | None = None, min_confidence: float = 0.0) -> Dict[str, float]:
        try:
            cur = self.conn.cursor()
            params: List[Any] = [float(min_confidence)]
            where = "WHERE s.confidence >= ?"
            if since_iso:
                where += " AND (a.published_at >= ? OR a.published_at = '')"
                params.append(str(since_iso))

            rows = cur.execute(
                f"""
                SELECT s.symbol, AVG(COALESCE(t.sentiment, 0.0)) AS avg_sent
                FROM article_symbols s
                JOIN articles a ON a.digest = s.digest
                LEFT JOIN article_sentiment t ON t.digest = s.digest
                {where}
                GROUP BY s.symbol
                """,
                params,
            ).fetchall()

            out: Dict[str, float] = {}
            for sym, avg_sent in rows:
                sym = str(sym).strip().upper()
                if not sym:
                    continue
                out[sym] = _safe_float(avg_sent, 0.0)
            return out
        except Exception:
            return {}
