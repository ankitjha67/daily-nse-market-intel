# src/market_intel/news/collector.py
from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence
from urllib.parse import quote_plus, urlencode

import feedparser
import requests
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class Article:
    title: str
    url: str
    source: str
    published_at: datetime
    summary: str = ""
    language: str = "en"
    raw: Optional[Dict[str, Any]] = None

    @property
    def uid(self) -> str:
        h = hashlib.sha256()
        h.update((self.title or "").encode("utf-8", errors="ignore"))
        h.update((self.url or "").encode("utf-8", errors="ignore"))
        h.update((self.source or "").encode("utf-8", errors="ignore"))
        return h.hexdigest()[:16]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _to_dt(value: Any) -> datetime:
    """Best-effort conversion for RSS/GDELT timestamps. Falls back to now UTC."""
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    if isinstance(value, str):
        for fmt in (
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                dt = datetime.strptime(value, fmt)
                return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
            except Exception:
                continue
    return _now_utc()


def _clean_text(s: str) -> str:
    s = s or ""
    s = re.sub(r"\s+", " ", s).strip()
    return s


def collect_rss(urls: Iterable[str], source_name: str, max_items: int = 50) -> List[Article]:
    """Collect generic RSS feeds using feedparser. Any parsing errors are logged and skipped."""
    out: List[Article] = []
    for url in urls:
        try:
            parsed = feedparser.parse(url)
        except Exception as e:
            logger.warning("RSS parse failed: source=%s url=%s err=%s", source_name, url, e)
            continue

        entries = getattr(parsed, "entries", []) or []
        for e in entries[:max_items]:
            title = _clean_text(getattr(e, "title", "") or "")
            link = getattr(e, "link", "") or ""
            summary = _clean_text(getattr(e, "summary", "") or getattr(e, "description", "") or "")
            lang = getattr(parsed, "language", None) or "en"

            pub = None
            if hasattr(e, "published_parsed") and e.published_parsed:
                try:
                    pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                except Exception:
                    pub = None
            if pub is None:
                pub = _to_dt(getattr(e, "published", "") or getattr(e, "updated", "") or "")

            if not title and not link:
                continue

            out.append(
                Article(
                    title=title or link,
                    url=link,
                    source=source_name,
                    published_at=pub,
                    summary=summary,
                    language=(lang or "en"),
                    raw={"rss_entry": dict(e) if hasattr(e, "items") else None},
                )
            )
    return out


def collect_google_news_rss(
    queries: Optional[Sequence[str]] = None,
    *,
    query: Optional[str] = None,
    hl: str = "en-IN",
    gl: str = "IN",
    ceid: str = "IN:en",
    max_items: int = 50,
) -> List[Article]:
    """
    Backward-compatible Google News RSS collector.

    Supports BOTH call styles:
      - collect_google_news_rss(query="NSE OR NIFTY ...", ...)
      - collect_google_news_rss(queries=["...", "..."], ...)

    Also safely URL-encodes spaces/operators to avoid InvalidURL.
    """
    if queries is None:
        queries = []
    if query:
        queries = list(queries) + [query]

    urls: List[str] = []
    for q in queries:
        params = {"q": q, "hl": hl, "gl": gl, "ceid": ceid}
        # quote_plus encodes spaces as '+', safe=":" keeps ceid like IN:en intact
        url = "https://news.google.com/rss/search?" + urlencode(params, quote_via=quote_plus, safe=":")
        urls.append(url)

    return collect_rss(urls, source_name="GoogleNewsRSS", max_items=max_items)


class _GDELTNonJSON(RuntimeError):
    pass


def _is_json_response(resp: requests.Response) -> bool:
    ctype = (resp.headers.get("Content-Type") or "").lower()
    return "application/json" in ctype or "json" in ctype


@retry(
    retry=retry_if_exception_type((requests.RequestException, _GDELTNonJSON, json.JSONDecodeError)),
    wait=wait_exponential(multiplier=1, min=1, max=20),
    stop=stop_after_attempt(4),
    reraise=True,
)
def _gdelt_get(params: Dict[str, Any], timeout: int = 20) -> Dict[str, Any]:
    """Fetch GDELT doc API safely. Retries non-JSON/empty/HTML responses."""
    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    headers = {
        "Accept": "application/json",
        "User-Agent": "daily-nse-market-intel/1.0 (+https://github.com)",
    }
    resp = requests.get(base, params=params, headers=headers, timeout=timeout)

    if resp.status_code != 200:
        body_preview = (resp.text or "")[:200].replace("\n", " ")
        raise _GDELTNonJSON(f"GDELT non-200 status={resp.status_code} body={body_preview}")

    if not resp.text or not resp.text.strip():
        raise _GDELTNonJSON("GDELT empty response body")

    if not _is_json_response(resp):
        body_preview = (resp.text or "")[:200].replace("\n", " ")
        raise _GDELTNonJSON(f"GDELT non-JSON content-type={resp.headers.get('Content-Type')} body={body_preview}")

    return resp.json()


def collect_gdelt(
    query: str,
    mode: str = "ArtList",
    max_records: int = 50,
    format_: str = "json",
    sort: str = "HybridRel",
    timespan: str = "1d",
) -> List[Article]:
    """
    Collect news from GDELT Doc 2.1 API.
    Never crashes pipeline: on failure returns [] with a warning.
    """
    params: Dict[str, Any] = {
        "query": query,
        "mode": mode,
        "format": format_,
        "sort": sort,
        "maxrecords": int(max_records),
        "timespan": timespan,
    }

    try:
        data = _gdelt_get(params)
    except Exception as e:
        logger.warning("GDELT fetch failed (skipping): query=%s err=%s", query, e)
        return []

    articles: List[Article] = []
    raw_articles = (data.get("articles") or []) if isinstance(data, dict) else []
    for a in raw_articles:
        try:
            title = _clean_text(str(a.get("title", "") or ""))
            url = str(a.get("url", "") or "")
            source = str(a.get("sourceCountry", "") or a.get("sourceCollection", "") or "GDELT")
            summary = _clean_text(str(a.get("snippet", "") or a.get("seendate", "") or ""))

            pub_raw = a.get("seendate") or a.get("date") or a.get("publishedAt")
            pub_dt = _now_utc()
            if isinstance(pub_raw, str) and re.fullmatch(r"\d{14}", pub_raw):
                try:
                    pub_dt = datetime.strptime(pub_raw, "%Y%m%d%H%M%S").replace(tzinfo=timezone.utc)
                except Exception:
                    pub_dt = _now_utc()
            else:
                pub_dt = _to_dt(pub_raw)

            if not title and not url:
                continue

            articles.append(
                Article(
                    title=title or url,
                    url=url,
                    source=f"GDELT:{source}" if source else "GDELT",
                    published_at=pub_dt,
                    summary=summary,
                    language="en",
                    raw={"gdelt": a},
                )
            )
        except Exception as e:
            logger.debug("Skipping bad GDELT article row: err=%s", e)
            continue

    return articles
