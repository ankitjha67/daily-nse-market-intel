from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from urllib.parse import urlencode

import feedparser
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from market_intel.utils.text import stable_digest

log = logging.getLogger(__name__)

GDELT_ENDPOINT = "https://api.gdeltproject.org/api/v2/doc/doc"
DEFAULT_UA = "daily-nse-market-intel/0.1 (GitHub Actions)"


def _gdelt_headers() -> Dict[str, str]:
    """Conservative headers to reduce non-JSON / rate-limit HTML responses."""
    return {
        "User-Agent": DEFAULT_UA,
        "Accept": "application/json,text/json;q=0.9,*/*;q=0.1",
    }


def _parse_date(entry: Any) -> str:
    # feedparser gives time.struct_time in `published_parsed`/`updated_parsed`
    for attr in ("published_parsed", "updated_parsed"):
        try:
            st = getattr(entry, attr, None)
            if st:
                dt = datetime(*st[:6])
                return dt.isoformat()
        except Exception:
            pass

    # fallback: raw strings if present
    return str(getattr(entry, "published", "") or getattr(entry, "updated", "") or "")


def _norm_article(source: str, title: str, url: str, published_at: str, summary: str) -> Dict[str, Any]:
    d = stable_digest(source, title, url)
    return {
        "digest": d,
        "source": source,
        "title": title,
        "url": url,
        "published_at": published_at,
        "summary": summary,
    }


def build_google_news_rss_url(query: str, hl: str, gl: str, ceid: str) -> str:
    # urlencode() safely encodes spaces as '+'
    return f"https://news.google.com/rss/search?{urlencode({'q': query, 'hl': hl, 'gl': gl, 'ceid': ceid})}"


def collect_google_news_rss(
    query: Optional[str] = None,
    *,
    # backward compat: some older code calls this with q=
    q: Optional[str] = None,
    hl: str = "en-IN",
    gl: str = "IN",
    ceid: str = "IN:en",
    max_items: int = 50,
    **_: Any,
) -> List[Dict[str, Any]]:
    """
    Collect Google News RSS items.
    Compatible with both:
      - collect_google_news_rss(query="...")
      - collect_google_news_rss(q="...")  (legacy)
    """
    query_final = (query if query is not None else q) or ""
    if not query_final.strip():
        return []

    url = build_google_news_rss_url(query=query_final, hl=hl, gl=gl, ceid=ceid)

    try:
        parsed = feedparser.parse(url, agent=DEFAULT_UA)
    except TypeError:
        # Some feedparser versions don't accept agent kwarg
        parsed = feedparser.parse(url)

    out: List[Dict[str, Any]] = []
    for e in (parsed.entries or [])[: int(max_items)]:
        title = str(getattr(e, "title", "") or "")
        link = str(getattr(e, "link", "") or "")
        summary = str(getattr(e, "summary", "") or getattr(e, "description", "") or "")
        published_at = _parse_date(e)
        out.append(_norm_article("GoogleNewsRSS", title, link, published_at, summary))
    return out


FeedDef = Union[str, Dict[str, Any]]


def _normalize_feed_def(feed: FeedDef, default_source_name: str) -> Tuple[str, str]:
    """
    Supports:
      - "https://example.com/rss"
      - {"name": "Reuters Business", "url": "https://..."}
      - {"url": "https://..."}  (name optional)
    """
    if isinstance(feed, str):
        return default_source_name, feed.strip()

    if isinstance(feed, dict):
        name = str(feed.get("name") or default_source_name).strip() or default_source_name
        url = str(feed.get("url") or "").strip()
        return name, url

    return default_source_name, ""


def collect_rss(
    feeds: Iterable[FeedDef],
    source_name: str = "RSS",
    *,
    max_items: int = 80,
    **_: Any,
) -> List[Dict[str, Any]]:
    """
    Collect generic RSS feeds.
    Backward compatible with older signatures that required `source_name`.

    `feeds` may be:
      - list[str]
      - list[{"name": ..., "url": ...}]
    """
    out: List[Dict[str, Any]] = []

    for f in list(feeds or []):
        name, url = _normalize_feed_def(f, default_source_name=source_name)
        if not url:
            continue

        try:
            try:
                parsed = feedparser.parse(url, agent=DEFAULT_UA)
            except TypeError:
                parsed = feedparser.parse(url)
        except Exception as e:
            log.warning("RSS parse failed: source=%s url=%s err=%s", name, url, e)
            continue

        for e in (parsed.entries or [])[: int(max_items)]:
            title = str(getattr(e, "title", "") or "")
            link = str(getattr(e, "link", "") or "")
            summary = str(getattr(e, "summary", "") or getattr(e, "description", "") or "")
            published_at = _parse_date(e)
            out.append(_norm_article(name, title, link, published_at, summary))

    return out


def _wrap_or_query(q: str) -> str:
    """
    GDELT requires queries containing OR terms to be wrapped with parentheses.
    """
    s = (q or "").strip()
    if " OR " in s and not (s.startswith("(") and s.endswith(")")):
        return f"({s})"
    return s


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _gdelt_get(params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(GDELT_ENDPOINT, params=params, headers=_gdelt_headers(), timeout=20)

    # Retry on transient HTTP errors / throttling
    if r.status_code in (429, 500, 502, 503, 504):
        r.raise_for_status()
    r.raise_for_status()

    # GDELT can return non-JSON (HTML) or empty bodies.
    ct = str(r.headers.get("Content-Type", ""))
    body_preview = (r.text or "")[:200].replace("\n", " ").replace("\r", " ")

    if "json" not in ct.lower():
        raise ValueError(
            f"GDELT non-JSON content-type={ct}; body={body_preview}"
        )

    try:
        return r.json()
    except Exception as e:
        raise ValueError(f"GDELT JSON decode failed; body={body_preview}") from e


def collect_gdelt(query: str, max_records: int = 50) -> List[Dict[str, Any]]:
    """
    Fetch articles from GDELT.
    Never crashes the pipeline: failures are logged and return [].
    """
    q = _wrap_or_query(query)

    params = {
        "query": q,
        "mode": "ArtList",
        "format": "json",
        "sort": "HybridRel",
        "maxrecords": int(max_records),
    }

    try:
        data = _gdelt_get(params)
    except Exception as e:
        log.warning("GDELT fetch failed (skipping): query=%s err=%s", q, e)
        return []

    arts = data.get("articles") or []
    out: List[Dict[str, Any]] = []
    for a in arts:
        title = str(a.get("title") or "")
        url = str(a.get("url") or "")
        source = str(a.get("sourceCountry") or "GDELT")
        published_at = str(a.get("seendate") or "")
        summary = str(a.get("summary") or "")
        out.append(_norm_article(f"GDELT:{source}", title, url, published_at, summary))
    return out
