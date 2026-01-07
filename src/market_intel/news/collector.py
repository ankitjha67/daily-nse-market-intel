from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlencode, quote_plus

import feedparser
import requests
from tenacity import retry, stop_after_attempt, wait_exponential

from market_intel.utils.text import stable_digest

log = logging.getLogger(__name__)


def _parse_date(entry: Any) -> str:
    try:
        if getattr(entry, "published_parsed", None):
            dt = datetime(*entry.published_parsed[:6])
            return dt.isoformat()
    except Exception:
        pass
    return str(getattr(entry, "published", "") or "")


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
    # q must be encoded; spaces otherwise cause InvalidURL in urllib
    q = quote_plus(query)
    return f"https://news.google.com/rss/search?{urlencode({'q': q, 'hl': hl, 'gl': gl, 'ceid': ceid})}"


def collect_google_news_rss(query: str, hl: str = "en-IN", gl: str = "IN", ceid: str = "IN:en", max_items: int = 50) -> List[Dict[str, Any]]:
    url = build_google_news_rss_url(query=query, hl=hl, gl=gl, ceid=ceid)
    parsed = feedparser.parse(url)
    out: List[Dict[str, Any]] = []
    for e in parsed.entries[:max_items]:
        title = str(getattr(e, "title", "") or "")
        link = str(getattr(e, "link", "") or "")
        summary = str(getattr(e, "summary", "") or getattr(e, "description", "") or "")
        published_at = _parse_date(e)
        out.append(_norm_article("GoogleNewsRSS", title, link, published_at, summary))
    return out


def collect_rss(feeds: List[Dict[str, str]], max_items: int = 80) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for f in feeds:
        name = f.get("name", "RSS")
        url = f.get("url", "")
        if not url:
            continue
        parsed = feedparser.parse(url)
        for e in parsed.entries[:max_items]:
            title = str(getattr(e, "title", "") or "")
            link = str(getattr(e, "link", "") or "")
            summary = str(getattr(e, "summary", "") or getattr(e, "description", "") or "")
            published_at = _parse_date(e)
            out.append(_norm_article(name, title, link, published_at, summary))
    return out


@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=8))
def _gdelt_get(params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def collect_gdelt(query: str, max_records: int = 50) -> List[Dict[str, Any]]:
    params = {
        "query": query,
        "mode": "ArtList",
        "format": "json",
        "sort": "HybridRel",
        "maxrecords": int(max_records),
    }
    data = _gdelt_get(params)
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
