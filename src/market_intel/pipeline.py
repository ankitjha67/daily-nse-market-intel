from __future__ import annotations

import csv
import logging
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Optional

from market_intel.config import Cfg
from market_intel.fundamentals.provider import YFinanceFundamentalsProvider
from market_intel.mailer.smtp_mailer import SMTPConfig, send_email
from market_intel.market_data.providers.yfinance_provider import YFinanceProvider
from market_intel.news.collector import collect_gdelt, collect_google_news_rss, collect_rss
from market_intel.news.dedup import dedup_articles
from market_intel.nlp.entity_extractor import extract_entities
from market_intel.nlp.mapper import SymbolMapper
from market_intel.reporting.executive_brief import build_brief_markdown, render_brief_pdf
from market_intel.reporting.html_report import render_html_report
from market_intel.scoring.scorer import Scorer
from market_intel.sectors.boom import compute_sector_boom
from market_intel.storage.db import SQLiteStore
from market_intel.technicals.indicators import compute_technicals
from market_intel.universe.loader import load_baseline_symbols, load_symbol_master, load_watchlist
from market_intel.utils.dates import now_local, since_hours
from market_intel.utils.http import install_cache
from market_intel.utils.log import setup_logging

from market_intel.sentiment.vader_model import VaderSentiment

log = logging.getLogger(__name__)


def _read_disclaimer() -> str:
    p = Path("DISCLAIMER.md")
    return p.read_text(encoding="utf-8") if p.exists() else "Disclaimer: Educational only."


def _ensure_dir(p: str) -> Path:
    d = Path(p)
    d.mkdir(parents=True, exist_ok=True)
    return d


def _pick_universe(cfg: Cfg, news_syms: List[str], baseline: List[str], watchlist: List[str]) -> List[str]:
    include_baseline = bool(cfg.get("universe.include_baseline", True))
    include_watchlist = bool(cfg.get("universe.include_watchlist", True))
    max_symbols = int(cfg.get("run.max_symbols", 120))

    merged: List[str] = []
    seen: set[str] = set()

    def add_many(xs: List[str]) -> None:
        for s in xs:
            s = (s or "").strip().upper()
            if not s or s in seen:
                continue
            seen.add(s)
            merged.append(s)

    add_many(news_syms)
    if include_baseline:
        add_many(baseline)
    if include_watchlist:
        add_many(watchlist)

    return merged[:max_symbols]


def _fundamentals_from_yf(info: Dict[str, Any]) -> Dict[str, Any]:
    pe = info.get("forwardPE") or info.get("trailingPE")
    roe = info.get("returnOnEquity")

    q = 0.5
    try:
        if roe is not None:
            q = max(0.0, min(1.0, 0.5 + float(roe) * 1.0))
    except Exception:
        pass

    fair_pe = 18.0
    try:
        if roe is not None and float(roe) > 0.15:
            fair_pe = 22.0
        if roe is not None and float(roe) < 0.08:
            fair_pe = 14.0
    except Exception:
        pass

    value_gap = 0.0
    has = False
    try:
        if pe and float(pe) > 0:
            value_gap = max(-1.0, min(2.0, (fair_pe / float(pe)) - 1.0))
            has = True
    except Exception:
        pass

    return {"has_fundamentals": has, "quality_score": float(q), "value_gap": float(value_gap), "pe": pe, "roe": roe}


def run_pipeline(cfg: Cfg) -> Path:
    setup_logging("INFO")
    install_cache(
        cache_name=str(cfg.get("run.http_cache_name", ".cache/http_cache")),
        backend=str(cfg.get("run.http_cache_backend", "sqlite")),
        expire_after=int(cfg.get("run.http_cache_expire_seconds", 3600)),
    )

    tz = str(cfg.get("run.timezone", "Asia/Kolkata"))
    run_dt = now_local(tz)
    run_date = run_dt.date().isoformat()

    out_dir = _ensure_dir(os.path.join(str(cfg.get("run.out_dir", "artifacts")), run_date))

    lookback_hours = int(cfg.get("run.lookback_hours", 72))
    since_dt = since_hours(tz, lookback_hours)

    log.info("Run date=%s tz=%s since=%s out=%s", run_date, tz, since_dt.isoformat(), out_dir)

    master = load_symbol_master(str(cfg.get("universe.symbol_master_path")))
    baseline_path = str(cfg.get("universe.baseline_symbols_path"))
    watchlist_path = str(cfg.get("universe.watchlist_path"))
    baseline = load_baseline_symbols(baseline_path) if os.path.exists(baseline_path) else []
    watchlist = load_watchlist(watchlist_path) if os.path.exists(watchlist_path) else []

    mapper = SymbolMapper(master, manual_aliases_path=str(cfg.get("universe.manual_aliases_path", "")))

    # News collection
    articles: List[Dict[str, Any]] = []
    if bool(cfg.get("news.google_news.enabled", True)):
        articles += collect_google_news_rss(
            query=str(cfg.get("news.google_news.query")),
            hl=str(cfg.get("news.google_news.hl", "en-IN")),
            gl=str(cfg.get("news.google_news.gl", "IN")),
            ceid=str(cfg.get("news.google_news.ceid", "IN:en")),
            max_items=int(cfg.get("news.max_articles", 150)),
        )
    if bool(cfg.get("news.gdelt.enabled", True)):
        articles += collect_gdelt(
            query=str(cfg.get("news.gdelt.query")),
            max_records=int(cfg.get("news.gdelt.max_records", 50)),
        )
    if bool(cfg.get("news.rss.enabled", True)):
        articles += collect_rss(list(cfg.get("news.rss.feeds", [])) or [], max_items=40)

    articles = dedup_articles(articles)
    log.info("Collected %d articles (deduped)", len(articles))

    # Entities -> symbols
    ents = extract_entities(articles, use_spacy=True)
    mapped = mapper.map_entities(ents)
    news_syms = [m.symbol for m in mapped]
    log.info("News-driven symbols: %d", len(news_syms))

    universe = _pick_universe(cfg, news_syms, baseline, watchlist)
    log.info("Final analysis universe: %d symbols", len(universe))

    # Store + sentiment
    store = SQLiteStore(path=str(cfg.get("run.state_db_path", ".cache/state.db")))
    store.upsert_articles(articles)

    sent_model = VaderSentiment()
    for a in articles:
        text = f"{a.get('title','')}\n{a.get('summary','')}"
        s, conf = sent_model.score(text)
        store.save_article_sentiment(a["digest"], s, conf, "vader")

    # attach all mapped symbols to each article (simplistic)
    store_syms = [(m.symbol, min(1.0, m.score / 100.0)) for m in mapped]
    for a in articles:
        store.save_article_symbols(a["digest"], store_syms)

    sym_sent = store.aggregate_symbol_sentiment(since_iso=since_dt.isoformat(), min_confidence=0.4)

    mdp = YFinanceProvider()
    fprov = YFinanceFundamentalsProvider()
    scorer = Scorer(weights=dict(cfg.get("scoring.weights", {})), thresholds=dict(cfg.get("scoring.thresholds", {})))

    sym_to_row = {r.symbol: r for r in master}

    def analyze(sym: str) -> Optional[Dict[str, Any]]:
        row = sym_to_row.get(sym)
        yahoo = row.yahoo if row else f"{sym}.NS"

        df = mdp.history(yahoo, days=int(cfg.get("market_data.history_days", 550)), interval=str(cfg.get("market_data.interval", "1d")))
        tech = compute_technicals(df, sma_fast=int(cfg.get("technicals.sma_fast", 20)), sma_slow=int(cfg.get("technicals.sma_slow", 50)), rsi_period=int(cfg.get("technicals.rsi_period", 14)))
        price = float(tech.get("last_close")) if tech and tech.get("last_close") is not None else None

        fraw = fprov.get(yahoo)
        info = dict((fraw.get("info") or {}))
        fun = _fundamentals_from_yf(info)

        sent = sym_sent.get(sym)
        scored = scorer.score_one(sentiment=sent, fundamentals=fun, technicals=tech)

        if price is None:
            target_range = "NA"
        else:
            vg = float(scored.get("value_gap", 0.0))
            base = price * (1.0 + max(-0.5, min(1.5, vg)))
            low = base * 0.85
            high = base * 1.15
            target_range = f"{low:.1f}–{high:.1f}"

        why = f"Score={scored['score']:.2f} ({scored['recommendation']}); Sent={sent if sent is not None else 'NA'}; ValueGap={fun.get('value_gap',0.0):+.1f}; Tech={tech.get('technical_score',0.5):.2f}"

        return {
            "symbol": sym,
            "company": row.name if row else sym,
            "sector": row.sector if row else "Unknown",
            "yahoo": yahoo,
            "price": price,
            "target_range": target_range,
            "sentiment": sent,
            **scored,
            "why": why,
        }

    results: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=int(cfg.get("market_data.max_workers", 6))) as ex:
        futs = {ex.submit(analyze, sym): sym for sym in universe}
        for fut in as_completed(futs):
            r = fut.result()
            if r:
                results.append(r)

    results.sort(key=lambda x: float(x.get("score", 0.0)), reverse=True)

    # sector boom
    sector_boom = compute_sector_boom([{"sector": r.get("sector"), "article_sentiment": r.get("sentiment")} for r in results])

    # outputs
    csv_path = out_dir / "recommendations.csv"
    if results:
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            w.writeheader()
            w.writerows(results)
    else:
        csv_path.write_text("note\nNo symbols selected today\n", encoding="utf-8")

    disclaimer = _read_disclaimer()

    html = render_html_report(run_date=run_date, tz=tz, rows=results[:40], articles=articles, disclaimer=disclaimer)
    html_path = out_dir / "report.html"
    html_path.write_text(html, encoding="utf-8")

    brief_md = build_brief_markdown(
        run_date=run_date,
        top_rows=results[:10],
        sector_boom=sector_boom,
        news_titles=[a.get("title", "") for a in articles],
        disclaimer=disclaimer,
    )
    brief_md_path = out_dir / "executive_brief.md"
    brief_md_path.write_text(brief_md, encoding="utf-8")

    brief_pdf_path = out_dir / "executive_brief.pdf"
    render_brief_pdf(brief_md, str(brief_pdf_path))

    # email: full disclaimer in body
    if bool(cfg.get("email.enabled", True)):
        to_val = cfg.get("email.to", [])
        to_list = [str(x).strip() for x in (to_val if isinstance(to_val, list) else str(to_val).split(",")) if str(x).strip()]
        smtp_cfg = SMTPConfig(
            host=str(cfg.get("email.smtp_host", "smtp.gmail.com")),
            port=int(cfg.get("email.smtp_port", 587)),
            from_env=str(cfg.get("email.from_env", "GMAIL_USER")),
            app_password_env=str(cfg.get("email.app_password_env", "GMAIL_APP_PASSWORD")),
            to=to_list,
        )
        prefix = str(cfg.get("email.subject_prefix", "[Daily NSE Intel]"))
        subject = f"{prefix} {run_date} — {len(results)} symbols"
        body = (
            f"Daily NSE Market Intelligence — {run_date}\n\n"
            f"Generated files:\n"
            f"- report.html\n- recommendations.csv\n- executive_brief.pdf\n\n"
            f"DISCLAIMER (full):\n\n{disclaimer}\n"
        )
        attachments = [
            ("report.html", html_path.read_bytes(), "text/html"),
            ("recommendations.csv", csv_path.read_bytes(), "text/csv"),
            ("executive_brief.pdf", brief_pdf_path.read_bytes(), "application/pdf"),
        ]
        send_email(smtp_cfg, subject=subject, body=body, attachments=attachments)

    store.close()
    log.info("Done. Wrote outputs to %s", out_dir)
    return out_dir
