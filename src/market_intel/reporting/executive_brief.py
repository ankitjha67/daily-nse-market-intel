from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate,
    Paragraph,
    Spacer,
    Table,
    TableStyle,
    PageBreak,
    KeepTogether,
)

# ----------------------------
# Public API (used by pipeline)
# ----------------------------

def build_brief_markdown(
    *,
    run_date: str,
    top_rows: List[Dict[str, Any]],
    sector_boom: Dict[str, float],
    news_titles: List[str],
    disclaimer: str,
) -> str:
    """
    Human-readable Markdown used for:
      - email body (plain text / markdown)
      - PDF generation (render_brief_pdf parses this markdown)
    """
    lines: List[str] = []
    lines.append(f"# Executive Brief — {run_date}")
    lines.append("")
    lines.append("## Top Calls (data-driven signals, NOT advice)")
    if not top_rows:
        lines.append("- No symbols selected today.")
    else:
        for r in top_rows:
            lines.append(
                f"- **{r.get('symbol','')}** ({r.get('company','')}) — **{r.get('recommendation','')}**, "
                f"score={float(r.get('score',0.0)):.2f}, conf={float(r.get('confidence',0.0)):.2f}"
            )
            lines.append(
                f"  - Price={_fmt_num(r.get('price'))}, TargetRange={r.get('target_range','NA')}, Sentiment={_fmt_num(r.get('sentiment'))}"
            )
            why = str(r.get("why", "")).strip()
            if why:
                lines.append(f"  - Why: {why}")

    lines.append("")
    lines.append("## Sector / Theme Momentum (news sentiment)")
    if sector_boom:
        top = sorted(sector_boom.items(), key=lambda x: x[1], reverse=True)[:8]
        for k, v in top:
            lines.append(f"- {k}: {v:+.2f}")
    else:
        lines.append("- Not enough data for sector aggregation today.")

    lines.append("")
    lines.append("## Key News Drivers (sample)")
    for t in (news_titles or [])[:10]:
        lines.append(f"- {t}")

    lines.append("")
    lines.append("## Disclaimer")
    # Ensure disclaimer is not accidentally markdown-titled again
    lines.append(_strip_md(disclaimer).strip())
    lines.append("")
    return "\n".join(lines)


def render_brief_pdf(md_text: str, out_path: str) -> None:
    """
    Executive-grade PDF renderer:
      - Parses the markdown produced by build_brief_markdown()
      - Renders tables + bullet lists + clean disclaimer page
    """
    parsed = _parse_brief_markdown(md_text)

    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        leftMargin=1.4 * cm,
        rightMargin=1.4 * cm,
        topMargin=1.4 * cm,
        bottomMargin=1.4 * cm,
        title="Executive Brief",
        author="market-intel",
    )

    styles = getSampleStyleSheet()
    style_title = ParagraphStyle(
        "EB_Title",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=16,
        leading=18,
        spaceAfter=10,
    )
    style_subtitle = ParagraphStyle(
        "EB_Subtitle",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=10,
        leading=12,
        textColor=colors.grey,
        spaceAfter=14,
    )
    style_h = ParagraphStyle(
        "EB_H",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=14,
        spaceBefore=10,
        spaceAfter=8,
    )
    style_body = ParagraphStyle(
        "EB_Body",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=9,
        leading=11,
    )
    style_small = ParagraphStyle(
        "EB_Small",
        parent=styles["Normal"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=10.5,
    )

    story: List[Any] = []

    # Title block
    story.append(Paragraph("Executive Brief", style_title))
    story.append(Paragraph(f"Run date: {parsed.run_date or 'NA'}", style_subtitle))

    # Top Calls
    story.append(Paragraph("Top Calls (data-driven signals, NOT advice)", style_h))
    if not parsed.top_calls:
        story.append(Paragraph("No symbols selected today.", style_body))
    else:
        story.append(_build_top_calls_table(parsed.top_calls, style_small))

    # Sector momentum
    story.append(Spacer(1, 10))
    story.append(Paragraph("Sector / Theme Momentum (news sentiment)", style_h))
    sector_rows = [(k, v) for (k, v) in (parsed.sector_boom or []) if k and k.strip()]
    sector_rows = sorted(sector_rows, key=lambda x: x[1], reverse=True)

    # Hide "Unknown only" to avoid exec confusion
    if not sector_rows or (len(sector_rows) == 1 and sector_rows[0][0].strip().lower() == "unknown"):
        story.append(Paragraph("Sector momentum unavailable or insufficiently classified today.", style_body))
    else:
        story.append(_build_sector_table(sector_rows[:8], style_small))

    # News drivers
    story.append(Spacer(1, 10))
    story.append(Paragraph("Key News Drivers (sample)", style_h))
    if not parsed.news_titles:
        story.append(Paragraph("No news titles available today.", style_body))
    else:
        for t in parsed.news_titles[:12]:
            story.append(Paragraph(f"• {_escape(t)}", style_body))

    # Disclaimer as last page
    story.append(PageBreak())
    story.append(Paragraph("Disclaimer", style_h))
    disclaimer_clean = _strip_md(parsed.disclaimer or "").strip()
    if disclaimer_clean:
        for para in _split_paragraphs(disclaimer_clean):
            story.append(Paragraph(_escape(para), style_body))
            story.append(Spacer(1, 6))
    else:
        story.append(Paragraph("No disclaimer text provided.", style_body))

    def _on_page(canvas, doc_obj):
        canvas.saveState()
        canvas.setFont("Helvetica", 8)
        footer_left = "Educational / research use only — NOT investment advice."
        canvas.setFillColor(colors.grey)
        canvas.drawString(doc_obj.leftMargin, 0.9 * cm, footer_left)
        canvas.drawRightString(A4[0] - doc_obj.rightMargin, 0.9 * cm, f"Page {doc_obj.page}")
        canvas.restoreState()

    doc.build(story, onFirstPage=_on_page, onLaterPages=_on_page)


# ----------------------------
# Internals
# ----------------------------

@dataclass
class _TopCall:
    ticker: str
    company: str
    signal: str
    score: Optional[float]
    conf: Optional[float]
    price: str
    target_range: str
    sentiment: str
    drivers: str


@dataclass
class _ParsedBrief:
    run_date: str
    top_calls: List[_TopCall]
    sector_boom: List[Tuple[str, float]]
    news_titles: List[str]
    disclaimer: str


def _fmt_num(x: Any) -> str:
    if x is None:
        return "NA"
    try:
        xf = float(x)
        return f"{xf:.2f}"
    except Exception:
        s = str(x).strip()
        return s if s else "NA"


def _strip_md(s: str) -> str:
    # remove common markdown tokens but keep meaning
    s = re.sub(r"^\s*#+\s*", "", s, flags=re.M)  # headings like # Title
    s = s.replace("**", "")
    s = s.replace("__", "")
    return s


def _escape(s: str) -> str:
    # ReportLab Paragraph supports a subset of HTML; escape the basics
    return (
        str(s)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _split_paragraphs(s: str) -> List[str]:
    parts = [p.strip() for p in re.split(r"\n\s*\n+", s) if p.strip()]
    return parts or [s.strip()] if s.strip() else []


def _parse_brief_markdown(md: str) -> _ParsedBrief:
    lines = md.splitlines()

    run_date = ""
    top_calls: List[_TopCall] = []
    sector_boom: List[Tuple[str, float]] = []
    news_titles: List[str] = []
    disclaimer = ""

    # run_date from first header
    for ln in lines:
        m = re.match(r"^\s*#\s*Executive Brief\s+—\s*(.+?)\s*$", ln.strip())
        if m:
            run_date = m.group(1).strip()
            break

    # Find sections
    def find_idx(title: str) -> int:
        for i, ln in enumerate(lines):
            if ln.strip().lower() == title.strip().lower():
                return i
        return -1

    i_top = find_idx("## Top Calls (data-driven signals, NOT advice)")
    i_sector = find_idx("## Sector / Theme Momentum (news sentiment)")
    i_news = find_idx("## Key News Drivers (sample)")
    i_disc = find_idx("## Disclaimer")

    # Parse top calls block
    if i_top != -1:
        end = min([x for x in [i_sector, i_news, i_disc, len(lines)] if x != -1])
        block = lines[i_top + 1 : end]

        cur: Dict[str, Any] = {}
        for ln in block:
            s = ln.rstrip()
            if s.startswith("- **") and "—" in s:
                # flush previous
                if cur:
                    top_calls.append(_topcall_from_cur(cur))
                    cur = {}
                # main line
                # - **TICKER** (Company) — **Signal**, score=0.86, conf=0.76
                mm = re.match(r"-\s*\*\*(.+?)\*\*\s*\((.*?)\)\s*—\s*\*\*(.+?)\*\*.*?score=([0-9.]+).*?conf=([0-9.]+)", s)
                if mm:
                    cur["ticker"] = mm.group(1).strip()
                    cur["company"] = mm.group(2).strip()
                    cur["signal"] = mm.group(3).strip()
                    cur["score"] = float(mm.group(4))
                    cur["conf"] = float(mm.group(5))
                else:
                    # fallback
                    cur["raw"] = s

            elif s.strip().startswith("- Price=") or s.strip().startswith("Price="):
                #  - Price=..., TargetRange=..., Sentiment=...
                cur["price_line"] = s.strip("- ").strip()
            elif s.strip().startswith("- Why:") or s.strip().startswith("Why:"):
                cur["why"] = s.strip("- ").strip().replace("Why:", "").strip()

        if cur:
            top_calls.append(_topcall_from_cur(cur))

    # Parse sector block
    if i_sector != -1:
        end = min([x for x in [i_news, i_disc, len(lines)] if x != -1 and x > i_sector] or [len(lines)])
        for ln in lines[i_sector + 1 : end]:
            m = re.match(r"^\s*-\s*(.+?)\s*:\s*([+-]?\d+(?:\.\d+)?)\s*$", ln.strip())
            if m:
                k = m.group(1).strip()
                v = float(m.group(2))
                sector_boom.append((k, v))

    # Parse news titles
    if i_news != -1:
        end = min([x for x in [i_disc, len(lines)] if x != -1 and x > i_news] or [len(lines)])
        for ln in lines[i_news + 1 : end]:
            if ln.strip().startswith("- "):
                news_titles.append(ln.strip()[2:].strip())

    # Parse disclaimer
    if i_disc != -1:
        disclaimer = "\n".join(lines[i_disc + 1 :]).strip()

    return _ParsedBrief(
        run_date=run_date,
        top_calls=top_calls,
        sector_boom=sector_boom,
        news_titles=news_titles,
        disclaimer=disclaimer,
    )


def _topcall_from_cur(cur: Dict[str, Any]) -> _TopCall:
    ticker = str(cur.get("ticker", "")).strip()
    company = str(cur.get("company", "")).strip()
    signal = str(cur.get("signal", "")).strip()

    score = cur.get("score")
    conf = cur.get("conf")

    price = "NA"
    target_range = "NA"
    sentiment = "NA"
    price_line = str(cur.get("price_line", "")).strip()
    if price_line:
        # Price=..., TargetRange=..., Sentiment=...
        # tolerate different key names
        parts = [p.strip() for p in price_line.split(",")]
        kv = {}
        for p in parts:
            if "=" in p:
                k, v = p.split("=", 1)
                kv[k.strip().lower()] = v.strip()
        price = kv.get("price", "NA")
        target_range = kv.get("targetrange", kv.get("target_range", kv.get("target", "NA")))
        sentiment = kv.get("sentiment", "NA")

        # round sentiment if numeric
        sentiment = _fmt_num(sentiment)

    drivers = str(cur.get("why", "")).strip()
    drivers = _shorten_drivers(drivers)

    return _TopCall(
        ticker=ticker,
        company=company,
        signal=signal,
        score=float(score) if isinstance(score, (int, float)) else None,
        conf=float(conf) if isinstance(conf, (int, float)) else None,
        price=_fmt_num(price),
        target_range=str(target_range),
        sentiment=str(sentiment),
        drivers=drivers,
    )


def _shorten_drivers(s: str) -> str:
    s = _strip_md(s)
    s = re.sub(r"\s+", " ", s).strip()
    # common compressions
    s = s.replace("Final score", "Score")
    s = s.replace("confidence", "Conf")
    # round any long floats to 2 decimals
    def repl(m):
        try:
            return f"{float(m.group(0)):.2f}"
        except Exception:
            return m.group(0)
    s = re.sub(r"[+-]?\d+\.\d{4,}", repl, s)
    # keep it short for table cell
    if len(s) > 140:
        s = s[:137].rstrip() + "…"
    return s


def _build_top_calls_table(rows: List[_TopCall], style_small: ParagraphStyle) -> KeepTogether:
    # Column widths tuned for A4 minus margins (~18.2 cm usable if 1.4cm margins both sides)
    col_widths = [
        2.2 * cm,  # Ticker
        4.6 * cm,  # Company
        2.2 * cm,  # Signal
        1.2 * cm,  # Score
        1.2 * cm,  # Conf
        1.6 * cm,  # Price
        2.8 * cm,  # Target Range
        1.2 * cm,  # Sent
        3.0 * cm,  # Key drivers
    ]

    data: List[List[Any]] = []
    data.append(
        [
            Paragraph("<b>Ticker</b>", style_small),
            Paragraph("<b>Company</b>", style_small),
            Paragraph("<b>Signal</b>", style_small),
            Paragraph("<b>Score</b>", style_small),
            Paragraph("<b>Conf</b>", style_small),
            Paragraph("<b>Price</b>", style_small),
            Paragraph("<b>Target Range</b>", style_small),
            Paragraph("<b>Sent</b>", style_small),
            Paragraph("<b>Key drivers</b>", style_small),
        ]
    )

    for r in rows[:12]:
        data.append(
            [
                Paragraph(f"<b>{_escape(r.ticker)}</b>", style_small),
                Paragraph(_escape(r.company), style_small),
                Paragraph(_escape(r.signal), style_small),
                Paragraph(_escape(f"{r.score:.2f}" if r.score is not None else "NA"), style_small),
                Paragraph(_escape(f"{r.conf:.2f}" if r.conf is not None else "NA"), style_small),
                Paragraph(_escape(r.price), style_small),
                Paragraph(_escape(r.target_range), style_small),
                Paragraph(_escape(r.sentiment), style_small),
                Paragraph(_escape(r.drivers), style_small),
            ]
        )

    tbl = Table(data, colWidths=col_widths, repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEF2F7")),
                ("TEXTCOLOR", (0, 0), (-1, 0), colors.black),
                ("LINEBELOW", (0, 0), (-1, 0), 1, colors.HexColor("#CBD5E1")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (3, 1), (7, -1), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return KeepTogether([tbl])


def _build_sector_table(rows: List[Tuple[str, float]], style_small: ParagraphStyle) -> KeepTogether:
    data: List[List[Any]] = []
    data.append([Paragraph("<b>Sector</b>", style_small), Paragraph("<b>Sentiment</b>", style_small)])
    for k, v in rows:
        data.append([Paragraph(_escape(k), style_small), Paragraph(_escape(f"{v:+.2f}"), style_small)])

    tbl = Table(data, colWidths=[10.5 * cm, 3.0 * cm], repeatRows=1)
    tbl.setStyle(
        TableStyle(
            [
                ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#EEF2F7")),
                ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#E5E7EB")),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("ALIGN", (1, 1), (1, -1), "RIGHT"),
                ("LEFTPADDING", (0, 0), (-1, -1), 6),
                ("RIGHTPADDING", (0, 0), (-1, -1), 6),
                ("TOPPADDING", (0, 0), (-1, -1), 4),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
            ]
        )
    )
    return KeepTogether([tbl])
