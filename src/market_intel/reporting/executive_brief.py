from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    PageBreak,
    Paragraph,
    SimpleDocTemplate,
    Spacer,
    Table,
    TableStyle,
)

# --------------------------------------------------------------------------------------
# MARKDOWN BUILDER (email-friendly, log-friendly)
# --------------------------------------------------------------------------------------


def _fmt_num(x: Any, *, decimals: int = 2) -> str:
    try:
        v = float(x)
    except Exception:
        return str(x) if x is not None else "NA"
    # Avoid ugly float artifacts (like 431.6499938964844)
    return f"{v:,.{decimals}f}"


def build_brief_markdown(
    *,
    run_date: str,
    top_rows: List[Dict[str, Any]],
    sector_boom: Dict[str, float],
    news_titles: List[str],
    disclaimer: str,
) -> str:
    """
    Produces a clean markdown-like plaintext body suitable for email and logs.
    PDF rendering is handled separately by render_brief_pdf() using a layout engine.
    """
    lines: List[str] = []
    lines.append(f"# Executive Brief — {run_date}")
    lines.append("")
    lines.append("## Top Calls (data-driven signals, NOT advice)")
    if not top_rows:
        lines.append("- No symbols selected today.")
    else:
        for r in top_rows:
            symbol = str(r.get("symbol", "")).strip()
            company = str(r.get("company", "")).strip()
            rec = str(r.get("recommendation", "NA")).strip()
            score = r.get("score", None)
            conf = r.get("confidence", None)

            price = _fmt_num(r.get("price", "NA"), decimals=2)
            target = str(r.get("target_range", "NA"))
            sent = r.get("sentiment", "NA")

            why = str(r.get("why", "")).strip()

            lines.append(
                f"- **{symbol}** ({company}) — **{rec}**, score={float(score):.2f}, conf={float(conf):.2f}"
                if score is not None and conf is not None
                else f"- **{symbol}** ({company}) — **{rec}**"
            )
            lines.append(f"  - Price={price}, Target={target}, Sentiment={sent}")
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
    if not news_titles:
        lines.append("- No news titles collected.")
    else:
        for t in news_titles[:10]:
            lines.append(f"- {t}")

    lines.append("")
    lines.append("## Disclaimer")
    lines.append(disclaimer.strip())
    lines.append("")
    return "\n".join(lines)


# --------------------------------------------------------------------------------------
# PDF RENDERER (executive-grade layout)
# Parses the markdown we produce above into structured sections and renders a styled PDF.
# --------------------------------------------------------------------------------------


@dataclass
class TopCall:
    symbol: str
    company: str
    recommendation: str
    score: Optional[float]
    confidence: Optional[float]
    price: str
    target: str
    sentiment: str
    why: str


def _safe_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except Exception:
        return None


_TOP_LINE_RE = re.compile(
    r"^- \*\*(?P<symbol>[^*]+)\*\* \((?P<company>[^)]*)\) — \*\*(?P<rec>[^*]+)\*\*, score=(?P<score>[0-9.]+), conf=(?P<conf>[0-9.]+)\s*$"
)
_TOP_LINE_FALLBACK_RE = re.compile(
    r"^- \*\*(?P<symbol>[^*]+)\*\* \((?P<company>[^)]*)\) — \*\*(?P<rec>[^*]+)\*\*\s*$"
)
_PRICE_LINE_RE = re.compile(
    r"^\s{2}- Price=(?P<price>.*?), Target=(?P<target>.*?), Sentiment=(?P<sent>.*)\s*$"
)
_WHY_LINE_RE = re.compile(r"^\s{2}- Why:\s*(?P<why>.*)\s*$")


def _parse_md_sections(md_text: str) -> Tuple[str, List[TopCall], List[Tuple[str, str]], List[str], str]:
    """
    Returns:
      run_date_title, top_calls, sector_pairs, news_titles, disclaimer_text
    """
    lines = md_text.splitlines()
    run_title = "Executive Brief"
    top_calls: List[TopCall] = []
    sector_pairs: List[Tuple[str, str]] = []
    news: List[str] = []
    disclaimer_lines: List[str] = []

    section = None
    i = 0

    # Find title
    for ln in lines:
        if ln.startswith("# "):
            run_title = ln[2:].strip()
            break

    while i < len(lines):
        ln = lines[i].rstrip("\n")

        if ln.startswith("## "):
            hdr = ln[3:].strip().lower()
            if hdr.startswith("top calls"):
                section = "top"
            elif hdr.startswith("sector / theme momentum"):
                section = "sector"
            elif hdr.startswith("key news drivers"):
                section = "news"
            elif hdr.startswith("disclaimer"):
                section = "disclaimer"
            else:
                section = None
            i += 1
            continue

        if section == "top":
            m = _TOP_LINE_RE.match(ln) or _TOP_LINE_FALLBACK_RE.match(ln)
            if m:
                symbol = (m.group("symbol") or "").strip()
                company = (m.group("company") or "").strip()
                rec = (m.group("rec") or "").strip()
                score = _safe_float(m.groupdict().get("score", "") or "")
                conf = _safe_float(m.groupdict().get("conf", "") or "")

                price = "NA"
                target = "NA"
                sent = "NA"
                why = ""

                # lookahead for price/why lines
                if i + 1 < len(lines):
                    pm = _PRICE_LINE_RE.match(lines[i + 1])
                    if pm:
                        price = (pm.group("price") or "NA").strip()
                        target = (pm.group("target") or "NA").strip()
                        sent = (pm.group("sent") or "NA").strip()
                        i += 1

                if i + 1 < len(lines):
                    wm = _WHY_LINE_RE.match(lines[i + 1])
                    if wm:
                        why = (wm.group("why") or "").strip()
                        i += 1

                top_calls.append(
                    TopCall(
                        symbol=symbol,
                        company=company,
                        recommendation=rec,
                        score=score,
                        confidence=conf,
                        price=price,
                        target=target,
                        sentiment=sent,
                        why=why,
                    )
                )
                i += 1
                continue

        if section == "sector":
            # expects "- Something: +0.44"
            if ln.startswith("- "):
                txt = ln[2:].strip()
                if ":" in txt:
                    k, v = txt.split(":", 1)
                    sector_pairs.append((k.strip(), v.strip()))
            i += 1
            continue

        if section == "news":
            if ln.startswith("- "):
                news.append(ln[2:].strip())
            i += 1
            continue

        if section == "disclaimer":
            # keep all lines verbatim
            if ln.strip() != "":
                disclaimer_lines.append(ln)
            else:
                disclaimer_lines.append("")  # preserve paragraph breaks
            i += 1
            continue

        i += 1

    disclaimer_text = "\n".join(disclaimer_lines).strip()
    return run_title, top_calls, sector_pairs, news, disclaimer_text


def _shorten_why(why: str, max_chars: int = 140) -> str:
    """
    Converts verbose model diagnostics into something an exec can scan.
    Keeps first 2 clauses if semicolon-separated; otherwise truncates.
    """
    if not why:
        return ""
    parts = [p.strip() for p in why.split(";") if p.strip()]
    if len(parts) >= 2:
        out = f"{parts[0]}; {parts[1]}"
    else:
        out = why.strip()
    if len(out) > max_chars:
        out = out[: max_chars - 1].rstrip() + "…"
    return out


def _footer(canvas, doc):
    canvas.saveState()
    w, h = A4
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(colors.grey)

    # footer left: disclaimer reminder
    canvas.drawString(2 * cm, 1.2 * cm, "Educational / research use only — NOT investment advice.")
    # footer right: page number
    canvas.drawRightString(w - 2 * cm, 1.2 * cm, f"Page {doc.page}")
    canvas.restoreState()


def render_brief_pdf(md_text: str, out_path: str) -> None:
    """
    Executive-grade PDF rendering:
    - Proper headings
    - Top Calls table (wrapped cells)
    - Clean bullet lists
    - Dedicated Disclaimer page
    - Page footer + page numbers
    """
    title, top_calls, sector_pairs, news_titles, disclaimer_text = _parse_md_sections(md_text)

    doc = SimpleDocTemplate(
        out_path,
        pagesize=A4,
        leftMargin=2 * cm,
        rightMargin=2 * cm,
        topMargin=2 * cm,
        bottomMargin=2 * cm,
        title=title,
    )

    styles = getSampleStyleSheet()

    title_style = ParagraphStyle(
        "ExecTitle",
        parent=styles["Title"],
        fontName="Helvetica-Bold",
        fontSize=18,
        leading=22,
        spaceAfter=10,
    )

    h2_style = ParagraphStyle(
        "ExecH2",
        parent=styles["Heading2"],
        fontName="Helvetica-Bold",
        fontSize=12,
        leading=14,
        spaceBefore=10,
        spaceAfter=6,
    )

    body_style = ParagraphStyle(
        "ExecBody",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=9.5,
        leading=12,
        spaceAfter=4,
    )

    small_style = ParagraphStyle(
        "ExecSmall",
        parent=styles["BodyText"],
        fontName="Helvetica",
        fontSize=8.5,
        leading=11,
        spaceAfter=3,
    )

    story: List[Any] = []

    # Title
    story.append(Paragraph(title, title_style))
    story.append(Spacer(1, 6))

    # Top Calls
    story.append(Paragraph("Top Calls (data-driven signals, NOT advice)", h2_style))

    if not top_calls:
        story.append(Paragraph("No symbols selected today.", body_style))
    else:
        # Table: Symbol | Company | Signal | Score | Conf | Price | Target | Sent | Key Drivers
        header = [
            Paragraph("<b>Ticker</b>", small_style),
            Paragraph("<b>Company</b>", small_style),
            Paragraph("<b>Signal</b>", small_style),
            Paragraph("<b>Score</b>", small_style),
            Paragraph("<b>Conf</b>", small_style),
            Paragraph("<b>Price</b>", small_style),
            Paragraph("<b>Target Range</b>", small_style),
            Paragraph("<b>Sent</b>", small_style),
            Paragraph("<b>Key drivers</b>", small_style),
        ]

        rows: List[List[Any]] = [header]
        for r in top_calls[:12]:  # keep exec view tight; rest can live in HTML report
            score = f"{r.score:.2f}" if r.score is not None else "NA"
            conf = f"{r.confidence:.2f}" if r.confidence is not None else "NA"

            rows.append(
                [
                    Paragraph(f"<b>{r.symbol}</b>", small_style),
                    Paragraph(r.company or "—", small_style),
                    Paragraph(r.recommendation or "—", small_style),
                    Paragraph(score, small_style),
                    Paragraph(conf, small_style),
                    Paragraph(str(r.price), small_style),
                    Paragraph(str(r.target), small_style),
                    Paragraph(str(r.sentiment), small_style),
                    Paragraph(_shorten_why(r.why) or "—", small_style),
                ]
            )

        # column widths tuned for A4 with 2cm margins
        col_widths = [2.0 * cm, 4.0 * cm, 2.4 * cm, 1.2 * cm, 1.2 * cm, 2.0 * cm, 3.0 * cm, 1.2 * cm, 4.0 * cm]

        tbl = Table(rows, colWidths=col_widths, repeatRows=1, hAlign="LEFT")
        tbl.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F0F3F6")),
                    ("TEXTCOLOR", (0, 0), (-1, 0), colors.HexColor("#111111")),
                    ("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.HexColor("#C9D3DD")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#E0E6ED")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        story.append(tbl)

    # Sector momentum
    story.append(Spacer(1, 10))
    story.append(Paragraph("Sector / Theme Momentum (news sentiment)", h2_style))

    if sector_pairs:
        # keep top 6
        sp = sector_pairs[:6]
        sec_rows = [[Paragraph("<b>Sector</b>", small_style), Paragraph("<b>Sentiment</b>", small_style)]]
        for k, v in sp:
            sec_rows.append([Paragraph(k, small_style), Paragraph(v, small_style)])

        sec_tbl = Table(sec_rows, colWidths=[10 * cm, 4 * cm], repeatRows=1, hAlign="LEFT")
        sec_tbl.setStyle(
            TableStyle(
                [
                    ("BACKGROUND", (0, 0), (-1, 0), colors.HexColor("#F0F3F6")),
                    ("GRID", (0, 0), (-1, -1), 0.25, colors.HexColor("#E0E6ED")),
                    ("VALIGN", (0, 0), (-1, -1), "TOP"),
                    ("LEFTPADDING", (0, 0), (-1, -1), 5),
                    ("RIGHTPADDING", (0, 0), (-1, -1), 5),
                    ("TOPPADDING", (0, 0), (-1, -1), 4),
                    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
                ]
            )
        )
        story.append(sec_tbl)
    else:
        story.append(Paragraph("Not enough data for sector aggregation today.", body_style))

    # News drivers
    story.append(Spacer(1, 10))
    story.append(Paragraph("Key News Drivers (sample)", h2_style))
    if not news_titles:
        story.append(Paragraph("No news titles collected.", body_style))
    else:
        for t in news_titles[:10]:
            story.append(Paragraph(f"• {t}", body_style))

    # Disclaimer page
    story.append(PageBreak())
    story.append(Paragraph("Disclaimer", h2_style))

    if disclaimer_text:
        # Preserve paragraphs: convert blank lines to <br/><br/>
        esc = (
            disclaimer_text.replace("&", "&amp;")
            .replace("<", "&lt;")
            .replace(">", "&gt;")
        )
        html = "<br/>".join(esc.splitlines())
        story.append(Paragraph(html, body_style))
    else:
        story.append(
            Paragraph(
                "DISCLAIMER: Educational / research use only. Not investment advice.",
                body_style,
            )
        )

    doc.build(story, onFirstPage=_footer, onLaterPages=_footer)
