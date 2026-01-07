from __future__ import annotations

from typing import Any, Dict, List

from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.pdfgen.canvas import Canvas


def build_brief_markdown(*, run_date: str, top_rows: List[Dict[str, Any]], sector_boom: Dict[str, float], news_titles: List[str], disclaimer: str) -> str:
    lines: List[str] = []
    lines.append(f"# Executive Brief — {run_date}")
    lines.append("")
    lines.append("## Top Calls (data-driven signals, NOT advice)")
    if not top_rows:
        lines.append("- No symbols selected today.")
    for r in top_rows:
        lines.append(f"- **{r['symbol']}** ({r.get('company','')}) — **{r['recommendation']}**, score={r['score']:.2f}, conf={r['confidence']:.2f}")
        lines.append(f"  - Price={r.get('price','NA')}, Target={r.get('target_range','NA')}, Sentiment={r.get('sentiment','NA')}")
        lines.append(f"  - Why: {r.get('why','')}")
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
    for t in news_titles[:10]:
        lines.append(f"- {t}")
    lines.append("")
    lines.append("## Disclaimer")
    lines.append(disclaimer.strip())
    lines.append("")
    return "\n".join(lines)


def render_brief_pdf(md_text: str, out_path: str) -> None:
    c = Canvas(out_path, pagesize=A4)
    width, height = A4
    x = 2 * cm
    y = height - 2 * cm

    c.setFont("Helvetica-Bold", 14)
    c.drawString(x, y, "Executive Brief")
    y -= 0.8 * cm

    c.setFont("Helvetica", 9)
    for line in md_text.splitlines():
        if y < 2 * cm:
            c.showPage()
            y = height - 2 * cm
            c.setFont("Helvetica", 9)

        chunk = line
        while len(chunk) > 115:
            c.drawString(x, y, chunk[:115])
            chunk = chunk[115:]
            y -= 0.45 * cm
            if y < 2 * cm:
                c.showPage()
                y = height - 2 * cm
                c.setFont("Helvetica", 9)

        c.drawString(x, y, chunk)
        y -= 0.45 * cm

    c.save()
