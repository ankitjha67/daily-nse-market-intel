from __future__ import annotations

from typing import Any, Dict, List

from jinja2 import Template


_HTML = Template("""<!doctype html>
<html>
<head>
<meta charset="utf-8">
<title>Daily NSE Market Intelligence — {{ run_date }}</title>
<style>
body { font-family: Arial, sans-serif; margin: 18px; color: #111; }
h1 { margin: 0 0 6px 0; }
.small { color: #444; font-size: 12px; }
.card { border: 1px solid #ddd; border-radius: 10px; padding: 14px; margin: 12px 0; }
table { border-collapse: collapse; width: 100%; }
th, td { border-bottom: 1px solid #eee; padding: 8px; text-align: left; font-size: 13px; }
th { background: #fafafa; }
.badge { padding: 2px 8px; border-radius: 999px; font-size: 12px; border: 1px solid #ddd; }
</style>
</head>
<body>
<h1>Daily NSE Market Intelligence</h1>
<div class="small">Run date: {{ run_date }} | Timezone: {{ tz }} | Articles: {{ n_articles }} | Symbols: {{ n_symbols }}</div>

<div class="card">
  <h2>Top Calls (signals, NOT advice)</h2>
  {% if rows %}
  <table>
    <tr>
      <th>Symbol</th><th>Company</th><th>Reco</th><th>Score</th><th>Conf</th><th>Price</th><th>Target (Low–High)</th><th>Why</th>
    </tr>
    {% for r in rows %}
    <tr>
      <td><b>{{ r.symbol }}</b></td>
      <td>{{ r.company }}</td>
      <td><span class="badge">{{ r.recommendation }}</span></td>
      <td>{{ "%.2f"|format(r.score) }}</td>
      <td>{{ "%.2f"|format(r.confidence) }}</td>
      <td>{{ "%.2f"|format(r.price) if r.price is not none else "NA" }}</td>
      <td>{{ r.target_range }}</td>
      <td>{{ r.why }}</td>
    </tr>
    {% endfor %}
  </table>
  {% else %}
  <div>No symbols were selected today (news + baseline + watchlist produced an empty universe).</div>
  {% endif %}
</div>

<div class="card">
  <h2>News (sample)</h2>
  <ul>
    {% for a in articles %}
    <li><b>{{ a.source }}</b>: <a href="{{ a.url }}">{{ a.title }}</a></li>
    {% endfor %}
  </ul>
</div>

<div class="card">
  <h2>Disclaimer</h2>
  <pre style="white-space: pre-wrap; font-size: 12px; line-height: 1.35;">{{ disclaimer }}</pre>
</div>

</body>
</html>
""")


def render_html_report(*, run_date: str, tz: str, rows: List[Dict[str, Any]], articles: List[Dict[str, Any]], disclaimer: str) -> str:
    return _HTML.render(
        run_date=run_date,
        tz=tz,
        rows=rows,
        articles=articles[:20],
        n_articles=len(articles),
        n_symbols=len(rows),
        disclaimer=disclaimer,
    )
