# Daily NSE Market Intelligence (Clean Fixed Build)

A GitHub Actions–automated Python pipeline that:
- collects India + global market news (RSS + Google News RSS + GDELT),
- extracts entities and maps them to NSE symbols,
- fetches market data (yfinance),
- computes sentiment (VADER + optional LLM stub),
- computes technicals + fundamentals (best-effort with graceful degradation),
- scores and generates recommendations,
- produces **HTML report + CSV + Executive Brief (MD + PDF)**,
- emails outputs via Gmail SMTP (GitHub Secrets).

## Quickstart (Local)
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -U pip
pip install -e .
python -m spacy download en_core_web_sm  # optional (improves NER)
market-intel run --config config/config.yaml
```

Outputs are written under `artifacts/YYYY-MM-DD/`.

## GitHub Actions
- Workflow: `.github/workflows/daily.yml`
- Schedule: Mon–Fri **08:30 IST = 03:00 UTC** (cron uses UTC).
- Set secrets:
  - `GMAIL_USER`
  - `GMAIL_APP_PASSWORD`

## Notes / Limitations
- Fundamentals for many NSE tickers may be incomplete via yfinance. The system degrades gracefully and lowers confidence.
- News and feeds are deduped best-effort; Google News RSS can rate-limit occasionally.
- This is **not** investment advice. See `DISCLAIMER.md`.
