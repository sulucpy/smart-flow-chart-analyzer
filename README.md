# Smart Flow Gold V3 PRO MAX — Latest Full Staging Package

**Package status:** staging candidate only; not approved for production deployment.
**Build date:** 2026-10-27
**Source:** corrected merged Smart Flow Gold candidate with Time Machine + Risk Manager staging changes.
**SHA-256 (`app.py`):** `490a7d2bef0622ce9b5887cde7092c5640c9a7a631454e4d067aa2211e9d1fcb`

## What this package includes
- Existing Quick Trade mobile-first interface and Full Pro mode.
- Existing XAUUSD live-data, multi-timeframe, structure/liquidity, VWAP, signal and target/stop modules.
- Existing paper-trade logging and Telegram alert code (credentials must be configured in Streamlit Secrets).
- Strategy Lab historical backtest.
- Time Machine + Risk Manager research mode.
- Turtle breakout module and configurable risk inputs up to 5% in this staging build (not a recommendation).
- Scanner and gold news guide/manual event context.
- Updated Time Machine label that explicitly distinguishes trade-outcome bootstrap from a full intraday OHLC candle-path replay.
- Audit notes, feature mapping, regression checklist, and static verification script.

## Integrating the new Day Trader Skill ideas from the screenshots
The most relevant Gold-compatible concepts are already represented in this source as existing modules: scanner, news/event risk guide, early-entry/trigger map, stop/target planning, historical Strategy Lab, Time Machine/Monte Carlo, Risk Manager, and Telegram price/signal alerts. They are retained rather than duplicated as separate competing engines.

US-equity-specific Earnings Analyst and Pelosi Tracker concepts are not copied into XAUUSD; they do not directly fit spot Gold. COMEX/COT data must remain unavailable unless a genuine source and freshness are verified. This package does not claim that those external data sources are live.

## Important limitations
- Time Machine is currently a historical strategy test plus bootstrap of observed trade R outcomes. It is **not** the upstream skill's 10,000 intraday OHLC path replay.
- Risk sizing depends on the broker's contract size, point/tick value, account currency, lot step, spread, fees, and slippage. Verify them independently.
- No automatic broker order execution is added.
- Syntax/static checks are not proof of trading accuracy or a successful Streamlit runtime.
- Streamlit Cloud, SiftingIO live/historical API, Telegram delivery, all UI modes, and all market scenarios have not been end-to-end tested in this environment.

## Setup in a separate staging app
1. Extract the ZIP to a separate folder/branch; keep the production app unchanged.
2. Install dependencies from `requirements.txt` in a clean environment.
3. Configure `SIFTINGIO_API_KEY` and any Telegram settings through Streamlit Secrets only; never put credentials in source code or chat.
4. Run `python verify_source.py` and `python -m py_compile app.py`.
5. Start the Streamlit app and test every interface mode on desktop and mobile.
6. Use paper/research mode only until live data, risk math, historical tests, and all regression checks pass.

See `AUDIT_REPORT.md`, `FEATURE_INTEGRATION_NOTES.md`, and `REGRESSION_CHECKLIST.md` before deployment.
