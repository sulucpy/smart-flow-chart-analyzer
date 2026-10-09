# Smart Flow Gold — FINAL V3 PRO + Turtle (merge candidate)

## Run
1. Configure `SIFTINGIO_API_KEY`, `TELEGRAM_BOT_TOKEN`, and `TELEGRAM_CHAT_ID` in Streamlit Secrets.
2. Install dependencies: `pip install -r requirements.txt`
3. Run: `streamlit run app.py`

## Included
- Existing Quick Trade / Full Pro mobile-oriented dashboard and Order Block map
- SiftingIO XAUUSD WebSocket, 1H/15M/5M analysis, Smart Flow, structure, VWAP, early-entry, institutional confluence, triggers/retests, trade levels, Telegram alerts, paper trading, data health and master snapshot
- Separate Turtle module (20-day breakout, 10-day exit, ATR-based sizing, max 1% account risk)
- Official CFTC COMEX Gold COT/OI only when source data is available; unavailable values remain unavailable
- Added candle-derived Market Regime, Equal High/Low liquidity estimates, and a diagnostic A+ gate. These do not override the main trade confirmation.

## Important validation limits
`python -m py_compile app.py` passed. This is a syntax/dependency-package check, not an end-to-end live API/WebSocket, Streamlit UI, or trading-performance test. No broker orders are placed. Test with paper trading first.
