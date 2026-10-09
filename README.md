# Smart Flow Gold — V3 PRO Scope Fix V2 + SiftingIO Auth Diagnostics

## Deploy
1. Keep `app.py` in the repository root.
2. In Streamlit Cloud, open **Manage app -> Settings -> Secrets**.
3. Add/update `SIFTINGIO_API_KEY` with your original plain ASCII SiftingIO API token.
4. Optional Telegram alerts require `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
5. Install dependencies from `requirements.txt`.

## Included
- Quick Trade / Full Pro mobile-oriented dashboard and Order Block map
- SiftingIO XAUUSD WebSocket and historical bars, multi-timeframe analysis, Smart Flow, structure, VWAP, early-entry engine, institutional confluence, triggers/retests and trade levels
- Telegram alerts, paper-trading log, data-health display and master snapshot
- Turtle breakout module with ATR sizing and maximum 1% planned account risk
- Official CFTC COMEX Gold COT/OI only when source data is available
- Market Regime, Equal-High/Low liquidity estimates and diagnostic A+ gate

## API key / HTTP 401
- The app trims whitespace and removes non-ASCII characters from the SiftingIO token before using it in HTTP headers.
- HTTP 401 now gives an authentication/access checklist; HTTP 403 gives a permission warning.
- A code update cannot make an invalid/revoked key valid or grant missing API permissions. If 401 remains, verify the key and endpoint permissions with SiftingIO.
- Never put API keys or Telegram tokens in the Python file or GitHub repository.

## Validation limits
Python syntax compilation and ZIP structure are checked. Live SiftingIO REST/WebSocket requests, the deployed Streamlit UI, and trading performance have not been verified here. Signals are informational only; use paper trading first. No broker orders are placed.
