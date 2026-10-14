# Smart Flow Gold — V3 PRO Auth Fix + Magic Ratio Area

## Deploy
1. Keep `app.py` in the repository root.
2. In Streamlit Cloud, open **Manage app -> Settings -> Secrets**.
3. Add/update `SIFTINGIO_API_KEY` with your original plain ASCII SiftingIO API token.
4. Optional Telegram alerts require `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID`.
5. Install dependencies from `requirements.txt`.

## Preserved features
- Quick Trade / Full Pro mobile-oriented dashboard and Order Block map
- SiftingIO XAUUSD WebSocket and historical bars, multi-timeframe analysis, Smart Flow, structure, VWAP, early-entry engine, institutional confluence, triggers/retests and trade levels
- Telegram alerts, paper-trading log, data-health display and master snapshot
- Turtle breakout module with ATR sizing and maximum 1% planned account risk
- Official CFTC COMEX Gold COT/OI only when source data is available
- Market Regime, Equal-High/Low liquidity estimates and diagnostic A+ gate
- Existing SiftingIO API-key normalization and HTTP 401/403 diagnostic guidance

## Added: Magic Ratio Area
- Uses confirmed 5-minute swing pivots to estimate the 0.50–0.618 Fibonacci retracement zone.
- Displays Bullish/Bearish swing bias, zone, midpoint Magic Level, whether price is inside the zone, and illustrative ATR-based stop/target levels with a configurable default 1:2 planning ratio.
- Available in Quick Trade and Full Pro as a collapsible panel.
- It does not override the existing Smart Flow signal logic or place broker orders. Zone touch alone is not a trade signal.
- This is a transparent Fibonacci-style approximation, not a copy of the proprietary indicator shown in the reference video.

## API key / HTTP 401
- The app trims whitespace and removes non-ASCII characters from the SiftingIO token before using it in HTTP headers.
- HTTP 401 gives an authentication/access checklist; HTTP 403 gives a permission warning.
- A code update cannot make an invalid/revoked key valid or grant missing API permissions. If 401 remains, verify the key and endpoint permissions with SiftingIO.
- Never put API keys or Telegram tokens in the Python file or GitHub repository.

## Validation limits
Python syntax compilation, Magic Ratio helper smoke test, and ZIP structure were checked. Live SiftingIO REST/WebSocket requests, the deployed Streamlit UI, and trading performance have not been verified here. Signals and levels are informational only; use paper trading first. No broker orders are placed.
