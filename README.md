# Smart Flow Gold — V3 PRO Professional Workspace

This update keeps the existing Smart Flow Gold / XAUUSD analysis engine and organizes the app so it is easier to use on a phone.

## What's improved

- **Quick Trade** remains the default: one clear decision, live price, BUY/SELL trigger levels, score summary, Order Block and confirmation checklist.
- **Full Pro — All Tools** is organized into tabs: Overview, Setups, Liquidity & Risk, News, and Scanner.
- Added the existing CRT / Power of 3 panel into the Setups tab so it is easy to find.
- The first screen explains the workflow: check data → read bias → wait for confirmation → plan risk.
- App-native only: no TradingView chart is embedded.
- Existing live feed, Smart Flow scoring, multi-timeframe levels, Inside Bar, A+ scanner, Order Block, Magic Ratio, Liquidity/Risk planner, economic-news guide/planner, Golden Chicken proxy scanner, paper trading, Telegram alerts, Turtle module and official CFTC COT panel are retained.

## Deploy

1. Back up the current GitHub repository.
2. Extract this ZIP.
3. Replace the repository-root `app.py` and `requirements.txt` with the files from this ZIP.
4. Keep `SIFTINGIO_API_KEY` and optional `TELEGRAM_BOT_TOKEN` / `TELEGRAM_CHAT_ID` in Streamlit Cloud **Secrets** only. Never commit keys to GitHub or share them in chat.
5. Reboot the Streamlit app.
6. Start in **Quick Trade**. Select **Full Pro — All Tools** in the sidebar when you want the complete analysis workspace.

## Important limitations

- The app does not place broker orders.
- Live feed/API availability depends on the SiftingIO key and its permissions. HTTP 401 means the API provider rejected the credentials/access; UI changes cannot fix an invalid or unauthorized key.
- The economic-news planner uses manually entered values; it is not a live economic calendar.
- Liquidity, order blocks, BOS/CHoCH, CRT/Power of 3 and signal scores are rule-based estimates from available candles, not proof of institutional orders or guaranteed accuracy.
- Backtest statistics are historical/proxy results and may not include spread, commission or slippage. Validate with paper trading before risking money.


Professional native chart UI update: mobile-friendly timeframe selector, OHLC summary metrics, enhanced candle styling, EMA overlays, Order Block zones, trigger/entry/SL/target lines, interactive pan/zoom and clearer stale-feed warning. No TradingView embed.
