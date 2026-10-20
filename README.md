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


Chart removal edition: the native Plotly candlestick chart and its render calls have been removed as requested. Existing Quick Trade, master signal, strategy and analysis modules are retained in app.py.


Refresh + Confluence preservation fix
- Native chart remains removed.
- Smart Flow Confluence remains visible in Quick Trade.
- The timed Streamlit fragment is attached to `live_dashboard()` (not the backtest helper), so the live quote/signal area refreshes at the selected 5–15 second interval without forcing the whole app to rerun.
- Backtest calculation remains a normal function and does not run as a timed UI fragment.


## New: Strategy Lab / Historical Backtest
Choose **📊 Strategy Lab — Backtest** from the sidebar. Upload a historical XAUUSD OHLC CSV (`time` optional; `open`, `high`, `low`, `close` required; at least 250 candles). It compares EMA Trend Pullback, 20-bar Breakout, Liquidity Sweep Reversal, Hybrid Trend + Breakout, and RSI + Bollinger Mean Reversion using ATR-based stops and configurable R targets. It reports win rate, profit factor, Net R, maximum drawdown, average R and downloadable trade log.

**Important:** No historical dataset is bundled, so this ZIP does not claim any strategy has already been proven best. The backtest uses the CSV you supply. It is a research tool, not a guarantee; include realistic spread/slippage and validate on unseen data before risking money. Existing live app modules and Quick Trade interface are retained.


## Strategy Lab and Order Block alerts
- Quick Trade mobile interface and Full Pro tools are retained. Strategy Lab compares five candidate rule sets on user-provided OHLC CSV data; it does not claim a universal best strategy.
- Order Block Telegram alerts are deduplicated per detected zone within the running Streamlit session. Configure `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` in Streamlit Secrets. The app must be running and refreshing; for reliable alerts while the UI is closed, use a persistent server-side worker/monitor.
- Order Blocks are candle-derived heuristics, not exchange order-book data. Confirm the zone with BOS/retest and apply risk controls.
- No historical candles were bundled and no live XAUUSD backtest was claimed. Upload historical OHLC data in Strategy Lab to compare results. API access and Telegram delivery must be tested in your deployment.

## Fixed Range Volume Profile (FRVP) integration
- Added to **Full Pro — All Tools → Liquidity & Risk**.
- Select manual start/end candles and price-row resolution; shows estimated POC, VAH, VAL, volume bars, HVN/LVN and a current-price context note.
- Uses the feed's reported candle volume distributed across each candle's high-low range. This is an OHLCV-based approximation, not exchange-level bid/ask volume-at-price.
- FRVP is contextual only and does not override the existing master signal or place orders.
- Validate against the deployed data feed; if positive volume is unavailable, the app displays a warning rather than fabricating a profile.


## Order Block detection refinement (2026-10-20)
- Active OBs are selected from fresh, unmitigated 5M candle-derived candidates; touched zones are shown separately as historical.
- The final possibly-forming 5M candle is excluded from mitigation/invalidation checks to reduce premature status changes.
- If the newest candidate is mitigated, the detector continues searching for an older still-fresh candidate.
- This remains a heuristic based on OHLC candles, not verified institutional order flow. Live signals remain paused unless the WebSocket feed is fresh.
