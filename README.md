# Smart Flow Gold V3 PRO — Inside Bar + Gold A+ panels

This build keeps the existing Smart Flow Gold app and adds app-only analysis panels:

- Closed-candle Inside Bar detection and close-based breakout status on 5M candles.
- Gold A+ checklist using existing 1H/15M trend, 5M structure, Order Block, liquidity sweep, BOS and current signal gate.
- XAUUSD economic-news risk guide. This is educational guidance only; no live calendar API is connected.

## Deploy
1. Back up the current GitHub repository.
2. Replace the repository-root `app.py` with this `app.py` and retain/update `requirements.txt`.
3. Keep `SIFTINGIO_API_KEY` and optional Telegram secrets in Streamlit Cloud Secrets, not in source code.
4. Reboot the Streamlit app and check Data Health / freshness.

## Important limitations
- This does not connect to TradingView or place broker orders.
- Inside Bar uses completed candles when the current row is a live WebSocket candle.
- SMT is not fabricated; a real comparison-symbol feed is needed.
- The news panel is not a live calendar and does not automatically block trades.
- New features are rule-based aids, not guaranteed trading signals. Paper-test before real money.


## CRT / Power of 3 panel (app-only)
- Adds a candle-based reference range, sweep/reclaim status, and close-outside-range status using completed 5-minute candles.
- Displays 1H and 15M trend context and warns when live WebSocket freshness is unavailable.
- This is a heuristic, not a verified ICT/CRT implementation and not an entry by itself. Existing Smart Flow, OB, BOS, liquidity, Magic Ratio, Inside Bar, News Guide, Turtle and COT sections are retained.


## Gold Buy/Sell Scanner + Strategy Stats (app-only)
- Added an app-native scanner showing 1H/15M/5M trend alignment and the current Smart Flow decision.
- Added a transparent historical 5M EMA20/EMA50 pullback proxy backtest with ATR stop and 2R target.
- The stats are not the proprietary Golden Chicken indicator and do not imply a 90% win rate. Spread, commission, and slippage are not modeled.
- This app does not place broker orders. Verify signals with paper trading first.
