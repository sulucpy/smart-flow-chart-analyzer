# Smart Flow Gold V3 PRO MAX — London Kill Zone Staging

This is an additive staging build. It does not deploy or modify the production app.

## Added
- Named-timezone session clock using `zoneinfo` so America/New_York and Europe/London daylight-saving rules are handled by the timezone database.
- Asian range levels (19:00–01:29 local time) and 02:00–04:59 local Kill Zone watch.
- Candle-derived Asian High/Low sweep/reclaim, three-candle FVG status, and informational bullish/bearish watch.
- Optional deduplicated Telegram session-state alert (disabled by default; requires existing Telegram secrets).
- CSV OHLC backtest in Full Pro: FVG midpoint entry, stop at sweep extreme, fixed R target, conservative same-candle SL/TP handling, downloadable trade log.

## Backtest limitations
- Upload real historical XAUUSD candles with timestamp + OHLC; no fake candles are generated.
- 5-minute candles are preferred. Timestamp values must correctly represent UTC or contain an explicit timezone; pandas parses naive timestamps as UTC.
- One trade per session; fixed target in R; spread, commission and slippage are not included.
- This is a simple research rule, not exact ICT/SMC replication and not a profitability claim.
- Session clock defaults to America/New_York, matching the screenshots' ET reference; it automatically switches between EST/EDT. For exact fixed EST use UTC; do not confuse EST with all-year New York local time.

## Setup
1. Extract to a separate folder or Git branch.
2. Install `pip install -r requirements.txt`.
3. Set SiftingIO and Telegram credentials only in Streamlit Secrets.
4. Run `python -m py_compile app.py`.
5. Test all existing modes and the new session panel before considering a production merge.

## Safety
No broker orders are placed. The existing Smart Flow Master confirmation gate remains unchanged. Live SiftingIO/Telegram and full Streamlit UI were not tested in this build environment.
