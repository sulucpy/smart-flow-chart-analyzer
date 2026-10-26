# Feature Integration Notes — screenshot review (2026-10-27)

## Decision: integrate only Gold-relevant concepts

| Day Trader Skill concept from supplied screenshots | Smart Flow Gold treatment | Status / caveat |
|---|---|---|
| Chart scanner | Existing Golden Chicken / multi-timeframe scanner | Retained; verify runtime and signal quality |
| News monitor | Existing US economic event planner and XAUUSD news-risk guide | Manual/context guide, not a guaranteed live news feed |
| Entry timer | Existing Early Entry Engine, A+ gate, Next Trigger + Retest Map | Not an instruction to enter without live-feed and risk checks |
| Exit planner | Existing SL, TP1–TP4 and Turtle 10-day exit logic | Broker-specific fill behavior not verified |
| Time Machine | Strategy Lab historical rules + Monte Carlo resampling of trade R outcomes | Not equivalent to full 10,000 OHLC intraday replay; label corrected in UI |
| Risk Manager | Existing cash-risk/estimated-size calculator and Turtle sizing | Configurable 0.1–5% in this staging build; 5% ceiling is not advice |
| Budget Manager | Basic equity/risk-budget calculation | No separate personal budget/portfolio accounting engine claimed |
| Order Executor | No auto-execution added | Safer: research/paper-only until a broker adapter is implemented and tested |
| Price alerts | Existing Telegram A/A+ signal and Order Block alerts | Signal/zone alerts, not arbitrary user-defined price-level alerts |
| Earnings Analyst | Not added | US-equity earnings concept is not directly applicable to XAUUSD |
| Pelosi Tracker | Not added | US political-stock-disclosure concept is not relevant to Gold signal generation |

## Existing Smart Flow features to regression-test

- Quick Trade mobile interface and Full Pro interface
- XAUUSD SiftingIO feed, reconnect/stale-data guards, 1H/15M/5M
- EMA/RSI/ATR/ADX-DI/VWAP, structure, BOS/CHoCH, liquidity sweeps, equal highs/lows
- Support/resistance, key levels, FVG/order blocks/retest, CRT/Power of 3, Inside Bar
- Big Candle/Early Entry, institutional confluence, A+ signal gate, trigger/retest map
- BUY/SELL/WAIT/NO TRADE, SL/TP levels, paper trading, Telegram alerts
- Strategy Lab, Time Machine + Risk Manager, Turtle strategy
- COMEX OI/COT must use real, dated sources or clearly show unavailable

A code marker or visible control is not proof that the feature works. Complete the regression checklist in a separate staging environment.
