# London Kill Zone integration notes

- Kept existing Smart Flow functions and master BUY/SELL gate; additions are independent.
- Session clock uses IANA named timezones and daylight-saving-aware conversions.
- Asia range: previous local day 19:00 through current day 01:29.
- Kill Zone: 02:00–04:59 in the selected timezone (default America/New_York).
- Sweep requires wick beyond Asian High/Low and candle close back inside the range.
- FVG uses a 3-candle imbalance definition; entry is midpoint after formation.
- Stop is at sweep extreme; target is configurable R multiple.
- If stop and target both hit in one OHLC candle, stop is counted first.
- Historical metrics omit transaction costs; interpret as gross research results.

- Visual UI update: circular session clock, local time/timezone label, and Plotly candlestick map. Asian High/Low lines and FVG band are overlaid only when the source data provides them.

- Unified Smart View block: trend alignment (1H/15M/5M), Master decision/score, Smart Flow/Early Move, regime, ATR, VWAP if available, session, sweep/FVG/OB status, and conditional BUY/SELL Entry/SL/TP1-TP3 with exit framework. ATR levels are explicitly labelled watch-plan only and never override the master gate.
