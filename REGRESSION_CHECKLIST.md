# Regression Checklist — Smart Flow Gold V3 PRO MAX

Use only in a separate staging app/branch. Production must remain unchanged until all critical checks pass.

## Build and runtime
- [ ] `python verify_source.py` passes
- [ ] `python -m py_compile app.py` passes
- [ ] Install requirements in clean environment; run `pip check`
- [ ] Streamlit app launches with staging Secrets; missing/invalid Secrets fail safely
- [ ] Quick Trade mobile view and Full Pro render without exceptions
- [ ] Strategy Lab and Time Machine + Risk Manager render and export CSV

## Feature regression
- [ ] 1H/15M/5M analysis, feed freshness, reconnect and stale-feed blocks
- [ ] Smart Flow scoring, EMA/RSI/ATR/ADX-DI/VWAP
- [ ] Liquidity sweep, equal highs/lows, BOS/CHoCH, S/R and key levels
- [ ] FVG, Order Block invalidation/consumption, retest confirmation
- [ ] CRT/Power of 3, Inside Bar, Turtle 20-day entry/10-day exit/ATR sizing
- [ ] Early Entry, Institutional Confluence, A+ Gate, Next Trigger/Retest Map
- [ ] BUY/SELL/WAIT/NO TRADE, SL/TP1–TP4, paper trade state
- [ ] Telegram A/A+ and Order Block alerts deduplicate; secrets never appear in logs
- [ ] News guide correctly identifies manual/context-only data where applicable
- [ ] COMEX OI/COT show only verified, dated source data or unavailable

## Risk Manager (current staging choice: no 1% hard cap)
- [ ] Test 0.1%, 0.5%, 1%, 2%, 5% inputs and confirm displayed cash risk arithmetic
- [ ] Test invalid/zero balance, stop distance, contract size, NaN/infinity at calculation boundaries
- [ ] Verify broker contract size, account currency conversion, lot minimum/step, spread and slippage
- [ ] Confirm no automatic broker order is claimed or placed
- [ ] Higher risk visibly warns that losses can occur rapidly

## Historical testing
- [ ] Confirm SiftingIO source, timeframe, timestamps and candle gaps
- [ ] Test 401/403, timeout, empty/malformed/stale data; never fabricate results
- [ ] Manually validate trade entry/exit, costs, R outcomes and metrics against fixtures
- [ ] Test same-candle stop/target ambiguity and look-ahead bias
- [ ] Understand that Time Machine currently resamples observed trade R outcomes; it is not full OHLC path replay
- [ ] Compare Strategy Lab rules with live Smart Flow signals; run out-of-sample/walk-forward tests

## Release gate
- [ ] Save logs/screenshots and document failures/unavailable checks
- [ ] Confirm source hash and rollback copy
- [ ] Explicitly approve release only after runtime and live-data tests pass
