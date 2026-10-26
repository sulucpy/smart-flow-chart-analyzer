Smart Flow Gold V3 PRO — Corrected merged candidate

Source baseline: Smart_Flow_Gold_V3_PRO_FINAL_MERGED_CANDIDATE.py (the prior consolidated candidate, not the older StaleData_Fix_Candidate by itself).
New layer: Time Machine + Risk Manager UI, historical test layer, Monte Carlo resampling and downloadable trade log.

Preserved correction markers checked in the resulting source:
- Reject stale/future XAUUSD tick timestamps.
- Reject out-of-order ticks.
- Stable structure() result schema for insufficient candle history.
- Telegram alert deduplication state persists across Streamlit reruns/session.
- Existing feature modes remain in the app; new mode is additive.

Validation performed for this package:
- Python AST parse: PASS.
- Python bytecode compile: PASS.
- Source lines: 3616.
- SHA-256: e7063b08c2e103515d989b880c8423adc9cbc6fe8b7d677e48cd08799621cb88

Not verified: Streamlit app launch, SiftingIO real API/WebSocket, Telegram delivery, full UI behavior, and strategy parity between historical Time Machine and every live signal rule. Keep this as a staging candidate. Do not replace production until those tests pass.

Important: the Time Machine uses the existing Strategy Lab historical rules; it is not yet proven to reproduce every live Smart Flow entry/exit rule exactly. Monte Carlo resamples historical trade outcomes and is not a price prediction.
