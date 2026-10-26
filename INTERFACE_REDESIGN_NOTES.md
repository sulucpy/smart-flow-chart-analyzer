# Smart Flow Gold V3 PRO MAX — One-Glance Interface Update

## What changed
- Added a compact six-card **At a glance** strip immediately beneath the existing Command Center/feed status.
- Shows feed freshness, Master decision and score, 1H/15M/5M trend states, market regime/strength, Smart Flow + Early Engine scores, and fresh Order Block availability.
- Cards use responsive CSS: 6 across on wide desktop, 3 across on medium screens, and 2 across on phones.
- Preserved the existing Command Center, BUY/SELL trigger and retest map, Quick Trade decision gate, Full Pro tabs, Strategy Lab, Time Machine, Risk Manager, paper trading and Telegram code.
- Clarified the Quick Trade confluence text so it no longer claims a fixed 1% risk limit; the staging risk input remains configurable up to its existing 5% technical ceiling.

## Behavior/safety
- This is a presentation-only summary of values already calculated by the app. It does not create a new signal engine and does not change the master BUY/SELL gate.
- “A+” is a rule-based confirmation label, not a promise of profit.
- Trend labels and OB status depend on available source data; missing data is shown as unavailable/no fresh zone rather than fabricated.
- A visual strip cannot replace regression testing. Verify all four interface modes, mobile layout, stale feed, missing API key, historical API errors, and Telegram test credentials in a separate staging deployment.
