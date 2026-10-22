# Smart Flow Gold V3 PRO — Consolidated Candidate Test Report

## Candidate
- File: `Smart_Flow_Gold_V3_PRO_FINAL_MERGED_CANDIDATE.py`
- SHA-256: `502a1de6e3ff30d3f5c9d960812baceb910f5a48a88f90ee7e6de1b1a1f99735`
- Python AST parse / bytecode compile: PASS
- Top-level functions/classes: original 66, candidate 66
- Missing original top-level definitions: none
- Added top-level definitions: none
- Duplicate top-level definitions: none

## Behavioral tests executed against candidate function bodies
- BUY paper trade gap-through-stop uses actual observed exit price and can lose more than -1R: PASS
- SELL paper trade gap-through-stop uses actual observed exit price and can lose more than -1R: PASS
- NaN exit price ignored: PASS
- Qualified paper signal logged: PASS
- Duplicate OPEN paper signal suppressed: PASS
- Invalid target ordering rejected: PASS
- Below-threshold paper signal rejected: PASS
- Existing OPEN trades preserved while closed history is capped: PASS

## Static integration checks
- Telegram alert state uses persistent Streamlit session-state key: PASS
- Next-trigger map consumes enhanced liquidity helper: PASS
- Order-block confirmation contains sweep/BOS sequence logic: PASS
- Conservative stop-gap handling present in backtest code: PASS

## Limitations
These tests execute selected extracted function bodies with mocked Streamlit session state and synthetic inputs. They do not launch the whole Streamlit app. Streamlit is not installed in this runtime; no live SiftingIO WebSocket, real XAUUSD market data, Telegram API, broker order execution, or production persistence has been tested. Static feature inventory does not guarantee each feature is operational. Do not deploy to the live app until a full runtime smoke test and data-freshness test pass.
