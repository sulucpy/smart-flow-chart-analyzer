# Smart Flow Gold V3 PRO — Staging Runbook

**Status:** candidate only; not deployed and not end-to-end live-tested.

## Candidate identity

- File: `Smart_Flow_Gold_V3_PRO_FINAL_MERGED_CANDIDATE.py`
- SHA-256: `502a1de6e3ff30d3f5c9d960812baceb910f5a48a88f90ee7e6de1b1a1f99735`
- Source lines: 3,531
- Top-level functions/classes: 66
- Python AST parse/compile: PASS

## 1. Make a staging copy

Do not overwrite the existing production `app.py`. Create a separate Git branch or staging app, then place the candidate there as `app.py`.

## 2. Install dependencies

Use `requirements_smart_flow_gold.txt` as a starting point. In the staging environment:

```bash
python -m pip install -r requirements_smart_flow_gold.txt
python -m pip check
python -m py_compile app.py
streamlit run app.py
```

The versions are ranges, not a lock file. Once the app launches successfully, pin the exact versions tested in staging.

## 3. Configure secrets safely

Set these only in the host's secret manager (for Streamlit Community Cloud: App → Settings → Secrets):

- `SIFTINGIO_API_KEY`
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

Do not paste credentials into chat or commit them to GitHub. First test with Telegram alerts disabled or directed to a private test chat.

## 4. Required staging checks

- App starts without import/runtime exceptions.
- Missing secrets produce a clear safe state; no secret values appear in logs.
- WebSocket connects, ticks are fresh, reconnect/disconnect states are visible.
- Stale feed prevents new signals; stale 1H/15M/5M candle data is handled safely.
- Signal chain is checked with replay/synthetic data: Smart Flow → Early Entry → institutional confirmation → OB confirmation → Entry/SL/TP1–TP4.
- BUY and SELL paths both tested; invalid levels and insufficient history return WAIT/unavailable rather than fabricated values.
- Telegram duplicate suppression is verified in a private test chat.
- Paper trade opens once, exits using simulated observed price, handles gaps conservatively, and preserves OPEN trades when history is trimmed.
- Backtest checks for look-ahead bias, stop/target same-candle ambiguity, and drawdown calculation.
- COMEX OI/COT explicitly reports unavailable when no trustworthy source data exists.
- Test Quick Trade and full dashboard on mobile-sized viewport and desktop.

## 5. Deployment gate

Do not promote to production until staging checks pass, the tested dependency versions are pinned, and a rollback copy of the previous production source exists. Paper trading is simulation, not broker execution. Do not use this candidate for real-money trading based only on syntax or synthetic tests.

## Current limitation

The current test container has no Streamlit installation, and no live SiftingIO or Telegram end-to-end check has been performed. Therefore this runbook is a safe test plan, not evidence that those checks passed.
