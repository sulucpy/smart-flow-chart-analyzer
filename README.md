# Smart Flow Gold V3 PRO

## Streamlit Cloud deployment
1. Keep `app.py`, `requirements.txt`, and this README in the repository root.
2. In Streamlit Community Cloud, set Main file path to `app.py`.
3. Under Manage app → Settings → Secrets, add the secrets below (use your real values locally in the Streamlit Secrets editor; do not commit them to GitHub):

```toml
SIFTINGIO_API_KEY = "your_real_key"
TELEGRAM_BOT_TOKEN = "your_bot_token"
TELEGRAM_CHAT_ID = "your_chat_id"
```

4. Save and reboot the app. Allow about a minute for Secrets to propagate.
5. Confirm the Data health panel shows fresh ticks and current candle history before considering any signal.

## Safety notes
- A WebSocket tick alone is not enough to authorize a trade; the app also checks candle-history freshness.
- `NO TRADE` / `WAIT` means do not enter. Trigger levels are watch conditions, not automatic entries.
- This package was syntax-checked locally. Live SiftingIO authentication, provider responses, Telegram delivery, and Streamlit Cloud deployment were not tested from this environment.
