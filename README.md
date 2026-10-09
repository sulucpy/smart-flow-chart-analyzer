# Smart Flow Gold — Encoding Fix

Streamlit entry point: `app.py`

## Deploy
1. Back up the current repository before replacing files.
2. Put `app.py` and `requirements.txt` in the repository root.
3. In Streamlit Cloud, set Main file path to `app.py`.
4. Add `SIFTINGIO_API_KEY` under Streamlit Secrets. The key should be the original plain ASCII token only; never paste it into chat or source code.
5. Reboot the app and check the data-freshness panel. A successful code compile does not guarantee the API key or live feed is valid.
