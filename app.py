# CONSOLIDATED MERGED CANDIDATE — NOT DEPLOYED
# Built from the latest PaperTrading Logging Fix candidate.
# Includes merged alert deduplication, signal/liquidity integration, sequence-safe OB,
# paper-trade risk/validation and preservation of OPEN paper trades.
# Live SiftingIO, Streamlit-host, and Telegram end-to-end tests have NOT been run.


import json
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import numpy as np
import pandas as pd
import requests
import streamlit as st
import websocket

# ============================================================
# Smart Flow Gold — Live XAUUSD + Big Candle + Telegram Alerts
# ============================================================
# IMPORTANT:
# - Put secrets in Streamlit -> Manage app -> Settings -> Secrets.
# - Never put Telegram token or SiftingIO key in GitHub code.
#
# Required secrets:
# SIFTINGIO_API_KEY = "..."
# TELEGRAM_BOT_TOKEN = "..."
# TELEGRAM_CHAT_ID = "..."
#
# The app uses SiftingIO's live XAUUSD WebSocket to build the
# current 5-minute candle locally, so a stale REST 5M bar does
# not block the early-entry engine.
# ============================================================

st.set_page_config(
    page_title="Smart Flow Gold Alerts",
    page_icon="🟡",
    layout="wide",
    initial_sidebar_state="expanded",
)

BASE = "https://api.sifting.io"
WS_URL = "wss://stream.sifting.io/ws/v1"
SYMBOL = "XAUUSD"


# -----------------------------
# Secrets / configuration
# -----------------------------
def normalize_api_key(value: str) -> str:
    """Normalize a pasted SiftingIO token to plain ASCII without exposing it."""
    value = str(value or "").strip()
    return "".join(ch for ch in value if ch.isascii() and not ch.isspace())


def get_secret(name: str, default: str = "") -> str:
    try:
        value = st.secrets.get(name, default)
        value = str(value) if value is not None else default
    except Exception:
        value = default
    if name == "SIFTINGIO_API_KEY":
        return normalize_api_key(value)
    return str(value).strip()


SIFTING_KEY = get_secret("SIFTINGIO_API_KEY")
TELEGRAM_TOKEN = get_secret("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = get_secret("TELEGRAM_CHAT_ID")


# -----------------------------
# Live WebSocket feed
# -----------------------------
class GoldFeed:
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.lock = threading.Lock()
        self.ticks = deque(maxlen=6000)
        self.completed = deque(maxlen=300)
        self.current = None
        self.last_tick = None
        self.last_error = ""
        self.connected = False
        self.stop_event = threading.Event()
        self.thread = None

    @staticmethod
    def bucket(ts_ms: int) -> int:
        return (ts_ms // 300_000) * 300_000

    def _update_candle(self, price: float, ts_ms: int):
        bucket = self.bucket(ts_ms)
        with self.lock:
            if self.current is None or bucket != self.current["t"]:
                if self.current is not None:
                    self.completed.append(dict(self.current))
                self.current = {
                    "t": bucket,
                    "o": price,
                    "h": price,
                    "l": price,
                    "c": price,
                    "v": 1,
                }
            else:
                self.current["h"] = max(self.current["h"], price)
                self.current["l"] = min(self.current["l"], price)
                self.current["c"] = price
                self.current["v"] += 1

            self.last_tick = {
                "price": price,
                "t": ts_ms,
                "received": time.time(),
            }

    def _handle(self, raw):
        try:
            msg = json.loads(raw)
        except Exception:
            return

        if msg.get("f") == "tick" and msg.get("s") == SYMBOL:
            try:
                price = float(msg["p"])
                ts_ms = int(msg["t"])
                if not (price > 0 and ts_ms > 0 and np.isfinite(price)):
                    return
                # Reject provider ticks whose event timestamp is too old or implausibly
                # in the future. Fresh receipt time alone must not make delayed data look live.
                now_ms = int(time.time() * 1000)
                if ts_ms < now_ms - 120_000 or ts_ms > now_ms + 30_000:
                    with self.lock:
                        self.last_error = "Ignored stale/future XAUUSD tick timestamp."
                    return
                # Reject out-of-order provider ticks so an old timestamp cannot
                # roll the current 5M candle backwards or make stale data look live.
                with self.lock:
                    previous = self.last_tick
                    if previous and ts_ms < int(previous.get("t", 0)):
                        self.last_error = "Ignored out-of-order XAUUSD tick."
                        return
                    self.ticks.append((ts_ms, price))
                self._update_candle(price, ts_ms)
                # Mark the feed connected only after a valid, non-stale XAUUSD tick arrives.
                with self.lock:
                    self.connected = True
                    self.last_error = ""
            except Exception:
                pass

        elif msg.get("f") == "error":
            with self.lock:
                self.last_error = f'{msg.get("code", "error")}: {msg.get("message", "")}'

    def _run(self):
        while not self.stop_event.is_set():
            ws = None
            ping_stop = threading.Event()
            try:
                ws = websocket.create_connection(
                    f"{WS_URL}?key={self.api_key}",
                    timeout=20,
                    enable_multithread=True,
                )
                ws.settimeout(5)
                ws.send(json.dumps({
                    "op": "subscribe",
                    "product": "com",
                    "symbols": [SYMBOL],
                }))
                # A successful socket open/subscription send is not proof that
                # the provider accepted the subscription. Wait for a valid tick.
                with self.lock:
                    self.connected = False
                    self.last_error = "Socket open; waiting for first XAUUSD tick/subscription confirmation."

                def pinger():
                    while not ping_stop.wait(30):
                        try:
                            ws.send(json.dumps({"op": "ping"}))
                        except Exception:
                            break

                threading.Thread(target=pinger, daemon=True).start()

                while not self.stop_event.is_set():
                    try:
                        raw = ws.recv()
                        if raw:
                            self._handle(raw)
                    except websocket.WebSocketTimeoutException:
                        continue
                    except Exception:
                        break

            except Exception as e:
                with self.lock:
                    self.last_error = str(e)[:240]
            finally:
                ping_stop.set()
                try:
                    if ws:
                        ws.close()
                except Exception:
                    pass
                with self.lock:
                    self.connected = False

            self.stop_event.wait(2)

    def start(self):
        if self.thread and self.thread.is_alive():
            return
        self.stop_event.clear()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self.lock:
            return {
                "connected": self.connected,
                "current": dict(self.current) if self.current else None,
                "completed": list(self.completed),
                "last_tick": dict(self.last_tick) if self.last_tick else None,
                "error": self.last_error,
            }


@st.cache_resource(show_spinner=False)
def get_feed(api_key: str):
    feed = GoldFeed(api_key)
    feed.start()
    return feed


def get_alert_state():
    """Persist Telegram deduplication state across Streamlit reruns in this session."""
    defaults = {"last_key": "", "last_sent": 0.0, "last_result": ""}
    try:
        if hasattr(st, "session_state"):
            state = st.session_state.setdefault("_telegram_alert_state", defaults.copy())
            for key, value in defaults.items():
                state.setdefault(key, value)
            return state
    except Exception:
        pass
    # Non-Streamlit/unit-test fallback: keep a module-level state object.
    global _TELEGRAM_ALERT_STATE
    try:
        state = _TELEGRAM_ALERT_STATE
    except NameError:
        _TELEGRAM_ALERT_STATE = defaults.copy()
        state = _TELEGRAM_ALERT_STATE
    return state


# -----------------------------
# REST historical data
# -----------------------------
@st.cache_data(ttl=60, show_spinner=False)
def get_bars(api_key: str, interval: str, limit: int = 250) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame()

    url = f"{BASE}/v1/hist/commodities/{SYMBOL}/bars"
    # First request needs a start. SiftingIO's first page is ordered from
    # the requested start, so choose a recent window that is just large
    # enough to include the latest bars we need for the indicators.
    # This avoids accidentally loading an old first page (which previously
    # made the 1H/15M/5M REST data appear weeks behind the live WebSocket).
    span_days = {
        "1h": 14,
        "15m": 4,
        "5m": 2,
        "1d": 520,
    }.get(interval, 4)
    now = datetime.now(timezone.utc)
    start = (now - timedelta(days=span_days)).strftime("%Y-%m-%dT%H:%M:%SZ")
    params = {
        "start": start,
        "end": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "interval": interval,
        "limit": min(max(int(limit), 320), 2000),
    }
    headers = {
        "X-API-Key": api_key,
        "Accept-Encoding": "gzip",
    }
    r = requests.get(url, params=params, headers=headers, timeout=8)
    if r.status_code == 401:
        raise RuntimeError(
            "SiftingIO returned HTTP 401 (unauthorized). Check that SIFTINGIO_API_KEY "
            "is the current plain ASCII API token and that the account/key has access "
            "to the XAUUSD historical-bars endpoint. Update Streamlit Cloud -> "
            "Manage app -> Settings -> Secrets, then reboot the app. Do not paste the key into chat."
        )
    if r.status_code == 403:
        raise RuntimeError(
            "SiftingIO returned HTTP 403 (forbidden). The key was rejected for this endpoint "
            "or the account may not have the required market-data permission."
        )
    r.raise_for_status()
    body = r.json()
    rows = body.get("data", body if isinstance(body, list) else [])
    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    rename = {"t": "time", "o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
    df = df.rename(columns=rename)
    for col in ["open", "high", "low", "close"]:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    df = df.dropna(subset=["time", "open", "high", "low", "close"]).sort_values("time").drop_duplicates("time")
    return df.tail(limit).reset_index(drop=True)


# -----------------------------
# Fixed Range Volume Profile (FRVP)
# -----------------------------
def calculate_fixed_range_volume_profile(candles: pd.DataFrame, bins: int = 32, value_area_pct: float = 0.70):
    """Approximate FRVP by distributing each candle's reported volume across its price range.

    This is an OHLCV candle-based estimate, not exchange-level bid/ask volume-at-price.
    Requires a usable volume column; never fabricates volume when it is absent.
    """
    required = {"high", "low", "close", "volume"}
    if candles is None or candles.empty or not required.issubset(candles.columns):
        return None, "Selected candles do not contain OHLCV volume data. FRVP cannot be calculated honestly."
    x = candles.copy()
    for col in required:
        x[col] = pd.to_numeric(x[col], errors="coerce")
    x = x.dropna(subset=["high", "low", "close", "volume"])
    x = x[(x["high"] >= x["low"]) & (x["volume"] > 0)]
    if len(x) < 2 or float(x["volume"].sum()) <= 0:
        return None, "Not enough valid candles with positive volume in this range."
    lo, hi = float(x["low"].min()), float(x["high"].max())
    if hi <= lo:
        return None, "Selected range has no usable price span."
    bins = int(max(12, min(bins, 80)))
    edges = np.linspace(lo, hi, bins + 1)
    vol = np.zeros(bins, dtype=float)
    for row in x[["low", "high", "volume"]].itertuples(index=False, name=None):
        candle_low, candle_high, candle_vol = map(float, row)
        if candle_high <= candle_low:
            idx = int(np.clip(np.searchsorted(edges, candle_low, side="right") - 1, 0, bins - 1))
            vol[idx] += candle_vol
            continue
        first = max(0, int(np.searchsorted(edges, candle_low, side="right") - 1))
        last = min(bins - 1, int(np.searchsorted(edges, candle_high, side="left")))
        idxs = np.arange(first, last + 1)
        overlap = np.maximum(0.0, np.minimum(edges[idxs + 1], candle_high) - np.maximum(edges[idxs], candle_low))
        total = overlap.sum()
        if total > 0:
            vol[idxs] += candle_vol * overlap / total
        else:
            vol[int(np.clip(first, 0, bins - 1))] += candle_vol
    centers = (edges[:-1] + edges[1:]) / 2
    poc_idx = int(np.argmax(vol))
    total_vol = float(vol.sum())
    # Expand value area from POC, adding the larger adjacent volume bin until target is met.
    left = right = poc_idx
    area_vol = float(vol[poc_idx])
    target = total_vol * float(value_area_pct)
    while area_vol < target and (left > 0 or right < bins - 1):
        left_v = vol[left - 1] if left > 0 else -1
        right_v = vol[right + 1] if right < bins - 1 else -1
        if right_v > left_v:
            right += 1
            area_vol += float(vol[right])
        else:
            left -= 1
            area_vol += float(vol[left])
    profile = pd.DataFrame({"Price": centers, "Volume": vol})
    profile["Relative volume"] = profile["Volume"] / max(float(vol.max()), 1e-12)
    return {
        "profile": profile, "poc": float(centers[poc_idx]),
        "vah": float(edges[right + 1]), "val": float(edges[left]),
        "total_volume": total_vol, "bins": bins, "candles": len(x),
        "range_low": lo, "range_high": hi,
    }, None


def render_fixed_range_volume_profile(candles: pd.DataFrame, current_price=None):
    st.markdown("#### 📊 Fixed Range Volume Profile (FRVP)")
    st.caption("Manual candle range · POC / VAH / VAL / HVN / LVN. Profile uses the candle feed's volume field distributed across each candle's high–low range; it is an estimate, not exchange-level volume-at-price.")
    if candles is None or candles.empty or "time" not in candles.columns:
        st.warning("FRVP-യ്ക്ക് candle history ലഭ്യമല്ല. Data feed പരിശോധിക്കുക.")
        return
    x = candles.dropna(subset=["time"]).sort_values("time").drop_duplicates("time").reset_index(drop=True)
    if len(x) < 2:
        st.warning("FRVP range തിരഞ്ഞെടുക്കാൻ കുറഞ്ഞത് 2 candles വേണം.")
        return
    # Give users an explicit start/end candle selector, newest first for mobile convenience.
    labels = [pd.Timestamp(t).strftime("%Y-%m-%d %H:%M UTC") for t in x["time"]]
    options = list(range(len(x)))
    default_start = max(0, len(x) - min(60, len(x)))
    c1, c2, c3 = st.columns([2, 2, 1])
    with c1:
        start_idx = st.selectbox("Start candle", options, index=default_start, format_func=lambda i: labels[i], key="frvp_start_candle")
    with c2:
        end_idx = st.selectbox("End candle", options, index=len(x)-1, format_func=lambda i: labels[i], key="frvp_end_candle")
    with c3:
        bins = st.selectbox("Price rows", [20, 32, 48, 64], index=1, key="frvp_bins")
    if start_idx > end_idx:
        st.error("Start candle, End candle-ന് മുമ്പായിരിക്കണം.")
        return
    selected = x.iloc[start_idx:end_idx + 1].copy()
    result, error = calculate_fixed_range_volume_profile(selected, bins=bins)
    if error:
        st.warning(error)
        st.info("Volume ഇല്ലാത്തപ്പോൾ യഥാർത്ഥ FRVP കാണിക്കുന്നതായി നടിക്കില്ല; volume data ലഭിക്കുന്ന feed ഉപയോഗിക്കുക.")
        return
    m1, m2, m3, m4 = st.columns(4)
    m1.metric("POC", f"{result['poc']:,.2f}")
    m2.metric("VAH", f"{result['vah']:,.2f}")
    m3.metric("VAL", f"{result['val']:,.2f}")
    m4.metric("Candles", result["candles"])
    st.caption(f"Range: {labels[start_idx]} → {labels[end_idx]} · Profile volume total: {result['total_volume']:,.0f}")
    prof = result["profile"].sort_values("Price", ascending=False).copy()
    peak = max(float(prof["Volume"].max()), 1e-12)
    # Native HTML bars: no TradingView embed or extra chart dependency.
    rows = []
    for r in prof.itertuples(index=False):
        price_level = float(r.Price)
        pct = max(1, int(float(r.Volume) / peak * 100))
        if abs(price_level - result["poc"]) <= (result["range_high"] - result["range_low"]) / result["bins"] / 2:
            tag, color = "POC", "#f59e0b"
        elif result["val"] <= price_level <= result["vah"]:
            tag, color = "VA", "#38bdf8"
        else:
            tag, color = "", "#64748b"
        rows.append(f'<div style="display:grid;grid-template-columns:90px 1fr 70px;gap:8px;align-items:center;margin:3px 0;font:12px system-ui"><span>{price_level:,.2f} <b>{tag}</b></span><div style="background:#172033;border-radius:3px;height:12px;overflow:hidden"><div style="width:{pct}%;height:12px;background:{color}"></div></div><span style="text-align:right">{float(r.Volume):,.0f}</span></div>')
    st.markdown('<div style="padding:10px;border:1px solid #334155;border-radius:8px">' + ''.join(rows) + '</div>', unsafe_allow_html=True)
    # Identify high/low volume nodes relative to profile's mean, clearly as heuristic labels.
    mean_v = float(prof["Volume"].mean())
    hvn = prof[prof["Volume"] >= prof["Volume"].quantile(0.85)].sort_values("Volume", ascending=False).head(3)
    lvn = prof[(prof["Volume"] <= prof["Volume"].quantile(0.15)) & (prof["Volume"] < mean_v)].sort_values("Volume").head(3)
    a, b = st.columns(2)
    with a:
        st.markdown("**HVN — high-volume nodes (estimate)**")
        st.write(" · ".join(f"{v:,.2f}" for v in hvn["Price"].tolist()) or "—")
    with b:
        st.markdown("**LVN — low-volume nodes (estimate)**")
        st.write(" · ".join(f"{v:,.2f}" for v in lvn["Price"].tolist()) or "—")
    if current_price is not None and np.isfinite(float(current_price)):
        cp = float(current_price)
        if cp > result["vah"]:
            context = "Price above VAH — above the selected range's value area. Wait for acceptance/retest; not a standalone BUY."
        elif cp < result["val"]:
            context = "Price below VAL — below the selected range's value area. Wait for acceptance/retest; not a standalone SELL."
        elif abs(cp - result["poc"]) <= (result["range_high"] - result["range_low"]) / result["bins"]:
            context = "Price near POC — high-volume balance area; avoid treating this level alone as a directional trigger."
        else:
            context = "Price is inside the value area but away from POC; confirm with existing Smart Flow structure and retest."
        st.info("**FRVP + Smart Flow context:** " + context)
        st.caption("FRVP is contextual only in this version: it does not override the existing master signal or automatically trigger orders.")


# -----------------------------
# Live REST quote fallback
# -----------------------------
@st.cache_data(ttl=5, show_spinner=False)
def get_live_quote(api_key: str):
    """Get the latest SiftingIO XAUUSD quote when WebSocket is reconnecting.

    This is a price fallback only. It must NOT be used to build the current
    5M candle or trigger live signals because it is a snapshot, not a tick stream.
    """
    if not api_key:
        return None
    url = f"{BASE}/v1/last/quote/commodities/{SYMBOL}"
    try:
        r = requests.get(
            url,
            headers={"X-API-Key": api_key, "Accept-Encoding": "gzip"},
            timeout=4,
        )
        if r.status_code == 401:
            return {"price": None, "error": "SiftingIO HTTP 401: check SIFTINGIO_API_KEY and XAUUSD quote access."}
        if r.status_code == 403:
            return {"price": None, "error": "SiftingIO HTTP 403: check account permissions for XAUUSD quote access."}
        r.raise_for_status()
        body = r.json()
        row = body.get("data", body) if isinstance(body, dict) else body
        if isinstance(row, list):
            row = row[0] if row else {}
        bid = float(row.get("b")) if row.get("b") is not None else None
        ask = float(row.get("a")) if row.get("a") is not None else None
        last = row.get("p")
        if last is not None:
            last = float(last)
        elif bid is not None and ask is not None:
            last = (bid + ask) / 2.0
        elif bid is not None:
            last = bid
        elif ask is not None:
            last = ask
        if last is None:
            return None
        ts = row.get("t")
        if ts is not None:
            try:
                ts = int(ts)
                if ts < 10_000_000_000:
                    ts *= 1000
            except Exception:
                ts = None
        return {"price": last, "bid": bid, "ask": ask, "t": ts, "received": time.time()}
    except Exception:
        return None


# -----------------------------
# Indicators
# -----------------------------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def rsi(s, n=14):
    d = s.diff()
    up = d.clip(lower=0)
    dn = -d.clip(upper=0)
    au = up.ewm(alpha=1 / n, adjust=False).mean()
    ad = dn.ewm(alpha=1 / n, adjust=False).mean()
    rs = au / ad.replace(0, np.nan)
    out = 100 - (100 / (1 + rs))
    return out.fillna(50)


def atr(df, n=14):
    prev = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1 / n, adjust=False).mean()


def adx_di(df, n=14):
    h, l, c = df["high"], df["low"], df["close"]
    up = h.diff()
    down = -l.diff()
    plus_dm = pd.Series(np.where((up > down) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((down > up) & (down > 0), down, 0.0), index=df.index)
    tr = pd.concat([
        h - l,
        (h - c.shift()).abs(),
        (l - c.shift()).abs(),
    ], axis=1).max(axis=1)
    atrv = tr.ewm(alpha=1 / n, adjust=False).mean().replace(0, np.nan)
    pdi = 100 * plus_dm.ewm(alpha=1 / n, adjust=False).mean() / atrv
    mdi = 100 * minus_dm.ewm(alpha=1 / n, adjust=False).mean() / atrv
    dx = 100 * (pdi - mdi).abs() / (pdi + mdi).replace(0, np.nan)
    adx = dx.ewm(alpha=1 / n, adjust=False).mean()
    return adx.fillna(0), pdi.fillna(0), mdi.fillna(0)


def enrich(df):
    if df.empty:
        return df
    x = df.copy()
    x["ema20"] = ema(x.close, 20)
    x["ema50"] = ema(x.close, 50)
    x["ema200"] = ema(x.close, 200)
    x["rsi"] = rsi(x.close, 14)
    x["atr"] = atr(x, 14)
    x["adx"], x["pdi"], x["mdi"] = adx_di(x, 14)
    # Session VWAP approximation from available bars.
    day = x["time"].dt.date
    tp = (x.high + x.low + x.close) / 3
    x["vwap"] = (tp * x.get("volume", pd.Series(1, index=x.index)).fillna(1)).groupby(day).cumsum() / (
        x.get("volume", pd.Series(1, index=x.index)).fillna(1).groupby(day).cumsum()
    )
    return x


def trend_state(row):
    if row.empty:
        return "NEUTRAL"
    if row.ema20.iloc[-1] > row.ema50.iloc[-1] > row.ema200.iloc[-1] and row.close.iloc[-1] > row.ema20.iloc[-1]:
        return "BULL"
    if row.ema20.iloc[-1] < row.ema50.iloc[-1] < row.ema200.iloc[-1] and row.close.iloc[-1] < row.ema20.iloc[-1]:
        return "BEAR"
    return "NEUTRAL"


def structure(df):
    # Keep a stable result schema even when candle history is insufficient.
    # Downstream signal functions access sweep_low/sweep_high directly.
    if df is None or len(df) < 25:
        return {
            "sweep": False, "choch": False,
            "bos_bull": False, "bos_bear": False,
            "sweep_low": False, "sweep_high": False,
        }
    x = df
    last = x.iloc[-1]
    prev_hi = x.high.iloc[-11:-1].max()
    prev_lo = x.low.iloc[-11:-1].min()
    prior_hi = x.high.iloc[-21:-11].max()
    prior_lo = x.low.iloc[-21:-11].min()

    bos_bull = bool(last.close > prev_hi)
    bos_bear = bool(last.close < prev_lo)

    sweep_low = bool(last.low < prev_lo and last.close > prev_lo)
    sweep_high = bool(last.high > prev_hi and last.close < prev_hi)

    choch = bool(
        (last.close > prior_hi and last.close > prev_hi) or
        (last.close < prior_lo and last.close < prev_lo)
    )
    return {
        "sweep": sweep_low or sweep_high,
        "choch": choch,
        "bos_bull": bos_bull,
        "bos_bear": bos_bear,
        "sweep_low": sweep_low,
        "sweep_high": sweep_high,
    }


def support_resistance(df, max_levels=4):
    if len(df) < 30:
        return {"support": [], "resistance": []}
    x = df.copy()
    a = float(x["atr"].iloc[-1]) if "atr" in x and pd.notna(x["atr"].iloc[-1]) else float(x["close"].iloc[-1]) * 0.002
    tol = max(a * 0.55, 0.8)
    highs, lows = [], []
    for i in range(3, len(x) - 3):
        if x.high.iloc[i] == x.high.iloc[i-3:i+4].max():
            highs.append(float(x.high.iloc[i]))
        if x.low.iloc[i] == x.low.iloc[i-3:i+4].min():
            lows.append(float(x.low.iloc[i]))

    def cluster(vals):
        out = []
        for v in sorted(vals):
            if not out or abs(v - out[-1]) > tol:
                out.append(v)
            else:
                out[-1] = (out[-1] + v) / 2
        return out

    price = float(x.close.iloc[-1])
    sup = [v for v in cluster(lows) if v <= price]
    res = [v for v in cluster(highs) if v >= price]
    return {"support": sup[-max_levels:], "resistance": res[:max_levels]}


def nearest(levels, price, direction):
    arr = levels.get(direction, [])
    if not arr:
        return None
    if direction == "support":
        return max([x for x in arr if x <= price], default=None)
    return min([x for x in arr if x >= price], default=None)


# -----------------------------
# Order Block Detection (5M)
# -----------------------------
def order_block_map(df, price, lookback=80):
    """Detect active, fresh 5M order blocks and separately retain the last mitigated zone.

    Bullish OB: last bearish candle before bullish displacement closing above
    the preceding local swing high. Bearish OB is the inverse. A zone is not
    active after any later candle trades back into it (mitigation), and is
    invalidated by a decisive close through its distal edge. This is a
    candle-derived heuristic, not exchange order-book data.
    """
    empty = {
        "bullish": None, "bearish": None,
        "last_mitigated_bullish": None, "last_mitigated_bearish": None,
        "near_bullish": False, "near_bearish": False,
        "note": "Not enough candles to detect order blocks."
    }
    if df is None or df.empty or len(df) < 25:
        return empty
    required = {"open", "high", "low", "close"}
    if not required.issubset(df.columns):
        empty["note"] = "Order Block unavailable: OHLC columns missing."
        return empty

    x = enrich(df).tail(max(lookback, 40)).reset_index(drop=True)
    if len(x) < 25:
        return empty
    last = x.iloc[-1]
    atrv = float(last["atr"]) if pd.notna(last.get("atr")) and float(last["atr"]) > 0 else max(abs(float(price)) * 0.001, 1.0)
    found = {"bullish": None, "bearish": None}
    mitigated = {"bullish": None, "bearish": None}

    # Evaluate only confirmed historical candles. The most recent row may be
    # the currently-forming candle, so it is never used as the OB origin or
    # displacement candle.
    last_origin = len(x) - 3
    first_origin = max(10, last_origin - 35)
    for i in range(last_origin, first_origin - 1, -1):
        c = x.iloc[i]
        nxt = x.iloc[i + 1]
        prev_window = x.iloc[max(0, i - 10):i]
        if len(prev_window) < 5:
            continue
        swing_hi = float(prev_window["high"].max())
        swing_lo = float(prev_window["low"].min())
        body = abs(float(nxt["close"] - nxt["open"]))
        candle_range = max(float(nxt["high"] - nxt["low"]), 1e-9)
        displacement = body >= 0.55 * atrv and body / candle_range >= 0.55
        candidates = []
        if c["close"] < c["open"] and nxt["close"] > nxt["open"] and displacement and nxt["close"] > swing_hi:
            candidates.append(("bullish", float(c["low"]), float(c["open"])))
        if c["close"] > c["open"] and nxt["close"] < nxt["open"] and displacement and nxt["close"] < swing_lo:
            candidates.append(("bearish", float(c["open"]), float(c["high"])))

        for side, low, high in candidates:
            if found[side] is not None:
                continue
            if not (np.isfinite(low) and np.isfinite(high) and low < high):
                continue
            # Ignore the final row for mitigation/invalidation because it may be the still-forming 5M candle.
            later = x.iloc[i + 2:-1]
            if side == "bullish":
                invalid = not later.empty and bool((later["close"] < low - 0.05 * atrv).any())
            else:
                invalid = not later.empty and bool((later["close"] > high + 0.05 * atrv).any())
            if invalid:
                continue
            touched = not later.empty and bool(((later["low"] <= high) & (later["high"] >= low)).any())
            zone = {
                "low": low, "high": high, "time": str(c["time"]),
                "status": "MITIGATED / RETESTED" if touched else "FRESH",
                "fresh": not touched,
            }
            if touched:
                # Preserve the newest mitigated zone for diagnostics, but keep searching
                # older candidates for a still-fresh, unmitigated zone.
                if mitigated[side] is None:
                    mitigated[side] = zone
            else:
                found[side] = zone
        if all(found[s] is not None for s in ("bullish", "bearish")):
            break

    found["last_mitigated_bullish"] = mitigated["bullish"]
    found["last_mitigated_bearish"] = mitigated["bearish"]
    bull, bear = found["bullish"], found["bearish"]
    found["near_bullish"] = bool(bull and bull["low"] <= float(price) <= bull["high"])
    found["near_bearish"] = bool(bear and bear["low"] <= float(price) <= bear["high"])
    found["note"] = (
        "Active OB shows only fresh, unmitigated zones. Previously touched zones are listed as mitigated, "
        "not as active entry zones. Zones are 5M candle-derived estimates, not verified institutional orders."
    )
    return found


# -----------------------------
# Magic Ratio Area (5M swing retracement)
# -----------------------------
def magic_ratio_map(df, price, pivot_len=5, lookback=180, atr_mult=1.5, reward_r=2.0):
    """Calculate a confirmed-swing 0.50-0.618 retracement zone.

    This is a transparent Fibonacci-style approximation, not a copy of any
    proprietary TradingView indicator and not a standalone trade signal.
    Pivots are confirmed only after pivot_len candles on the right exist.
    """
    result = {
        "ok": False, "bias": "NEUTRAL", "zone_low": None, "zone_high": None,
        "level": None, "inside": False, "stop": None, "target": None,
        "rr": reward_r, "swing_high": None, "swing_low": None,
        "note": "Waiting for enough confirmed 5M swing points."
    }
    if df is None or df.empty or not {"high", "low", "close"}.issubset(df.columns):
        return result
    x = df.tail(max(lookback, pivot_len * 4 + 10)).reset_index(drop=True)
    if len(x) < pivot_len * 2 + 10:
        return result

    highs = pd.to_numeric(x["high"], errors="coerce").to_numpy()
    lows = pd.to_numeric(x["low"], errors="coerce").to_numpy()
    pivots = []
    # Exclude the last pivot_len bars: they are not confirmed yet.
    for i in range(pivot_len, len(x) - pivot_len):
        win_h = highs[i-pivot_len:i+pivot_len+1]
        win_l = lows[i-pivot_len:i+pivot_len+1]
        if np.isfinite(highs[i]) and highs[i] == np.nanmax(win_h) and np.count_nonzero(win_h == highs[i]) == 1:
            pivots.append((i, "H", float(highs[i])))
        if np.isfinite(lows[i]) and lows[i] == np.nanmin(win_l) and np.count_nonzero(win_l == lows[i]) == 1:
            pivots.append((i, "L", float(lows[i])))
    pivots.sort(key=lambda z: z[0])
    # Collapse adjacent same-type pivots, keeping the more extreme one.
    clean = []
    for piv in pivots:
        if clean and clean[-1][1] == piv[1]:
            if (piv[1] == "H" and piv[2] >= clean[-1][2]) or (piv[1] == "L" and piv[2] <= clean[-1][2]):
                clean[-1] = piv
        else:
            clean.append(piv)
    if len(clean) < 2:
        return result

    # Find the latest consecutive swing pair with opposite pivot types.
    a, b = clean[-2], clean[-1]
    if a[1] == b[1]:
        return result
    if a[1] == "L" and b[1] == "H":
        swing_low, swing_high = a[2], b[2]
        span = swing_high - swing_low
        if span <= 0:
            return result
        zone_low = swing_high - span * 0.618
        zone_high = swing_high - span * 0.50
        bias = "BULLISH BIAS"
        stop = float(price) - float(atr(x, 14).iloc[-1]) * atr_mult if len(x) >= 15 else None
        target = float(price) + (float(price) - stop) * reward_r if stop is not None and stop < price else None
    else:
        swing_high, swing_low = a[2], b[2]
        span = swing_high - swing_low
        if span <= 0:
            return result
        zone_low = swing_low + span * 0.50
        zone_high = swing_low + span * 0.618
        bias = "BEARISH BIAS"
        stop = float(price) + float(atr(x, 14).iloc[-1]) * atr_mult if len(x) >= 15 else None
        target = float(price) - (stop - float(price)) * reward_r if stop is not None and stop > price else None

    zone_low, zone_high = min(zone_low, zone_high), max(zone_low, zone_high)
    result.update({
        "ok": True, "bias": bias, "zone_low": float(zone_low), "zone_high": float(zone_high),
        "level": float((zone_low + zone_high) / 2.0),
        "inside": bool(zone_low <= float(price) <= zone_high),
        "stop": float(stop) if stop is not None and np.isfinite(stop) else None,
        "target": float(target) if target is not None and np.isfinite(target) else None,
        "rr": float(reward_r), "swing_high": float(swing_high), "swing_low": float(swing_low),
        "note": "0.50–0.618 zone from the latest confirmed 5M swing pair. Stop/target are illustrative ATR-based planning levels, not an entry instruction."
    })
    return result


def render_magic_ratio_panel(magic, price):
    """Render the same Magic Ratio information in Quick Trade and Full Pro."""
    with st.expander("🟣 Magic Ratio Area — Bias / Stop / Target", expanded=False):
        if not magic.get("ok"):
            st.info(magic.get("note", "Waiting for confirmed swing points."))
            return
        bias = magic.get("bias", "NEUTRAL")
        if bias == "BULLISH BIAS":
            st.success("🟢 " + bias)
        else:
            st.error("🔴 " + bias)
        c1, c2, c3 = st.columns(3)
        c1.metric("Magic Ratio Area", f"{magic['zone_low']:,.2f} – {magic['zone_high']:,.2f}")
        c2.metric("Magic Level", f"{magic['level']:,.2f}")
        c3.metric("Price inside zone", "YES" if magic.get("inside") else "NO")
        c4, c5, c6 = st.columns(3)
        c4.metric("Current XAUUSD", f"{float(price):,.2f}")
        c5.metric("Illustrative Stop", f"{magic['stop']:,.2f}" if magic.get("stop") is not None else "—")
        c6.metric("Illustrative Target", f"{magic['target']:,.2f}" if magic.get("target") is not None else "—")
        st.caption(f"Risk/Reward planning ratio: 1 : {magic.get('rr', 2.0):.2f}. {magic.get('note', '')}")
        st.warning("Magic Ratio zone touch alone is not a BUY/SELL signal. Wait for the existing Smart Flow break + retest + BOS/confirmation rules.")


# -----------------------------
# Live 5M engine
# -----------------------------
def make_live_df(snap):
    rows = list(snap.get("completed", []))
    cur = snap.get("current")
    if cur:
        rows.append(cur)
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["t"], unit="ms", utc=True)
    return df[["time", "o", "h", "l", "c", "v"]].rename(
        columns={"o": "open", "h": "high", "l": "low", "c": "close", "v": "volume"}
    )


def early_engine(live5, h1, m15, price):
    if live5.empty or len(live5) < 15:
        return {
            "status": "WAIT", "direction": "NEUTRAL", "score": 0,
            "quality": "C", "up_trigger": price, "down_trigger": price,
            "reason": "Waiting for live 5M candles.",
        }

    x = enrich(live5)
    cur = x.iloc[-1]
    atrv = float(x.atr.iloc[-1]) if pd.notna(x.atr.iloc[-1]) else max(price * 0.001, 1)
    prior = x.iloc[-2]
    micro_hi = float(x.high.iloc[-6:-1].max())
    micro_lo = float(x.low.iloc[-6:-1].min())

    body = abs(float(cur.close - cur.open))
    rng = max(float(cur.high - cur.low), 1e-9)
    body_ratio = body / rng
    speed = 0.0
    if len(live5) >= 3:
        dt = max((live5.time.iloc[-1] - live5.time.iloc[-3]).total_seconds(), 1)
        speed = float((live5.close.iloc[-1] - live5.close.iloc[-3]) / dt)

    atr_ratio = rng / max(atrv, 1e-9)
    h1e = enrich(h1) if not h1.empty else h1
    m15e = enrich(m15) if not m15.empty else m15
    h1trend = trend_state(h1e)
    m15trend = trend_state(m15e)

    bull = 0
    bear = 0
    bull_reasons, bear_reasons = [], []

    if h1trend == "BULL":
        bull += 20; bull_reasons.append("1H bullish")
    elif h1trend == "BEAR":
        bear += 20; bear_reasons.append("1H bearish")

    if m15trend == "BULL":
        bull += 15; bull_reasons.append("15M bullish")
    elif m15trend == "BEAR":
        bear += 15; bear_reasons.append("15M bearish")

    if cur.close > cur.ema20:
        bull += 10; bull_reasons.append("5M above EMA20")
    else:
        bear += 10; bear_reasons.append("5M below EMA20")

    if cur.pdi > cur.mdi:
        bull += 10; bull_reasons.append("+DI pressure")
    else:
        bear += 10; bear_reasons.append("-DI pressure")

    if cur.rsi >= 52:
        bull += 8; bull_reasons.append("RSI pressure")
    elif cur.rsi <= 48:
        bear += 8; bear_reasons.append("RSI pressure")

    if body_ratio >= 0.60:
        if cur.close > cur.open:
            bull += 12; bull_reasons.append("strong bullish body")
        else:
            bear += 12; bear_reasons.append("strong bearish body")

    if atr_ratio >= 0.65:
        if cur.close > cur.open:
            bull += 10; bull_reasons.append("range expansion UP")
        else:
            bear += 10; bear_reasons.append("range expansion DOWN")

    if cur.close > micro_hi:
        bull += 15; bull_reasons.append("micro-high breakout")
    if cur.close < micro_lo:
        bear += 15; bear_reasons.append("micro-low breakout")

    up_trigger = micro_hi + max(atrv * 0.03, 0.05)
    down_trigger = micro_lo - max(atrv * 0.03, 0.05)

    sr = support_resistance(x)
    sup = nearest(sr, price, "support")
    res = nearest(sr, price, "resistance")

    if sup is not None and abs(price - sup) <= atrv * 0.30:
        bull += 8; bull_reasons.append("near support")
    if res is not None and abs(price - res) <= atrv * 0.30:
        bear += 8; bear_reasons.append("near resistance")

    best = max(bull, bear)
    direction = "UP" if bull > bear else "DOWN" if bear > bull else "NEUTRAL"
    score = int(min(100, best))

    # Anti-chase: a large move already far from its trigger is not a new entry.
    extension = abs(price - (micro_hi if direction == "UP" else micro_lo if direction == "DOWN" else price))
    chase = extension > atrv * 0.90 and atr_ratio > 1.15

    if direction == "UP" and bull >= 70 and not chase:
        status = "BIG CANDLE BUY NOW" if cur.close > micro_hi else "READY UP"
    elif direction == "DOWN" and bear >= 70 and not chase:
        status = "BIG CANDLE SELL NOW" if cur.close < micro_lo else "READY DOWN"
    elif direction == "UP" and bull >= 55:
        status = "BUILDING UP"
    elif direction == "DOWN" and bear >= 55:
        status = "BUILDING DOWN"
    else:
        status = "WAIT"

    if chase:
        status = "EXTENDED — DON'T CHASE"

    quality = "A+" if score >= 85 else "A" if score >= 70 else "B" if score >= 55 else "C"

    return {
        "status": status,
        "direction": direction,
        "score": score,
        "quality": quality,
        "up_trigger": up_trigger,
        "down_trigger": down_trigger,
        "body_ratio": body_ratio,
        "atr_ratio": atr_ratio,
        "speed": speed,
        "micro_hi": micro_hi,
        "micro_lo": micro_lo,
        "h1trend": h1trend,
        "m15trend": m15trend,
        "support": sup,
        "resistance": res,
        "reasons": bull_reasons if direction == "UP" else bear_reasons,
        "atr": atrv,
    }


# -----------------------------
# Institutional / Confluence Engine
# -----------------------------
def _session_levels(df, start_hour, end_hour):
    if df.empty:
        return None, None
    x = df.copy()
    h = x["time"].dt.hour
    if start_hour < end_hour:
        z = x[(h >= start_hour) & (h < end_hour)]
    else:
        z = x[(h >= start_hour) | (h < end_hour)]
    if z.empty:
        return None, None
    return float(z.high.max()), float(z.low.min())


def institutional_engine(h1, m15, m5, live_engine, price):
    """Rule-based institutional-style confluence.

    Important: this is NOT exchange order-book data. XAUUSD spot volume and
    price/volume pressure are treated as proxies. True COMEX depth/order flow
    requires a dedicated futures market-depth feed.
    """
    if h1.empty or m15.empty or m5.empty:
        return {"signal":"WAIT", "score":0, "buy":0, "sell":0,
                "reason":"Insufficient data for institutional confluence."}

    H, M, X = enrich(h1), enrich(m15), enrich(m5)
    ht, mt, xt = trend_state(H), trend_state(M), trend_state(X)
    stc = structure(X)
    sr = support_resistance(X)
    last = X.iloc[-1]
    atrv = float(last.atr) if pd.notna(last.atr) else max(price*0.001, 1.0)

    buy = sell = 0
    br, srx = [], []

    # 1) Higher timeframe bias — 15 points
    if ht == "BULL": buy += 8; br.append("1H bullish")
    elif ht == "BEAR": sell += 8; srx.append("1H bearish")
    if mt == "BULL": buy += 7; br.append("15M bullish")
    elif mt == "BEAR": sell += 7; srx.append("15M bearish")

    # 2) Structure — 15 points
    if stc.get("sweep_low"): buy += 5; br.append("sell-side liquidity sweep")
    if stc.get("sweep_high"): sell += 5; srx.append("buy-side liquidity sweep")
    if stc.get("choch"):
        if stc.get("bos_bull"): buy += 4; br.append("bullish CHoCH")
        elif stc.get("bos_bear"): sell += 4; srx.append("bearish CHoCH")
    if stc.get("bos_bull"): buy += 6; br.append("BOS up")
    if stc.get("bos_bear"): sell += 6; srx.append("BOS down")

    # 3) Liquidity map — previous UTC day + major sessions, 15 points
    day = X["time"].dt.date
    prev_day = X[day < day.iloc[-1]]
    pdh = float(prev_day.high.max()) if not prev_day.empty else None
    pdl = float(prev_day.low.min()) if not prev_day.empty else None
    asia_hi, asia_lo = _session_levels(X, 0, 8)
    lon_hi, lon_lo = _session_levels(X, 8, 13)
    ny_hi, ny_lo = _session_levels(X, 13, 21)
    levels = [v for v in [pdh,pdl,asia_hi,asia_lo,lon_hi,lon_lo,ny_hi,ny_lo] if v is not None]
    near = min((abs(price-v),v) for v in levels) if levels else (None,None)
    if near[0] is not None and near[0] <= atrv*0.45:
        if price >= near[1]:
            buy += 7; br.append("near liquidity above")
        else:
            sell += 7; srx.append("near liquidity below")
    if pdl is not None and price <= pdl + atrv*0.35:
        buy += 4; br.append("near previous-day low")
    if pdh is not None and price >= pdh - atrv*0.35:
        sell += 4; srx.append("near previous-day high")
    if asia_hi is not None and price > asia_hi: buy += 2
    if asia_lo is not None and price < asia_lo: sell += 2

    # 4) VWAP — 10 points
    vwap = float(last.vwap) if pd.notna(last.vwap) else price
    if price > vwap and last.close >= last.open:
        buy += 10; br.append("above session VWAP")
    elif price < vwap and last.close <= last.open:
        sell += 10; srx.append("below session VWAP")
    elif price > vwap:
        buy += 5
    elif price < vwap:
        sell += 5

    # 5) Volume expansion — 15 points (SiftingIO spot volume proxy)
    vol = pd.to_numeric(X["volume"], errors="coerce").fillna(0)
    if len(vol) >= 21 and float(vol.iloc[-1]) > float(vol.iloc[-21:-1].median()) * 1.35:
        if last.close > last.open:
            buy += 15; br.append("volume expansion UP (proxy)")
        elif last.close < last.open:
            sell += 15; srx.append("volume expansion DOWN (proxy)")
    elif len(vol) >= 21 and float(vol.iloc[-1]) > float(vol.iloc[-21:-1].median()) * 1.10:
        if last.close > last.open: buy += 7
        elif last.close < last.open: sell += 7

    # 6) Price/volume pressure proxy — 10 points
    recent = X.tail(8)
    up = int((recent.close > recent.open).sum())
    down = int((recent.close < recent.open).sum())
    if up >= 6 and price > vwap:
        buy += 10; br.append("bullish pressure proxy")
    elif down >= 6 and price < vwap:
        sell += 10; srx.append("bearish pressure proxy")
    elif up > down: buy += 5
    elif down > up: sell += 5

    # 7) Big-move engine — 10 points
    if live_engine.get("direction") == "UP":
        buy += min(10, int(live_engine.get("score",0) * 0.10))
        if live_engine.get("score",0) >= 70: br.append("early expansion UP")
    elif live_engine.get("direction") == "DOWN":
        sell += min(10, int(live_engine.get("score",0) * 0.10))
        if live_engine.get("score",0) >= 70: srx.append("early expansion DOWN")

    # 8) S/R location — 10 points
    ns = nearest(sr, price, "support")
    nr = nearest(sr, price, "resistance")
    if ns is not None and abs(price-ns) <= atrv*0.35:
        buy += 10; br.append("near support")
    if nr is not None and abs(price-nr) <= atrv*0.35:
        sell += 10; srx.append("near resistance")

    buy = min(100, buy); sell = min(100, sell)
    best = max(buy, sell)
    direction = "BUY" if buy > sell else "SELL" if sell > buy else "WAIT"
    margin = abs(buy-sell)

    # Conservative gate: avoid calling a trade A+ when the higher timeframe
    # is strongly opposite or when the two sides are too close.
    if best >= 80 and margin >= 10:
        if direction == "BUY" and ht != "BEAR" and mt != "BEAR":
            signal, label = "BUY", "A+ BUY"
        elif direction == "SELL" and ht != "BULL" and mt != "BULL":
            signal, label = "SELL", "A+ SELL"
        else:
            signal, label = "WAIT", "CONFLICT"
    elif best >= 70 and margin >= 8:
        signal, label = "WATCH", "WATCH"
    else:
        signal, label = "WAIT", "NO TRADE"

    return {
        "signal": signal, "label": label, "score": int(best), "buy": int(buy),
        "sell": int(sell), "margin": int(margin), "vwap": vwap,
        "pdh": pdh, "pdl": pdl, "asia_hi": asia_hi, "asia_lo": asia_lo,
        "london_hi": lon_hi, "london_lo": lon_lo, "ny_hi": ny_hi, "ny_lo": ny_lo,
        "support": ns, "resistance": nr,
        "buy_reasons": br, "sell_reasons": srx,
        "data_note": "Volume/order-flow values are XAUUSD spot proxies; true COMEX depth requires a futures depth feed.",
    }




def market_regime(h1, m15, m5):
    """Classify broad conditions from available candle data; diagnostic only."""
    if h1.empty or m15.empty or m5.empty:
        return {"regime": "UNKNOWN", "score": 0, "reason": "Insufficient data"}
    H, M, X = enrich(h1), enrich(m15), enrich(m5)
    if H.empty or M.empty or X.empty:
        return {"regime": "UNKNOWN", "score": 0, "reason": "Insufficient indicators"}
    h, m, x = H.iloc[-1], M.iloc[-1], X.iloc[-1]
    adx = float(x.adx) if pd.notna(x.adx) else 0.0
    rng = float((X.high - X.low).tail(6).mean())
    base = float((X.high - X.low).iloc[-36:-6].median()) if len(X) >= 36 else rng
    bull = h.ema20 > h.ema50 and m.ema20 > m.ema50 and x.ema20 > x.ema50
    bear = h.ema20 < h.ema50 and m.ema20 < m.ema50 and x.ema20 < x.ema50
    expansion = base > 0 and rng > 1.25 * base
    compression = base > 0 and rng < 0.85 * base
    if expansion and adx >= 22: regime = "BREAKOUT / EXPANSION"
    elif bull and adx >= 22: regime = "TREND UP"
    elif bear and adx >= 22: regime = "TREND DOWN"
    elif compression: regime = "COMPRESSION / BUILDING"
    elif adx < 17: regime = "RANGE / LOW TREND"
    else: regime = "TRANSITION"
    return {"regime": regime, "score": int(min(100, max(0, adx * 2))), "adx": adx, "expansion": expansion, "compression": compression}


def liquidity_enhanced(m5):
    """Estimate equal-high/low pools from prior candles, then test the latest candle for a sweep.
    These are candle-based heuristics, not exchange order-book liquidity.
    """
    if m5 is None or m5.empty or len(m5) < 20:
        return {"eq_high": None, "eq_low": None, "sweep_high": False, "sweep_low": False}
    x = m5.copy()
    if not {"high", "low", "close"}.issubset(x.columns):
        return {"eq_high": None, "eq_low": None, "sweep_high": False, "sweep_low": False}
    highs, lows = x.high.tail(20).to_numpy(), x.low.tail(20).to_numpy()
    # Exclude the current candle from pool construction so a sweep candle
    # cannot erase the equal-high/low pool it is meant to sweep.
    prior_highs, prior_lows = highs[:-1][-6:], lows[:-1][-6:]
    last = x.iloc[-1]
    tol = max(float(np.std(np.asarray(prior_highs, dtype=float))) * 0.12,
              abs(float(last.close)) * 0.00025)
    eqh = float(np.median(prior_highs)) if len(prior_highs) >= 5 and np.ptp(prior_highs) <= tol else None
    eql = float(np.median(prior_lows)) if len(prior_lows) >= 5 and np.ptp(prior_lows) <= tol else None
    return {"eq_high": eqh, "eq_low": eql,
            "sweep_high": bool(eqh is not None and float(last.high) > eqh + tol * 0.25 and float(last.close) < eqh),
            "sweep_low": bool(eql is not None and float(last.low) < eql - tol * 0.25 and float(last.close) > eql)}


def a_plus_signal_gate(h1, m15, m5, engine, trigger_map, regime, price):
    """Conservative diagnostic checklist; does not override the existing trade engine."""
    if h1.empty or m15.empty or m5.empty:
        return {"signal": "NO TRADE", "score": 0, "reasons": [], "missing": ["market data"]}
    H, M, X = enrich(h1), enrich(m15), enrich(m5)
    ht, mt = trend_state(H), trend_state(M)
    stc, liq = structure(X), liquidity_enhanced(X)
    side_scores = {}
    side_reasons = {}
    side_missing = {}
    for side in ("BUY", "SELL"):
        pts, reasons, missing = 0, [], []
        bullish = side == "BUY"
        if ht == ("BULL" if bullish else "BEAR"):
            pts += 20; reasons.append("1H trend aligned")
        else: missing.append("1H trend alignment")
        if mt == ("BULL" if bullish else "BEAR"):
            pts += 15; reasons.append("15M trend aligned")
        else: missing.append("15M trend alignment")
        # Align the A+ checklist with the trigger map's enhanced equal-high/low sweep detector.
        sweep = (
            stc.get("sweep_low" if bullish else "sweep_high")
            or liq.get("sweep_low" if bullish else "sweep_high")
        )
        if sweep: pts += 15; reasons.append("liquidity sweep")
        else: missing.append("liquidity sweep")
        if stc.get("bos_bull" if bullish else "bos_bear") or stc.get("choch"):
            pts += 15; reasons.append("BOS/CHoCH")
        else: missing.append("BOS/CHoCH")
        vwap = float(X.vwap.iloc[-1]) if pd.notna(X.vwap.iloc[-1]) else price
        if (price > vwap) == bullish:
            pts += 10; reasons.append("VWAP aligned")
        else: missing.append("VWAP alignment")
        if engine.get("direction") == ("UP" if bullish else "DOWN") and engine.get("score", 0) >= 65:
            pts += 10; reasons.append("early expansion")
        else: missing.append("early expansion")
        trigger = trigger_map.get("buy_score" if bullish else "sell_score", 0)
        if trigger >= 65: pts += 15; reasons.append("trigger/retest score")
        else: missing.append("trigger/retest confirmation")
        if regime.get("regime") == "RANGE / LOW TREND": pts = max(0, pts - 10); missing.append("avoid low-trend range")
        side_scores[side], side_reasons[side], side_missing[side] = pts, reasons, missing
    side = max(side_scores, key=side_scores.get)
    score = int(side_scores[side])
    signal = side if score >= 80 and side_scores[side] - side_scores["SELL" if side == "BUY" else "BUY"] >= 10 else "WATCH" if score >= 65 else "NO TRADE"
    return {"signal": signal, "side": side, "score": score, "reasons": side_reasons[side], "missing": side_missing[side]}


# -----------------------------
# Next Trigger Map + Liquidity Sweep / Retest Engine
# -----------------------------
def next_trigger_map(h1, m15, m5, engine, institutional, price):
    """Build a live roadmap for the next high-quality move.

    This does not predict the next candle. It answers:
      1) Where is the nearest meaningful breakout level?
      2) Was liquidity swept?
      3) What structure/price confirmation is still missing?
      4) Is a retest preferable to chasing?

    XAUUSD volume/order-flow remains a spot proxy; true COMEX depth requires
    a dedicated futures market-depth feed.
    """
    out = {
        "state": "NO CLEAR TRIGGER",
        "buy_trigger": float(price), "sell_trigger": float(price),
        "buy_break": float(price), "sell_break": float(price),
        "buy_retest_low": float(price), "buy_retest_high": float(price),
        "sell_retest_low": float(price), "sell_retest_high": float(price),
        "buy_state": "BREAKOUT PENDING", "sell_state": "BREAKDOWN PENDING",
        "buy_missing": [], "sell_missing": [],
        "buy_score": 0, "sell_score": 0,
        "buy_sweep": False, "sell_sweep": False,
        "buy_ready": False, "sell_ready": False,
        "vwap": float(institutional.get("vwap", price) or price),
        "support": None, "resistance": None,
        "reason": "Waiting for a clean structural trigger."
    }
    if m5.empty:
        out["reason"] = "Waiting for live 5M structure."
        return out

    # Always initialize local state variables before the decision tree.
    # Without this, a run where none of the later branches matches can raise
    # UnboundLocalError when the function builds its return dictionary.
    state = out["state"]
    buy_state = out["buy_state"]
    sell_state = out["sell_state"]
    reason = out["reason"]
    # Initialize return values before the decision tree so every Streamlit
    # rerun/market state has a safe value.
    vwap = out["vwap"]
    s5 = out["support"]
    r5 = out["resistance"]

    X = enrich(m5)
    M = enrich(m15) if not m15.empty else m15
    H = enrich(h1) if not h1.empty else h1
    st5 = structure(X)
    # Reuse the equal-high/low pool sweep detector in the trigger map so
    # the same candle-based liquidity sweep can inform trigger diagnostics.
    liq5 = liquidity_enhanced(X)
    ht = trend_state(H) if not H.empty else "NEUTRAL"
    mt = trend_state(M) if not M.empty else "NEUTRAL"
    last = X.iloc[-1]
    atrv = float(last.atr) if pd.notna(last.atr) else max(price * 0.001, 1.0)
    vwap = float(institutional.get("vwap", last.vwap if pd.notna(last.vwap) else price) or price)

    sr5 = support_resistance(X)
    sr15 = support_resistance(M) if not M.empty else {"support": [], "resistance": []}
    r5 = nearest(sr5, price, "resistance")
    s5 = nearest(sr5, price, "support")
    r15 = nearest(sr15, price, "resistance")
    s15 = nearest(sr15, price, "support")

    # Confirmed micro levels are based on completed bars before the current bar.
    look = X.tail(8)
    if len(look) >= 3:
        micro_hi = float(look.high.iloc[:-1].max())
        micro_lo = float(look.low.iloc[:-1].min())
    else:
        micro_hi = price + 0.20 * atrv
        micro_lo = price - 0.20 * atrv

    # Use the closest meaningful barrier on each side.
    buy_candidates = [v for v in [micro_hi, r5, r15] if v is not None and v > price]
    sell_candidates = [v for v in [micro_lo, s5, s15] if v is not None and v < price]
    buy_break = min(buy_candidates) if buy_candidates else price + 0.20 * atrv
    sell_break = max(sell_candidates) if sell_candidates else price - 0.20 * atrv

    # Small buffer prevents a touch of the level from becoming a false trigger.
    buffer = max(0.04 * atrv, 0.05)
    buy_trigger = float(buy_break + buffer)
    sell_trigger = float(sell_break - buffer)

    # Retest zones are deliberately narrow: broken level ± 0.15 ATR.
    retest_pad = max(0.15 * atrv, 0.10)
    buy_retest_low = float(buy_break - retest_pad)
    buy_retest_high = float(buy_break + retest_pad)
    sell_retest_low = float(sell_break - retest_pad)
    sell_retest_high = float(sell_break + retest_pad)

    # Liquidity sweep flags from the 5M structure engine.
    buy_sweep = bool(st5.get("sweep_low") or liq5.get("sweep_low"))
    sell_sweep = bool(st5.get("sweep_high") or liq5.get("sweep_high"))

    # ---------------- BUY confirmation score ----------------
    bs = 0
    bmiss = []
    if ht == "BULL": bs += 20
    elif ht == "NEUTRAL": bs += 8; bmiss.append("1H bias bullish/neutral")
    else: bmiss.append("resolve 1H bearish conflict")
    if mt == "BULL": bs += 15
    else: bmiss.append("15M bullish structure")
    if st5.get("bos_bull"): bs += 15
    elif st5.get("choch") and st5.get("sweep_low"): bs += 10
    else: bmiss.append("5M BOS UP or sweep + CHoCH")
    if buy_sweep: bs += 10
    else: bmiss.append("sell-side liquidity sweep")
    if price > vwap: bs += 10
    else: bmiss.append(f"reclaim VWAP {vwap:,.2f}")
    if float(last.pdi) > float(last.mdi): bs += 10
    else: bmiss.append("+DI pressure")
    if engine.get("direction") == "UP" and engine.get("score", 0) >= 60: bs += 10
    else: bmiss.append("early expansion UP ≥60")
    if price >= buy_trigger: bs += 10
    else: bmiss.append(f"break {buy_trigger:,.2f}")

    # ---------------- SELL confirmation score ----------------
    ss = 0
    smiss = []
    if ht == "BEAR": ss += 20
    elif ht == "NEUTRAL": ss += 8; smiss.append("1H bias bearish/neutral")
    else: smiss.append("resolve 1H bullish conflict")
    if mt == "BEAR": ss += 15
    else: smiss.append("15M bearish structure")
    if st5.get("bos_bear"): ss += 15
    elif st5.get("choch") and st5.get("sweep_high"): ss += 10
    else: smiss.append("5M BOS DOWN or sweep + CHoCH")
    if sell_sweep: ss += 10
    else: smiss.append("buy-side liquidity sweep")
    if price < vwap: ss += 10
    else: smiss.append(f"lose VWAP {vwap:,.2f}")
    if float(last.mdi) > float(last.pdi): ss += 10
    else: smiss.append("-DI pressure")
    if engine.get("direction") == "DOWN" and engine.get("score", 0) >= 60: ss += 10
    else: smiss.append("early expansion DOWN ≥60")
    if price <= sell_trigger: ss += 10
    else: smiss.append(f"break {sell_trigger:,.2f}")

    # A trigger can be active without becoming an A+ trade. The institutional
    # engine remains the final gate for actual BUY/SELL levels.
    buy_ready = price >= buy_trigger and bs >= 65 and not (ht == "BEAR" and mt == "BEAR")
    sell_ready = price <= sell_trigger and ss >= 65 and not (ht == "BULL" and mt == "BULL")

    # Retest confirmation: after a trigger, prefer a return to the broken level
    # instead of entering after a large extension.
    buy_retest = price >= buy_trigger and buy_retest_low <= price <= buy_retest_high
    sell_retest = price <= sell_trigger and sell_retest_low <= price <= sell_retest_high

    if buy_retest and bs >= ss:
        state = "BUY RETEST — WATCH"
        reason = "BUY trigger broke and price is testing the breakout zone; this is preferable to chasing a stretched candle."
        buy_state = "RETEST CONFIRMATION"
    elif sell_retest and ss > bs:
        state = "SELL RETEST — WATCH"
        reason = "SELL trigger broke and price is testing the breakdown zone; this is preferable to chasing a stretched candle."
        sell_state = "RETEST CONFIRMATION"
    elif buy_ready and bs > ss:
        state = "BUY TRIGGER ACTIVE"
        reason = "BUY trigger is active. Require the 5M confirmation/retest before treating it as a trade."
        buy_state = "TRIGGER ACTIVE"
    elif sell_ready and ss > bs:
        state = "SELL TRIGGER ACTIVE"
        reason = "SELL trigger is active. Require the 5M confirmation/retest before treating it as a trade."
        sell_state = "TRIGGER ACTIVE"
    elif engine.get("status") == "EXTENDED — DON'T CHASE":
        state = "MOVE EXTENDED — WAIT RETEST"
        reason = "The move is already extended. Do not chase; wait for the marked breakout level to be retested."
    elif bs > ss:
        state = "BUY SETUP BUILDING"
        reason = "BUY side has more confirmation, but the trigger has not cleared all required conditions."
    elif ss > bs:
        state = "SELL SETUP BUILDING"
        reason = "SELL side has more confirmation, but the trigger has not cleared all required conditions."

    return {
        **out,
        "state": state,
        "buy_trigger": buy_trigger, "sell_trigger": sell_trigger,
        "buy_break": float(buy_break), "sell_break": float(sell_break),
        "buy_retest_low": buy_retest_low, "buy_retest_high": buy_retest_high,
        "sell_retest_low": sell_retest_low, "sell_retest_high": sell_retest_high,
        "buy_state": buy_state, "sell_state": sell_state,
        "buy_missing": bmiss[:6], "sell_missing": smiss[:6],
        "buy_score": int(min(bs, 100)), "sell_score": int(min(ss, 100)),
        "buy_sweep": buy_sweep, "sell_sweep": sell_sweep,
        "buy_ready": bool(buy_ready), "sell_ready": bool(sell_ready),
        "vwap": vwap, "support": s5, "resistance": r5, "reason": reason,
    }


# -----------------------------
# Strict A+ confirmation: OB + recent liquidity sweep + structure break
# -----------------------------
def order_block_confirmation(df, price, ob_map, lookback=4):
    """Conservative A+ gate: matching OB plus sweep followed by a later BOS.

    Sweep and BOS must occur on distinct candles and in that order. This is a
    candle-derived heuristic, not exchange order-book data; validate with
    backtesting before live use.
    """
    result = {
        "buy_ok": False, "sell_ok": False,
        "buy_ob_near": False, "sell_ob_near": False,
        "buy_sweep": False, "sell_sweep": False,
        "buy_break": False, "sell_break": False,
        "buy_sequence": False, "sell_sequence": False,
        "buy_reason": "Insufficient 5M candles",
        "sell_reason": "Insufficient 5M candles",
    }
    if df is None or df.empty or len(df) < 25:
        return result

    x = enrich(df).reset_index(drop=True)
    atrv = float(x.atr.iloc[-1]) if pd.notna(x.atr.iloc[-1]) and float(x.atr.iloc[-1]) > 0 else max(float(price) * 0.001, 1.0)
    pad = max(0.20 * atrv, 0.10)
    bull = ob_map.get("bullish") if ob_map else None
    bear = ob_map.get("bearish") if ob_map else None
    result["buy_ob_near"] = bool(bull and bull["low"] - pad <= price <= bull["high"] + pad)
    result["sell_ob_near"] = bool(bear and bear["low"] - pad <= price <= bear["high"] + pad)

    # Record event candle indices. A BOS on the same candle as a sweep is not
    # accepted as a sequence; a directional break must happen on a later bar.
    sweep_low_idx = sweep_high_idx = None
    bos_bull_idx = bos_bear_idx = None
    begin = max(24, len(x) - max(int(lookback), 1))
    for end in range(begin, len(x)):
        stc = structure(x.iloc[:end + 1])
        if stc.get("sweep_low"):
            sweep_low_idx = end
            result["buy_sweep"] = True
        if stc.get("sweep_high"):
            sweep_high_idx = end
            result["sell_sweep"] = True
        if stc.get("bos_bull"):
            bos_bull_idx = end
            result["buy_break"] = True
        if stc.get("bos_bear"):
            bos_bear_idx = end
            result["sell_break"] = True

    result["buy_sequence"] = bool(sweep_low_idx is not None and bos_bull_idx is not None and bos_bull_idx > sweep_low_idx)
    result["sell_sequence"] = bool(sweep_high_idx is not None and bos_bear_idx is not None and bos_bear_idx > sweep_high_idx)
    result["buy_ok"] = bool(result["buy_ob_near"] and result["buy_sequence"])
    result["sell_ok"] = bool(result["sell_ob_near"] and result["sell_sequence"])

    def reason(side):
        ob_key, sweep_key, break_key, seq_key = (
            f"{side}_ob_near", f"{side}_sweep", f"{side}_break", f"{side}_sequence"
        )
        missing = []
        if not result[ob_key]:
            missing.append("price not at matching OB")
        if not result[sweep_key]:
            missing.append("no recent liquidity sweep")
        if not result[break_key]:
            missing.append("no recent BOS")
        if result[sweep_key] and result[break_key] and not result[seq_key]:
            missing.append("BOS did not occur after sweep")
        return "Confirmed: OB + sweep then BOS" if not missing else "Waiting: " + ", ".join(missing)

    result["buy_reason"] = reason("buy")
    result["sell_reason"] = reason("sell")
    return result


# -----------------------------
# Smart Flow signal
# -----------------------------
def smart_flow_signal(h1, m15, m5, live_engine):
    if h1.empty or m15.empty or m5.empty:
        return {"signal": "WAIT", "score": 0, "reason": "Insufficient market data."}

    H, M, X = enrich(h1), enrich(m15), enrich(m5)
    ht, mt, xt = trend_state(H), trend_state(M), trend_state(X)
    stc = structure(X)
    sr = support_resistance(X)
    price = float(X.close.iloc[-1])

    score_b = 0
    score_s = 0
    br, sr_reasons = [], []

    if ht == "BULL": score_b += 25; br.append("1H BULL")
    if ht == "BEAR": score_s += 25; sr_reasons.append("1H BEAR")
    if mt == "BULL": score_b += 20; br.append("15M BULL")
    if mt == "BEAR": score_s += 20; sr_reasons.append("15M BEAR")
    if xt == "BULL": score_b += 10; br.append("5M BULL")
    if xt == "BEAR": score_s += 10; sr_reasons.append("5M BEAR")

    if stc["sweep_low"]: score_b += 12; br.append("liquidity sweep low")
    if stc["sweep_high"]: score_s += 12; sr_reasons.append("liquidity sweep high")
    if stc["bos_bull"]: score_b += 18; br.append("BOS UP")
    if stc["bos_bear"]: score_s += 18; sr_reasons.append("BOS DOWN")

    last = X.iloc[-1]
    if last.rsi > 52: score_b += 5
    if last.rsi < 48: score_s += 5

    ns = nearest(sr, price, "support")
    nr = nearest(sr, price, "resistance")
    if ns is not None and abs(price - ns) <= last.atr * 0.35:
        score_b += 8; br.append("near support")
    if nr is not None and abs(price - nr) <= last.atr * 0.35:
        score_s += 8; sr_reasons.append("near resistance")

    if live_engine["direction"] == "UP" and live_engine["score"] >= 70:
        score_b += 10; br.append("live early UP")
    if live_engine["direction"] == "DOWN" and live_engine["score"] >= 70:
        score_s += 10; sr_reasons.append("live early DOWN")

    if score_b >= 70 and score_b > score_s:
        return {"signal": "BUY", "score": min(100, score_b), "reason": ", ".join(br)}
    if score_s >= 70 and score_s > score_b:
        return {"signal": "SELL", "score": min(100, score_s), "reason": ", ".join(sr_reasons)}
    return {
        "signal": "WAIT",
        "score": max(score_b, score_s),
        "reason": "15M/5M setup not sufficiently confirmed.",
    }


def trade_levels(signal, price, m5):
    """Build trade levels only from finite, positive price and ATR inputs."""
    if signal not in ("BUY", "SELL"):
        return None
    try:
        entry = float(price)
    except (TypeError, ValueError, OverflowError):
        return None
    if not np.isfinite(entry) or entry <= 0:
        return None
    if m5 is None or getattr(m5, "empty", True):
        return None
    x = enrich(m5)
    if x is None or x.empty or "atr" not in x.columns:
        return None
    try:
        a = float(x.atr.iloc[-1])
    except (TypeError, ValueError, OverflowError, IndexError):
        return None
    if not np.isfinite(a) or a <= 0:
        return None
    risk = 1.15 * a
    if not np.isfinite(risk) or risk <= 0:
        return None
    if signal == "BUY":
        sl = entry - risk
        levels = {
            "entry": entry, "sl": sl,
            "tp1": entry + 1.0 * risk,
            "tp2": entry + 1.7 * risk,
            "tp3": entry + 2.4 * risk,
            "tp4": entry + 3.2 * risk,
            "tp5": entry + 4.0 * risk,
            "tp6": entry + 5.0 * risk,
        }
    else:
        sl = entry + risk
        levels = {
            "entry": entry, "sl": sl,
            "tp1": entry - 1.0 * risk,
            "tp2": entry - 1.7 * risk,
            "tp3": entry - 2.4 * risk,
            "tp4": entry - 3.2 * risk,
            "tp5": entry - 4.0 * risk,
            "tp6": entry - 5.0 * risk,
        }
    if not all(np.isfinite(v) and v > 0 for v in levels.values()):
        return None
    if signal == "BUY" and not (levels["sl"] < entry < levels["tp1"] < levels["tp2"] < levels["tp3"] < levels["tp4"] < levels["tp5"] < levels["tp6"]):
        return None
    if signal == "SELL" and not (levels["sl"] > entry > levels["tp1"] > levels["tp2"] > levels["tp3"] > levels["tp4"] > levels["tp5"] > levels["tp6"]):
        return None
    return levels


# -----------------------------
# Backtest / Best Filter Engine
# -----------------------------
@st.cache_data(ttl=900, show_spinner=False)
def get_backtest_5m(api_key: str, days: int = 30) -> pd.DataFrame:
    if not api_key:
        return pd.DataFrame()
    end = datetime.now(timezone.utc)
    start_all = end - timedelta(days=days)
    frames = []
    cursor_end = end
    while cursor_end > start_all:
        cursor_start = max(start_all, cursor_end - timedelta(days=6))
        url = f"{BASE}/v1/hist/commodities/{SYMBOL}/bars"
        params = {
            "start": cursor_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "end": cursor_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "interval": "5m",
            "limit": 2000,
        }
        try:
            r = requests.get(url, params=params, headers={"X-API-Key": api_key, "Accept-Encoding": "gzip"}, timeout=20)
            r.raise_for_status()
            body = r.json()
            rows = body.get("data", body if isinstance(body, list) else [])
            if rows:
                d = pd.DataFrame(rows).rename(columns={"t":"time","o":"open","h":"high","l":"low","c":"close","v":"volume"})
                for c in ["open","high","low","close"]:
                    d[c] = pd.to_numeric(d[c], errors="coerce")
                # SiftingIO timestamps are epoch milliseconds.
                d["time"] = pd.to_datetime(d["time"], unit="ms", utc=True)
                frames.append(d.dropna(subset=["time","open","high","low","close"]))
        except requests.HTTPError as exc:
            status = getattr(getattr(exc, "response", None), "status_code", None)
            if status == 401:
                raise RuntimeError("SiftingIO HTTP 401: SIFTINGIO_API_KEY token is invalid/expired or not authorized for historical XAUUSD bars. Update Streamlit Secrets and reboot the app.") from exc
            if status == 403:
                raise RuntimeError("SiftingIO HTTP 403: the API key/account does not have permission for historical XAUUSD bars.") from exc
            raise RuntimeError(f"SiftingIO historical backtest request failed: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Could not download XAUUSD historical candles: {exc}") from exc
        cursor_end = cursor_start - timedelta(minutes=5)
    if not frames:
        return pd.DataFrame()
    return (pd.concat(frames, ignore_index=True)
            .sort_values("time").drop_duplicates("time").reset_index(drop=True))

@st.cache_data(ttl=900, show_spinner=False)
def prepare_backtest(df: pd.DataFrame):
    """Prepare indicators once. The old version rebuilt these for every candidate,
    which made the Backtest Lab look frozen on iPhone."""
    x = enrich(df.copy()).reset_index(drop=True)
    h15 = (x.set_index("time").resample("15min", label="left", closed="left")
           .agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"})
           .dropna().reset_index())
    h1 = (x.set_index("time").resample("1h", label="left", closed="left")
          .agg({"open":"first","high":"max","low":"min","close":"last","volume":"sum"})
          .dropna().reset_index())
    h15 = enrich(h15)
    h1 = enrich(h1)
    # Align higher-timeframe context once. The previous version did a
    # DataFrame .loc[:t] search inside every trade/candidate, which could
    # make the Backtest Lab appear frozen on mobile.
    c15 = h15[["time","ema20","ema50","close"]].rename(columns={"ema20":"m15_ema20","ema50":"m15_ema50","close":"m15_close"})
    c1 = h1[["time","ema20","ema50","close"]].rename(columns={"ema20":"h1_ema20","ema50":"h1_ema50","close":"h1_close"})
    aligned = pd.merge_asof(x.sort_values("time"), c15.sort_values("time"), on="time", direction="backward")
    aligned = pd.merge_asof(aligned.sort_values("time"), c1.sort_values("time"), on="time", direction="backward")
    return aligned.reset_index(drop=True), h15.set_index("time"), h1.set_index("time")

def _backtest_metrics(trades):
    if not trades:
        return {"trades":0,"win_rate":0.0,"pf":0.0,"avg_r":0.0,"net_r":0.0,"max_dd":0.0}
    rs = np.array([t["r"] for t in trades], dtype=float)
    wins = rs[rs > 0].sum()
    losses = -rs[rs < 0].sum()
    equity = np.cumsum(rs)
    peak = np.maximum.accumulate(np.r_[0.0, equity])
    dd = np.maximum(0.0, peak[1:] - equity)
    return {
        "trades": int(len(rs)),
        "win_rate": float((rs > 0).mean() * 100),
        "pf": float(wins / losses) if losses > 0 else (999.0 if wins > 0 else 0.0),
        "avg_r": float(rs.mean()),
        "net_r": float(rs.sum()),
        "max_dd": float(dd.max()) if len(dd) else 0.0,
    }

def _run_prepared(prep, mode="COMPOSITE", adx_min=20, body_min=0.50, rr=1.7, start_i=250, end_i=None):
    x, _, _ = prep
    if x.empty or len(x) < 300:
        return _backtest_metrics([])
    end_i = min(len(x)-2, end_i if end_i is not None else len(x)-2)
    start_i = max(250, int(start_i))

    # Vectorized signal conditions: this is much faster than doing DataFrame
    # slicing for every bar/candidate.
    bull15 = (x.m15_ema20 > x.m15_ema50) & (x.m15_close > x.m15_ema20)
    bear15 = (x.m15_ema20 < x.m15_ema50) & (x.m15_close < x.m15_ema20)
    bull1 = x.h1_ema20 > x.h1_ema50
    bear1 = x.h1_ema20 < x.h1_ema50
    body = (x.close - x.open).abs() / (x.high - x.low).clip(lower=1e-9)
    micro_hi = x.high.shift(1).rolling(5).max()
    micro_lo = x.low.shift(1).rolling(5).min()

    long = bull15 & bull1 & (x.close > x.ema20)
    short = bear15 & bear1 & (x.close < x.ema20)
    if mode in ("COMPOSITE", "EMA_RSI_ADX"):
        long &= (x.rsi >= 52) & (x.adx >= adx_min) & (x.pdi > x.mdi)
        short &= (x.rsi <= 48) & (x.adx >= adx_min) & (x.mdi > x.pdi)
    elif mode == "PULLBACK":
        long &= (x.adx >= adx_min) & (body >= body_min) & (x.low <= x.ema20) & (x.close > x.open) & (x.close > x.ema20)
        short &= (x.adx >= adx_min) & (body >= body_min) & (x.high >= x.ema20) & (x.close < x.open) & (x.close < x.ema20)
    elif mode == "BREAKOUT":
        long &= (x.adx >= adx_min) & (body >= body_min) & (x.close > micro_hi)
        short &= (x.adx >= adx_min) & (body >= body_min) & (x.close < micro_lo)
    if mode == "COMPOSITE":
        long &= (x.close > micro_hi) & (body >= body_min)
        short &= (x.close < micro_lo) & (body >= body_min)

    # Make an independent writable NumPy array before masking.
    # Some pandas/NumPy combinations can return a read-only view here.
    mask = np.asarray((long | short).to_numpy(dtype=bool), dtype=bool).copy()
    mask[:start_i] = False
    mask[end_i+1:] = False
    entries = np.flatnonzero(mask)
    if len(entries) == 0:
        return _backtest_metrics([])

    # Entry-by-entry execution is retained for conservative same-bar handling,
    # but only actual candidate bars are scanned, not every historical bar.
    trades=[]; next_allowed=start_i
    highs=x.high.to_numpy(dtype=float); lows=x.low.to_numpy(dtype=float)
    closes=x.close.to_numpy(dtype=float); atrs=x.atr.to_numpy(dtype=float)
    long_arr=long.to_numpy();
    for i in entries:
        if i < next_allowed: continue
        atrv=atrs[i]
        if not np.isfinite(atrv) or atrv <= 0: continue
        side=1 if long_arr[i] else -1
        entry=float(closes[i]); risk=1.15*float(atrv)
        sl=entry-side*risk; tp=entry+side*risk*rr
        result=None; exit_i=i
        stop=min(i+61, len(x))
        for j in range(i+1, stop):
            hi=float(highs[j]); lo=float(lows[j])
            hit_sl = lo <= sl if side==1 else hi >= sl
            hit_tp = hi >= tp if side==1 else lo <= tp
            if hit_sl:
                result=-1.0; exit_i=j; break
            if hit_tp:
                result=rr; exit_i=j; break
        if result is not None:
            trades.append({"r":result}); next_allowed=exit_i+4
    return _backtest_metrics(trades)

def _run_filter_backtest(df, mode="COMPOSITE", adx_min=20, body_min=0.50, rr=1.7):
    return _run_prepared(prepare_backtest(df), mode, adx_min, body_min, rr)

def run_best_filter_search(df):
    prep = prepare_backtest(df)
    modes = ["PULLBACK", "EMA_RSI_ADX", "BREAKOUT", "COMPOSITE"]
    candidates = []
    for mode in modes:
        adxs = [18,22] if mode == "PULLBACK" else [18,20,22,25]
        bodies = [0.50] if mode == "PULLBACK" else ([0.45,0.50,0.60] if mode == "COMPOSITE" else [0.50])
        for adx_min in adxs:
            for body_min in bodies:
                for rr in [1.5,1.7,2.0]:
                    m = _run_prepared(prep, mode, adx_min, body_min, rr)
                    if m["trades"] >= 20:
                        score = m["pf"] * max(m["avg_r"], 0) * 100 + m["win_rate"] * 0.15 - m["max_dd"] * 0.08
                        candidates.append((score, mode, adx_min, body_min, rr, m))
    candidates.sort(reverse=True, key=lambda z: z[0])
    return candidates[:10]


# -----------------------------
# Forward / Paper Trading Log
# -----------------------------
def _paper_trades():
    if "paper_trades" not in st.session_state:
        st.session_state["paper_trades"] = []
    return st.session_state["paper_trades"]

def update_paper_trades(price):
    """Close open paper trades on SL/TP1 touch using the observed exit price.

    R is calculated from the actual simulated exit price, so a price gap through
    the stop can produce a loss worse than -1R. Tick-only simulation cannot prove
    intrabar touches between ticks; use candle backtests for that.
    """
    try:
        px = float(price)
    except (TypeError, ValueError, OverflowError):
        return
    if not np.isfinite(px) or px <= 0:
        return

    trades = _paper_trades()
    for t in trades:
        if t.get("status") != "OPEN":
            continue
        try:
            entry = float(t["entry"])
            sl = float(t["sl"])
            tp1 = float(t["tp1"])
            side = t["side"]
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if not all(np.isfinite(v) and v > 0 for v in (entry, sl, tp1)):
            continue

        if side == "BUY":
            risk = entry - sl
            if risk <= 0:
                continue
            if px <= sl:
                t.update(status="LOSS", exit=px, r=(px - entry) / risk)
            elif px >= tp1:
                t.update(status="TP1", exit=px, r=(px - entry) / risk)
        elif side == "SELL":
            risk = sl - entry
            if risk <= 0:
                continue
            if px >= sl:
                t.update(status="LOSS", exit=px, r=(entry - px) / risk)
            elif px <= tp1:
                t.update(status="TP1", exit=px, r=(entry - px) / risk)


def maybe_log_paper_signal(signal, engine, price, levels, smart_flow_score=None):
    """Log qualified, geometrically valid paper signals without deleting open trades."""
    if not isinstance(signal, dict) or not isinstance(engine, dict) or not isinstance(levels, dict):
        return
    side = signal.get("signal")
    if side not in ("BUY", "SELL"):
        return
    try:
        vals = {k: float(levels[k]) for k in ("entry", "sl", "tp1", "tp2", "tp3", "tp4")}
        flow_score = float(signal.get("score", 0) if smart_flow_score is None else smart_flow_score)
        early_score = float(engine.get("score", 0))
    except (KeyError, TypeError, ValueError, OverflowError):
        return
    if not all(np.isfinite(v) and v > 0 for v in vals.values()):
        return
    if not np.isfinite(flow_score) or not np.isfinite(early_score):
        return
    if flow_score < 70 or early_score < 70:
        return
    if side == "BUY":
        if not (vals["sl"] < vals["entry"] < vals["tp1"] < vals["tp2"] < vals["tp3"] < vals["tp4"]):
            return
    else:
        if not (vals["sl"] > vals["entry"] > vals["tp1"] > vals["tp2"] > vals["tp3"] > vals["tp4"]):
            return

    trades = _paper_trades()
    key = f'{side}|{round(vals["entry"],2)}|{round(vals["sl"],2)}|{round(vals["tp1"],2)}'
    if any(t.get("key") == key for t in trades if t.get("status") == "OPEN"):
        return
    try:
        institutional_score = int(float(signal.get("score", 0)))
    except (TypeError, ValueError, OverflowError):
        institutional_score = 0
    trades.append({
        "key": key,
        "time": datetime.now(timezone.utc),
        "side": side,
        **vals,
        "signal_score": int(flow_score),
        "institutional_score": institutional_score,
        "early_score": int(early_score),
        "status": "OPEN",
        "exit": None,
        "r": None,
    })
    # Trim closed history only; never discard an OPEN paper trade.
    closed = [i for i, t in enumerate(trades) if t.get("status") != "OPEN"]
    excess = len(closed) - 100
    if excess > 0:
        remove = set(closed[:excess])
        trades[:] = [t for i, t in enumerate(trades) if i not in remove]



# -----------------------------
# Telegram
# -----------------------------
def telegram_send(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False, "Telegram secrets not configured."

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID.strip(),
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=15)
        try:
            data = r.json()
        except Exception:
            data = {}
        if r.ok and data.get("ok") is True:
            return True, "Telegram message sent successfully."
        desc = data.get("description", r.text[:240])
        return False, f"Telegram HTTP {r.status_code}: {desc}"
    except requests.RequestException as e:
        return False, f"Telegram network error: {e}"
    except Exception as e:
        return False, f"Telegram error: {e}"


def maybe_alert_order_block(ob_map, price, feed_live, enabled=True):
    """Send a Telegram notification once per newly detected OB zone per app session.

    This is a candle-derived heuristic, not exchange order-book data. It requires
    a fresh live WebSocket price and configured Telegram secrets. Streamlit must
    remain running for polling-based notifications; this is not a serverless push worker.
    """
    if not enabled or not feed_live or not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        if not hasattr(st, "session_state"):
            return
        seen = st.session_state.setdefault("_ob_telegram_seen", set())
        for side, label in (("bullish", "🟢 BULLISH ORDER BLOCK"), ("bearish", "🔴 BEARISH ORDER BLOCK")):
            zone = ob_map.get(side) if isinstance(ob_map, dict) else None
            if not isinstance(zone, dict):
                continue
            low = zone.get("low")
            high = zone.get("high")
            if low is None or high is None:
                continue
            low, high = float(low), float(high)
            if not (low > 0 and high >= low):
                continue
            # Rounded zone coordinates prevent duplicate alerts on every rerun.
            key = (side, round(low, 2), round(high, 2))
            if key in seen:
                continue
            message = (f"{label} — XAUUSD\n"
                       f"Zone: {low:.2f} – {high:.2f}\n"
                       f"Current price: {float(price):.2f}\n"
                       "Detected from 5M candle structure. Confirm BOS/retest and risk before any trade.")
            ok, _ = telegram_send(message)
            if ok:
                seen.add(key)
                # Bound session memory; retain recent zones only.
                if len(seen) > 200:
                    st.session_state["_ob_telegram_seen"] = set(list(seen)[-100:])
                    seen = st.session_state["_ob_telegram_seen"]
    except Exception:
        # Alert failures must not break the market dashboard.
        return


def telegram_diagnostics():
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False, "Missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID in Streamlit Secrets."
    try:
        me = requests.get(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe",
            timeout=15,
        )
        try:
            md = me.json()
        except Exception:
            md = {}
        if not (me.ok and md.get("ok") is True):
            return False, f"Bot token check failed: HTTP {me.status_code}: {md.get('description', me.text[:200])}"
        bot_name = md.get("result", {}).get("username", "unknown")
        return True, f"Bot connected: @{bot_name} • Chat ID configured: {TELEGRAM_CHAT_ID.strip()}"
    except Exception as e:
        return False, f"Telegram connection check failed: {e}"


def maybe_alert(signal, engine, price, levels, smart_flow_score=None):
    # Only high-quality signals. This is an alert, not an automatic order.
    # The final signal score is institutional; use Smart Flow score separately.
    alert_state = get_alert_state()
    if signal.get("signal") not in ("BUY", "SELL"):
        return
    try:
        flow_score = float(signal.get("score", 0) if smart_flow_score is None else smart_flow_score)
        institutional_score = float(signal.get("score", 0))
        early_score = float(engine.get("score", 0))
        px = float(price)
    except (TypeError, ValueError, OverflowError):
        return
    if not all(np.isfinite(v) for v in (flow_score, institutional_score, early_score, px)) or px <= 0:
        return
    if flow_score < 70 or early_score < 70:
        return
    if not levels:
        return

    key = f'{signal["signal"]}|{round(px,2)}|{engine.get("status", "")}|{int(flow_score)}|{int(early_score)}'
    now = time.time()
    # Avoid repeated alerts for the same state for 10 minutes.
    if alert_state["last_key"] == key and now - alert_state["last_sent"] < 600:
        return

    emoji = "🟢" if signal["signal"] == "BUY" else "🔴"
    text = (
        f"{emoji} SMART FLOW GOLD — {signal['signal']} A+\n\n"
        f"XAUUSD: {px:.2f}\n"
        f"Smart Flow score: {flow_score:.0f}/100\n"
        f"Institutional score: {institutional_score:.0f}/100\n"
        f"Early score: {early_score:.0f}/100\n"
        f"Live engine: {engine.get('status', 'N/A')}\n"
        f"1H: {engine.get('h1trend', 'N/A')} | 15M: {engine.get('m15trend', 'N/A')}\n\n"
        f"ENTRY: {levels['entry']:.2f}\n"
        f"SL: {levels['sl']:.2f}\n"
        f"TP1: {levels['tp1']:.2f}\n"
        f"TP2: {levels['tp2']:.2f}\n"
        f"TP3: {levels['tp3']:.2f}\n"
        f"TP4: {levels['tp4']:.2f}\n\n"
        f"⚠️ Alert only — not an automatic order."
    )
    ok, result = telegram_send(text)
    if ok:
        alert_state["last_key"] = key
        alert_state["last_sent"] = now
        alert_state["last_result"] = "Telegram alert sent"
    else:
        alert_state["last_result"] = result


# -----------------------------
# UI
# -----------------------------

# -----------------------------
# Professional terminal styling
# -----------------------------
PRO_CSS = """
<style>
:root { --bg:#071019; --panel:#0d1722; --panel2:#101d2a; --line:#203142; --text:#eef4f8; --muted:#8ea0b2; --green:#23d18b; --red:#ff5c67; --amber:#f4b740; --blue:#55a7ff; }
.stApp { background: radial-gradient(circle at 15% 0%, #112233 0%, #071019 42%, #050b11 100%); color:var(--text); }
.block-container { max-width: 1500px; padding-top: 1.2rem; padding-bottom: 2rem; }
section[data-testid="stSidebar"] { background:#071019; border-right:1px solid var(--line); }
.terminal-header { display:flex; justify-content:space-between; align-items:flex-end; gap:18px; padding:18px 20px; border:1px solid var(--line); border-radius:18px; background:linear-gradient(135deg,rgba(16,29,42,.98),rgba(7,16,25,.96)); box-shadow:0 14px 38px rgba(0,0,0,.22); margin-bottom:14px; }
.terminal-title { font-size:1.55rem; font-weight:800; letter-spacing:.2px; margin:0; }
.terminal-sub { color:var(--muted); font-size:.86rem; margin-top:5px; }
.live-pill { border:1px solid rgba(35,209,139,.45); color:#9ff1ce; background:rgba(35,209,139,.09); padding:7px 11px; border-radius:999px; font-weight:700; white-space:nowrap; }
.command { border:1px solid var(--line); border-radius:20px; background:linear-gradient(145deg,#0f1d2a,#09131d); padding:18px; box-shadow:0 16px 44px rgba(0,0,0,.24); margin:8px 0 16px; }
.command-grid { display:grid; grid-template-columns:1.25fr .8fr .8fr .8fr; gap:10px; margin-top:13px; }
.kpi { background:rgba(255,255,255,.025); border:1px solid #1d2d3c; border-radius:14px; padding:12px 14px; }
.kpi-label { color:var(--muted); font-size:.72rem; text-transform:uppercase; letter-spacing:.08em; }
.kpi-value { color:var(--text); font-size:1.18rem; font-weight:800; margin-top:4px; }
.kpi-note { color:var(--muted); font-size:.75rem; margin-top:3px; }
.action { margin-top:12px; border-radius:14px; padding:12px 14px; border:1px solid #2a3c4d; background:rgba(255,255,255,.025); }
.action b { color:#fff; }
.action-green { border-color:rgba(35,209,139,.45); background:rgba(35,209,139,.07); }
.action-red { border-color:rgba(255,92,103,.45); background:rgba(255,92,103,.07); }
.action-amber { border-color:rgba(244,183,64,.45); background:rgba(244,183,64,.06); }
.action-neutral { border-color:#2a3c4d; }
.path-grid { display:grid; grid-template-columns:1fr 1fr; gap:12px; margin-top:12px; }
.path { border-radius:15px; padding:14px; border:1px solid #243747; background:#0b1620; }
.path.buy { border-color:rgba(35,209,139,.35); }
.path.sell { border-color:rgba(255,92,103,.35); }
.path-head { font-weight:800; font-size:.9rem; }
.path-price { font-size:1.25rem; font-weight:850; margin:5px 0; }
.path-muted { color:var(--muted); font-size:.76rem; }
.section-head { margin-top:18px; margin-bottom:7px; font-size:1.02rem; font-weight:800; letter-spacing:.01em; }
.glance-wrap { margin:12px 0 16px; }
.glance-title { color:#8ea0b2; font-size:.72rem; font-weight:800; letter-spacing:.1em; text-transform:uppercase; margin:0 0 8px 2px; }
.glance-grid { display:grid; grid-template-columns:repeat(6,minmax(0,1fr)); gap:8px; }
.glance-card { min-width:0; border:1px solid #203142; border-radius:13px; padding:11px 12px; background:linear-gradient(145deg,#101d2a,#0a141e); }
.glance-label { color:#8ea0b2; font-size:.68rem; text-transform:uppercase; letter-spacing:.06em; }
.glance-value { color:#eef4f8; font-size:1rem; font-weight:850; margin-top:5px; overflow-wrap:anywhere; }
.glance-note { color:#8ea0b2; font-size:.7rem; margin-top:3px; }
.glance-good { border-color:rgba(35,209,139,.5); } .glance-warn { border-color:rgba(244,183,64,.5); } .glance-bad { border-color:rgba(255,92,103,.5); }
@media (max-width: 1000px) { .glance-grid { grid-template-columns:repeat(3,minmax(0,1fr)); } }
@media (max-width: 800px) { .terminal-header { align-items:flex-start; flex-direction:column; } .command-grid,.path-grid { grid-template-columns:1fr 1fr; } }
@media (max-width: 520px) { .block-container { padding-left: .65rem !important; padding-right: .65rem !important; } .glance-grid { grid-template-columns:repeat(2,minmax(0,1fr)); gap:7px; } .glance-card { padding:10px; } .glance-value { font-size:.94rem; } div[data-testid="stMetric"] { padding: .55rem .6rem; border: 1px solid rgba(130,160,180,.18); border-radius: 12px; background: rgba(255,255,255,.025); } div[data-testid="stMetricValue"] { font-size: 1.05rem; } }
</style>
"""
st.markdown(PRO_CSS, unsafe_allow_html=True)

st.markdown("""<div class="terminal-header"><div><div class="terminal-title">🟡 Smart Flow Gold <span style="color:#8ea0b2;font-weight:600">/ XAUUSD</span></div><div class="terminal-sub">One clear workflow: Check data → Read bias → Wait for confirmation → Plan risk</div></div><div class="live-pill">● LIVE MARKET MODE</div></div>""", unsafe_allow_html=True)
st.caption("XAUUSD only · App-native analysis · No TradingView chart · No automatic broker orders")

with st.sidebar:
    st.header("⚙️ Connection")
    if SIFTING_KEY:
        st.success("SiftingIO key: loaded from Secrets")
    else:
        entered_key = st.text_input("SiftingIO API key", type="password")
        SIFTING_KEY = normalize_api_key(entered_key)
        if entered_key and SIFTING_KEY != entered_key.strip():
            st.warning("Spaces or non-ASCII characters were removed from the API key. If connection still fails, paste the original plain ASCII token again.")

    st.markdown("**Telegram Secrets**")
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        st.success("🔔 Telegram A/A+ alerts: ON")
    else:
        st.warning("Telegram alerts are OFF")
        st.caption("Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Streamlit Secrets.")

    refresh = st.slider("Live refresh (seconds)", 5, 15, 5)
    ob_alerts_enabled = st.checkbox("🔔 Telegram Order Block alerts", value=True, help="Send one alert when a new bullish/bearish 5M Order Block zone is detected during a live app session.")
    view_mode = st.radio("Interface", ["⚡ Quick Trade", "🧭 Full Pro — All Tools", "📊 Strategy Lab — Backtest", "🧪 Time Machine + Risk Manager"], index=0)
    st.caption("Quick Trade = simple mobile decision screen. Full Pro = organized Overview, Setups, Liquidity, News and Scanner tabs plus diagnostics.")
    st.caption("Only the live dashboard refreshes; Backtest/Telegram sections stay stable.")
    st.caption("A/A+ alert rule: Smart Flow ≥70 AND Early Engine ≥70.")
    london_timezone = st.selectbox("London Kill Zone clock", ["America/New_York", "Europe/London", "Asia/Kolkata", "UTC"], index=0, help="Named timezones apply daylight-saving changes automatically.")
    st.info("🟢 Support / 🔴 Resistance are rule-based pivot estimates, not a private TradingView indicator copy.")
    st.caption("SiftingIO XAUUSD is an aggregated/reference price feed. Test before real-money use.")



# ============================================================
# London Kill Zone research module — staging additive feature
# Times are interpreted in the selected named timezone (default
# America/New_York), not hard-coded UTC offsets. All candle times
# are normalized to UTC before conversion. This is a research aid,
# not an automatic trade authorization.
# ============================================================
LONDON_TZ_DEFAULT = "America/New_York"

def _safe_zone(name):
    try:
        return ZoneInfo(str(name))
    except Exception:
        return ZoneInfo("UTC")

def _normalize_ohlc_time(df):
    """Return a copy with UTC-aware time and numeric OHLC columns."""
    if df is None or getattr(df, "empty", True):
        return pd.DataFrame(columns=["time", "open", "high", "low", "close"])
    x = df.copy()
    x.columns = [str(c).strip().lower() for c in x.columns]
    aliases = {"timestamp":"time", "datetime":"time", "date":"time",
               "o":"open", "h":"high", "l":"low", "c":"close"}
    x = x.rename(columns={c: aliases[c] for c in x.columns if c in aliases})
    if "time" not in x.columns:
        if isinstance(x.index, pd.DatetimeIndex):
            x["time"] = x.index
        else:
            raise ValueError("Historical candles need a time/timestamp column.")
    required = ["open", "high", "low", "close"]
    missing = [c for c in required if c not in x.columns]
    if missing:
        raise ValueError("Missing OHLC columns: " + ", ".join(missing))
    x["time"] = pd.to_datetime(x["time"], errors="coerce", utc=True)
    for c in required:
        x[c] = pd.to_numeric(x[c], errors="coerce")
    x = x.dropna(subset=["time"] + required)
    x = x[(x["high"] >= x["low"]) &
          (x["high"] >= x[["open", "close"]].max(axis=1)) &
          (x["low"] <= x[["open", "close"]].min(axis=1))]
    return x.sort_values("time").drop_duplicates("time").reset_index(drop=True)

def london_session_snapshot(df, now_utc=None, timezone_name=LONDON_TZ_DEFAULT):
    """Compute Asian range and live London-window state without look-ahead."""
    x = _normalize_ohlc_time(df)
    tz = _safe_zone(timezone_name)
    now = pd.Timestamp(now_utc or datetime.now(timezone.utc))
    if now.tzinfo is None:
        now = now.tz_localize("UTC")
    else:
        now = now.tz_convert("UTC")
    local_now = now.tz_convert(tz)
    result = {"timezone": timezone_name, "local_time": local_now,
              "session": "OUTSIDE LONDON WINDOW", "asian_high": None,
              "asian_low": None, "sweep": "NONE", "fvg": "NOT CONFIRMED",
              "bias": "WAIT", "note": "Insufficient timestamped OHLC data."}
    if x.empty:
        return result
    x["local_time"] = x["time"].dt.tz_convert(tz)
    day = local_now.date()
    # Source screenshot's model: Asian range from 19:00 previous evening
    # through 01:29 local time; Kill Zone is 02:00–05:00 local time.
    asia_start = pd.Timestamp(f"{day} 00:00", tz=tz) - pd.Timedelta(hours=5)
    # Filter using local clock minutes and current session date, allowing midnight crossover.
    lt = x["local_time"]
    mins = lt.dt.hour * 60 + lt.dt.minute
    is_asia = ((lt.dt.date == day) & (mins < 90)) | (
        (lt.dt.date == (local_now - pd.Timedelta(days=1)).date()) & (mins >= 19*60))
    asia = x[is_asia & (x["time"] <= now)]
    if not asia.empty:
        ah, al = float(asia["high"].max()), float(asia["low"].min())
        result["asian_high"], result["asian_low"] = ah, al
        result["note"] = "Asian range uses available candles from 19:00 through 01:29 local time."
        win = (lt.dt.date == day) & (mins >= 120) & (mins < 300) & (x["time"] <= now)
        kz = x[win].copy()
        if 120 <= local_now.hour*60 + local_now.minute < 300:
            result["session"] = "LONDON KILL ZONE"
        if not kz.empty:
            # Sweep means wick beyond Asian boundary and close back inside.
            low_sweep = (kz["low"] < al) & (kz["close"] > al)
            high_sweep = (kz["high"] > ah) & (kz["close"] < ah)
            if bool(low_sweep.any()) and not bool(high_sweep.any()):
                result["sweep"], result["bias"] = "ASIAN LOW SWEPT", "BULLISH WATCH"
            elif bool(high_sweep.any()) and not bool(low_sweep.any()):
                result["sweep"], result["bias"] = "ASIAN HIGH SWEPT", "BEARISH WATCH"
            elif bool(low_sweep.any()) and bool(high_sweep.any()):
                result["sweep"], result["bias"] = "BOTH SIDES SWEPT", "WAIT"
            # Three-candle fair value gap, checked only on completed bars.
            found = False
            for i in range(2, len(kz)):
                a, b, c = kz.iloc[i-2], kz.iloc[i-1], kz.iloc[i]
                if float(c["low"]) > float(a["high"]):
                    result["fvg"] = f"BULLISH FVG {float(a['high']):,.2f}–{float(c['low']):,.2f}"
                    found = True
                elif float(c["high"]) < float(a["low"]):
                    result["fvg"] = f"BEARISH FVG {float(c['high']):,.2f}–{float(a['low']):,.2f}"
                    found = True
            if not found:
                result["fvg"] = "NO 3-CANDLE FVG"
    return result

def london_killzone_backtest(raw, timezone_name=LONDON_TZ_DEFAULT,
                              rr_target=2.0, max_hold_bars=36):
    """Conservative one-trade-per-session research backtest on real supplied OHLC candles.
    Signal: Asian-range sweep/reclaim followed by directional 3-candle FVG.
    Entry: FVG midpoint touched after formation. Stop: sweep extreme.
    TP: fixed R multiple. If SL and TP hit in the same bar, stop wins conservatively.
    """
    x = _normalize_ohlc_time(raw)
    if len(x) < 100:
        raise ValueError(f"At least 100 valid timestamped candles required; found {len(x)}.")
    if not (1.0 <= float(rr_target) <= 5.0):
        raise ValueError("RR target must be between 1R and 5R.")
    tz = _safe_zone(timezone_name)
    x["local_time"] = x["time"].dt.tz_convert(tz)
    x["local_date"] = x["local_time"].dt.date
    mins = x["local_time"].dt.hour * 60 + x["local_time"].dt.minute
    trades = []
    # Process each local calendar date; Asian range is previous evening + current pre-market.
    for day in sorted(x["local_date"].unique()):
        prev_day = day - pd.Timedelta(days=1)
        asia_mask = (((x["local_date"] == prev_day) & (mins >= 19*60)) |
                     ((x["local_date"] == day) & (mins < 90)))
        asia = x[asia_mask]
        kz = x[(x["local_date"] == day) & (mins >= 120) & (mins < 300)]
        if len(asia) < 3 or len(kz) < 4:
            continue
        ah, al = float(asia.high.max()), float(asia.low.min())
        direction = None
        sweep_extreme = None
        fvg = None
        fvg_idx = None
        # Find first sweep/reclaim, then directional FVG after the sweep.
        for j in range(len(kz)):
            row = kz.iloc[j]
            if direction is None:
                if row.low < al and row.close > al:
                    direction, sweep_extreme = "BUY", float(row.low)
                elif row.high > ah and row.close < ah:
                    direction, sweep_extreme = "SELL", float(row.high)
            if direction and j >= 2:
                a, b, c = kz.iloc[j-2], kz.iloc[j-1], kz.iloc[j]
                if direction == "BUY" and float(c.low) > float(a.high):
                    fvg = (float(a.high), float(c.low)); fvg_idx = j; break
                if direction == "SELL" and float(c.high) < float(a.low):
                    fvg = (float(c.high), float(a.low)); fvg_idx = j; break
        if not direction or not fvg or fvg_idx is None:
            continue
        entry = (fvg[0] + fvg[1]) / 2.0
        stop = sweep_extreme
        risk = abs(entry - stop)
        if not np.isfinite(risk) or risk <= 0:
            continue
        target = entry + float(rr_target)*risk if direction == "BUY" else entry - float(rr_target)*risk
        outcome, exit_price, exit_time, exit_r = "OPEN_AT_WINDOW_END", float(kz.iloc[-1].close), kz.iloc[-1].time, 0.0
        # Only candles after the FVG candle can fill the midpoint entry.
        future = kz.iloc[fvg_idx+1:fvg_idx+1+int(max_hold_bars)]
        entered = False
        for _, bar in future.iterrows():
            if not entered:
                if float(bar.low) <= entry <= float(bar.high):
                    entered = True
                else:
                    continue
            sl_hit = float(bar.low) <= stop if direction == "BUY" else float(bar.high) >= stop
            tp_hit = float(bar.high) >= target if direction == "BUY" else float(bar.low) <= target
            if sl_hit:  # Conservative if both levels are inside one OHLC bar.
                outcome, exit_price, exit_time, exit_r = "LOSS", stop, bar.time, -1.0
                break
            if tp_hit:
                outcome, exit_price, exit_time, exit_r = "WIN", target, bar.time, float(rr_target)
                break
        if not entered:
            outcome, exit_r = "NO FILL", 0.0
        trades.append({"session_date": str(day), "side": direction,
                       "asian_high": ah, "asian_low": al,
                       "fvg_low": fvg[0], "fvg_high": fvg[1],
                       "entry": entry, "stop_loss": stop, "take_profit": target,
                       "outcome": outcome, "R": exit_r,
                       "entry_time_utc": str(kz.iloc[fvg_idx].time),
                       "exit_time_utc": str(exit_time)})
    t = pd.DataFrame(trades)
    if t.empty:
        metrics = {"trades": 0, "win_rate_pct": 0.0, "net_R": 0.0,
                   "profit_factor": 0.0, "max_drawdown_R": 0.0}
    else:
        rs = pd.to_numeric(t["R"], errors="coerce").fillna(0.0)
        gains, losses = float(rs[rs > 0].sum()), float(-rs[rs < 0].sum())
        eq = rs.cumsum()
        peak = eq.cummax().clip(lower=0)
        dd = peak - eq
        decided = t[t["outcome"].isin(["WIN", "LOSS"])]
        metrics = {"trades": int(len(t)),
                   "win_rate_pct": float((decided["outcome"] == "WIN").mean()*100) if len(decided) else 0.0,
                   "net_R": float(rs.sum()),
                   "profit_factor": gains/losses if losses else (999.0 if gains else 0.0),
                   "max_drawdown_R": float(dd.max()) if len(dd) else 0.0}
    return t, metrics

def render_london_killzone_live(m5e, feed_live, price, timezone_name=LONDON_TZ_DEFAULT):
    """Compact live panel; no alert is sent unless user explicitly enables one."""
    st.markdown("### 🕒 London Kill Zone · Asian Liquidity · FVG")
    try:
        snap = london_session_snapshot(m5e, timezone_name=timezone_name)
        local_now = snap["local_time"]
        st.caption(f"Clock: {local_now.strftime('%Y-%m-%d %H:%M:%S %Z')} · timezone {timezone_name} · UTC-aware DST conversion")
        c1, c2, c3 = st.columns(3)
        c1.metric("Session", snap["session"])
        c2.metric("Asian High", f"{snap['asian_high']:,.2f}" if snap["asian_high"] is not None else "Unavailable")
        c3.metric("Asian Low", f"{snap['asian_low']:,.2f}" if snap["asian_low"] is not None else "Unavailable")
        c4, c5, c6 = st.columns(3)
        c4.metric("Sweep", snap["sweep"])
        c5.metric("FVG", snap["fvg"])
        c6.metric("Bias", snap["bias"])
        st.caption(snap["note"])
        st.info("Research rule only: sweep + reclaim + FVG are not enough by themselves. Check feed freshness, structure, spread, risk, and the existing Master confirmation gate.")
        alert_on = st.checkbox("Enable session status Telegram alert (one per session state)", value=False, key="london_kz_alert_toggle")
        if alert_on and feed_live and TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            key = f"{snap['session']}|{snap['sweep']}|{snap['fvg']}|{snap['bias']}"
            seen = st.session_state.setdefault("_london_kz_alert_seen", set())
            if key not in seen and (snap["sweep"] != "NONE" or snap["fvg"].startswith(("BULLISH FVG", "BEARISH FVG"))):
                msg = (f"🟡 Smart Flow Gold — London KZ\\nXAUUSD {price:,.2f}\\n"
                       f"Session: {snap['session']}\\nAsian H/L: {snap['asian_high']} / {snap['asian_low']}\\n"
                       f"Sweep: {snap['sweep']}\\nFVG: {snap['fvg']}\\nBias: {snap['bias']}\\n"
                       "Research alert only — not an order.")
                ok, _ = telegram_send(msg)
                if ok:
                    seen.add(key)
                    st.session_state["_london_kz_alert_seen"] = set(list(seen)[-100:])
    except Exception as exc:
        st.warning(f"London session module unavailable: {type(exc).__name__}: {exc}")

def render_london_killzone_backtest_ui():
    st.subheader("🕒 London Kill Zone Backtest — CSV OHLC")
    st.caption("Upload genuine historical XAUUSD OHLC candles (5M preferred). This tests a simple, explicit sweep → FVG midpoint → SL/TP rule; it does not prove profitability or replace the Master engine.")
    tz_choice = st.selectbox("Session clock timezone", ["America/New_York", "Europe/London", "Asia/Kolkata", "UTC"],
                             index=0, key="london_bt_timezone")
    rr = st.slider("Take-profit target (R)", 1.0, 5.0, 2.0, 0.5, key="london_bt_rr")
    upload = st.file_uploader("Historical candles CSV (time, open, high, low, close)", type=["csv"], key="london_bt_csv")
    if upload is None:
        st.info("Upload historical candles to run the backtest. No sample/fake market data is generated.")
        return
    try:
        raw = pd.read_csv(upload)
        trades, metrics = london_killzone_backtest(raw, timezone_name=tz_choice, rr_target=rr)
        a, b, c, d, e = st.columns(5)
        a.metric("Trades", metrics["trades"])
        b.metric("Win rate", f"{metrics['win_rate_pct']:.1f}%")
        c.metric("Net R", f"{metrics['net_R']:+.2f}R")
        d.metric("Profit Factor", f"{metrics['profit_factor']:.2f}" if metrics["profit_factor"] < 999 else "∞")
        e.metric("Max DD", f"{metrics['max_drawdown_R']:.2f}R")
        st.caption("Assumptions: one trade per local session; Asia 19:00–01:29; Kill Zone 02:00–04:59; FVG midpoint fill; stop at sweep extreme; fixed R target; stop wins if SL and TP touch in same candle; no spread/commission/slippage included.")
        if not trades.empty:
            st.dataframe(trades, use_container_width=True)
            st.download_button("Download London KZ trade log CSV", trades.to_csv(index=False).encode("utf-8"),
                               "smart_flow_london_killzone_backtest.csv", "text/csv")
        else:
            st.warning("No qualifying trades found. Check candle timestamps, timezone, and sample coverage.")
    except Exception as exc:
        st.error(f"Backtest could not run: {type(exc).__name__}: {exc}")


# ---------------- Strategy Lab: CSV-based historical comparison ----------------
def _bt_prepare(raw):
    x=raw.copy(); x.columns=[str(c).strip().lower() for c in x.columns]
    aliases={"timestamp":"time","datetime":"time","date":"time","o":"open","h":"high","l":"low","c":"close","vol":"volume","tick_volume":"volume"}
    x=x.rename(columns={c:aliases[c] for c in x.columns if c in aliases})
    need=["open","high","low","close"]
    miss=[c for c in need if c not in x.columns]
    if miss: raise ValueError("CSV must contain Open, High, Low, Close. Missing: "+", ".join(miss))
    for c in need: x[c]=pd.to_numeric(x[c],errors="coerce")
    x=x.dropna(subset=need)
    if "time" in x.columns:
        x["time"]=pd.to_datetime(x["time"],errors="coerce",utc=True); x=x.dropna(subset=["time"]).sort_values("time")
    x=x.reset_index(drop=True)
    if len(x)<250: raise ValueError(f"At least 250 candles required; found {len(x)}")
    c,h,l=x.close,x.high,x.low
    x["ema20"]=c.ewm(span=20,adjust=False).mean(); x["ema50"]=c.ewm(span=50,adjust=False).mean(); x["ema200"]=c.ewm(span=200,adjust=False).mean()
    d=c.diff(); g=d.clip(lower=0).ewm(alpha=1/14,adjust=False).mean(); loss=(-d.clip(upper=0)).ewm(alpha=1/14,adjust=False).mean(); rs=g/loss.replace(0,float('nan'))
    x["rsi"]=100-100/(1+rs); prev=c.shift(1); tr=pd.concat([h-l,(h-prev).abs(),(l-prev).abs()],axis=1).max(axis=1); x["atr"]=tr.ewm(alpha=1/14,adjust=False).mean()
    x["dh"]=h.shift(1).rolling(20).max(); x["dl"]=l.shift(1).rolling(20).min(); x["sh"]=h.shift(1).rolling(10).max(); x["sl"]=l.shift(1).rolling(10).min()
    x["mid"]=c.rolling(20).mean(); sd=c.rolling(20).std(); x["bbh"]=x.mid+2*sd; x["bbl"]=x.mid-2*sd
    return x

def _bt_run(x,buy,sell,atr_mult,rr,cost,max_bars=30):
    if atr_mult <= 0 or rr <= 0 or cost < 0 or max_bars < 1:
        raise ValueError("ATR multiplier, reward R and max bars must be positive; cost cannot be negative.")
    trades=[]; i=201
    while i<len(x)-1:
        buy_now=bool(buy.iloc[i]); sell_now=bool(sell.iloc[i])
        if buy_now and sell_now:
            i+=1; continue  # ambiguous same-candle direction: skip rather than silently prefer BUY
        side=1 if buy_now else (-1 if sell_now else 0)
        if not side: i+=1; continue
        ei=i+1; entry=float(x.open.iloc[ei]); atr=x.atr.iloc[i]
        if not np.isfinite(entry) or pd.isna(atr) or not np.isfinite(float(atr)) or atr<=0: i+=1; continue
        risk=float(atr)*atr_mult
        if not np.isfinite(risk) or risk<=0: i+=1; continue
        stop=entry-side*risk; target=entry+side*risk*rr; end=min(len(x)-1,ei+max_bars); ex=None; outcome='TIME'; xi=end
        for j in range(ei,end+1):
            op,hi,lo=float(x.open.iloc[j]),float(x.high.iloc[j]),float(x.low.iloc[j])
            # If price gaps through a stop, model the worse opening fill. Target fills remain at target (conservative limit assumption).
            stop_gap = (side==1 and op<=stop) or (side==-1 and op>=stop)
            if stop_gap:
                ex=op; outcome='SL'; xi=j; break
            stop_hit=(side==1 and lo<=stop) or (side==-1 and hi>=stop)
            target_hit=(side==1 and hi>=target) or (side==-1 and lo<=target)
            if stop_hit: ex=stop; outcome='SL'; xi=j; break
            if target_hit: ex=target; outcome='TP'; xi=j; break
        if ex is None: ex=float(x.close.iloc[end])
        trades.append({"Entry time":str(x.time.iloc[ei]) if 'time' in x.columns else ei,"Side":"BUY" if side==1 else "SELL","Entry":entry,"Exit":ex,"Result":outcome,"R":(side*(ex-entry)-cost)/risk,"Bars held":xi-ei})
        i=xi+1
    t=pd.DataFrame(trades)
    if t.empty: return t,{"Trades":0,"Win rate %":0.0,"Profit factor":0.0,"Net R":0.0,"Max drawdown R":0.0,"Avg R":0.0}
    wins=t.loc[t.R>0,'R'].sum(); losses=-t.loc[t.R<0,'R'].sum()
    # Include initial equity (0R), so the first losing trade correctly counts as drawdown.
    eq=np.concatenate(([0.0], t.R.astype(float).cumsum().to_numpy()))
    dd=eq-np.maximum.accumulate(eq)
    return t,{"Trades":len(t),"Win rate %":round(float((t.R>0).mean()*100),2),"Profit factor":round(float(wins/losses),2) if losses else (float('inf') if wins else 0.0),"Net R":round(float(t.R.sum()),2),"Max drawdown R":round(float(dd.min()),2),"Avg R":round(float(t.R.mean()),3)}

def _render_bt_lab():
    st.title("📊 Strategy Lab — XAUUSD Backtest")
    st.markdown("CSV upload ആവശ്യമില്ല. SiftingIO-യിൽ നിന്ന് 5M historical candles സ്വയം എടുക്കാൻ ശ്രമിക്കും. API key-ന് history access വേണം.")
    a,b,c=st.columns(3)
    days=a.selectbox("History period", [7, 14, 30], index=2, format_func=lambda d: f"Last {d} days")
    am=b.slider('Stop distance (ATR ×)',0.5,3.0,1.5,0.25)
    rr=c.slider('Target (R)',1.0,4.0,2.0,0.5)
    cost=st.number_input('Round-trip cost (price units)',min_value=0.0,value=0.0,step=0.1)
    st.caption("Closed-candle signal → next candle open entry; one position at a time; if SL and TP occur in the same candle, SL is counted first. Costs are deducted in price units; set a realistic value for your feed/broker.")
    if not SIFTING_KEY:
        st.error("SIFTINGIO_API_KEY ലഭ്യമല്ല. Streamlit Cloud → Manage app → Settings → Secrets-ൽ ശരിയായ key ചേർത്ത് reboot ചെയ്യുക. Key ഇവിടെ paste ചെയ്യരുത്.")
        return
    if st.button("⬇️ Download candles & run backtest", type="primary"):
        try:
            with st.spinner(f"SiftingIO-യിൽ നിന്ന് കഴിഞ്ഞ {days} ദിവസത്തെ XAUUSD 5M candles എടുക്കുന്നു…"):
                raw=get_backtest_5m(SIFTING_KEY, days=int(days))
            if raw is None or raw.empty:
                st.error("Historical candles ലഭിച്ചില്ല. SiftingIO API key/plan/endpoint permission പരിശോധിക്കുക; ഡാറ്റയില്ലാതെ ഫലം സൃഷ്ടിക്കില്ല.")
                return
            if len(raw) < 250:
                st.warning(f"മാത്രം {len(raw)} candles ലഭിച്ചു. കുറഞ്ഞത് 250 വേണം; അതിനാൽ ഫലം വിശ്വസനീയമല്ല.")
                return
            x=_bt_prepare(raw); close=x.close
            sig={
              'EMA Trend Pullback':(((close>x.ema50)&(x.ema20>x.ema50)&(x.low<=x.ema20)&(close>x.open)),((close<x.ema50)&(x.ema20<x.ema50)&(x.high>=x.ema20)&(close<x.open))),
              '20-bar Breakout':((close>x.dh),(close<x.dl)),
              'Liquidity Sweep Reversal':(((x.low<x.sl)&(close>x.sl)&(close>x.open)),((x.high>x.sh)&(close<x.sh)&(close<x.open))),
              'Hybrid Trend + Breakout':(((close>x.ema200)&(x.ema20>x.ema50)&(close>x.dh)&x.rsi.between(50,72)),((close<x.ema200)&(x.ema20<x.ema50)&(close<x.dl)&x.rsi.between(28,50))),
              'RSI + Bollinger Mean Reversion':(((close<x.bbl)&(x.rsi<30)),((close>x.bbh)&(x.rsi>70))) }
            rows=[]; logs=[]
            for name,(bu,se) in sig.items():
                t,m=_bt_run(x,bu.fillna(False),se.fillna(False),am,rr,cost)
                rows.append({'Strategy':name,**m})
                if not t.empty: t.insert(0,'Strategy',name); logs.append(t)
            res=pd.DataFrame(rows).sort_values(['Net R','Profit factor'],ascending=False)
            st.success(f"Historical candles loaded: {len(x)} · {x.time.min()} → {x.time.max()}")
            st.subheader('Strategy comparison')
            st.dataframe(res,use_container_width=True,hide_index=True)
            good=res[(res.Trades>=30)&(res['Profit factor']>1)&(res['Net R']>0)]
            if good.empty:
                st.warning('ഈ sample-ൽ കുറഞ്ഞത് 30 trades, PF > 1, Net R > 0 എന്നീ മാനദണ്ഡങ്ങൾ ഒരുമിച്ച് നേടിയ strategy ഇല്ല. ഒന്നിനെയും best എന്ന് പ്രഖ്യാപിക്കുന്നില്ല.')
            else:
                best=good.sort_values(['Net R','Max drawdown R'],ascending=[False,False]).iloc[0]
                st.success(f"ഈ sample-ൽ മുൻപന്തിയിൽ: {best['Strategy']} · {int(best.Trades)} trades · win {best['Win rate %']}% · PF {best['Profit factor']} · Net {best['Net R']}R")
            st.warning('ഇത് initial in-sample comparison മാത്രമാണ്; SiftingIO feed/broker spread, slippage, out-of-sample/walk-forward test, BUY/SELL വേർതിരിച്ചുള്ള ഫലം എന്നിവ പരിശോധിക്കാതെ real money ഉപയോഗിക്കരുത്. Backtest ഭാവി ലാഭം ഉറപ്പാക്കില്ല.')
            if logs:
                with st.expander('Trade-by-trade log'):
                    all_t=pd.concat(logs,ignore_index=True)
                    st.dataframe(all_t,use_container_width=True,hide_index=True)
                    st.download_button('Download trade log CSV',all_t.to_csv(index=False).encode(), 'smart_flow_backtest_trades.csv','text/csv')
        except Exception as e:
            st.error(f"Automatic backtest failed: {e}")
            st.info("401 ആണെങ്കിൽ key/token അല്ലെങ്കിൽ account permission പ്രശ്നമാണ്; 403 ആണെങ്കിൽ endpoint permission പരിശോധിക്കുക. API key chat-ൽ അയക്കരുത്.")


def _render_time_machine_risk_lab():
    st.title("🧪 Smart Flow — Time Machine + Risk Manager")
    st.caption("Research-only tool. It does not send broker orders and does not change the live signal engine.")
    st.subheader("1) Risk Manager — position sizing")
    st.warning("Risk is configurable up to 5% per trade in this staging build. Higher risk can cause rapid losses; this tool does not place broker orders.")
    c1,c2,c3=st.columns(3)
    equity=c1.number_input("Account equity",min_value=1.0,value=1000.0,step=100.0,help="Account currency units; use the currency of your account.")
    risk_pct=c2.number_input("Risk per trade (%)",min_value=0.1,max_value=5.0,value=0.5,step=0.1)
    stop_distance=c3.number_input("Entry-to-SL distance (price units)",min_value=0.0001,value=5.0,step=0.5,format="%.4f")
    contract_size=st.number_input("Units per 1.0 lot (broker-specific)",min_value=0.0001,value=100.0,step=1.0,help="Verify your broker's XAUUSD contract specification. This default is only an example.")
    risk_cash=equity*risk_pct/100.0
    raw_lots=risk_cash/(stop_distance*contract_size)
    st.metric("Cash risk limit",f"{risk_cash:,.2f}")
    st.metric("Estimated position size",f"{raw_lots:.4f} lots",help="Approximation before commission, spread, slippage and broker lot-step/minimum rules.")
    st.warning("Verify contract size, account currency conversion, lot minimum/step, spread and slippage with your broker before using this number. A displayed size is not an order instruction.")

    st.divider()
    st.subheader("2) Time Machine — historical outcomes + Monte Carlo")
    st.write("SiftingIO-യിൽ നിന്ന് ലഭിക്കുന്ന 5-minute candles ഉപയോഗിച്ച് Strategy Lab-ലെ വേർതിരിച്ച historical signal rules പരിശോധിക്കുന്നു. തുടർന്ന് trade R outcomes-നെ bootstrap ചെയ്ത് hypothetical equity paths കാണിക്കുന്നു. ഇത് 10,000 intraday OHLC path-കളുടെ candle-by-candle replay അല്ല; Monte Carlo result ഒരു price forecast അല്ല.")
    a,b,c=st.columns(3)
    days=a.selectbox("Historical period",[7,14,30],index=2,key="tm_days")
    atr_mult=b.slider("Stop distance (ATR ×)",0.5,3.0,1.5,0.25,key="tm_atr")
    rr=c.slider("Target (R)",1.0,4.0,2.0,0.5,key="tm_rr")
    cost=st.number_input("Round-trip cost (price units)",min_value=0.0,value=0.0,step=0.1,key="tm_cost")
    strat=st.selectbox("Signal rule",["EMA Trend Pullback","20-bar Breakout","Liquidity Sweep Reversal","Hybrid Trend + Breakout","RSI + Bollinger Mean Reversion"],key="tm_strategy")
    sim_count=st.selectbox("Monte Carlo paths",[1000,5000,10000],index=2,key="tm_sim_count")
    sim_trades=st.slider("Trades per simulated path",10,200,50,10,key="tm_sim_trades")
    if not SIFTING_KEY:
        st.info("Historical run ചെയ്യാൻ Streamlit Secrets-ൽ SIFTINGIO_API_KEY വേണം. മുകളിലെ Risk Manager അതില്ലാതെയും ഉപയോഗിക്കാം.")
        return
    if st.button("Run Time Machine",type="primary",key="tm_run"):
        try:
            with st.spinner(f"കഴിഞ്ഞ {days} ദിവസത്തെ 5M candles എടുക്കുന്നു…"):
                raw=get_backtest_5m(SIFTING_KEY,days=int(days))
            if raw is None or raw.empty:
                st.error("Historical candles ലഭിച്ചില്ല. ഡാറ്റയില്ലാതെ ഫലം സൃഷ്ടിക്കില്ല.")
                return
            x=_bt_prepare(raw)
            close=x.close
            sig={
              'EMA Trend Pullback':(((close>x.ema50)&(x.ema20>x.ema50)&(x.low<=x.ema20)&(close>x.open)),((close<x.ema50)&(x.ema20<x.ema50)&(x.high>=x.ema20)&(close<x.open))),
              '20-bar Breakout':((close>x.dh),(close<x.dl)),
              'Liquidity Sweep Reversal':(((x.low<x.sl)&(close>x.sl)&(close>x.open)),((x.high>x.sh)&(close<x.sh)&(close<x.open))),
              'Hybrid Trend + Breakout':(((close>x.ema200)&(x.ema20>x.ema50)&(close>x.dh)&x.rsi.between(50,72)),((close<x.ema200)&(x.ema20<x.ema50)&(close<x.dl)&x.rsi.between(28,50))),
              'RSI + Bollinger Mean Reversion':(((close<x.bbl)&(x.rsi<30)),((close>x.bbh)&(x.rsi>70))) }
            bu,se=sig[strat]
            trades,metrics=_bt_run(x,bu.fillna(False),se.fillna(False),float(atr_mult),float(rr),float(cost))
            st.subheader("Historical sample")
            st.write(f"Candles: {len(x)} · Strategy: {strat}")
            st.dataframe(pd.DataFrame([metrics]),use_container_width=True,hide_index=True)
            if trades.empty or len(trades)<5:
                st.warning(f"ഈ sample-ൽ {len(trades)} trades മാത്രമാണ്. Monte Carlo കണക്കുകൾ വിശ്വസനീയമല്ല; കൂടുതൽ historical data ഉപയോഗിക്കുക.")
                if not trades.empty: st.dataframe(trades,use_container_width=True,hide_index=True)
                return
            # Bootstrap observed R-multiples; this is a scenario distribution, not a price forecast.
            rng=np.random.default_rng(42)
            outcomes=trades["R"].astype(float).to_numpy()
            sampled=rng.choice(outcomes,size=(int(sim_count),int(sim_trades)),replace=True)
            risk_fraction=float(risk_pct)/100.0
            # Compound the same fixed fractional risk for each sampled trade; cap catastrophic negative equity.
            returns=np.clip(sampled*risk_fraction,-0.99,10.0)
            paths=np.cumprod(1.0+returns,axis=1)
            ending=paths[:,-1]
            peak=np.maximum.accumulate(np.concatenate([np.ones((len(paths),1)),paths],axis=1),axis=1)[:,1:]
            drawdown=paths/np.maximum(peak,1e-12)-1.0
            st.subheader(f"Monte Carlo: {sim_count:,} paths × {sim_trades} trades")
            mc=pd.DataFrame([{"Metric":"Ending equity P5", "Value":round(equity*float(np.percentile(ending,5)),2)},
                             {"Metric":"Ending equity median", "Value":round(equity*float(np.percentile(ending,50)),2)},
                             {"Metric":"Ending equity P95", "Value":round(equity*float(np.percentile(ending,95)),2)},
                             {"Metric":"Chance of ending below start", "Value":round(float((ending<1).mean()*100),2)},
                             {"Metric":"Median worst drawdown (%)", "Value":round(float(np.median(drawdown.min(axis=1))*100),2)},
                             {"Metric":"P95 worst drawdown (%)", "Value":round(float(np.percentile(drawdown.min(axis=1),5)*100),2)}])
            st.dataframe(mc,use_container_width=True,hide_index=True)
            st.line_chart(pd.DataFrame({"P5":np.percentile(paths,5,axis=0),"Median":np.percentile(paths,50,axis=0),"P95":np.percentile(paths,95,axis=0)}))
            st.dataframe(trades,use_container_width=True,hide_index=True)
            st.download_button("Download Time Machine trades CSV",trades.to_csv(index=False).encode(),"smart_flow_time_machine_trades.csv","text/csv")
            st.warning("Monte Carlo assumes the historical trade outcomes are representative and independently resampleable. It does not predict future prices or guarantee returns. Results can be distorted by small samples, changing market regimes, spread, slippage and feed differences. Do out-of-sample/walk-forward tests before considering real-money use.")
        except Exception as e:
            st.error(f"Time Machine failed: {e}")
            st.info("Check SiftingIO historical access and Secrets. Never paste API keys into chat or source code.")


if view_mode == "🧪 Time Machine + Risk Manager":
    _render_time_machine_risk_lab()
    st.stop()

if view_mode == "📊 Strategy Lab — Backtest":
    _render_bt_lab()
    st.stop()

if not SIFTING_KEY:
    st.error("Add SIFTINGIO_API_KEY in Streamlit Secrets first.")
    st.stop()

def _trend_label(df, min_bars=55):
    """EMA-based trend label; never invent a direction when data is insufficient."""
    if df is None or df.empty or "close" not in df.columns or len(df) < min_bars:
        return "INSUFFICIENT DATA"
    close = pd.to_numeric(df["close"], errors="coerce").dropna()
    if len(close) < min_bars:
        return "INSUFFICIENT DATA"
    e20 = close.ewm(span=20, adjust=False).mean().iloc[-1]
    e50 = close.ewm(span=50, adjust=False).mean().iloc[-1]
    last = float(close.iloc[-1])
    if last > e20 and e20 > e50:
        return "BULLISH"
    if last < e20 and e20 < e50:
        return "BEARISH"
    return "NEUTRAL"


def _resample_ohlcv(df, rule):
    if df is None or df.empty or "time" not in df.columns:
        return pd.DataFrame()
    x = df.copy()
    x["time"] = pd.to_datetime(x["time"], utc=True, errors="coerce")
    x = x.dropna(subset=["time"]).set_index("time").sort_index()
    agg = {"open": "first", "high": "max", "low": "min", "close": "last"}
    if "volume" in x.columns:
        agg["volume"] = "sum"
    out = x.resample(rule).agg(agg).dropna(subset=["open", "high", "low", "close"]).reset_index()
    return out


def render_multitimeframe_target_panel(h1, daily, price, levels, feed_live, atr_value):
    """App-only multi-timeframe trend table and target ladder inspired by user's screenshot."""
    h4 = _resample_ohlcv(h1, "4h") if h1 is not None and not h1.empty else pd.DataFrame()
    weekly = _resample_ohlcv(daily, "W") if daily is not None and not daily.empty else pd.DataFrame()
    monthly = _resample_ohlcv(daily, "MS") if daily is not None and not daily.empty else pd.DataFrame()
    with st.expander("📊 Multi-Timeframe Trend + Target Ladder", expanded=True):
        st.caption("EMA 20/50 trend status from available candles. This is a dashboard aid, not a guaranteed signal.")
        labels = [
            ("Trend (60m)", _trend_label(h1)),
            ("Trend (240m)", _trend_label(h4)),
            ("Trend (D)", _trend_label(daily)),
            ("Trend (W)", _trend_label(weekly)),
            ("Trend (M)", _trend_label(monthly)),
        ]
        cols = st.columns(5)
        for col, (name, trend) in zip(cols, labels):
            if trend == "BULLISH":
                col.metric(name, "🟢 Bullish")
            elif trend == "BEARISH":
                col.metric(name, "🔴 Bearish")
            elif trend == "NEUTRAL":
                col.metric(name, "🟡 Neutral")
            else:
                col.metric(name, "— Insufficient")
        st.caption("A timeframe may show Insufficient when enough real candles are unavailable; the app does not fabricate missing trend data.")
        a, b, c = st.columns(3)
        a.metric("ATR Filter", "OFF")
        b.metric("ATR(14)", f"{atr_value:,.2f}" if atr_value is not None else "Unavailable")
        c.metric("Unrealized PnL", "N/A — no broker position")
        st.caption("ATR is displayed for context; it is not currently used as an extra signal gate. This app does not place broker orders or know your external open position, so unrealized PnL is not estimated.")
        st.markdown("**Entry / Stop / Target ladder**")
        if not feed_live:
            st.warning("Live WebSocket is not fresh. Trade levels are hidden until the live signal engine is active.")
        elif levels and levels.get("entry") is not None:
            side = final_side = "BUY" if levels.get("sl", price) < levels.get("entry", price) else "SELL"
            st.write(f"**{side} plan** • Entry: `{levels['entry']:,.2f}` • Stop: `{levels['sl']:,.2f}`")
            target_cols = st.columns(3)
            for idx, key in enumerate(("tp1", "tp2", "tp3", "tp4", "tp5", "tp6")):
                if key in levels:
                    target_cols[idx % 3].metric(f"Target {idx+1}", f"{levels[key]:,.2f}")
            risk = abs(float(levels["entry"]) - float(levels["sl"]))
            if risk > 0:
                rr = abs(float(levels.get("tp2", levels["entry"])) - float(levels["entry"])) / risk
                st.caption(f"TP2 risk/reward: 1:{rr:.2f}. Levels are ATR-based planning references, not broker orders.")
        else:
            st.info("No active confirmed BUY/SELL levels. Wait for the existing Smart Flow confirmation rules.")


# -----------------------------
# Inside Bar + Gold A+ strategy panels (app-only)
# -----------------------------
def inside_bar_analysis(df, live_candle=True):
    """Detect a closed-candle inside-bar setup and its close-based confirmation.

    When the latest row is the live WebSocket candle, exclude it so the result
    uses completed candles only. This is a pattern detector, not a trade signal.
    """
    out = {"status": "INSUFFICIENT DATA", "mother_high": None, "mother_low": None,
           "inside_high": None, "inside_low": None, "close": None,
           "note": "Need at least three completed candles."}
    if df is None or df.empty or not {"high", "low", "close"}.issubset(df.columns):
        return out
    x = df.iloc[:-1].copy() if live_candle and len(df) else df.copy()
    x = x.dropna(subset=["high", "low", "close"]).reset_index(drop=True)
    if len(x) < 3:
        return out
    mother, inside, confirm = x.iloc[-3], x.iloc[-2], x.iloc[-1]
    mh, ml = float(mother.high), float(mother.low)
    ih, il, cc = float(inside.high), float(inside.low), float(confirm.close)
    out.update({"mother_high": mh, "mother_low": ml, "inside_high": ih,
                "inside_low": il, "close": cc})
    if ih > mh or il < ml:
        out["status"] = "NO INSIDE BAR"
        out["note"] = "The middle candle is not fully inside the mother candle range."
    elif cc > ih:
        out["status"] = "BULLISH BREAKOUT CLOSE"
        out["note"] = "Completed candle closed above the inside bar high; wait for context and risk confirmation."
    elif cc < il:
        out["status"] = "BEARISH BREAKOUT CLOSE"
        out["note"] = "Completed candle closed below the inside bar low; wait for context and risk confirmation."
    else:
        out["status"] = "INSIDE BAR VALID — WAIT"
        out["note"] = "Inside-bar structure is valid, but no close-based breakout has confirmed."
    return out


def render_inside_bar_panel(df, live_candle=True):
    state = inside_bar_analysis(df, live_candle=live_candle)
    with st.expander("🕯️ Inside Bar — Close Confirmation", expanded=False):
        if state["mother_high"] is None:
            st.info(state["note"])
            return state
        st.write(f"Mother candle range: **{state['mother_low']:,.2f} – {state['mother_high']:,.2f}**")
        st.write(f"Inside candle range: **{state['inside_low']:,.2f} – {state['inside_high']:,.2f}**")
        if state["status"] == "BULLISH BREAKOUT CLOSE":
            st.success("🟢 " + state["status"])
        elif state["status"] == "BEARISH BREAKOUT CLOSE":
            st.error("🔴 " + state["status"])
        elif state["status"] == "INSIDE BAR VALID — WAIT":
            st.warning("🟡 " + state["status"])
        else:
            st.info(state["status"])
        st.caption(state["note"] + " Signal is based on completed 5M candles; a breakout by itself does not authorize a trade.")
    return state


def render_gold_aplus_panel(h1e, m15e, m5e, ob_map, ob_confirmation, trigger_map,
                            final, levels, feed_live, price, inside_state):
    """Show a transparent Gold A+ checklist using the app's existing data and gates."""
    with st.expander("🏅 Gold A+ Strategy — 1H Structure + 5M OB + Liquidity", expanded=False):
        h1_trend = trend_state(h1e) if h1e is not None and not h1e.empty else "UNAVAILABLE"
        m15_trend = trend_state(m15e) if m15e is not None and not m15e.empty else "UNAVAILABLE"
        st.write(f"**1H trend:** {h1_trend}  ·  **15M trend:** {m15_trend}")
        if m5e is not None and not m5e.empty:
            stc = structure(m5e)
            st.write(f"**5M structure:** BOS BUY {'YES' if stc.get('bos_bull') else 'NO'} · BOS SELL {'YES' if stc.get('bos_bear') else 'NO'} · CHoCH {'YES' if stc.get('choch') else 'NO'}")
        else:
            st.write("**5M structure:** unavailable")
        bull = (ob_map or {}).get("bullish")
        bear = (ob_map or {}).get("bearish")
        st.write("**Bullish 5M OB:** " + (f"{bull['low']:,.2f} – {bull['high']:,.2f}" if bull else "No valid recent zone"))
        st.write("**Bearish 5M OB:** " + (f"{bear['low']:,.2f} – {bear['high']:,.2f}" if bear else "No valid recent zone"))
        st.write(f"**OB + sweep + BOS gate:** BUY {'PASS' if (ob_confirmation or {}).get('buy_ok') else 'WAIT'} · SELL {'PASS' if (ob_confirmation or {}).get('sell_ok') else 'WAIT'}")
        if inside_state:
            st.write(f"**Inside Bar:** {inside_state.get('status', 'UNAVAILABLE')}")
        st.write(f"**Next liquidity / trigger map:** BUY break {trigger_map['buy_break']:,.2f} · SELL break {trigger_map['sell_break']:,.2f}")
        if not feed_live:
            st.warning("A+ decision blocked: fresh WebSocket tick is unavailable. Historical price does not authorize a trade.")
        elif final and final.get("signal") in ("BUY", "SELL") and levels:
            st.success(f"App signal: {final['signal']} · Entry {levels.get('entry', price):,.2f} · SL {levels.get('sl', price):,.2f}")
        else:
            st.info("NO A+ ENTRY — wait for the existing Smart Flow score + OB + sweep + BOS confirmation. No automatic broker order is placed.")
        st.caption("SMT needs a real comparison-symbol feed and is not inferred here. A+ means rule confluence, not a guaranteed win rate.")



def crt_power_of_three_analysis(df, live_candle=True):
    """Conservative candle-based CRT/Power-of-3 context; not institutional order-flow data."""
    out = {"status": "WAIT — INSUFFICIENT DATA", "direction": "NEUTRAL",
           "range_high": None, "range_low": None, "sweep": "NONE", "note": "Need more completed 5M candles."}
    if df is None or df.empty or not {"open", "high", "low", "close"}.issubset(df.columns):
        return out
    x = df.iloc[:-1].copy() if live_candle and len(df) else df.copy()
    x = x.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    if len(x) < 8:
        return out
    # Treat the prior completed candle as the CRT range and the latest completed candle as the sweep/close test.
    prior = x.iloc[-2]
    last = x.iloc[-1]
    rh, rl = float(prior.high), float(prior.low)
    c, h, l = float(last.close), float(last.high), float(last.low)
    out.update({"range_high": rh, "range_low": rl})
    swept_low = l < rl and c > rl
    swept_high = h > rh and c < rh
    if swept_low and not swept_high:
        out.update({"status": "BULLISH SWEEP — WAIT CONFIRMATION", "direction": "BULLISH", "sweep": "SELL-SIDE LIQUIDITY"})
        out["note"] = "Price swept below the prior candle low and closed back above it. Wait for structure/displacement confirmation."
    elif swept_high and not swept_low:
        out.update({"status": "BEARISH SWEEP — WAIT CONFIRMATION", "direction": "BEARISH", "sweep": "BUY-SIDE LIQUIDITY"})
        out["note"] = "Price swept above the prior candle high and closed back below it. Wait for structure/displacement confirmation."
    elif c > rh:
        out.update({"status": "RANGE BREAK UP — WAIT RETEST", "direction": "BULLISH", "sweep": "NO RECLAIM SWEEP"})
        out["note"] = "Completed candle closed above the reference range. Avoid chasing; wait for retest and higher-timeframe alignment."
    elif c < rl:
        out.update({"status": "RANGE BREAK DOWN — WAIT RETEST", "direction": "BEARISH", "sweep": "NO RECLAIM SWEEP"})
        out["note"] = "Completed candle closed below the reference range. Avoid chasing; wait for retest and higher-timeframe alignment."
    else:
        out.update({"status": "ACCUMULATION / RANGE — WAIT", "sweep": "NONE"})
        out["note"] = "No confirmed sweep/reclaim or close outside the reference range. This is only a candle-based approximation of Power of 3."
    return out


def render_crt_power_of_three_panel(h1e, m15e, m5e, price, feed_live):
    with st.expander("🧭 CRT / Power of 3 — Sweep + Close Confirmation", expanded=False):
        h1trend = trend_state(h1e) if h1e is not None and not h1e.empty else "UNAVAILABLE"
        m15trend = trend_state(m15e) if m15e is not None and not m15e.empty else "UNAVAILABLE"
        st.write(f"**Higher-timeframe context:** 1H **{h1trend}** · 15M **{m15trend}**")
        state = crt_power_of_three_analysis(m5e, live_candle=feed_live)
        if state["range_high"] is None:
            st.info(state["note"])
            return state
        st.write(f"**Reference candle range:** {state['range_low']:,.2f} – {state['range_high']:,.2f}")
        st.write(f"**Sweep:** {state['sweep']}")
        st.write(f"**Current price:** {price:,.2f}")
        if state["direction"] == "BULLISH":
            st.success("🟢 " + state["status"])
        elif state["direction"] == "BEARISH":
            st.error("🔴 " + state["status"])
        else:
            st.warning("🟡 " + state["status"])
        st.caption(state["note"])
        st.info("Power of 3 phases (accumulation → manipulation → distribution) cannot be proven from one candle alone. This panel is a heuristic, not a standalone entry signal.")
        if not feed_live:
            st.warning("Live WebSocket tick is not fresh; treat this as historical context only. No trade authorization.")
        elif state["direction"] == "BULLISH" and h1trend == "BULLISH" and m15trend != "BEARISH":
            st.success("Context aligns bullishly, but still require the app's existing BOS + Order Block + sweep confirmation gate.")
        elif state["direction"] == "BEARISH" and h1trend == "BEARISH" and m15trend != "BULLISH":
            st.error("Context aligns bearishly, but still require the app's existing BOS + Order Block + sweep confirmation gate.")
        else:
            st.caption("Timeframes are not aligned enough for an A+ setup. Wait for confirmation.")
        return state

def _liquidity_levels(df, lookback=120):
    """Return recent swing liquidity pools from real OHLC candles only."""
    if df is None or df.empty or not {"high", "low", "close"}.issubset(df.columns) or len(df) < 12:
        return [], []
    x = df.tail(lookback).copy().reset_index(drop=True)
    for c in ("high", "low", "close"):
        x[c] = pd.to_numeric(x[c], errors="coerce")
    highs, lows = [], []
    for i in range(2, len(x)-2):
        if pd.notna(x.high.iloc[i]) and x.high.iloc[i] >= x.high.iloc[i-2:i+3].max():
            highs.append(float(x.high.iloc[i]))
        if pd.notna(x.low.iloc[i]) and x.low.iloc[i] <= x.low.iloc[i-2:i+3].min():
            lows.append(float(x.low.iloc[i]))
    # Cluster nearby swing points into approximate equal-high/low pools.
    def cluster(vals):
        vals = sorted(vals)
        groups = []
        for v in vals:
            if groups and abs(v - float(np.mean(groups[-1]))) <= max(0.5, abs(v)*0.00025):
                groups[-1].append(v)
            else:
                groups.append([v])
        return sorted([(float(np.mean(g)), len(g)) for g in groups], key=lambda z: z[1], reverse=True)[:4]
    return cluster(highs), cluster(lows)


def render_liquidity_risk_panel(m5e, price, feed_live):
    """App-only liquidity pools and manual risk/reward calculator."""
    with st.expander("💧 Liquidity Map + Risk/Reward Planner", expanded=False):
        st.caption("Swing highs/lows are estimated from candles available to this app; they are zones of interest, not guaranteed stop clusters or trade signals.")
        highs, lows = _liquidity_levels(m5e)
        left, right = st.columns(2)
        with left:
            st.markdown("**Buy-side liquidity · swing highs**")
            if highs:
                for level, touches in highs:
                    st.write(f"• `{level:,.2f}` · {touches} nearby swing point(s)")
            else:
                st.info("Insufficient candles to estimate swing-high liquidity.")
        with right:
            st.markdown("**Sell-side liquidity · swing lows**")
            if lows:
                for level, touches in lows:
                    st.write(f"• `{level:,.2f}` · {touches} nearby swing point(s)")
            else:
                st.info("Insufficient candles to estimate swing-low liquidity.")
        if not feed_live:
            st.warning("Live feed is not fresh. Treat the displayed price as unavailable for live execution.")
        st.divider()
        st.markdown("**Manual trade planner**")
        side = st.selectbox("Direction", ["BUY / LONG", "SELL / SHORT"], key="sf_rr_side")
        default_entry = float(price) if price is not None and np.isfinite(price) else 0.0
        e1, e2, e3 = st.columns(3)
        entry = e1.number_input("Entry price", min_value=0.0, value=default_entry, step=0.1, format="%.2f", key="sf_rr_entry")
        stop = e2.number_input("Stop-loss price", min_value=0.0, value=max(0.0, default_entry-5) if side == "BUY / LONG" else default_entry+5, step=0.1, format="%.2f", key="sf_rr_stop")
        target = e3.number_input("Target price", min_value=0.0, value=default_entry+10 if side == "BUY / LONG" else max(0.0, default_entry-10), step=0.1, format="%.2f", key="sf_rr_target")
        valid = entry > 0 and stop > 0 and target > 0 and ((side == "BUY / LONG" and stop < entry < target) or (side == "SELL / SHORT" and target < entry < stop))
        if valid:
            risk = abs(entry-stop)
            reward = abs(target-entry)
            a,b,c = st.columns(3)
            a.metric("Risk per unit", f"{risk:,.2f}")
            b.metric("Reward per unit", f"{reward:,.2f}")
            c.metric("Reward : Risk", f"1 : {risk/reward:.2f}" if reward else "N/A")
        else:
            st.warning("Check the prices: BUY needs Stop < Entry < Target; SELL needs Target < Entry < Stop.")
        st.caption("This calculator does not place orders or calculate lot size. Add spread, slippage and your account-risk limit before trading.")


def render_economic_event_planner():
    """User-entered event actual/forecast comparison; deliberately not a live calendar."""
    with st.expander("🗞️ US Economic News — Actual vs Forecast (Manual) + Gold Context", expanded=False):
        st.caption("No verified live calendar is connected. Enter values from a trusted economic calendar after the release; do not treat the direction guide as a prediction.")
        event = st.selectbox("Event", ["CPI / Core CPI", "PPI / Core PPI", "NFP / Employment", "Unemployment Claims", "Retail Sales", "PCE / Core PCE", "GDP", "FOMC / Fed Rate"], key="sf_event_name")
        x1,x2,x3 = st.columns(3)
        forecast = x1.number_input("Forecast", value=0.0, step=0.1, format="%.3f", key="sf_event_forecast")
        actual = x2.number_input("Actual", value=0.0, step=0.1, format="%.3f", key="sf_event_actual")
        previous = x3.number_input("Previous (optional reference)", value=0.0, step=0.1, format="%.3f", key="sf_event_previous")
        direction = st.selectbox("How should this release usually be read?", ["Inflation / activity metric (higher may support USD/yields)", "Employment strength metric (higher may support USD/yields)", "Unemployment claims/rate (higher may weaken USD)", "Fed decision / statement (context-dependent)"], key="sf_event_type")
        if st.button("Compare Actual vs Forecast", key="sf_event_compare"):
            delta = actual - forecast
            st.metric("Actual − Forecast", f"{delta:+.3f}")
            if abs(delta) < 1e-9:
                st.info("Actual matched forecast. The gold reaction can still be volatile due to positioning and other details.")
            else:
                st.write("**Release surprise:** " + ("Above forecast" if delta > 0 else "Below forecast"))
                if "claims/rate" in direction:
                    st.write("Higher claims/rate can sometimes weaken USD and support gold; lower can do the opposite, subject to context.")
                elif "context-dependent" in direction:
                    st.write("Read the statement, rate path, Treasury yields and USD reaction; the rate number alone is insufficient.")
                else:
                    st.write("Above-forecast inflation/activity/employment can sometimes support USD/yields and pressure gold; below-forecast can sometimes do the reverse. Confirm with actual USD/yield reaction.")
            st.caption(f"Event: {event} · Previous entered: {previous:.3f}. Values are user-entered, not fetched or independently verified.")
        st.warning("Avoid assuming ‘good news = gold down’ every time. Revisions, core data, expectations, yields and the Fed outlook can reverse the first move.")


def render_gold_news_guide():
    """Educational reminder only; no live economic calendar is connected."""
    with st.expander("🗞️ XAUUSD News Risk Guide (not a live calendar)", expanded=False):
        st.caption("This app does not currently fetch a verified live economic calendar. Check event time from a trusted calendar before trading.")
        st.write("**High-impact US events to watch:** FOMC / Fed rate decision, CPI, PCE inflation, NFP / employment, unemployment rate, GDP, PPI and retail sales.")
        st.write("**General tendency, not a fixed rule:** stronger US yields / USD can pressure gold; weaker yields / USD can support gold. Actual reaction depends on expectations, positioning and the statement/data details.")
        st.warning("Around major releases, spreads and volatility can expand. Consider waiting for the release and a confirmed post-news structure; this guide does not automatically block signals.")


# Stable live refresh architecture:
# The old version used a full-page st_autorefresh every few seconds. That
# caused the entire app (including heavy sections) to rerun repeatedly.
# Streamlit fragments now refresh only the live dashboard section, while
# Backtest/Telegram controls remain stable.

def _golden_chicken_backtest(df, max_bars=1200, atr_mult=1.5, reward_r=2.0):
    """Transparent historical EMA-pullback proxy; never label it as the original proprietary indicator."""
    result = {"trades": 0, "wins": 0, "losses": 0, "win_rate": None, "net_r": 0.0,
              "profit_factor": None, "note": "Insufficient completed OHLC candles for a meaningful test."}
    if df is None or df.empty or not {"open", "high", "low", "close"}.issubset(df.columns):
        return result
    x = df.tail(max_bars).copy().reset_index(drop=True)
    for c in ("open", "high", "low", "close"):
        x[c] = pd.to_numeric(x[c], errors="coerce")
    x = x.dropna(subset=["open", "high", "low", "close"]).reset_index(drop=True)
    if len(x) < 100:
        return result
    close = x["close"]
    fast = close.ewm(span=20, adjust=False).mean()
    slow = close.ewm(span=50, adjust=False).mean()
    prev_close = close.shift(1)
    tr = pd.concat([x["high"] - x["low"], (x["high"] - prev_close).abs(), (x["low"] - prev_close).abs()], axis=1).max(axis=1)
    atr = tr.rolling(14).mean()
    wins = losses = 0
    rs = []
    i = 55
    while i < len(x) - 1:
        # Entry after a completed bar when EMA trend and close confirm direction.
        direction = 1 if fast.iloc[i] > slow.iloc[i] and close.iloc[i] > fast.iloc[i] and close.iloc[i-1] <= fast.iloc[i-1] else -1 if fast.iloc[i] < slow.iloc[i] and close.iloc[i] < fast.iloc[i] and close.iloc[i-1] >= fast.iloc[i-1] else 0
        av = atr.iloc[i]
        if not direction or pd.isna(av) or av <= 0:
            i += 1
            continue
        # Signal is known only after candle i closes. Use the next candle open
        # as the executable proxy entry, and evaluate that candle's range too.
        entry = float(x.open.iloc[i + 1])
        if not np.isfinite(entry):
            i += 1
            continue
        risk = float(av) * atr_mult
        stop = entry - direction * risk
        target = entry + direction * risk * reward_r
        outcome = None
        j = i + 1
        while j < len(x):
            op, hi, lo = float(x.open.iloc[j]), float(x.high.iloc[j]), float(x.low.iloc[j])
            # Conservative fill: a gap through the stop exits at the worse open.
            stop_gap = (direction == 1 and op <= stop) or (direction == -1 and op >= stop)
            stop_hit = lo <= stop if direction == 1 else hi >= stop
            target_hit = hi >= target if direction == 1 else lo <= target
            if stop_gap:
                # A gap-through-stop exits at the worse opening price; express
                # the actual loss in R instead of capping every stop at -1R.
                outcome = direction * (op - entry) / risk
                break
            # If SL and TP both touch in one candle, count SL first.
            if stop_hit:
                outcome = -1.0
                break
            if target_hit:
                outcome = float(reward_r)
                break
            j += 1
        if outcome is not None:
            rs.append(outcome)
            if outcome > 0: wins += 1
            else: losses += 1
            i = j + 1
        else:
            # Ignore still-open end-of-sample trades rather than mark them as wins/losses.
            break
    n = len(rs)
    gross_win = sum(r for r in rs if r > 0)
    gross_loss = -sum(r for r in rs if r < 0)
    result.update({"trades": n, "wins": wins, "losses": losses,
                   "win_rate": (wins / n * 100) if n else None,
                   "net_r": sum(rs),
                   "profit_factor": (gross_win / gross_loss) if gross_loss else (float("inf") if gross_win else None),
                   "note": "Historical 5M EMA20/50 pullback proxy, ATR(14) 1.5x stop and 2R target. Not the original Golden Chicken formula; costs/slippage are not included."})
    return result


def render_golden_chicken_scanner(h1e, m15e, m5e, final, levels, feed_live):
    """App-only confluence scanner and honest historical proxy stats."""
    with st.expander("🐔 Gold Buy/Sell Scanner — Strategy Stats + HTF Alignment", expanded=False):
        t1 = trend_state(h1e) if h1e is not None and not h1e.empty else "UNAVAILABLE"
        t15 = trend_state(m15e) if m15e is not None and not m15e.empty else "UNAVAILABLE"
        t5 = trend_state(m5e) if m5e is not None and not m5e.empty else "UNAVAILABLE"
        aligned_buy = [t for t in (t1, t15, t5) if t != "UNAVAILABLE"].count("BULLISH")
        aligned_sell = [t for t in (t1, t15, t5) if t != "UNAVAILABLE"].count("BEARISH")
        available = sum(t != "UNAVAILABLE" for t in (t1, t15, t5))
        if not feed_live:
            setup = "WAIT — LIVE FEED NOT FRESH"
        elif final and final.get("signal") in ("BUY", "SELL") and levels:
            setup = final["signal"]
        elif available >= 2 and aligned_buy == available:
            setup = "BULLISH BIAS — WAIT FOR ENTRY GATE"
        elif available >= 2 and aligned_sell == available:
            setup = "BEARISH BIAS — WAIT FOR ENTRY GATE"
        else:
            setup = "WAIT — TIMEFRAMES NOT ALIGNED"
        c1, c2, c3 = st.columns(3)
        c1.metric("Current Setup", setup)
        c2.metric("HTF Alignment", f"{max(aligned_buy, aligned_sell)}/{available}" if available else "Unavailable")
        c3.metric("Strategy Code", "SF-GC-EMA-20-50-2R")
        st.write(f"**Trend map:** 1H **{t1}** · 15M **{t15}** · 5M **{t5}**")
        if setup == "BUY":
            st.success("BUY confirmed by the app's existing signal gate. Verify stop, spread and position size before acting.")
        elif setup == "SELL":
            st.error("SELL confirmed by the app's existing signal gate. Verify stop, spread and position size before acting.")
        else:
            st.info("No new entry is authorized by this scanner. Trend alignment alone is not an entry signal.")
        stats = _golden_chicken_backtest(m5e)
        st.markdown("**Historical proxy backtest — 5M data**")
        a, b, c, d = st.columns(4)
        a.metric("Closed Trades", stats["trades"])
        b.metric("Wins / Losses", f'{stats["wins"]} / {stats["losses"]}')
        b_rate = stats["win_rate"]
        c.metric("Win Rate", f"{b_rate:.1f}%" if b_rate is not None else "N/A")
        pf = stats["profit_factor"]
        d.metric("Profit Factor", f"{pf:.2f}" if pf is not None and pf != float("inf") else ("∞*" if pf == float("inf") else "N/A"))
        st.metric("Net Result", f'{stats["net_r"]:+.2f} R')
        st.caption(stats["note"])
        st.caption("Statistics are recalculated from candles currently supplied to the app; they are not proof of future accuracy. A same-candle stop/target collision is counted as a stop. No commission, spread or slippage model is included.")



@st.fragment(run_every=refresh)
def live_dashboard():
    """Refresh only live prices and unified signal; keep the rest of the app stable."""
    feed = get_feed(SIFTING_KEY)
    snap = feed.snapshot()

    # History (cached REST calls; these do not refetch on every live tick).
    try:
        h1 = get_bars(SIFTING_KEY, "1h", 260)
        m15 = get_bars(SIFTING_KEY, "15m", 300)
        rest5 = get_bars(SIFTING_KEY, "5m", 300)

        if not rest5.empty:
            rest5_age_min = (datetime.now(timezone.utc) - rest5.time.iloc[-1].to_pydatetime()).total_seconds() / 60
        else:
            rest5_age_min = float("inf")

        if rest5.empty or rest5_age_min > 15:
            one_min = get_bars(SIFTING_KEY, "1m", 2000)
            if not one_min.empty:
                one_min = one_min.set_index("time").sort_index()
                rest5_fallback = one_min.resample("5min", label="left", closed="left").agg({
                    "open": "first", "high": "max", "low": "min", "close": "last", "volume": "sum",
                }).dropna(subset=["open", "high", "low", "close"]).reset_index()
                rest5 = rest5_fallback.tail(350).reset_index(drop=True)
    except Exception as e:
        st.error(f"SiftingIO history error: {e}")
        h1 = m15 = rest5 = pd.DataFrame()

    # Daily candles feed the D/W/M trend table. Isolated so failure here does not disable core live analysis.
    try:
        daily_trend = get_bars(SIFTING_KEY, "1d", 500)
    except Exception as daily_error:
        daily_trend = pd.DataFrame()
        st.caption(f"Higher-timeframe daily history unavailable: {daily_error}")

    live5 = make_live_df(snap)
    if not live5.empty and not rest5.empty:
        base5 = pd.concat([rest5, live5], ignore_index=True).drop_duplicates("time", keep="last").sort_values("time").tail(350).reset_index(drop=True)
    else:
        base5 = live5 if not live5.empty else rest5

    h1e = enrich(h1) if not h1.empty else h1
    m15e = enrich(m15) if not m15.empty else m15
    m5e = enrich(base5) if not base5.empty else base5

    # Do not authorize signals from stale/missing candle history, even when
    # the tick stream itself is live. Intrabar WebSocket candles keep 5M data
    # current; REST 1H/15M history must also be reasonably recent.
    now_utc = datetime.now(timezone.utc)
    def _latest_age_minutes(frame):
        if frame is None or frame.empty or "time" not in frame.columns:
            return float("inf")
        ts = pd.to_datetime(frame["time"].iloc[-1], utc=True, errors="coerce")
        if pd.isna(ts):
            return float("inf")
        return max(0.0, (now_utc - ts.to_pydatetime()).total_seconds() / 60.0)

    h1_age_min = _latest_age_minutes(h1)
    m15_age_min = _latest_age_minutes(m15)
    m5_age_min = _latest_age_minutes(base5)
    history_fresh = (
        not h1.empty and not m15.empty and not base5.empty
        and h1_age_min <= 120
        and m15_age_min <= 35
        and m5_age_min <= 10
    )

    price = None
    last_tick_age = None
    price_source = "NONE"
    quote = None

    if snap["last_tick"]:
        last_tick_age = max(0, time.time() - snap["last_tick"]["received"])
        if last_tick_age <= 8:
            price = float(snap["last_tick"]["price"])
            price_source = "WEBSOCKET"

    if price is None:
        quote = get_live_quote(SIFTING_KEY)
        if quote and quote.get("price") is not None:
            price = float(quote["price"])
            price_source = "REST_QUOTE_FALLBACK"

    if price is None and not base5.empty:
        price = float(base5.close.iloc[-1])
        price_source = "HISTORICAL_FALLBACK"

    if price is None:
        st.warning("Waiting for live XAUUSD price…")
        return

    websocket_fresh = price_source == "WEBSOCKET" and last_tick_age is not None and last_tick_age <= 8
    feed_live = websocket_fresh and history_fresh
    if websocket_fresh and not history_fresh:
        st.warning(
            "Live tick is fresh, but candle history is stale or incomplete. "
            f"Signal generation is paused (1H {h1_age_min:.1f}m, "
            f"15M {m15_age_min:.1f}m, 5M {m5_age_min:.1f}m). "
            "Refresh/retry SiftingIO history before trading."
        )
    engine = early_engine(base5, h1, m15, price)
    institutional = institutional_engine(h1, m15, base5, engine, price)
    trigger_map = next_trigger_map(h1, m15, base5, engine, institutional, price)
    ob_map = order_block_map(base5, price)
    maybe_alert_order_block(ob_map, price, feed_live, enabled=ob_alerts_enabled)
    ob_confirmation = order_block_confirmation(base5, price, ob_map)
    magic_ratio = magic_ratio_map(base5, price)
    regime = market_regime(h1, m15, base5)
    equal_liquidity = liquidity_enhanced(base5)
    aplus_gate = a_plus_signal_gate(h1, m15, base5, engine, trigger_map, regime, price)
    if feed_live:
        signal = smart_flow_signal(h1, m15, base5, engine)
        proposed = institutional.get("signal")
        ob_gate = ob_confirmation.get("buy_ok") if proposed == "BUY" else ob_confirmation.get("sell_ok") if proposed == "SELL" else False
        final_signal = proposed if proposed in ("BUY", "SELL") and signal["score"] >= 60 and engine["score"] >= 60 and ob_gate else "WAIT"
        # Explain the first blocking condition rather than always blaming the OB.
        if final_signal in ("BUY", "SELL"):
            final_reason = institutional.get("label", final_signal)
        elif proposed not in ("BUY", "SELL"):
            final_reason = "NO TRADE — Waiting for institutional direction."
        elif signal.get("score", 0) < 60:
            final_reason = f"NO TRADE — Smart Flow score below 60 ({signal.get('score', 0)})."
        elif engine.get("score", 0) < 60:
            final_reason = f"NO TRADE — Early Engine score below 60 ({engine.get('score', 0)})."
        else:
            gate_reason = ob_confirmation.get("buy_reason") if proposed == "BUY" else ob_confirmation.get("sell_reason")
            final_reason = f"NO TRADE — {gate_reason or 'Order Block confirmation missing.'}"
        final = {"signal": final_signal, "score": institutional["score"], "reason": final_reason}
        levels = trade_levels(final_signal, price, base5)
        update_paper_trades(price)
        maybe_log_paper_signal(final, engine, price, levels, smart_flow_score=signal.get("score", 0))
        maybe_alert(final, engine, price, levels, smart_flow_score=signal.get("score", 0))
    else:
        signal = {"signal": "WAIT", "score": 0, "reason": "Live WebSocket is not fresh; waiting for live ticks."}
        institutional = {"signal":"WAIT","label":"NO TRADE","score":0,"buy":0,"sell":0,"margin":0,"buy_reasons":[],"sell_reasons":[],"data_note":"Live feed unavailable."}
        final = signal
        levels = None

    # --------------------------------------------------------
    # COMMAND CENTER — one master snapshot for the whole screen
    # --------------------------------------------------------
    master_score = int(institutional.get("score", 0))
    master_signal = final.get("signal", "WAIT") if feed_live else "WAIT"
    if master_signal == "BUY" and master_score >= 80:
        decision_label, action_class, action_text = "A+ BUY", "action-green", "BUY confirmation is active — use the displayed Entry / SL / TP plan."
    elif master_signal == "SELL" and master_score >= 80:
        decision_label, action_class, action_text = "A+ SELL", "action-red", "SELL confirmation is active — use the displayed Entry / SL / TP plan."
    else:
        # Early watch layer: surface a developing side without weakening the
        # master trade gate. WATCH is informational only and never authorizes entry.
        buy_watch_score = int(trigger_map.get("buy_score", 0) or 0)
        sell_watch_score = int(trigger_map.get("sell_score", 0) or 0)
        watch_side = None
        if feed_live and max(buy_watch_score, sell_watch_score) >= 60 and abs(buy_watch_score - sell_watch_score) >= 10:
            watch_side = "BUY" if buy_watch_score > sell_watch_score else "SELL"

        if watch_side == "BUY":
            decision_label, action_class, action_text = (
                "WATCH BUY", "action-green",
                f"Bullish setup developing. Watch break {trigger_map['buy_break']:,.2f}, then wait for retest + 5M structure + master confirmation. No entry yet."
            )
        elif watch_side == "SELL":
            decision_label, action_class, action_text = (
                "WATCH SELL", "action-red",
                f"Bearish setup developing. Watch break {trigger_map['sell_break']:,.2f}, then wait for retest + 5M structure + master confirmation. No entry yet."
            )
        else:
            decision_label, action_class, action_text = (
                "NO TRADE", "action-neutral",
                "Stay flat. The master confirmation gate has not approved a trade. "
                "Trigger levels below are watch conditions only; wait for full confirmation."
            )

    now_utc = datetime.now(timezone.utc)
    elapsed = now_utc.minute * 60 + now_utc.second
    remain = 300 - (elapsed % 300)
    mm, ss = divmod(remain, 60)

    st.markdown(f"""
    <div class="command">
      <div style="display:flex;justify-content:space-between;align-items:center;gap:12px">
        <div><div style="color:#8ea0b2;font-size:.72rem;text-transform:uppercase;letter-spacing:.09em">Trade Command Center</div><div style="font-size:1.65rem;font-weight:900;margin-top:2px">{decision_label}</div></div>
        <div style="text-align:right"><div style="color:#8ea0b2;font-size:.72rem;text-transform:uppercase;letter-spacing:.09em">Master Score</div><div style="font-size:1.45rem;font-weight:900">{master_score}<span style="font-size:.8rem;color:#8ea0b2">/100</span></div></div>
      </div>
      <div class="action {action_class}"><b>WHAT TO DO NOW:</b> {action_text}</div>
      <div style="color:#718096;font-size:.72rem;margin:8px 0 0">Single master snapshot • all trigger/score sections below use this same calculation</div>
      <div class="command-grid">
        <div class="kpi"><div class="kpi-label">XAUUSD</div><div class="kpi-value">{price:,.2f}</div><div class="kpi-note">{price_source.replace('_',' ')}</div></div>
        <div class="kpi"><div class="kpi-label">BUY Trigger</div><div class="kpi-value">{trigger_map['buy_break']:,.2f}</div><div class="kpi-note">{abs(trigger_map['buy_break']-price):.2f} away</div></div>
        <div class="kpi"><div class="kpi-label">SELL Trigger</div><div class="kpi-value">{trigger_map['sell_break']:,.2f}</div><div class="kpi-note">{abs(price-trigger_map['sell_break']):.2f} away</div></div>
        <div class="kpi"><div class="kpi-label">5M Candle</div><div class="kpi-value">{mm:02d}:{ss:02d}</div><div class="kpi-note">time remaining</div></div>
      </div>
      <div class="path-grid">
        <div class="path buy"><div class="path-head">🟢 BUY PLAN</div><div class="path-price">Break {trigger_map['buy_break']:,.2f}</div><div class="path-muted">Retest {trigger_map['buy_retest_low']:,.2f} – {trigger_map['buy_retest_high']:,.2f} • score {trigger_map['buy_score']}/100</div></div>
        <div class="path sell"><div class="path-head">🔴 SELL PLAN</div><div class="path-price">Break {trigger_map['sell_break']:,.2f}</div><div class="path-muted">Retest {trigger_map['sell_retest_low']:,.2f} – {trigger_map['sell_retest_high']:,.2f} • score {trigger_map['sell_score']}/100</div></div>
      </div>
    </div>
    """, unsafe_allow_html=True)

    if feed_live:
        st.success(f"🟢 LIVE WebSocket • tick {last_tick_age:.1f}s ago")
    elif price_source == "REST_QUOTE_FALLBACK":
        st.warning("🟡 WebSocket reconnecting — live REST quote shown. Signals/alerts are paused until ticks resume.")
    elif price_source == "HISTORICAL_FALLBACK":
        st.error("🔴 Live feed unavailable — historical price only. Signals/alerts are paused.")
    else:
        st.warning(f"🟠 WebSocket reconnecting… {snap['error']}")

    # One-glance dashboard: summarizes existing calculations without changing signal logic.
    _h1_bias = trend_state(h1e) if not h1e.empty else "UNAVAILABLE"
    _m15_bias = trend_state(m15e) if not m15e.empty else "UNAVAILABLE"
    _m5_bias = trend_state(m5e) if not m5e.empty else "UNAVAILABLE"
    _feed_label = "LIVE" if feed_live else "STALE / OFFLINE"
    _feed_class = "glance-good" if feed_live else "glance-bad"
    _signal_class = "glance-good" if decision_label in ("A+ BUY", "A+ SELL") else ("glance-warn" if decision_label.startswith("WATCH") else "")
    _regime_label = str(regime.get("regime", "UNKNOWN"))
    _ob_bull = ob_map.get("bullish")
    _ob_bear = ob_map.get("bearish")
    _ob_label = ("Bull + Bear" if _ob_bull and _ob_bear else "Bullish OB" if _ob_bull else "Bearish OB" if _ob_bear else "No fresh zone")
    st.markdown(f"""
    <div class="glance-wrap">
      <div class="glance-title">At a glance · six checks before any trade</div>
      <div class="glance-grid">
        <div class="glance-card {_feed_class}"><div class="glance-label">Market data</div><div class="glance-value">{_feed_label}</div><div class="glance-note">{last_tick_age:.1f}s since tick</div></div>
        <div class="glance-card {_signal_class}"><div class="glance-label">Master decision</div><div class="glance-value">{decision_label}</div><div class="glance-note">Score {master_score}/100 · no guarantee</div></div>
        <div class="glance-card"><div class="glance-label">Trend alignment</div><div class="glance-value">1H {_h1_bias}</div><div class="glance-note">15M {_m15_bias} · 5M {_m5_bias}</div></div>
        <div class="glance-card"><div class="glance-label">Market regime</div><div class="glance-value">{_regime_label}</div><div class="glance-note">ADX strength {regime.get('score', 0)}/100</div></div>
        <div class="glance-card"><div class="glance-label">Smart Flow / Early</div><div class="glance-value">{signal.get('score', 0)} / {engine.get('score', 0)}</div><div class="glance-note">Both scores are supporting signals</div></div>
        <div class="glance-card"><div class="glance-label">Order Block</div><div class="glance-value">{_ob_label}</div><div class="glance-note">Fresh, unmitigated zones only</div></div>
      </div>
    </div>
    """, unsafe_allow_html=True)


    # Unified Smart View: one-glance indicators + conditional BUY/SELL risk map.
    # This is a planning aid only; it never changes the master confirmation gate.
    st.markdown("## 🧠 Smart View — all checks in one place")
    st.caption("One snapshot • levels are conditional watch plans, not an entry signal. Only trade after the Master gate confirms.")

    _sv_atr = float(m5e.atr.iloc[-1]) if not m5e.empty and "atr" in m5e.columns and pd.notna(m5e.atr.iloc[-1]) else None
    _sv_vwap = None
    for _vwap_col in ("vwap", "VWAP", "session_vwap"):
        if not m5e.empty and _vwap_col in m5e.columns and pd.notna(m5e[_vwap_col].iloc[-1]):
            _sv_vwap = float(m5e[_vwap_col].iloc[-1])
            break
    _sv_sweep = str(snap.get("sweep", "UNAVAILABLE")) if isinstance(snap, dict) else "UNAVAILABLE"
    _sv_fvg = str(snap.get("fvg", "UNAVAILABLE")) if isinstance(snap, dict) else "UNAVAILABLE"
    _sv_session = str(snap.get("session", "UNAVAILABLE")) if isinstance(snap, dict) else "UNAVAILABLE"
    _sv_atr_text = f"{_sv_atr:.2f}" if _sv_atr is not None else "—"
    _sv_vwap_text = f"{_sv_vwap:,.2f}" if _sv_vwap is not None else "Unavailable"

    with st.container(border=True):
        st.markdown("**INDICATOR CHECKLIST**")
        _sv1, _sv2, _sv3 = st.columns(3)
        _sv1.metric("1H Bias", _h1_bias)
        _sv2.metric("15M Bias", _m15_bias)
        _sv3.metric("5M Bias", _m5_bias)
        _sv4, _sv5, _sv6 = st.columns(3)
        _sv4.metric("Master Decision", decision_label)
        _sv5.metric("Master Score", f"{master_score}/100")
        _sv6.metric("Market Regime", _regime_label)
        _sv7, _sv8, _sv9 = st.columns(3)
        _sv7.metric("Smart Flow", f"{signal.get('score', 0)}/100")
        _sv8.metric("Early Move", f"{engine.get('score', 0)}/100")
        _sv9.metric("5M ATR", _sv_atr_text)
        _sv10, _sv11, _sv12 = st.columns(3)
        _sv10.metric("VWAP", _sv_vwap_text)
        _sv11.metric("London Session", _sv_session)
        _sv12.metric("Data", "LIVE" if feed_live else "NOT LIVE")
        st.write(f"**Liquidity sweep:** {_sv_sweep}")
        st.write(f"**FVG status:** {_sv_fvg}")
        st.write(f"**Order Block:** {_ob_label}")
        st.caption("SMT, OTE, IFVG, pattern recognition and COMEX/COT data are shown only when their actual data/detection modules are available; missing values must not be fabricated.")

    # ATR-based conditional planning levels: informational only, not broker orders.
    with st.container(border=True):
        st.markdown("**BUY / SELL PLAN — ENTRY · STOP LOSS · TARGETS · EXIT RULE**")
        if _sv_atr is None or _sv_atr <= 0:
            st.warning("Valid 5M ATR unavailable. Entry/SL/TP plan is hidden rather than inventing levels.")
        else:
            _sv_risk_dist = max(_sv_atr * 1.5, 0.01)
            _sv_buy_entry = float(trigger_map["buy_break"])
            _sv_sell_entry = float(trigger_map["sell_break"])
            _sv_buy_sl = _sv_buy_entry - _sv_risk_dist
            _sv_buy_tp1 = _sv_buy_entry + _sv_risk_dist
            _sv_buy_tp2 = _sv_buy_entry + 2 * _sv_risk_dist
            _sv_buy_tp3 = _sv_buy_entry + 3 * _sv_risk_dist
            _sv_sell_sl = _sv_sell_entry + _sv_risk_dist
            _sv_sell_tp1 = _sv_sell_entry - _sv_risk_dist
            _sv_sell_tp2 = _sv_sell_entry - 2 * _sv_risk_dist
            _sv_sell_tp3 = _sv_sell_entry - 3 * _sv_risk_dist
            _sv_buy, _sv_sell = st.columns(2)
            with _sv_buy:
                st.markdown("🟢 **BUY — conditional watch plan**")
                st.metric("Entry trigger", f"{_sv_buy_entry:,.2f}")
                st.write(f"**SL:** `{_sv_buy_sl:,.2f}`")
                st.write(f"**TP1 (1R):** `{_sv_buy_tp1:,.2f}`")
                st.write(f"**TP2 (2R):** `{_sv_buy_tp2:,.2f}`")
                st.write(f"**TP3 (3R):** `{_sv_buy_tp3:,.2f}`")
                st.caption(f"Retest zone: {trigger_map['buy_retest_low']:,.2f} – {trigger_map['buy_retest_high']:,.2f}")
            with _sv_sell:
                st.markdown("🔴 **SELL — conditional watch plan**")
                st.metric("Entry trigger", f"{_sv_sell_entry:,.2f}")
                st.write(f"**SL:** `{_sv_sell_sl:,.2f}`")
                st.write(f"**TP1 (1R):** `{_sv_sell_tp1:,.2f}`")
                st.write(f"**TP2 (2R):** `{_sv_sell_tp2:,.2f}`")
                st.write(f"**TP3 (3R):** `{_sv_sell_tp3:,.2f}`")
                st.caption(f"Retest zone: {trigger_map['sell_retest_low']:,.2f} – {trigger_map['sell_retest_high']:,.2f}")
            st.info("Exit framework: consider partial exit at TP1, reassess/secure risk at TP2, and trail any runner only if the existing confirmation rules still hold. These ATR-based levels are illustrative; they do not replace structure-based stops or approve a trade.")
        if decision_label not in ("A+ BUY", "A+ SELL"):
            st.warning("MASTER GATE NOT APPROVED — remain WAIT / NO TRADE. The displayed levels are watch plans only.")
        else:
            st.success("Master gate reports a confirmed setup. Verify live data freshness and position size before any manual action.")


    # Additive London Kill Zone snapshot; it does not alter the Master trade gate.
    render_london_killzone_live(m5e, feed_live, price, london_timezone)

    # Keep the default mobile view uncluttered. Detailed tools are grouped below in Full Pro.
    atr_now = float(m5e.atr.iloc[-1]) if not m5e.empty and "atr" in m5e.columns and pd.notna(m5e.atr.iloc[-1]) else None

    # Quick Trade mode: keep the mobile screen focused on the decision.
    if view_mode == "⚡ Quick Trade":
        q1, q2, q3, q4 = st.columns(4)
        q1.metric("Action", decision_label)
        q2.metric("Smart Flow", f"{signal['score']}/100")
        q3.metric("Big Move", f"{engine['score']}/100")
        q4.metric("5M ADX", f"{float(m5e.adx.iloc[-1]):.1f}" if not m5e.empty and pd.notna(m5e.adx.iloc[-1]) else "—")

        # Smart Flow Confluence — visible in Quick Trade as requested.
        # This is a secondary rule-based readout; it never overrides Master Signal.
        st.markdown("### 🧭 Smart Flow Confluence — എളുപ്പത്തിലുള്ള തീരുമാനം")
        if not m5e.empty:
            cf = m5e.iloc[-1]
            cf_rsi = float(cf.rsi) if pd.notna(cf.rsi) else None
            bull_points = 0
            bear_points = 0
            cf_bull_reasons, cf_bear_reasons = [], []
            if pd.notna(cf.ema20) and pd.notna(cf.ema50):
                if cf.ema20 > cf.ema50:
                    bull_points += 1; cf_bull_reasons.append("EMA20 > EMA50")
                elif cf.ema20 < cf.ema50:
                    bear_points += 1; cf_bear_reasons.append("EMA20 < EMA50")
            if cf_rsi is not None:
                if cf_rsi >= 55:
                    bull_points += 1; cf_bull_reasons.append("RSI bullish")
                elif cf_rsi <= 45:
                    bear_points += 1; cf_bear_reasons.append("RSI bearish")
            if pd.notna(cf.pdi) and pd.notna(cf.mdi):
                if cf.pdi > cf.mdi:
                    bull_points += 1; cf_bull_reasons.append("+DI pressure")
                elif cf.mdi > cf.pdi:
                    bear_points += 1; cf_bear_reasons.append("-DI pressure")
            if pd.notna(cf.vwap):
                if float(cf.close) > float(cf.vwap):
                    bull_points += 1; cf_bull_reasons.append("Price above VWAP")
                elif float(cf.close) < float(cf.vwap):
                    bear_points += 1; cf_bear_reasons.append("Price below VWAP")
            cf_direction = "Bullish" if bull_points > bear_points else "Bearish" if bear_points > bull_points else "Neutral"
            c1, c2, c3 = st.columns(3)
            c1.metric("Direction", cf_direction)
            c2.metric("Bull / Bear points", f"{bull_points} / {bear_points}")
            c3.metric("RSI (14)", f"{cf_rsi:.1f}" if cf_rsi is not None else "—")
            # One prominent, unambiguous decision card directly below Confluence.
            # Confluence points are context only; never override the master confirmation gate.
            st.markdown("### 🎯 FINAL SIGNAL — BUY / SELL / WAIT")
            if not feed_live:
                st.error("🔴 NO TRADE — LIVE DATA NOT READY. Live price fresh അല്ല; പുതിയ trade signal സ്വീകരിക്കരുത്.")
            elif decision_label == "A+ BUY":
                st.success("🟢 FINAL SIGNAL: BUY — Master A+ confirmation passed.")
            elif decision_label == "A+ SELL":
                st.error("🔴 FINAL SIGNAL: SELL — Master A+ confirmation passed.")
            elif decision_label in ("WATCH BUY", "WATCH SELL"):
                watch_side = "BUY" if decision_label == "WATCH BUY" else "SELL"
                st.warning(f"🟠 FINAL SIGNAL: WAIT — {watch_side} setup developing; breakout, retest and master confirmation are still required.")
            else:
                st.warning("🟠 FINAL SIGNAL: WAIT / NO TRADE — Master confirmation ലഭിച്ചിട്ടില്ല. Setup വ്യക്തമായാൽ മാത്രം വീണ്ടും പരിശോധിക്കുക.")
            st.caption(f"Master score: {master_score}/100 · Smart Flow: {signal['score']}/100 · Early Move: {engine['score']}/100. Confluence points are supporting evidence only; they do not independently trigger a trade.")
            with st.expander("Confluence വിശദാംശങ്ങൾ"):
                st.write("🟢 Bullish factors: " + (" • ".join(cf_bull_reasons) or "ഒന്നുമില്ല"))
                st.write("🔴 Bearish factors: " + (" • ".join(cf_bear_reasons) or "ഒന്നുമില്ല"))
                st.caption("ഈ indicator 5M candle data മാത്രം ഉപയോഗിക്കുന്നു. Master Signal/A+ confirmation-നെ മറികടക്കില്ല. ഓരോ trade-ലും risk പരമാവധി account equity-യുടെ 1%.")
        else:
            st.warning("Smart Flow Confluence ലഭ്യമല്ല — ആവശ്യമായ 5M candle data ലഭിച്ചിട്ടില്ല.")

        if decision_label == "A+ BUY":
            st.success("🟢 TRADE NOW — BUY confirmation is active.")
        elif decision_label == "A+ SELL":
            st.error("🔴 TRADE NOW — SELL confirmation is active.")
        elif decision_label == "WATCH SELL":
            st.warning(f"🔴 WATCH SELL — wait for break {trigger_map['sell_break']:,.2f} + retest + confirmation.")
        elif decision_label == "WATCH BUY":
            st.success(f"🟢 WATCH BUY — wait for break {trigger_map['buy_break']:,.2f} + retest + confirmation.")
        else:
            st.info("⚪ WAIT — no clean trade setup yet.")
        with st.expander("🧱 Order Block Map"):
            bull_ob = ob_map.get("bullish")
            bear_ob = ob_map.get("bearish")
            st.write(
                "🟢 Bullish OB: " + (
                    f"{bull_ob['low']:,.2f} – {bull_ob['high']:,.2f} • {bull_ob['status']}"
                    if bull_ob else "No fresh, unmitigated zone"
                )
            )
            st.write(
                "🔴 Bearish OB: " + (
                    f"{bear_ob['low']:,.2f} – {bear_ob['high']:,.2f} • {bear_ob['status']}"
                    if bear_ob else "No fresh, unmitigated zone"
                )
            )
            st.write(f"Price inside bullish/bearish zone: **{'YES' if ob_map.get('near_bullish') else 'NO'} / {'YES' if ob_map.get('near_bearish') else 'NO'}**")
            st.caption(ob_map.get("note", ""))
            for side, label in (("bullish", "Last mitigated bullish OB"), ("bearish", "Last mitigated bearish OB")):
                old_zone = ob_map.get(f"last_mitigated_{side}")
                if old_zone:
                    st.caption(f"{label}: {old_zone['low']:,.2f} – {old_zone['high']:,.2f} · historical, not active")
        render_magic_ratio_panel(magic_ratio, price)
        with st.expander("📋 Confirmation checklist"):
            st.write(f"1H: **{trend_state(h1e) if not h1e.empty else '—'}**")
            st.write(f"5M BOS: **{'UP' if structure(m5e).get('bos_bull') else 'DOWN' if structure(m5e).get('bos_bear') else 'NO'}**" if not m5e.empty else "5M BOS: **—**")
            st.write(f"Liquidity sweep: **{'YES' if trigger_map['buy_sweep'] or trigger_map['sell_sweep'] else 'NO'}**")
            st.write(f"VWAP: **{trigger_map['vwap']:,.2f}**")
            st.write(f"Institutional: **{institutional.get('label','NO TRADE')} • {institutional.get('score',0)}/100**")
            st.write(f"Market Regime: **{regime.get('regime', 'UNKNOWN')}** • ADX score {regime.get('score', 0)}/100")
            st.write(f"Equal High liquidity: **{equal_liquidity.get('eq_high'):.2f}**" if equal_liquidity.get('eq_high') is not None else "Equal High liquidity: **unavailable**")
            st.write(f"Equal Low liquidity: **{equal_liquidity.get('eq_low'):.2f}**" if equal_liquidity.get('eq_low') is not None else "Equal Low liquidity: **unavailable**")
            st.caption("Equal-high/low levels are heuristic candle-based estimates, not live COMEX order-book data.")
            st.caption("Trigger touch alone is not an entry. Wait for break + retest + structure confirmation.")
        with st.expander("📊 Detailed diagnostics"):
            st.caption("Switch the sidebar Interface to 🧭 Full Pro — All Tools to open the organized analysis workspace.")
        return

    # ========================================================
    # FULL PRO WORKSPACE — grouped so the user can find tools fast
    # ========================================================
    st.markdown("### 🧭 Analysis Workspace")
    st.caption("Start at Overview, review the setup, then check liquidity/news. Use Scanner for a secondary rule-based view.")
    tab_overview, tab_setups, tab_liquidity, tab_news, tab_scanner = st.tabs([
        "📌 Overview", "🎯 Setups", "💧 Liquidity & Risk", "🗞️ News", "🔎 Scanner"
    ])
    with tab_overview:
        st.markdown("#### Multi-timeframe direction & trade levels")
        render_multitimeframe_target_panel(h1, daily_trend, price, levels, feed_live, atr_now)
        st.markdown("#### Current market state")
        st.write(f"**Master decision:** {decision_label} · **Smart Flow:** {signal['score']}/100 · **Early Move:** {engine['score']}/100")
        st.caption("A+ means the rule-based confirmation gate passed; it does not mean a guaranteed winning trade.")
    with tab_setups:
        inside_state = render_inside_bar_panel(base5, live_candle=feed_live)
        render_crt_power_of_three_panel(h1e, m15e, m5e, price, feed_live)
        render_gold_aplus_panel(h1e, m15e, m5e, ob_map, ob_confirmation, trigger_map,
                                final, levels, feed_live, price, inside_state)
        render_magic_ratio_panel(magic_ratio, price)
    with tab_liquidity:
        render_liquidity_risk_panel(m5e, price, feed_live)
        st.markdown("#### Order Block zones")
        bull_ob = ob_map.get("bullish")
        bear_ob = ob_map.get("bearish")
        st.write("🟢 **Bullish OB:** " + (f"{bull_ob['low']:,.2f} – {bull_ob['high']:,.2f} · {bull_ob['status']}" if bull_ob else "No valid recent zone"))
        st.write("🔴 **Bearish OB:** " + (f"{bear_ob['low']:,.2f} – {bear_ob['high']:,.2f} · {bear_ob['status']}" if bear_ob else "No valid recent zone"))
        st.caption(ob_map.get("note", "Order Block zones are estimated from available candles."))
        render_fixed_range_volume_profile(base5, current_price=price)
    with tab_news:
        render_gold_news_guide()
        render_economic_event_planner()
    with tab_scanner:
        render_golden_chicken_scanner(h1e, m15e, m5e, final, levels, feed_live)

    if final["signal"] == "BUY" and levels:
        st.success(f"🟢 A+ BUY — {master_score}/100")
    elif final["signal"] == "SELL" and levels:
        st.error(f"🔴 A+ SELL — {master_score}/100")
    else:
        st.info(f"⚪ {decision_label} • Smart Flow {signal['score']}/100 • Big Move {engine['score']}/100")

    st.markdown('<div class="section-head">🏦 Institutional Confluence</div>', unsafe_allow_html=True)

    ic1, ic2, ic3, ic4, ic5 = st.columns(5)
    ic1.metric("Decision", institutional.get("label","NO TRADE"))
    ic2.metric("BUY pressure", institutional.get("buy",0))
    ic3.metric("SELL pressure", institutional.get("sell",0))
    ic4.metric("Confluence", f"{institutional.get('score',0)}/100")
    ic5.metric("Margin", institutional.get("margin",0))
    if institutional.get("signal") == "BUY":
        st.success("🟢 A+ BUY — major confluence is aligned.")
    elif institutional.get("signal") == "SELL":
        st.error("🔴 A+ SELL — major confluence is aligned.")
    elif institutional.get("signal") == "WATCH":
        st.warning("🟡 WATCH — pressure is building, but the A+ gate is not confirmed.")
    else:
        st.info("⚪ NO TRADE — the confluence gate is intentionally strict.")
    with st.expander("🏦 Institutional map"):
        st.write(f"VWAP: **{institutional.get('vwap', price):,.2f}**")
        st.write(f"Previous day High / Low: **{institutional.get('pdh') or 0:,.2f} / {institutional.get('pdl') or 0:,.2f}**")
        st.write(f"Asia H/L: **{institutional.get('asia_hi') or 0:,.2f} / {institutional.get('asia_lo') or 0:,.2f}**")
        st.write(f"London H/L: **{institutional.get('london_hi') or 0:,.2f} / {institutional.get('london_lo') or 0:,.2f}**")
        st.write(f"NY H/L: **{institutional.get('ny_hi') or 0:,.2f} / {institutional.get('ny_lo') or 0:,.2f}**")
        st.write("BUY factors: " + (" • ".join(institutional.get("buy_reasons",[])) or "—"))
        st.write("SELL factors: " + (" • ".join(institutional.get("sell_reasons",[])) or "—"))
        st.caption(institutional.get("data_note",""))

    # --------------------------------------------------------
    # Next Trigger Map — use the SAME master snapshot calculated above.
    # Never recalculate it inside the same dashboard render, otherwise
    # live values can disagree between the Command Center and this section.
    # --------------------------------------------------------
    st.subheader("🎯 Next Trigger + Liquidity Sweep / Retest Map")
    st.caption("Condition map only — it does not predict the future. The final BUY/SELL gate remains Institutional Confluence.")

    tm1, tm2, tm3, tm4 = st.columns(4)
    tm1.metric("Market state", trigger_map["state"])
    tm2.metric("BUY trigger", f"{trigger_map['buy_trigger']:,.2f}", f"score {trigger_map['buy_score']}/100")
    tm3.metric("SELL trigger", f"{trigger_map['sell_trigger']:,.2f}", f"score {trigger_map['sell_score']}/100")
    tm4.metric("VWAP", f"{trigger_map['vwap']:,.2f}")

    bcol, scol = st.columns(2)
    with bcol:
        st.success(f"🟢 BUY path • {trigger_map['buy_state']}")
        st.write(f"Break level: **{trigger_map['buy_break']:,.2f}**")
        st.write(f"Retest zone: **{trigger_map['buy_retest_low']:,.2f} – {trigger_map['buy_retest_high']:,.2f}**")
        st.write(f"Liquidity sweep: **{'YES' if trigger_map['buy_sweep'] else 'NO'}**")
        if trigger_map["buy_missing"]:
            st.caption("BUY still needs: " + " • ".join(trigger_map["buy_missing"]))
        else:
            st.caption("BUY conditions are aligned — wait for trigger/retest confirmation.")
    with scol:
        st.error(f"🔴 SELL path • {trigger_map['sell_state']}")
        st.write(f"Break level: **{trigger_map['sell_break']:,.2f}**")
        st.write(f"Retest zone: **{trigger_map['sell_retest_low']:,.2f} – {trigger_map['sell_retest_high']:,.2f}**")
        st.write(f"Liquidity sweep: **{'YES' if trigger_map['sell_sweep'] else 'NO'}**")
        if trigger_map["sell_missing"]:
            st.caption("SELL still needs: " + " • ".join(trigger_map["sell_missing"]))
        else:
            st.caption("SELL conditions are aligned — wait for trigger/retest confirmation.")

    if trigger_map["state"].startswith("BUY"):
        st.info("🟢 " + trigger_map["reason"])
    elif trigger_map["state"].startswith("SELL"):
        st.info("🔴 " + trigger_map["reason"])
    elif "EXTENDED" in trigger_map["state"]:
        st.warning("🟡 " + trigger_map["reason"])
    else:
        st.info("⚪ " + trigger_map["reason"])

    st.subheader("🧱 Order Block Map — 5M")
    st.caption("Order Block is an additional location filter, not a standalone BUY/SELL signal.")
    ob1, ob2 = st.columns(2)
    bull_ob = ob_map.get("bullish")
    bear_ob = ob_map.get("bearish")
    with ob1:
        if bull_ob:
            st.success(f"🟢 Bullish OB • {bull_ob['status']}")
            st.write(f"Zone: **{bull_ob['low']:,.2f} – {bull_ob['high']:,.2f}**")
            st.write(f"Current price in zone: **{'YES' if ob_map['near_bullish'] else 'NO'}**")
            st.caption(f"Origin candle: {bull_ob['time']}")
        else:
            st.info("No fresh, unmitigated bullish Order Block found in recent 5M candles.")
    with ob2:
        if bear_ob:
            st.error(f"🔴 Bearish OB • {bear_ob['status']}")
            st.write(f"Zone: **{bear_ob['low']:,.2f} – {bear_ob['high']:,.2f}**")
            st.write(f"Current price in zone: **{'YES' if ob_map['near_bearish'] else 'NO'}**")
            st.caption(f"Origin candle: {bear_ob['time']}")
        else:
            st.info("No fresh, unmitigated bearish Order Block found in recent 5M candles.")
    st.caption(ob_map.get("note", ""))
    for side, label in (("bullish", "Last mitigated bullish OB"), ("bearish", "Last mitigated bearish OB")):
        old_zone = ob_map.get(f"last_mitigated_{side}")
        if old_zone:
            st.caption(f"{label}: {old_zone['low']:,.2f} – {old_zone['high']:,.2f} · {old_zone['status']} (historical; not active)")
    render_magic_ratio_panel(magic_ratio, price)
    with st.expander("🛡️ A+ False-Signal Filter"):
        fg1, fg2 = st.columns(2)
        with fg1:
            st.write("🟢 **BUY filter**")
            st.write(f"OB location: **{'YES' if ob_confirmation.get('buy_ob_near') else 'NO'}**")
            st.write(f"Recent liquidity sweep: **{'YES' if ob_confirmation.get('buy_sweep') else 'NO'}**")
            st.write(f"Recent BOS UP: **{'YES' if ob_confirmation.get('buy_break') else 'NO'}**")
            st.caption(ob_confirmation.get("buy_reason", "—"))
        with fg2:
            st.write("🔴 **SELL filter**")
            st.write(f"OB location: **{'YES' if ob_confirmation.get('sell_ob_near') else 'NO'}**")
            st.write(f"Recent liquidity sweep: **{'YES' if ob_confirmation.get('sell_sweep') else 'NO'}**")
            st.write(f"Recent BOS DOWN: **{'YES' if ob_confirmation.get('sell_break') else 'NO'}**")
            st.caption(ob_confirmation.get("sell_reason", "—"))
        st.warning("A+ BUY/SELL is blocked unless the matching Order Block, recent liquidity sweep, and directional BOS all agree. This may reduce signal frequency; it does not guarantee accuracy.")

    st.subheader("Smart Flow Checklist")
    cc1, cc2, cc3, cc4, cc5 = st.columns(5)
    cc1.metric("1H trend", trend_state(h1e) if not h1e.empty else "—")
    s5 = structure(m5e) if not m5e.empty else {}
    cc2.metric("Sweep", "YES" if s5.get("sweep") else "NO")
    cc3.metric("CHoCH", "YES" if s5.get("choch") else "NO")
    cc4.metric("BOS", "UP" if s5.get("bos_bull") else "DOWN" if s5.get("bos_bear") else "NO")
    cc5.metric("ADX", f"{m5e.adx.iloc[-1]:.1f}" if not m5e.empty else "—")

    st.subheader("Key Support / Resistance")
    sr5 = support_resistance(m5e) if not m5e.empty else {"support": [], "resistance": []}
    sr15 = support_resistance(m15e) if not m15e.empty else {"support": [], "resistance": []}
    sr1 = support_resistance(h1e) if not h1e.empty else {"support": [], "resistance": []}
    a, b = st.columns(2)
    with a:
        st.markdown("🟢 **Support**")
        st.write("1H:", ", ".join(f"{x:,.2f}" for x in sr1["support"]) or "—")
        st.write("15M:", ", ".join(f"{x:,.2f}" for x in sr15["support"]) or "—")
        st.write("5M:", ", ".join(f"{x:,.2f}" for x in sr5["support"]) or "—")
    with b:
        st.markdown("🔴 **Resistance**")
        st.write("1H:", ", ".join(f"{x:,.2f}" for x in sr1["resistance"]) or "—")
        st.write("15M:", ", ".join(f"{x:,.2f}" for x in sr15["resistance"]) or "—")
        st.write("5M:", ", ".join(f"{x:,.2f}" for x in sr5["resistance"]) or "—")

    st.subheader("⚡ Big Candle / Early Entry Engine")
    ec1, ec2, ec3, ec4 = st.columns(4)
    ec1.metric("Status", engine["status"]); ec2.metric("Direction", engine["direction"]); ec3.metric("Early Score", f"{engine['score']}/100"); ec4.metric("Quality", engine["quality"])
    st.info(f"🔴/🟢 {engine['status']} • UP trigger {engine['up_trigger']:,.2f} • DOWN trigger {engine['down_trigger']:,.2f}")
    e1, e2, e3 = st.columns(3)
    e1.metric("Live candle range / ATR", f"{engine.get('atr_ratio', 0):.2f}x")
    e2.metric("Body / range", f"{engine.get('body_ratio', 0):.0%}")
    e3.metric("Tick speed", f"{engine.get('speed', 0):+.3f}/s")
    if engine.get("reasons"):
        st.caption("Why: " + " • ".join(engine["reasons"]))

    with st.expander("📡 Data health / freshness"):
        st.write(f"WebSocket connected: **{snap['connected']}**")
        st.write(f"Last tick age: **{last_tick_age:.1f}s**" if last_tick_age is not None else "Last tick age: —")
        st.write(f"Displayed price source: **{price_source}**")
        for label, df in [("1H REST", h1), ("15M REST", m15), ("5M REST / 1M→5M fallback", rest5), ("Live 5M", live5)]:
            if not df.empty:
                ts = df.time.iloc[-1]
                age = (datetime.now(timezone.utc) - ts.to_pydatetime()).total_seconds() / 60
                st.write(f"{label}: {ts.strftime('%Y-%m-%d %H:%M UTC')} • {age:.1f} min old")
            else:
                st.write(f"{label}: unavailable")
        if snap["error"]:
            st.warning(snap["error"])

        # Pass the live fragment's diagnostic snapshot to the stable Full Pro section below.
        st.session_state["_full_pro_diagnostics"] = {
            "regime": regime,
            "equal_liquidity": equal_liquidity,
            "aplus_gate": aplus_gate,
        }

# Fragment reruns only the live dashboard. No full-page refresh loop.
live_dashboard()

# These are computed inside the fragment, but the Full Pro diagnostics are intentionally
# rendered outside it so backtest/Telegram controls do not refresh on every live tick.
_full_pro_diagnostics = st.session_state.get("_full_pro_diagnostics", {})
regime = _full_pro_diagnostics.get("regime", {"regime": "UNKNOWN", "score": 0, "adx": None})
equal_liquidity = _full_pro_diagnostics.get("equal_liquidity", {
    "eq_high": None, "eq_low": None, "sweep_high": False, "sweep_low": False,
})
aplus_gate = _full_pro_diagnostics.get("aplus_gate", {"signal": "NO TRADE", "score": 0, "reasons": [], "missing": []})

# Research/backtest UI intentionally separated from the live dashboard in V2.


if view_mode == "🧭 Full Pro — All Tools":

    render_london_killzone_backtest_ui()
    st.divider()

    st.subheader("🧭 Market Regime & Equal-High/Low Liquidity")
    rg1, rg2, rg3 = st.columns(3)
    rg1.metric("Market Regime", regime.get("regime", "UNKNOWN"))
    rg2.metric("Regime Strength", f"{regime.get('score', 0)}/100")
    rg3.metric("5M ADX", f"{regime.get('adx', 0):.1f}" if regime.get("adx") is not None else "—")
    lq1, lq2, lq3, lq4 = st.columns(4)
    lq1.metric("Equal High", f"{equal_liquidity['eq_high']:,.2f}" if equal_liquidity.get("eq_high") is not None else "Unavailable")
    lq2.metric("Equal Low", f"{equal_liquidity['eq_low']:,.2f}" if equal_liquidity.get("eq_low") is not None else "Unavailable")
    lq3.metric("Sweep High", "YES" if equal_liquidity.get("sweep_high") else "NO")
    lq4.metric("Sweep Low", "YES" if equal_liquidity.get("sweep_low") else "NO")
    st.caption("These are candle-derived estimates. They do not represent live COMEX order-book depth and do not independently authorize an entry.")
    st.subheader("🧠 A+ Signal Gate — diagnostic only")
    ga, gb = st.columns(2)
    ga.metric("Gate Status", aplus_gate.get("signal", "NO TRADE"))
    gb.metric("Gate Score", f"{aplus_gate.get('score', 0)}/100")
    st.write("Aligned: " + (" • ".join(aplus_gate.get("reasons", [])) or "—"))
    st.write("Missing: " + (" • ".join(aplus_gate.get("missing", [])) or "—"))
    st.caption("This separate checklist does not override the existing Smart Flow/Order Block confirmation or authorize automatic orders.")

    # Forward / Paper Testing
    st.subheader("📋 Forward / Paper Trading")
    st.caption("Live signals are recorded here without placing broker orders. This replaces the unstable OOS validation.")
    trades = _paper_trades()
    closed = [t for t in trades if t.get("status") != "OPEN"]
    open_trades = [t for t in trades if t.get("status") == "OPEN"]
    if closed:
        wins = sum(1 for t in closed if t.get("status") == "TP1")
        losses = sum(1 for t in closed if t.get("status") == "LOSS")
        total_r = sum(float(t.get("r") or 0) for t in closed)
        pf_num = sum(max(float(t.get("r") or 0),0) for t in closed)
        pf_den = -sum(min(float(t.get("r") or 0),0) for t in closed)
        pf = pf_num / pf_den if pf_den else (999.0 if pf_num else 0.0)
        f1,f2,f3,f4 = st.columns(4)
        f1.metric("Closed", len(closed))
        f2.metric("Win %", f"{(wins/len(closed)*100):.1f}%")
        f3.metric("Paper PF", f"{pf:.2f}")
        f4.metric("Net R", f"{total_r:+.1f}")
    else:
        st.info("No A+ paper trades recorded yet. The log starts only when Smart Flow ≥70 AND Early Engine ≥70.")
    if open_trades:
        st.write(f"🟡 Open paper trades: **{len(open_trades)}**")
    if trades:
        rows=[]
        for t in reversed(trades[-20:]):
            rows.append({
                "Time": t["time"].strftime("%d-%m %H:%M UTC"),
                "Side": t["side"],
                "Entry": round(t["entry"],2),
                "SL": round(t["sl"],2),
                "TP1": round(t["tp1"],2),
                "Signal": t["signal_score"],
                "Early": t["early_score"],
                "Status": t["status"],
                "R": t.get("r"),
            })
        st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)
    if st.button("🗑️ Clear paper-trading log"):
        st.session_state["paper_trades"] = []
        st.rerun()


    # Telegram status
    # Get a lightweight current price for the manual Telegram test without
    # depending on a variable inside the live fragment.
    test_price = None
    try:
        _q = get_live_quote(SIFTING_KEY)
        if _q and _q.get("price") is not None:
            test_price = float(_q["price"])
    except Exception:
        pass
    if test_price is None:
        test_price = 0.0

    with st.expander("🔔 Telegram A/A+ alerts"):
        if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
            st.success("Telegram configuration loaded.")
            st.caption("Alerts are sent only when Smart Flow ≥70 AND Early Engine ≥70. Duplicate states are throttled.")

            if st.button("🔎 Check Telegram connection"):
                ok, result = telegram_diagnostics()
                st.session_state["telegram_diag"] = (ok, result)

            if st.button("📩 Send Telegram test alert"):
                ok, result = telegram_send(
                    f"🟡 Smart Flow Gold TEST\nXAUUSD: {test_price:.2f}\nTelegram connection is working."
                )
                st.session_state["telegram_test"] = (ok, result)

            if "telegram_diag" in st.session_state:
                ok, result = st.session_state["telegram_diag"]
                (st.success if ok else st.error)(result)
            if "telegram_test" in st.session_state:
                ok, result = st.session_state["telegram_test"]
                (st.success if ok else st.error)(result)
        else:
            st.warning("Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to Streamlit Secrets.")

    st.divider()
    st.caption(
        "Institutional Engine uses rule-based confluence. XAUUSD volume/order-flow are proxies, not COMEX order-book data. "
        "Alerts are not guaranteed predictions or automatic orders; forward-test before real-money use."
    )


# ============================================================
# Separate Turtle Trading module (does not alter Smart Flow signals)
# ============================================================
def turtle_metrics(daily: pd.DataFrame):
    """Calculate classic 20-day breakout / 10-day exit and Wilder-style ATR proxy.

    Signal levels use only completed prior daily candles to avoid look-ahead.
    """
    if daily is None or daily.empty or len(daily) < 22:
        return {"ok": False, "reason": "At least 22 daily candles are required; daily history unavailable or insufficient."}
    d = daily.copy().sort_values("time").drop_duplicates("time").reset_index(drop=True)
    for c in ("open", "high", "low", "close"):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["high", "low", "close"])
    if len(d) < 22:
        return {"ok": False, "reason": "Insufficient valid daily candles."}
    prev_close = d["close"].shift(1)
    tr = pd.concat([d["high"]-d["low"], (d["high"]-prev_close).abs(), (d["low"]-prev_close).abs()], axis=1).max(axis=1)
    atr20 = float(tr.rolling(20).mean().iloc[-1])
    last = d.iloc[-1]
    # Exclude current daily candle from channel levels; avoids moving levels intraday.
    prior = d.iloc[:-1]
    high20 = float(prior["high"].tail(20).max())
    low10 = float(prior["low"].tail(10).min())
    close = float(last["close"])
    prev_close_value = float(prior["close"].iloc[-1])
    signal = "WAIT"
    if close > high20 and prev_close_value <= high20:
        signal = "BUY BREAKOUT"
    elif close < low10:
        signal = "EXIT LONG"
    return {"ok": True, "close": close, "high20": high20, "low10": low10,
            "atr20": atr20, "signal": signal, "last_time": last["time"],
            "bars": len(d), "source": "SiftingIO daily candles"}


def turtle_position_size(account_equity: float, entry: float, atr_value: float,
                          point_value_per_unit: float, risk_pct: float = 1.0,
                          atr_multiple: float = 2.0):
    """Return conservative risk budget and size; point_value is user-supplied contract value."""
    vals = [account_equity, entry, atr_value, point_value_per_unit, risk_pct, atr_multiple]
    if any(not np.isfinite(float(v)) for v in vals) or account_equity <= 0 or entry <= 0 or atr_value <= 0 or point_value_per_unit <= 0:
        return {"ok": False, "reason": "Enter positive valid account, ATR and contract point-value inputs."}
    risk_pct = min(max(float(risk_pct), 0.0), 5.0)  # configurable staging ceiling: 5%; no 1% hard cap
    stop_distance = atr_value * atr_multiple
    risk_budget = float(account_equity) * risk_pct / 100.0
    risk_per_unit = stop_distance * float(point_value_per_unit)
    if risk_per_unit <= 0:
        return {"ok": False, "reason": "Invalid per-unit risk."}
    quantity = risk_budget / risk_per_unit
    return {"ok": True, "risk_budget": risk_budget, "stop_distance": stop_distance,
            "stop_loss": float(entry) - stop_distance, "risk_per_unit": risk_per_unit,
            "quantity": quantity, "risk_pct": risk_pct}


# This module is intentionally separate from Smart Flow BUY/SELL scoring.
with st.expander("🐢 Turtle Trading Strategy — separate module", expanded=False):
    st.caption("20-day breakout entry • 10-day low exit • 2×ATR stop-distance default • configurable account risk (up to 5% in staging). No broker orders are placed.")
    daily = pd.DataFrame()
    turtle_result = {"ok": False, "reason": "Daily candle history not loaded."}
    try:
        daily = get_bars(SIFTING_KEY, "1d", 120)
        turtle_result = turtle_metrics(daily)
    except Exception as _turtle_error:
        turtle_result = {"ok": False, "reason": f"Daily data unavailable: {_turtle_error}"}
    if turtle_result.get("ok"):
        tc1, tc2, tc3, tc4 = st.columns(4)
        tc1.metric("Turtle Status", turtle_result["signal"])
        tc2.metric("20-day High", f'{turtle_result["high20"]:,.2f}')
        tc3.metric("10-day Low Exit", f'{turtle_result["low10"]:,.2f}')
        tc4.metric("ATR(20)", f'{turtle_result["atr20"]:,.2f}')
        st.caption(f'Daily candles: {turtle_result["bars"]} • Last candle: {turtle_result["last_time"]} • Source: {turtle_result["source"]}. Levels are based on prior completed daily candles.')
        with st.form("turtle_risk_form"):
            tr1, tr2 = st.columns(2)
            equity = tr1.number_input("Account equity (account currency)", min_value=1.0, value=100000.0, step=5000.0)
            point_value = tr2.number_input("Value of a 1.00 price move per 1 unit/lot (verify broker contract)", min_value=0.000001, value=1.0, step=0.1, format="%.6f")
            tr3, tr4 = st.columns(2)
            risk_pct = tr3.number_input("Risk per trade (%) — configurable, max 5%", min_value=0.1, max_value=5.0, value=1.0, step=0.1)
            atr_mult = tr4.number_input("ATR stop multiple", min_value=0.5, max_value=5.0, value=2.0, step=0.5)
            calculate_turtle = st.form_submit_button("Calculate Turtle position size")
        if calculate_turtle:
            sizing = turtle_position_size(equity, turtle_result["close"], turtle_result["atr20"], point_value, risk_pct, atr_mult)
            if sizing.get("ok"):
                sr1, sr2, sr3 = st.columns(3)
                sr1.metric("Max planned risk", f'{sizing["risk_budget"]:,.2f}')
                sr2.metric("ATR stop distance", f'{sizing["stop_distance"]:,.2f}')
                sr3.metric("Estimated size", f'{sizing["quantity"]:,.6f} units')
                st.write(f'Illustrative stop level from latest close: **{sizing["stop_loss"]:,.2f}**')
                st.caption(f'Estimated risk per unit: {sizing["risk_per_unit"]:,.4f}. Confirm contract specifications, tick value, leverage, slippage and currency conversion with your broker before using this size.')
            else:
                st.warning(sizing.get("reason", "Could not calculate size."))
    else:
        st.warning(turtle_result.get("reason", "Turtle daily data unavailable."))
        st.caption("The Turtle module will not fabricate levels when genuine daily candles are unavailable. Check SiftingIO's support for the 1d interval/API permissions.")

# COMEX Open Interest / COT: official CFTC report only; not a live order-book feed.
CFTC_DISAGG_URL = "https://www.cftc.gov/dea/newcot/f_disagg.txt"
@st.cache_data(ttl=900, show_spinner=False)
def get_comex_gold_cot():
    try:
        import csv
        r = requests.get(CFTC_DISAGG_URL, timeout=15, headers={"User-Agent":"Smart-Flow-Gold/1.0"})
        r.raise_for_status()
        found = None
        for line in r.text.splitlines():
            if line.startswith('"GOLD - COMMODITY EXCHANGE INC."'):
                found = next(csv.reader([line])); break
        if not found or len(found) < 71:
            raise ValueError("Official COMEX Gold CFTC row unavailable or unexpected format")
        # CFTC Disaggregated Futures-Only report columns, validated by row position.
        def num(i):
            try: return float(str(found[i]).strip())
            except Exception: return float("nan")
        oi, mm_long, mm_short = num(7), num(13), num(14)
        change_oi, chg_mm_long, chg_mm_short = num(55), num(61), num(62)
        if not np.isfinite(oi) or not np.isfinite(mm_long) or not np.isfinite(mm_short):
            raise ValueError("CFTC report returned non-numeric required fields")
        net, net_change = mm_long-mm_short, chg_mm_long-chg_mm_short
        bias = "BULLISH" if net > 0 and net_change > 0 else "BEARISH" if net < 0 and net_change < 0 else "NEUTRAL"
        return {"ok":True,"date":found[2],"open_interest":oi,"change_oi":change_oi,
                "mm_long":mm_long,"mm_short":mm_short,"mm_net":net,"mm_net_change":net_change,
                "bias":bias,"source":"CFTC Disaggregated Futures-Only COMEX Gold"}
    except Exception as e:
        return {"ok":False,"bias":"UNAVAILABLE","error":str(e),"source":"Official CFTC report"}

with st.expander("🏛️ COMEX Open Interest & COT — official data only", expanded=False):
    st.caption("Weekly CFTC futures positioning, not live COMEX order-book data. Unavailable fields remain unavailable; no synthetic values are used.")
    cot = get_comex_gold_cot()
    if cot.get("ok"):
        cc1,cc2,cc3,cc4,cc5 = st.columns(5)
        cc1.metric("Open Interest", f'{cot["open_interest"]:,.0f}')
        cc2.metric("OI change", f'{cot["change_oi"]:+,.0f}' if np.isfinite(cot["change_oi"]) else "Unavailable")
        cc3.metric("Managed Money Net", f'{cot["mm_net"]:+,.0f}')
        cc4.metric("Net change", f'{cot["mm_net_change"]:+,.0f}' if np.isfinite(cot["mm_net_change"]) else "Unavailable")
        cc5.metric("COT Bias", cot["bias"])
        st.caption(f'Report date: {cot["date"]} • Source: {cot["source"]}')
    else:
        st.warning(f'Official COT/OI data unavailable: {cot.get("error", "unknown error")}')
