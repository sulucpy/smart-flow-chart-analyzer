
import json
import threading
import time
from collections import deque
from datetime import datetime, timezone, timedelta

import numpy as np
import pandas as pd
import requests
import streamlit as st
import websocket
from streamlit_autorefresh import st_autorefresh

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
def get_secret(name: str, default: str = "") -> str:
    try:
        value = st.secrets.get(name, default)
        return str(value) if value is not None else default
    except Exception:
        return default


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
                self.ticks.append((ts_ms, price))
                self._update_candle(price, ts_ms)
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
                with self.lock:
                    self.connected = True
                    self.last_error = ""

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


@st.cache_resource(show_spinner=False)
def get_alert_state():
    return {"last_key": "", "last_sent": 0.0, "last_result": ""}


# -----------------------------
# REST historical data
# -----------------------------
@st.cache_data(ttl=15, show_spinner=False)
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
    r = requests.get(url, params=params, headers=headers, timeout=15)
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
# Live REST quote fallback
# -----------------------------
@st.cache_data(ttl=2, show_spinner=False)
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
            timeout=8,
        )
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
    if len(df) < 25:
        return {"sweep": False, "choch": False, "bos_bull": False, "bos_bear": False}
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
    x = enrich(m5)
    if x.empty:
        return None
    a = float(x.atr.iloc[-1])
    if signal == "BUY":
        entry = price
        sl = price - 1.15 * a
        risk = entry - sl
        return {
            "entry": entry, "sl": sl,
            "tp1": entry + 1.0 * risk,
            "tp2": entry + 1.7 * risk,
            "tp3": entry + 2.4 * risk,
            "tp4": entry + 3.2 * risk,
        }
    if signal == "SELL":
        entry = price
        sl = price + 1.15 * a
        risk = sl - entry
        return {
            "entry": entry, "sl": sl,
            "tp1": entry - 1.0 * risk,
            "tp2": entry - 1.7 * risk,
            "tp3": entry - 2.4 * risk,
            "tp4": entry - 3.2 * risk,
        }
    return None


# -----------------------------
# Telegram
# -----------------------------
def telegram_send(text):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return False, "Telegram secrets not configured."

    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "disable_web_page_preview": True,
    }
    try:
        r = requests.post(url, json=payload, timeout=10)
        if r.ok:
            return True, "sent"
        return False, f"Telegram HTTP {r.status_code}: {r.text[:180]}"
    except Exception as e:
        return False, str(e)[:180]


def maybe_alert(signal, engine, price, levels):
    # Only high-quality signals. This is an alert, not an automatic order.
    alert_state = get_alert_state()
    if signal["signal"] not in ("BUY", "SELL"):
        return
    if signal["score"] < 70:
        return
    if engine["score"] < 70:
        return

    key = f'{signal["signal"]}|{round(price,2)}|{engine["status"]}|{signal["score"]}'
    now = time.time()

    # Avoid repeated alerts for the same state for 10 minutes.
    if alert_state["last_key"] == key and now - alert_state["last_sent"] < 600:
        return

    if not levels:
        return

    emoji = "🟢" if signal["signal"] == "BUY" else "🔴"
    text = (
        f"{emoji} SMART FLOW GOLD — {signal['signal']} A+\n\n"
        f"XAUUSD: {price:.2f}\n"
        f"Signal score: {signal['score']}/100\n"
        f"Early score: {engine['score']}/100\n"
        f"Live engine: {engine['status']}\n"
        f"1H: {engine['h1trend']} | 15M: {engine['m15trend']}\n\n"
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
st.title("🟡 Smart Flow Gold — Live XAUUSD")
st.caption("Live WebSocket • Smart Flow • Big Candle / Early Entry • Support/Resistance • Telegram A+ Alerts")

with st.sidebar:
    st.header("⚙️ Connection")
    if SIFTING_KEY:
        st.success("SiftingIO key: loaded from Secrets")
    else:
        SIFTING_KEY = st.text_input("SiftingIO API key", type="password")

    st.markdown("**Telegram Secrets**")
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        st.success("🔔 Telegram A/A+ alerts: ON")
    else:
        st.warning("Telegram alerts are OFF")
        st.caption("Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID in Streamlit Secrets.")

    refresh = st.slider("Screen refresh (seconds)", 1, 10, 2)
    st.caption("A/A+ alert rule: Smart Flow ≥70 AND Early Engine ≥70.")
    st.info("🟢 Support / 🔴 Resistance are rule-based pivot estimates, not a private TradingView indicator copy.")
    st.caption("SiftingIO XAUUSD is an aggregated/reference price feed. Test before real-money use.")

if not SIFTING_KEY:
    st.error("Add SIFTINGIO_API_KEY in Streamlit Secrets first.")
    st.stop()

st_autorefresh(interval=refresh * 1000, key="gold_refresh")

feed = get_feed(SIFTING_KEY)
snap = feed.snapshot()

# History
try:
    h1 = get_bars(SIFTING_KEY, "1h", 260)
    m15 = get_bars(SIFTING_KEY, "15m", 300)
    rest5 = get_bars(SIFTING_KEY, "5m", 300)

    # SiftingIO may occasionally return a stale 5M page even while the
    # live WebSocket is fresh. Rebuild 5M history from recent 1M bars
    # whenever the 5M endpoint is materially behind the live clock.
    if not rest5.empty:
        rest5_age_min = (datetime.now(timezone.utc) - rest5.time.iloc[-1].to_pydatetime()).total_seconds() / 60
    else:
        rest5_age_min = float("inf")

    if rest5.empty or rest5_age_min > 15:
        one_min = get_bars(SIFTING_KEY, "1m", 2000)
        if not one_min.empty:
            one_min = one_min.set_index("time").sort_index()
            rest5_fallback = one_min.resample("5min", label="left", closed="left").agg({
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": "sum",
            }).dropna(subset=["open", "high", "low", "close"]).reset_index()
            rest5 = rest5_fallback.tail(350).reset_index(drop=True)
except Exception as e:
    st.error(f"SiftingIO history error: {e}")
    h1 = m15 = rest5 = pd.DataFrame()

live5 = make_live_df(snap)

# If local stream has not collected enough completed candles yet,
# use REST history plus the local current candle. Once live history
# grows, it naturally becomes the primary 5M input.
if not live5.empty and not rest5.empty:
    base5 = pd.concat([rest5, live5], ignore_index=True).drop_duplicates("time", keep="last").sort_values("time")
    base5 = base5.tail(350).reset_index(drop=True)
else:
    base5 = live5 if not live5.empty else rest5

if not h1.empty:
    h1e = enrich(h1)
else:
    h1e = h1
if not m15.empty:
    m15e = enrich(m15)
else:
    m15e = m15
if not base5.empty:
    m5e = enrich(base5)
else:
    m5e = base5

price = None
last_tick_age = None
price_source = "NONE"
quote = None

# Primary: live WebSocket tick.
if snap["last_tick"]:
    last_tick_age = max(0, time.time() - snap["last_tick"]["received"])
    # Treat the feed as truly live only while the most recent tick is fresh.
    if last_tick_age <= 8:
        price = float(snap["last_tick"]["price"])
        price_source = "WEBSOCKET"

# Fallback: latest REST quote while WebSocket is reconnecting.
# This keeps the displayed market price moving, but intentionally disables
# live signal generation because a REST snapshot is not a tick stream.
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
    st.stop()

feed_live = price_source == "WEBSOCKET" and last_tick_age is not None and last_tick_age <= 8

engine = early_engine(base5, h1, m15, price)
if feed_live:
    signal = smart_flow_signal(h1, m15, base5, engine)
    levels = trade_levels(signal["signal"], price, base5)
    # Auto Telegram only for A/A+ when the WebSocket feed is genuinely live.
    maybe_alert(signal, engine, price, levels)
else:
    signal = {"signal": "WAIT", "score": 0, "reason": "Live WebSocket is not fresh; waiting for live ticks."}
    levels = None

# Top metrics
c1, c2, c3, c4 = st.columns(4)
c1.metric("XAUUSD LIVE", f"{price:,.2f}")
c2.metric("1H Bias", trend_state(h1e) if not h1e.empty else "—")
c3.metric("15M Setup", signal["signal"], f"score {signal['score']}")
c4.metric("5M Confirm", trend_state(m5e) if not m5e.empty else "—")

st.caption(f"Price source: **{price_source}** • Display price is a SiftingIO aggregated/reference XAUUSD price.")

if feed_live:
    age_txt = f"{last_tick_age:.1f}s ago"
    st.success(f"🟢 LIVE WebSocket • tick {age_txt}")
elif price_source == "REST_QUOTE_FALLBACK":
    st.warning("🟡 WebSocket reconnecting — price shown from live REST quote. Signals/alerts are paused until ticks resume.")
elif price_source == "HISTORICAL_FALLBACK":
    st.error("🔴 Live feed unavailable — historical price only. Signals/alerts are paused.")
else:
    st.warning(f"🟠 WebSocket reconnecting… {snap['error']}")

# Smart Flow alert card
if signal["signal"] == "WAIT":
    st.warning(f"🟡 WAIT — {signal['reason']} | Best score {signal['score']}/100")
    st.info("Trade levels stay hidden while Smart Flow is WAIT.")
else:
    st.success(f"{'🟢 BUY' if signal['signal']=='BUY' else '🔴 SELL'} — score {signal['score']}/100")
    if levels:
        cols = st.columns(6)
        cols[0].metric("ENTRY", f"{levels['entry']:,.2f}")
        cols[1].metric("SL", f"{levels['sl']:,.2f}")
        cols[2].metric("TP1", f"{levels['tp1']:,.2f}")
        cols[3].metric("TP2", f"{levels['tp2']:,.2f}")
        cols[4].metric("TP3", f"{levels['tp3']:,.2f}")
        cols[5].metric("TP4", f"{levels['tp4']:,.2f}")

# Checklist
st.subheader("Smart Flow Checklist")
cc1, cc2, cc3, cc4, cc5 = st.columns(5)
cc1.metric("1H trend", trend_state(h1e) if not h1e.empty else "—")
s5 = structure(m5e) if not m5e.empty else {}
cc2.metric("Sweep", "YES" if s5.get("sweep") else "NO")
cc3.metric("CHoCH", "YES" if s5.get("choch") else "NO")
cc4.metric("BOS", "UP" if s5.get("bos_bull") else "DOWN" if s5.get("bos_bear") else "NO")
cc5.metric("ADX", f"{m5e.adx.iloc[-1]:.1f}" if not m5e.empty else "—")

# S/R
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

# Big Candle / Early Entry
st.subheader("⚡ Big Candle / Early Entry Engine")
ec1, ec2, ec3, ec4 = st.columns(4)
ec1.metric("Status", engine["status"])
ec2.metric("Direction", engine["direction"])
ec3.metric("Early Score", f"{engine['score']}/100")
ec4.metric("Quality", engine["quality"])

st.info(
    f"🔴/🟢 {engine['status']}  • "
    f"UP trigger {engine['up_trigger']:,.2f}  • "
    f"DOWN trigger {engine['down_trigger']:,.2f}"
)

e1, e2, e3 = st.columns(3)
e1.metric("Live candle range / ATR", f"{engine.get('atr_ratio', 0):.2f}x")
e2.metric("Body / range", f"{engine.get('body_ratio', 0):.0%}")
e3.metric("Tick speed", f"{engine.get('speed', 0):+.3f}/s")

if engine.get("reasons"):
    st.caption("Why: " + " • ".join(engine["reasons"]))

# Data health
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

# Telegram status
with st.expander("🔔 Telegram A/A+ alerts"):
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        st.success("Telegram configuration loaded.")
        st.caption("Alerts are sent only when Smart Flow ≥70 AND Early Engine ≥70. Duplicate states are throttled.")
        if st.button("Send Telegram test alert"):
            ok, result = telegram_send(
                f"🟡 Smart Flow Gold test\nXAUUSD: {price:.2f}\nTelegram connection is working."
            )
            if ok:
                st.success("Test alert sent. Check your Telegram bot.")
            else:
                st.error(result)
    else:
        st.warning("Add TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID to Streamlit Secrets.")

st.divider()
st.caption(
    "Alert system only — not guaranteed prediction and not automatic order execution. "
    "Backtest/forward-test before real-money use."
)
