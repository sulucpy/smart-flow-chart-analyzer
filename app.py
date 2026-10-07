
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

st.set_page_config(page_title="Smart Flow Gold — Live XAUUSD", page_icon="🟡", layout="wide")

API_BASE = "https://api.sifting.io"
WS_URL = "wss://stream.sifting.io/ws/v1"
SYMBOL = "XAUUSD"


# -----------------------------
# SiftingIO REST
# -----------------------------
def api_headers(key: str):
    return {"X-API-Key": key, "Accept-Encoding": "gzip"}


@st.cache_data(ttl=20, show_spinner=False)
def get_bars(api_key, interval, limit=500):
    url = f"{API_BASE}/v1/hist/commodities/{SYMBOL}/bars"

    # SiftingIO requires a start bound for the first historical-bars request.
    # Ask for a little more time than the requested bar count, then keep the
    # newest `limit` rows returned by the API.
    minutes_per_bar = {"5m": 5, "15m": 15, "30m": 30, "1h": 60, "1d": 1440}[interval]
    lookback_minutes = int(limit * minutes_per_bar * 1.15)
    start = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)

    r = requests.get(
        url,
        headers=api_headers(api_key),
        params={
            "start": start.isoformat().replace("+00:00", "Z"),
            "end": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "interval": interval,
            "limit": min(int(limit), 2000),
        },
        timeout=15,
    )
    r.raise_for_status()
    payload = r.json()
    rows = payload.get("data", payload if isinstance(payload, list) else [])
    if not rows:
        raise ValueError(f"No {interval} bars returned by SiftingIO.")
    df = pd.DataFrame(rows)

    # Flexible field handling
    rename = {}
    for c in df.columns:
        lc = c.lower()
        if lc in ("t", "timestamp", "time"):
            rename[c] = "time"
        elif lc == "o":
            rename[c] = "open"
        elif lc == "h":
            rename[c] = "high"
        elif lc == "l":
            rename[c] = "low"
        elif lc == "c":
            rename[c] = "close"
        elif lc == "v":
            rename[c] = "volume"
    df = df.rename(columns=rename)

    required = ["time", "open", "high", "low", "close"]
    missing = [x for x in required if x not in df.columns]
    if missing:
        raise ValueError(f"Unexpected bars response; missing: {missing}")

    if np.issubdtype(df["time"].dtype, np.number):
        df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    else:
        df["time"] = pd.to_datetime(df["time"], utc=True, errors="coerce")

    for c in ["open", "high", "low", "close"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    if "volume" in df.columns:
        df["volume"] = pd.to_numeric(df["volume"], errors="coerce").fillna(0)
    else:
        df["volume"] = 0

    df = df.dropna(subset=required).sort_values("time").drop_duplicates("time").reset_index(drop=True)
    return df


# -----------------------------
# Live WebSocket feed
# -----------------------------
class LiveGoldFeed:
    def __init__(self, key):
        self.key = key
        self.price = None
        self.bid = None
        self.ask = None
        self.ts = None
        self.status = "starting"
        self.error = None
        self._stop = False
        self.ticks = deque(maxlen=240)  # recent live ticks for intra-candle momentum
        self._lock = threading.Lock()
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()

    def snapshot(self):
        with self._lock:
            return {
                "price": self.price,
                "bid": self.bid,
                "ask": self.ask,
                "ts": self.ts,
                "status": self.status,
                "error": self.error,
                "ticks": list(self.ticks),
            }

    def _run(self):
        while not self._stop:
            try:
                self.status = "connecting"
                ws = websocket.create_connection(
                    f"{WS_URL}?key={self.key}",
                    timeout=10,
                    origin="https://stream.sifting.io",
                )
                ws.send(json.dumps({
                    "op": "subscribe",
                    "product": "com",
                    "symbols": [SYMBOL],
                }))
                last_ping = time.time()
                self.status = "live"
                self.error = None

                while not self._stop:
                    if time.time() - last_ping > 45:
                        ws.send(json.dumps({"op": "ping"}))
                        last_ping = time.time()

                    ws.settimeout(3)
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue

                    if not raw:
                        break

                    msg = json.loads(raw)
                    if msg.get("f") == "tick" and msg.get("s") == SYMBOL:
                        with self._lock:
                            self.price = float(msg.get("p")) if msg.get("p") is not None else self.price
                            self.bid = float(msg.get("b")) if msg.get("b") is not None else self.bid
                            self.ask = float(msg.get("a")) if msg.get("a") is not None else self.ask
                            self.ts = int(msg.get("t")) if msg.get("t") is not None else None
                            if self.price is not None and self.ts is not None:
                                self.ticks.append((self.ts, self.price))
                            self.status = "live"
                    elif msg.get("f") == "error":
                        self.error = msg.get("message", msg.get("code", "WebSocket error"))

                try:
                    ws.close()
                except Exception:
                    pass
            except Exception as e:
                self.status = "reconnecting"
                self.error = str(e)
                time.sleep(3)


@st.cache_resource
def live_feed(api_key):
    return LiveGoldFeed(api_key)


def build_live_5m_candle(base_df, ticks):
    """Build the active 5-minute candle from live WebSocket ticks.

    Historical REST bars are still used for context/indicators, but the active
    candle is replaced/added from ticks so the early-entry engine does not wait
    for a delayed REST OHLC update.
    """
    if not ticks:
        return base_df.copy(), None

    rows = []
    for ts, px in ticks:
        try:
            t = pd.to_datetime(float(ts), unit="ms", utc=True)
            if pd.isna(t):
                continue
            rows.append((t, float(px)))
        except Exception:
            continue
    if not rows:
        return base_df.copy(), None

    tick_df = pd.DataFrame(rows, columns=["time", "price"])
    tick_df["bar_time"] = tick_df["time"].dt.floor("5min")
    current_bar = tick_df["bar_time"].iloc[-1]
    cur = tick_df[tick_df["bar_time"] == current_bar].sort_values("time")
    if cur.empty:
        return base_df.copy(), None

    o = float(cur.price.iloc[0])
    h = float(cur.price.max())
    l = float(cur.price.min())
    c = float(cur.price.iloc[-1])
    v = float(len(cur))

    live_row = pd.DataFrame([{
        "time": current_bar,
        "open": o,
        "high": h,
        "low": l,
        "close": c,
        "volume": v,
    }])

    x = base_df.copy()
    if "time" in x.columns:
        x["time"] = pd.to_datetime(x["time"], utc=True)
    x = x[x["time"] != current_bar].copy()
    x = pd.concat([x, live_row], ignore_index=True).sort_values("time").drop_duplicates("time", keep="last")
    return x.reset_index(drop=True), current_bar


def live_candle_stats(live_df, ticks):
    if live_df is None or live_df.empty:
        return {"body": 0.0, "range": 0.0, "body_pct": 0.0, "speed": 0.0, "age": None}
    a = live_df.iloc[-1]
    body = float(a.close - a.open)
    rng = max(float(a.high - a.low), 1e-9)
    body_pct = abs(body) / rng
    speed = 0.0
    age = None
    if ticks and len(ticks) >= 5:
        try:
            t_now, p_now = ticks[-1]
            t_old, p_old = ticks[max(0, len(ticks)-10)]
            dt = max((float(t_now)-float(t_old))/1000.0, 0.5)
            speed = (float(p_now)-float(p_old)) / dt
            age = max(0.0, (time.time()*1000.0 - float(t_now))/1000.0)
        except Exception:
            pass
    return {"body": body, "range": rng, "body_pct": body_pct, "speed": speed, "age": age}


# -----------------------------
# Indicators
# -----------------------------
def ema(s, n):
    return s.ewm(span=n, adjust=False).mean()


def rsi(s, n=14):
    delta = s.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    avg_up = up.ewm(alpha=1/n, adjust=False).mean()
    avg_down = down.ewm(alpha=1/n, adjust=False).mean()
    rs = avg_up / avg_down.replace(0, np.nan)
    return 100 - (100 / (1 + rs))


def atr(df, n=14):
    prev = df["close"].shift(1)
    tr = pd.concat([
        df["high"] - df["low"],
        (df["high"] - prev).abs(),
        (df["low"] - prev).abs(),
    ], axis=1).max(axis=1)
    return tr.ewm(alpha=1/n, adjust=False).mean()


def adx(df, n=14):
    high, low, close = df["high"], df["low"], df["close"]
    up = high.diff()
    dn = -low.diff()
    plus_dm = pd.Series(np.where((up > dn) & (up > 0), up, 0.0), index=df.index)
    minus_dm = pd.Series(np.where((dn > up) & (dn > 0), dn, 0.0), index=df.index)
    prev = close.shift(1)
    tr = pd.concat([
        high-low, (high-prev).abs(), (low-prev).abs()
    ], axis=1).max(axis=1)
    atrv = tr.ewm(alpha=1/n, adjust=False).mean()
    plus_di = 100 * plus_dm.ewm(alpha=1/n, adjust=False).mean() / atrv.replace(0, np.nan)
    minus_di = 100 * minus_dm.ewm(alpha=1/n, adjust=False).mean() / atrv.replace(0, np.nan)
    dx = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
    return dx.ewm(alpha=1/n, adjust=False).mean(), plus_di, minus_di


def add_indicators(df):
    x = df.copy()
    x["ema20"] = ema(x.close, 20)
    x["ema50"] = ema(x.close, 50)
    x["ema200"] = ema(x.close, 200)
    x["rsi"] = rsi(x.close)
    x["atr"] = atr(x)
    x["adx"], x["plus_di"], x["minus_di"] = adx(x)

    # Session VWAP. XAUUSD is reference spot data; volume may be unavailable/zero,
    # so use typical-price VWAP only when positive volume exists.
    if x["volume"].sum() > 0:
        day = x["time"].dt.date
        tp = (x.high + x.low + x.close) / 3
        pv = tp * x.volume
        x["vwap"] = pv.groupby(day).cumsum() / x.volume.groupby(day).cumsum().replace(0, np.nan)
    else:
        x["vwap"] = x["close"].rolling(20).mean()

    return x


# -----------------------------
# Smart Flow structure
# -----------------------------
def structure_features(df, lookback=20):
    x = add_indicators(df)
    if len(x) < max(220, lookback + 10):
        return x, {"direction": "WAIT", "score": 0, "reason": "Not enough history"}

    a = x.iloc[-1]
    p = x.iloc[-2]
    recent = x.iloc[-lookback-1:-1]
    prior = x.iloc[-lookback*2:-lookback]

    swing_high = recent["high"].max()
    swing_low = recent["low"].min()
    prior_high = prior["high"].max() if len(prior) else swing_high
    prior_low = prior["low"].min() if len(prior) else swing_low

    # Candidate liquidity sweeps:
    bull_sweep = (a.low < swing_low) and (a.close > swing_low)
    bear_sweep = (a.high > swing_high) and (a.close < swing_high)

    # Simple CHoCH/BOS approximations using recent range breaks.
    bull_bos = a.close > swing_high
    bear_bos = a.close < swing_low
    bull_choch = (p.close < p.ema20) and (a.close > a.ema20)
    bear_choch = (p.close > p.ema20) and (a.close < a.ema20)

    # Red/green candidate zone: trend + EMA alignment + slope.
    bull_zone = a.close > a.ema20 > a.ema50 and a.ema20 > p.ema20
    bear_zone = a.close < a.ema20 < a.ema50 and a.ema20 < p.ema20

    if bull_zone and not bear_zone:
        zone = "GREEN"
    elif bear_zone and not bull_zone:
        zone = "RED"
    else:
        zone = "NEUTRAL"

    # Trend direction
    trend = "BULL" if a.ema20 > a.ema50 > a.ema200 else "BEAR" if a.ema20 < a.ema50 < a.ema200 else "MIXED"

    # Momentum
    momentum_bull = a.rsi >= 52 and a.plus_di > a.minus_di
    momentum_bear = a.rsi <= 48 and a.minus_di > a.plus_di
    strong = a.adx >= 20

    # Quality score: conservative by design.
    buy_score = 0
    sell_score = 0
    buy_reasons, sell_reasons = [], []

    if trend == "BULL":
        buy_score += 20; buy_reasons.append("EMA trend bullish")
    if trend == "BEAR":
        sell_score += 20; sell_reasons.append("EMA trend bearish")

    if zone == "GREEN":
        buy_score += 15; buy_reasons.append("green directional zone")
    if zone == "RED":
        sell_score += 15; sell_reasons.append("red directional zone")

    if momentum_bull:
        buy_score += 10; buy_reasons.append("RSI/DI bullish")
    if momentum_bear:
        sell_score += 10; sell_reasons.append("RSI/DI bearish")

    if strong:
        if a.plus_di > a.minus_di:
            buy_score += 10; buy_reasons.append("ADX trend strength")
        elif a.minus_di > a.plus_di:
            sell_score += 10; sell_reasons.append("ADX trend strength")

    if bull_sweep:
        buy_score += 15; buy_reasons.append("liquidity sweep")
    if bear_sweep:
        sell_score += 15; sell_reasons.append("liquidity sweep")

    if bull_choch:
        buy_score += 10; buy_reasons.append("CHoCH")
    if bear_choch:
        sell_score += 10; sell_reasons.append("CHoCH")

    if bull_bos:
        buy_score += 15; buy_reasons.append("BOS")
    if bear_bos:
        sell_score += 15; sell_reasons.append("BOS")

    # Avoid chasing overextended entries.
    buy_over = a.rsi > 72
    sell_over = a.rsi < 28
    if buy_over:
        buy_score -= 15
    if sell_over:
        sell_score -= 15

    if buy_score >= 70 and buy_score > sell_score + 10:
        direction = "BUY"
        score = min(100, max(0, buy_score))
        reasons = buy_reasons
    elif sell_score >= 70 and sell_score > buy_score + 10:
        direction = "SELL"
        score = min(100, max(0, sell_score))
        reasons = sell_reasons
    else:
        direction = "WAIT"
        score = max(buy_score, sell_score)
        reasons = buy_reasons if buy_score >= sell_score else sell_reasons

    return x, {
        "direction": direction,
        "score": int(score),
        "zone": zone,
        "trend": trend,
        "bull_sweep": bool(bull_sweep),
        "bear_sweep": bool(bear_sweep),
        "bull_choch": bool(bull_choch),
        "bear_choch": bool(bear_choch),
        "bull_bos": bool(bull_bos),
        "bear_bos": bool(bear_bos),
        "atr": float(a.atr),
        "rsi": float(a.rsi),
        "adx": float(a.adx),
        "vwap": float(a.vwap) if pd.notna(a.vwap) else np.nan,
        "reasons": reasons,
        "swing_high": float(swing_high),
        "swing_low": float(swing_low),
    }


def support_resistance(df, pivot=3, max_levels=4):
    """Estimate horizontal support/resistance from confirmed pivot highs/lows.
    This is a rule-based approximation of the chart's red/green horizontal levels,
    not an exact copy of the private TradingView indicator.
    """
    x = df.copy().reset_index(drop=True)
    if len(x) < 2 * pivot + 10:
        return {"support": [], "resistance": [], "nearest_support": np.nan, "nearest_resistance": np.nan}

    atrv = float(atr(x, 14).iloc[-1]) if len(x) >= 20 else float((x.high - x.low).tail(14).mean())
    tol = max(atrv * 0.18, float(x.close.iloc[-1]) * 0.00035)

    highs, lows = [], []
    for i in range(pivot, len(x) - pivot):
        hi = float(x.high.iloc[i])
        lo = float(x.low.iloc[i])
        if hi >= float(x.high.iloc[i-pivot:i+pivot+1].max()):
            highs.append(hi)
        if lo <= float(x.low.iloc[i-pivot:i+pivot+1].min()):
            lows.append(lo)

    def cluster(levels):
        out = []
        for level in reversed(levels):
            found = False
            for j, existing in enumerate(out):
                if abs(level - existing) <= tol:
                    out[j] = (existing + level) / 2.0
                    found = True
                    break
            if not found:
                out.append(level)
            if len(out) >= max_levels * 3:
                break
        return sorted(out)

    supports = cluster(lows)
    resistances = cluster(highs)
    last = float(x.close.iloc[-1])
    supports = [v for v in supports if v < last]
    resistances = [v for v in resistances if v > last]

    supports = sorted(supports, reverse=True)[:max_levels]
    resistances = sorted(resistances)[:max_levels]
    return {
        "support": supports,
        "resistance": resistances,
        "nearest_support": supports[0] if supports else np.nan,
        "nearest_resistance": resistances[0] if resistances else np.nan,
        "atr": atrv,
    }


def sr_context(df, levels):
    last = float(df.close.iloc[-1])
    atrv = float(levels.get("atr", np.nan))
    ns = levels.get("nearest_support", np.nan)
    nr = levels.get("nearest_resistance", np.nan)
    support_dist = (last - ns) if np.isfinite(ns) else np.inf
    resistance_dist = (nr - last) if np.isfinite(nr) else np.inf
    near_support = np.isfinite(ns) and support_dist <= 1.0 * atrv
    near_resistance = np.isfinite(nr) and resistance_dist <= 1.0 * atrv
    return {"near_support": bool(near_support), "near_resistance": bool(near_resistance),
            "support_dist": support_dist, "resistance_dist": resistance_dist}


def build_trade_plan(x5, direction, atr_value):
    a = x5.iloc[-1]
    entry = float(a.close)
    atrv = max(float(atr_value), entry * 0.0008)

    if direction == "BUY":
        sl = min(float(x5["low"].tail(8).min()), entry - 1.2 * atrv)
        risk = max(entry - sl, 0.5 * atrv)
        tps = [entry + risk, entry + 2*risk, entry + 3*risk, entry + 4*risk]
    elif direction == "SELL":
        sl = max(float(x5["high"].tail(8).max()), entry + 1.2 * atrv)
        risk = max(sl - entry, 0.5 * atrv)
        tps = [entry - risk, entry - 2*risk, entry - 3*risk, entry - 4*risk]
    else:
        return None

    return {"entry": entry, "sl": sl, "tp1": tps[0], "tp2": tps[1], "tp3": tps[2], "tp4": tps[3]}



def big_candle_detector(m5, m15f, h1f, m5sr, snap):
    """Live expansion detector. Uses the active 5M candle built from ticks.
    This is an alert engine, not a guaranteed prediction.
    """
    price = snap.get("price")
    if price is None or len(m5) < 30:
        return {"status": "WAIT", "direction": "NEUTRAL", "score": 0,
                "reason": "Waiting for live ticks/history", "up_trigger": np.nan,
                "dn_trigger": np.nan, "body_atr": 0.0, "momentum": 0.0,
                "speed": 0.0, "live_candle": False}

    a = m5.iloc[-1]
    atrv = float(a.atr) if pd.notna(a.atr) else float(atr(m5, 14).iloc[-1])
    atrv = max(atrv, price * 0.0005)
    bar_open = float(a.open)
    body = float(price - bar_open)
    body_abs = abs(body)
    body_atr = body_abs / atrv

    prior = m5.iloc[-21:-1]
    prior_high = float(prior.high.max())
    prior_low = float(prior.low.min())
    nr = m5sr.get("nearest_resistance", np.nan)
    ns = m5sr.get("nearest_support", np.nan)

    ticks = snap.get("ticks") or []
    momentum = 0.0
    speed = 0.0
    if len(ticks) >= 4:
        try:
            p_now = float(ticks[-1][1])
            idx = max(0, len(ticks)-10)
            p_old = float(ticks[idx][1])
            dt = max((float(ticks[-1][0])-float(ticks[idx][0]))/1000.0, 0.5)
            momentum = p_now - p_old
            speed = momentum / dt
        except Exception:
            pass
    mom_atr = momentum / max(atrv, 1e-9)

    up_trigger = nr if np.isfinite(nr) and nr > price else prior_high
    dn_trigger = ns if np.isfinite(ns) and ns < price else prior_low
    up_dist = max(0.0, up_trigger - price)
    dn_dist = max(0.0, price - dn_trigger)

    bull = 0
    bear = 0
    bull_reasons, bear_reasons = [], []

    # Live candle direction and expansion.
    if body > 0:
        bull += 25; bull_reasons.append("live bullish candle")
    elif body < 0:
        bear += 25; bear_reasons.append("live bearish candle")
    if body_atr >= 0.15:
        if body > 0: bull += 10; bull_reasons.append("body expanding")
        elif body < 0: bear += 10; bear_reasons.append("body expanding")
    if body_atr >= 0.35:
        if body > 0: bull += 15; bull_reasons.append("strong body")
        elif body < 0: bear += 15; bear_reasons.append("strong body")

    # Tick momentum is deliberately small-weighted: it confirms pressure,
    # rather than creating a signal by itself.
    if momentum > 0 and mom_atr >= 0.01:
        bull += 15; bull_reasons.append("live tick momentum UP")
    elif momentum < 0 and mom_atr <= -0.01:
        bear += 15; bear_reasons.append("live tick momentum DOWN")

    if speed > 0 and abs(speed) >= atrv / 90:
        bull += 10; bull_reasons.append("price speed rising")
    elif speed < 0 and abs(speed) >= atrv / 90:
        bear += 10; bear_reasons.append("price speed falling")

    # Micro breakout: current live price versus recent completed candles.
    if price > prior_high:
        bull += 25; bull_reasons.append("micro-high breakout")
    if price < prior_low:
        bear += 25; bear_reasons.append("micro-low breakdown")

    if h1f.get("trend") == "BULL":
        bull += 10; bull_reasons.append("1H bullish bias")
    elif h1f.get("trend") == "BEAR":
        bear += 10; bear_reasons.append("1H bearish bias")

    if m15f.get("trend") == "BULL" or m15f.get("zone") == "GREEN":
        bull += 10; bull_reasons.append("15M bullish pressure")
    if m15f.get("trend") == "BEAR" or m15f.get("zone") == "RED":
        bear += 10; bear_reasons.append("15M bearish pressure")

    near_up = up_dist <= 0.20 * atrv
    near_dn = dn_dist <= 0.20 * atrv
    if near_up: bull += 10; bull_reasons.append("near upside trigger")
    if near_dn: bear += 10; bear_reasons.append("near downside trigger")

    # Anti-chase: if the candle is already very large, don't encourage entry.
    extended = body_atr >= 1.20
    if extended:
        return {"status": "EXTENDED", "direction": "UP" if body > 0 else "DOWN",
                "score": min(100, max(bull, bear)),
                "reason": "Move already >1.2 ATR — avoid chasing",
                "up_trigger": up_trigger, "dn_trigger": dn_trigger,
                "body_atr": body_atr, "momentum": momentum, "speed": speed,
                "live_candle": True}

    # Strong signal: require live expansion + direction, and avoid firing when
    # the opposite side has comparable pressure.
    if bull >= 75 and bull >= bear + 15 and (price >= up_trigger or body_atr >= 0.35):
        return {"status": "BUY NOW", "direction": "UP", "score": min(100, bull),
                "reason": " • ".join(bull_reasons[-5:]), "up_trigger": up_trigger,
                "dn_trigger": dn_trigger, "body_atr": body_atr, "momentum": momentum,
                "speed": speed, "live_candle": True}
    if bear >= 75 and bear >= bull + 15 and (price <= dn_trigger or body_atr >= 0.35):
        return {"status": "SELL NOW", "direction": "DOWN", "score": min(100, bear),
                "reason": " • ".join(bear_reasons[-5:]), "up_trigger": up_trigger,
                "dn_trigger": dn_trigger, "body_atr": body_atr, "momentum": momentum,
                "speed": speed, "live_candle": True}

    if bull >= 55 and bull > bear + 10:
        return {"status": "READY UP", "direction": "UP", "score": min(100, bull),
                "reason": " • ".join(bull_reasons[-4:]), "up_trigger": up_trigger,
                "dn_trigger": dn_trigger, "body_atr": body_atr, "momentum": momentum,
                "speed": speed, "live_candle": True}
    if bear >= 55 and bear > bull + 10:
        return {"status": "READY DOWN", "direction": "DOWN", "score": min(100, bear),
                "reason": " • ".join(bear_reasons[-4:]), "up_trigger": up_trigger,
                "dn_trigger": dn_trigger, "body_atr": body_atr, "momentum": momentum,
                "speed": speed, "live_candle": True}

    return {"status": "BUILDING" if max(bull, bear) >= 45 else "WAIT",
            "direction": "UP" if bull > bear + 5 else "DOWN" if bear > bull + 5 else "NEUTRAL",
            "score": min(100, max(bull, bear)),
            "reason": "Pressure developing — waiting for live confirmation",
            "up_trigger": up_trigger, "dn_trigger": dn_trigger,
            "body_atr": body_atr, "momentum": momentum, "speed": speed,
            "live_candle": True}


def build_early_plan(price, direction, atr_value, live_candle):
    if price is None or direction not in ("UP", "DOWN"):
        return None
    atrv = max(float(atr_value), float(price) * 0.0005)
    if direction == "UP":
        base = float(live_candle.get("low", price)) if live_candle else price
        sl = min(base, price - 0.9 * atrv)
        risk = max(price - sl, 0.6 * atrv)
        return {"entry": price, "sl": sl, "tp1": price+risk, "tp2": price+2*risk,
                "tp3": price+3*risk, "tp4": price+4*risk}
    base = float(live_candle.get("high", price)) if live_candle else price
    sl = max(base, price + 0.9 * atrv)
    risk = max(sl - price, 0.6 * atrv)
    return {"entry": price, "sl": sl, "tp1": price-risk, "tp2": price-2*risk,
            "tp3": price-3*risk, "tp4": price-4*risk}

def fmt(v):
    return f"{v:,.2f}" if v is not None and np.isfinite(v) else "—"


# -----------------------------
# UI
# -----------------------------
st.title("🟡 Smart Flow Gold — Live XAUUSD")
st.caption("Live reference-price dashboard • Early Big-Candle + Smart Flow engine • Not financial advice")

# API key: prefer Streamlit Secrets so the user only enters it once.
# Fallback to the sidebar only when the secret has not been configured yet.
def get_saved_api_key():
    try:
        key = st.secrets.get("SIFTINGIO_API_KEY", "")
        return str(key).strip() if key else ""
    except Exception:
        return ""

saved_api_key = get_saved_api_key()

with st.sidebar:
    st.header("Connection")
    if saved_api_key:
        api_key = saved_api_key
        st.success("🟢 SiftingIO key loaded from Streamlit Secrets")
    else:
        api_key = st.text_input(
            "SiftingIO API key",
            type="password",
            help="Temporary fallback. Add SIFTINGIO_API_KEY in Streamlit Secrets to stop entering it repeatedly.",
        )
        st.info("One-time setup: Streamlit → Manage app → Settings → Secrets → add SIFTINGIO_API_KEY.")
    refresh = st.slider("Screen refresh (seconds)", 1, 10, 2)
    bars_n = st.selectbox("Bars per timeframe", [250, 350, 500], index=2)
    st.markdown("**Signal rule:** score ≥ 70 and clear direction → signal; otherwise WAIT.")
    st.warning("Green = estimated support; Red = estimated resistance. Levels are derived from price pivots, not copied from the private TradingView indicator.")

if not api_key:
    st.info("👈 First add SIFTINGIO_API_KEY in Streamlit Secrets. After that, the app will connect automatically without asking for the key.")
    st.stop()

feed = live_feed(api_key)
snap = feed.snapshot()

# Fetch history only once per minute per exact input; cached.
try:
    h1 = get_bars(api_key, "1h", bars_n)
    m15 = get_bars(api_key, "15m", bars_n)
    m5 = get_bars(api_key, "5m", bars_n)
except Exception as e:
    st.error(f"SiftingIO data error: {e}")
    st.stop()

# Build the active 5M candle locally from live WebSocket ticks.
# This is the key live-early-entry upgrade: the engine no longer waits for
# SiftingIO's REST 5M OHLC bar to update.
m5_live_raw, live_bar_time = build_live_5m_candle(m5, snap.get("ticks") or [])
h1i, h1f = structure_features(h1)
m15i, m15f = structure_features(m15)
m5i, m5f = structure_features(m5_live_raw)

# Horizontal support/resistance levels (green support / red resistance).
h1sr = support_resistance(h1i)
m15sr = support_resistance(m15i)
m5sr = support_resistance(m5i)
m15sr_ctx = sr_context(m15i, m15sr)
m5sr_ctx = sr_context(m5i, m5sr)

# Multi-timeframe agreement
bias = "BULL" if h1f["trend"] == "BULL" else "BEAR" if h1f["trend"] == "BEAR" else "MIXED"
final = m15f["direction"]

if final == "BUY" and bias != "BULL":
    final = "WAIT"
if final == "SELL" and bias != "BEAR":
    final = "WAIT"

# 5M confirmation: require same side or at least matching zone/momentum.
if final == "BUY" and not (m5f["zone"] == "GREEN" or m5f["trend"] == "BULL"):
    final = "WAIT"
if final == "SELL" and not (m5f["zone"] == "RED" or m5f["trend"] == "BEAR"):
    final = "WAIT"

# Location filter: prefer BUY near support and SELL near resistance.
# A clean BOS can qualify even when price has already moved away from the level.
if final == "BUY" and not (m15sr_ctx["near_support"] or m15f["bull_bos"] or m5f["bull_bos"]):
    final = "WAIT"
if final == "SELL" and not (m15sr_ctx["near_resistance"] or m15f["bear_bos"] or m5f["bear_bos"]):
    final = "WAIT"

final_score = min(m15f["score"], h1f["score"] if h1f["score"] else m15f["score"], m5f["score"] if m5f["score"] else m15f["score"])

# Live price card
price = snap["price"]
if price is None:
    price = float(m5i.iloc[-1].close)

bigmove = big_candle_detector(m5i, m15f, h1f, m5sr, snap)
live_stats = live_candle_stats(m5i, snap.get("ticks") or [])
live_candle = {"open": float(m5i.iloc[-1].open), "high": float(m5i.iloc[-1].high),
               "low": float(m5i.iloc[-1].low), "close": float(m5i.iloc[-1].close)}
early_plan = build_early_plan(price, bigmove["direction"], float(m5i.iloc[-1].atr), live_candle) if bigmove["status"] in ("BUY NOW", "SELL NOW") else None

# Data-health guard: never present a trade setup as live when the OHLC bars
# are materially behind the live feed. Historical bars are still used for
# structure, while the WebSocket provides the current reference price.
now_utc = pd.Timestamp.now(tz="UTC")
interval_minutes = {"1h": 60, "15m": 15, "5m": 5}
bar_ages = {
    "1H": (now_utc - pd.Timestamp(h1.iloc[-1]["time"])).total_seconds() / 60.0,
    "15M": (now_utc - pd.Timestamp(m15.iloc[-1]["time"])).total_seconds() / 60.0,
    "5M": (now_utc - pd.Timestamp(m5.iloc[-1]["time"])).total_seconds() / 60.0,
}
live_age = None
if snap.get("ts"):
    live_age = max(0.0, (now_utc - pd.to_datetime(snap["ts"], unit="ms", utc=True)).total_seconds())

# A completed/in-progress REST bar can naturally be behind the live feed.
# The active 5M candle is rebuilt locally from WebSocket ticks, so a stale
# REST 5M bar must NOT block the live signal engine when the live candle is healthy.
stale_limits = {"1H": 75.0, "15M": 30.0}
live_5m_healthy = (
    live_bar_time is not None
    and live_stats.get("age") is not None
    and live_stats["age"] <= 15.0
    and price is not None
)
data_stale = (
    bar_ages["1H"] > stale_limits["1H"]
    or bar_ages["15M"] > stale_limits["15M"]
    or (bar_ages["5M"] > 15.0 and not live_5m_healthy)
)
if data_stale:
    final = "WAIT"


c1, c2, c3, c4 = st.columns(4)
c1.metric("XAUUSD LIVE", fmt(price))
c2.metric("1H Bias", bias)
c3.metric("15M Setup", m15f["direction"], f"score {m15f['score']}")
c4.metric("5M Confirm", m5f["trend"], f"zone {m5f['zone']}")

with st.expander("Data health / freshness", expanded=False):
    h1t = pd.Timestamp(h1.iloc[-1]["time"]).strftime("%H:%M UTC")
    m15t = pd.Timestamp(m15.iloc[-1]["time"]).strftime("%H:%M UTC")
    m5t = pd.Timestamp(m5.iloc[-1]["time"]).strftime("%H:%M UTC")
    live_txt = "—" if live_age is None else f"{live_age:.1f}s ago"
    st.write(f"Live WebSocket: **{live_txt}**")
    st.write(f"Latest REST bars: **1H {h1t}** ({bar_ages['1H']:.0f}m old) · **15M {m15t}** ({bar_ages['15M']:.0f}m old) · **5M {m5t}** ({bar_ages['5M']:.0f}m old)")
    if live_bar_time is not None and live_stats.get("age") is not None:
        st.write(f"Live 5M candle: **{pd.Timestamp(live_bar_time).strftime('%H:%M UTC')}** · tick age **{live_stats['age']:.1f}s**")
    if data_stale:
        st.error("Live signal guard triggered: higher-timeframe data or the live 5M candle is too old.")
    elif bar_ages["5M"] > 15.0 and live_5m_healthy:
        st.success("Live 5M candle is healthy; stale REST 5M bar is ignored for live signaling.")
    else:
        st.success("Market-data freshness is within the signal guard limits.")


st.subheader("Key Support / Resistance")

level_cols = st.columns(3)
with level_cols[0]:
    st.markdown("**1H Levels**")
    st.write("🟢 Support: " + (", ".join(fmt(v) for v in h1sr["support"][:3]) if h1sr["support"] else "—"))
    st.write("🔴 Resistance: " + (", ".join(fmt(v) for v in h1sr["resistance"][:3]) if h1sr["resistance"] else "—"))
with level_cols[1]:
    st.markdown("**15M Levels**")
    st.write("🟢 Support: " + (", ".join(fmt(v) for v in m15sr["support"][:3]) if m15sr["support"] else "—"))
    st.write("🔴 Resistance: " + (", ".join(fmt(v) for v in m15sr["resistance"][:3]) if m15sr["resistance"] else "—"))
with level_cols[2]:
    st.markdown("**5M Levels**")
    st.write("🟢 Support: " + (", ".join(fmt(v) for v in m5sr["support"][:3]) if m5sr["support"] else "—"))
    st.write("🔴 Resistance: " + (", ".join(fmt(v) for v in m5sr["resistance"][:3]) if m5sr["resistance"] else "—"))

location_text = []
if m15sr_ctx["near_support"]:
    location_text.append("15M price is near support")
if m15sr_ctx["near_resistance"]:
    location_text.append("15M price is near resistance")
if not location_text:
    location_text.append("Price is not currently at a key 15M level")
st.caption("S/R filter: " + " • ".join(location_text))

st.subheader("⚡ Big Candle / Early Entry Engine")
b1, b2, b3, b4 = st.columns(4)
b1.metric("Status", bigmove["status"])
b2.metric("Direction", bigmove["direction"])
b3.metric("Early Score", f'{bigmove["score"]}/100')
b4.metric("Live Body / ATR", f'{bigmove["body_atr"]:.2f}x')

if live_bar_time is not None:
    st.caption(f"🟢 LIVE 5M CANDLE • {pd.Timestamp(live_bar_time).strftime('%H:%M UTC')} • built from WebSocket ticks")

if bigmove["status"] == "BUY NOW":
    st.success(f'🟢 BIG CANDLE BUY NOW — {bigmove["score"]}/100 | {bigmove["reason"]}')
elif bigmove["status"] == "SELL NOW":
    st.error(f'🔴 BIG CANDLE SELL NOW — {bigmove["score"]}/100 | {bigmove["reason"]}')
elif bigmove["status"] == "READY UP":
    st.info(f'🟢 READY UP — Trigger {fmt(bigmove["up_trigger"])} | {bigmove["reason"]}')
elif bigmove["status"] == "READY DOWN":
    st.info(f'🔴 READY DOWN — Trigger {fmt(bigmove["dn_trigger"])} | {bigmove["reason"]}')
elif bigmove["status"] == "EXTENDED":
    st.warning(f'⚠️ MOVE ALREADY EXTENDED — {bigmove["reason"]}')
else:
    st.info(f'🟡 {bigmove["status"]} — {bigmove["reason"]}')

tr1, tr2, tr3 = st.columns(3)
tr1.metric("UP trigger", fmt(bigmove["up_trigger"]))
tr2.metric("DOWN trigger", fmt(bigmove["dn_trigger"]))
tr3.metric("Tick speed", f'{bigmove.get("speed", 0.0):+.3f}/s')

if bigmove["status"] in ("BUY NOW", "SELL NOW") and early_plan:
    st.markdown("**Early Entry Plan**")
    p1, p2, p3, p4, p5, p6 = st.columns(6)
    p1.metric("Entry", fmt(early_plan["entry"]))
    p2.metric("SL", fmt(early_plan["sl"]))
    p3.metric("TP1", fmt(early_plan["tp1"]))
    p4.metric("TP2", fmt(early_plan["tp2"]))
    p5.metric("TP3", fmt(early_plan["tp3"]))
    p6.metric("TP4", fmt(early_plan["tp4"]))

st.caption('The early engine and 5M confirmation use the live WebSocket-built 5M candle, so stale REST 5M bars do not block live signaling when the tick feed is healthy. This is an alert system, not a guaranteed prediction or automatic order.')

st.divider()

if final == "BUY":
    st.success(f"🟢 BUY — A+ FILTERED | Quality {final_score}/100")
elif final == "SELL":
    st.error(f"🔴 SELL — A+ FILTERED | Quality {final_score}/100")
else:
    reason = 'live-data freshness guard triggered' if data_stale else 'No clean multi-timeframe setup'
    st.warning(f"🟡 WAIT — {reason} | Best score {final_score}/100")

plan = build_trade_plan(m5i, final, m5f["atr"])
if plan:
    p1, p2, p3, p4, p5, p6 = st.columns(6)
    p1.metric("Entry", fmt(plan["entry"]))
    p2.metric("SL", fmt(plan["sl"]))
    p3.metric("TP1", fmt(plan["tp1"]))
    p4.metric("TP2", fmt(plan["tp2"]))
    p5.metric("TP3", fmt(plan["tp3"]))
    p6.metric("TP4", fmt(plan["tp4"]))
else:
    st.info("Trade levels are hidden while the system is WAIT.")

st.subheader("Smart Flow Checklist")
check_cols = st.columns(6)
checks = [
    ("1H trend", h1f["trend"]),
    ("Zone", m15f["zone"]),
    ("Sweep", "YES" if (m15f["bull_sweep"] or m15f["bear_sweep"]) else "NO"),
    ("CHoCH", "YES" if (m15f["bull_choch"] or m15f["bear_choch"]) else "NO"),
    ("BOS", "YES" if (m15f["bull_bos"] or m15f["bear_bos"]) else "NO"),
    ("ADX", f"{m15f['adx']:.1f}"),
]
for col, (label, val) in zip(check_cols, checks):
    col.metric(label, val)

st.subheader("Timeframe Snapshot")
tab1, tab2, tab3 = st.tabs(["1H", "15M", "5M"])
for tab, data, feat in [(tab1, h1i, h1f), (tab2, m15i, m15f), (tab3, m5i, m5f)]:
    with tab:
        last = data.iloc[-1]
        a, b, c, d, e = st.columns(5)
        a.metric("Close", fmt(last.close))
        b.metric("EMA20", fmt(last.ema20))
        c.metric("EMA50", fmt(last.ema50))
        d.metric("RSI", f"{last.rsi:.1f}")
        e.metric("ADX", f"{last.adx:.1f}")
        st.dataframe(
            data[["time","open","high","low","close","ema20","ema50","ema200","rsi","adx","vwap"]].tail(30),
            use_container_width=True,
            hide_index=True,
        )

st.caption(
    "Data: SiftingIO aggregated/reference XAUUSD. Green/Red S/R levels are rule-based pivot estimates; they are not an exact copy of the TradingView indicator. Big-Candle Engine builds the active 5M candle from live WebSocket ticks and can trigger before candle close; it must be backtested/forward-tested before real-money use."
)

# Lightweight browser refresh; WebSocket supplies price without REST polling.
time.sleep(0.05)
st.markdown(
    f"<script>setTimeout(function(){{window.location.reload();}}, {int(refresh*1000)});</script>",
    unsafe_allow_html=True,
)
