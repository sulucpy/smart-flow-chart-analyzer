
import json
import threading
import time
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



def big_move_detector(df, sr=None):
    """Conservative pre-expansion detector for 5M XAUUSD.
    It detects compression + directional pressure + nearby S/R; it does not
    predict a candle with certainty and does not itself create a trade signal.
    """
    x = add_indicators(df.copy()).dropna(subset=["atr", "adx", "rsi"]).reset_index(drop=True)
    if len(x) < 80:
        return {"state": "WAIT", "direction": "NEUTRAL", "score": 0, "reasons": []}

    a = x.iloc[-1]
    p = x.iloc[-2]
    tr = pd.concat([
        x["high"] - x["low"],
        (x["high"] - x["close"].shift(1)).abs(),
        (x["low"] - x["close"].shift(1)).abs(),
    ], axis=1).max(axis=1)
    recent_tr = float(tr.tail(8).mean())
    base_tr = float(tr.iloc[-40:-8].median())
    recent_atr = float(x["atr"].iloc[-1])

    # Compression: recent candles are materially quieter than the recent baseline.
    compression = base_tr > 0 and recent_tr < 0.82 * base_tr

    # Pressure: repeated closes near one side of each candle + EMA slope.
    body = (x["close"] - x["open"]).abs()
    rng = (x["high"] - x["low"]).replace(0, np.nan)
    close_pos = ((x["close"] - x["low"]) / rng).clip(0, 1)
    bull_pressure = float(close_pos.tail(5).mean()) >= 0.68 and a.ema20 > p.ema20 and a.plus_di >= a.minus_di
    bear_pressure = float(close_pos.tail(5).mean()) <= 0.32 and a.ema20 < p.ema20 and a.minus_di >= a.plus_di

    # Directional squeeze: current range is still below expansion territory.
    current_range = float(a.high - a.low)
    expansion_threshold = max(1.35 * recent_atr, 1.35 * base_tr)
    not_expanded = current_range < expansion_threshold

    # Nearby horizontal level gives the squeeze a meaningful breakout point.
    near_support = bool(sr and sr.get("nearest_support") == sr.get("nearest_support") and
                       (float(a.close) - float(sr["nearest_support"])) <= 0.8 * float(sr.get("atr", recent_atr)))
    near_resistance = bool(sr and sr.get("nearest_resistance") == sr.get("nearest_resistance") and
                          (float(sr["nearest_resistance"]) - float(a.close)) <= 0.8 * float(sr.get("atr", recent_atr)))

    # Direction scores intentionally require several independent clues.
    bull = 0
    bear = 0
    reasons_bull = []
    reasons_bear = []
    if compression and not_expanded:
        bull += 25; bear += 25
        reasons_bull.append("volatility compression")
        reasons_bear.append("volatility compression")
    if bull_pressure:
        bull += 25; reasons_bull.append("bullish pressure")
    if bear_pressure:
        bear += 25; reasons_bear.append("bearish pressure")
    if near_resistance:
        bull += 25; reasons_bull.append("resistance nearby")
    if near_support:
        bear += 25; reasons_bear.append("support nearby")
    if a.rsi >= 52 and a.plus_di > a.minus_di:
        bull += 15; reasons_bull.append("momentum leaning up")
    if a.rsi <= 48 and a.minus_di > a.plus_di:
        bear += 15; reasons_bear.append("momentum leaning down")
    if a.close > a.ema20:
        bull += 10
    elif a.close < a.ema20:
        bear += 10

    # Detect an actual expansion only after the range has already started expanding.
    expansion = current_range >= expansion_threshold and current_range >= 1.15 * recent_tr
    if expansion and bull > bear + 10:
        return {"state": "EXPANSION", "direction": "UP", "score": min(100, bull),
                "reasons": reasons_bull + ["range expansion"]}
    if expansion and bear > bull + 10:
        return {"state": "EXPANSION", "direction": "DOWN", "score": min(100, bear),
                "reasons": reasons_bear + ["range expansion"]}

    best = max(bull, bear)
    if compression and best >= 50:
        direction = "UP" if bull > bear + 10 else "DOWN" if bear > bull + 10 else "NEUTRAL"
        reasons = reasons_bull if bull >= bear else reasons_bear
        state = "READY" if best >= 65 and direction != "NEUTRAL" else "BUILDING"
        return {"state": state, "direction": direction, "score": min(100, best), "reasons": reasons}

    return {"state": "WAIT", "direction": "NEUTRAL", "score": min(100, best), "reasons": []}


def early_move_engine(m5, m15f, h1f, sr, live_price=None):
    """Earlier warning engine: identifies pressure near a breakout level.
    It intentionally separates WATCH/TRIGGER from a confirmed A+ trade.
    """
    x = add_indicators(m5.copy()).dropna(subset=["atr", "rsi", "adx"]).reset_index(drop=True)
    if len(x) < 60:
        return {"state":"WAIT","direction":"NEUTRAL","score":0,"up_trigger":np.nan,
                "down_trigger":np.nan,"distance_up":np.nan,"distance_down":np.nan,
                "reasons":[]}

    a = x.iloc[-1]
    p = x.iloc[-2]
    price = float(live_price) if live_price is not None and np.isfinite(live_price) else float(a.close)
    atrv = max(float(a.atr), price * 0.0005)

    tr = pd.concat([
        x.high-x.low,
        (x.high-x.close.shift(1)).abs(),
        (x.low-x.close.shift(1)).abs()
    ], axis=1).max(axis=1)
    recent = float(tr.tail(6).mean())
    baseline = float(tr.iloc[-36:-6].median())
    compression = baseline > 0 and recent < 0.90 * baseline

    rng = (a.high-a.low)
    close_pos = (a.close-a.low)/rng if rng > 0 else 0.5
    close_pos5 = ((x.close-x.low)/(x.high-x.low).replace(0,np.nan)).clip(0,1).tail(5).mean()
    ema_slope = float(a.ema20-p.ema20)

    bull_pressure = close_pos5 >= 0.58 and ema_slope >= 0 and a.plus_di >= a.minus_di
    bear_pressure = close_pos5 <= 0.42 and ema_slope <= 0 and a.minus_di >= a.plus_di

    bull_momentum = a.rsi >= 51 and a.plus_di > a.minus_di
    bear_momentum = a.rsi <= 49 and a.minus_di > a.plus_di

    nr = sr.get("nearest_resistance", np.nan) if sr else np.nan
    ns = sr.get("nearest_support", np.nan) if sr else np.nan
    sr_atr = float(sr.get("atr", atrv)) if sr else atrv
    up_trigger = float(nr + max(0.08*sr_atr, 0.05)) if np.isfinite(nr) else np.nan
    down_trigger = float(ns - max(0.08*sr_atr, 0.05)) if np.isfinite(ns) else np.nan

    dist_up = up_trigger-price if np.isfinite(up_trigger) else np.inf
    dist_down = price-down_trigger if np.isfinite(down_trigger) else np.inf
    near_up = np.isfinite(nr) and (nr-price) <= 0.45*sr_atr and (nr-price) >= -0.20*sr_atr
    near_down = np.isfinite(ns) and (price-ns) <= 0.45*sr_atr and (price-ns) >= -0.20*sr_atr

    up = 0
    down = 0
    up_reasons, down_reasons = [], []

    if compression:
        up += 20; down += 20
        up_reasons.append("compression"); down_reasons.append("compression")
    if bull_pressure:
        up += 20; up_reasons.append("buy pressure")
    if bear_pressure:
        down += 20; down_reasons.append("sell pressure")
    if bull_momentum:
        up += 15; up_reasons.append("RSI/DI up")
    if bear_momentum:
        down += 15; down_reasons.append("RSI/DI down")
    if near_up:
        up += 25; up_reasons.append("resistance close")
    if near_down:
        down += 25; down_reasons.append("support close")
    if h1f.get("trend") == "BULL":
        up += 10; up_reasons.append("1H bullish")
    elif h1f.get("trend") == "BEAR":
        down += 10; down_reasons.append("1H bearish")
    if m15f.get("trend") == "BULL":
        up += 10; up_reasons.append("15M bullish")
    elif m15f.get("trend") == "BEAR":
        down += 10; down_reasons.append("15M bearish")

    # A breakout is active only after price clears the trigger, not merely touches it.
    up_break = np.isfinite(up_trigger) and price >= up_trigger
    down_break = np.isfinite(down_trigger) and price <= down_trigger

    if up_break and up >= 60 and up > down + 8:
        return {"state":"BREAKOUT","direction":"UP","score":min(100,up),
                "up_trigger":up_trigger,"down_trigger":down_trigger,
                "distance_up":dist_up,"distance_down":dist_down,
                "reasons":up_reasons+["trigger broken"]}
    if down_break and down >= 60 and down > up + 8:
        return {"state":"BREAKOUT","direction":"DOWN","score":min(100,down),
                "up_trigger":up_trigger,"down_trigger":down_trigger,
                "distance_up":dist_up,"distance_down":dist_down,
                "reasons":down_reasons+["trigger broken"]}

    best=max(up,down)
    if best >= 55:
        direction = "UP" if up > down + 8 else "DOWN" if down > up + 8 else "NEUTRAL"
        reasons = up_reasons if up >= down else down_reasons
        state = "READY" if direction != "NEUTRAL" and best >= 65 else "BUILDING"
        return {"state":state,"direction":direction,"score":min(100,best),
                "up_trigger":up_trigger,"down_trigger":down_trigger,
                "distance_up":dist_up,"distance_down":dist_down,
                "reasons":reasons}
    return {"state":"WAIT","direction":"NEUTRAL","score":min(100,best),
            "up_trigger":up_trigger,"down_trigger":down_trigger,
            "distance_up":dist_up,"distance_down":dist_down,"reasons":[]}


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


def build_hedge_plan(x5, direction, atr_value, max_entries=2):
    """Conservative two-entry + one-opposite-hedge plan.
    This is a decision aid, not an order executor. It never recommends
    unlimited averaging.
    """
    if direction not in ("BUY", "SELL"):
        return None
    a = x5.iloc[-1]
    entry = float(a.close)
    atrv = max(float(atr_value), entry * 0.0008)
    recent_hi = float(x5["high"].tail(8).max())
    recent_lo = float(x5["low"].tail(8).min())

    # Use ATR fractions rather than fixed dollar distances so the rules scale.
    step = max(0.50 * atrv, entry * 0.0006)
    hedge_step = max(0.75 * atrv, entry * 0.0008)

    if direction == "SELL":
        entry1 = entry
        entry2 = entry1 - step
        hedge = entry1 + hedge_step
        invalid = max(recent_hi, entry1 + 1.20 * atrv)
        tp1 = entry1 - 1.00 * atrv
        tp2 = entry1 - 2.00 * atrv
        return {"direction":"SELL", "entry1":entry1, "entry2":entry2,
                "hedge":hedge, "invalid":invalid, "tp1":tp1, "tp2":tp2,
                "step":step, "rule":"SELL #2 only after favorable move; BUY hedge only on confirmed reversal."}
    else:
        entry1 = entry
        entry2 = entry1 + step
        hedge = entry1 - hedge_step
        invalid = min(recent_lo, entry1 - 1.20 * atrv)
        tp1 = entry1 + 1.00 * atrv
        tp2 = entry1 + 2.00 * atrv
        return {"direction":"BUY", "entry1":entry1, "entry2":entry2,
                "hedge":hedge, "invalid":invalid, "tp1":tp1, "tp2":tp2,
                "step":step, "rule":"BUY #2 only after favorable move; SELL hedge only on confirmed reversal."}



def fmt(v):
    return f"{v:,.2f}" if v is not None and np.isfinite(v) else "—"


# -----------------------------
# UI
# -----------------------------
st.title("🟡 Smart Flow Gold — Live XAUUSD")
st.caption("Live reference-price dashboard • Conservative A+ style signal engine • Not financial advice")

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

h1i, h1f = structure_features(h1)
m15i, m15f = structure_features(m15)
m5i, m5f = structure_features(m5)

# Horizontal support/resistance levels (green support / red resistance).
h1sr = support_resistance(h1i)
m15sr = support_resistance(m15i)
m5sr = support_resistance(m5i)
m15sr_ctx = sr_context(m15i, m15sr)
m5sr_ctx = sr_context(m5i, m5sr)

# Big-move pre-expansion detector: warning only; it does not override the A+ trade filter.
big_move = big_move_detector(m5i, m5sr)
early_move = early_move_engine(m5i, m15f, h1f, m5sr, live_price=snap.get("price"))

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

# A completed/in-progress bar can naturally be up to one interval old.
# Allow a small buffer; if any timeframe is much older, force WAIT.
stale_limits = {"1H": 75.0, "15M": 30.0, "5M": 15.0}
data_stale = any(bar_ages[k] > stale_limits[k] for k in bar_ages)
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
    st.write(f"Latest bars: **1H {h1t}** ({bar_ages['1H']:.0f}m old) · **15M {m15t}** ({bar_ages['15M']:.0f}m old) · **5M {m5t}** ({bar_ages['5M']:.0f}m old)")
    if data_stale:
        st.error("Historical OHLC data is too old for a live trading signal. System is forced to WAIT.")
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

st.divider()

st.subheader("⚡ Big Move / Early Entry Engine")
bm1, bm2, bm3 = st.columns(3)
bm1.metric("Market state", early_move["state"])
bm2.metric("Next move", early_move["direction"])
bm3.metric("Early score", f"{early_move["score"]}/100")

if early_move["direction"] == "UP":
    st.success(
        f"🟢 WATCH UP — Break above **{fmt(early_move["up_trigger"])}** "
        f"({max(0, early_move["distance_up"]):.2f} away)."
    )
elif early_move["direction"] == "DOWN":
    st.error(
        f"🔴 WATCH DOWN — Break below **{fmt(early_move["down_trigger"])}** "
        f"({max(0, early_move["distance_down"]):.2f} away)."
    )
else:
    st.info("🟡 NO DIRECTION — wait for pressure to build.")

if early_move["state"] == "BREAKOUT":
    st.warning("⚡ BREAKOUT ACTIVE — do not chase the first tick; wait for 5M close/confirmation.")
elif early_move["state"] == "READY":
    st.info("🎯 READY — price is close to a trigger. The app is watching the breakout before giving an A+ entry.")
elif early_move["state"] == "BUILDING":
    st.info("🔋 BUILDING — volatility/pressure is developing, but direction is not confirmed enough to enter.")

tr1, tr2 = st.columns(2)
with tr1:
    st.metric("UP trigger", fmt(early_move["up_trigger"]))
with tr2:
    st.metric("DOWN trigger", fmt(early_move["down_trigger"]))
if early_move["reasons"]:
    st.caption("Why: " + " • ".join(early_move["reasons"]))
st.caption(
    "Important: WATCH/READY is an early-warning layer. A+ BUY/SELL still needs multi-timeframe confirmation."
)

if final == "BUY":
    st.success(f"🟢 BUY — A+ FILTERED | Quality {final_score}/100")
elif final == "SELL":
    st.error(f"🔴 SELL — A+ FILTERED | Quality {final_score}/100")
else:
    wait_reasons = []
    if data_stale:
        wait_reasons.append("data freshness")
    if h1f["trend"] in ("BULL","BEAR") and m15f["trend"] != h1f["trend"]:
        wait_reasons.append("1H and 15M disagree")
    if m15f["score"] < 70:
        wait_reasons.append("15M setup score below 70")
    if m5f["trend"] not in ("BULL","BEAR"):
        wait_reasons.append("5M direction unclear")
    if not wait_reasons:
        wait_reasons.append("confirmation not complete")
    st.warning(f"🟡 WAIT | Best score {final_score}/100")
    st.caption("Why WAIT: " + " • ".join(wait_reasons))

hedge = build_hedge_plan(m5i, final, m5f["atr"])
if hedge:
    st.subheader("🛡️ 2-Entry + 1-Hedge Engine")
    st.caption("Maximum 2 same-direction entries + 1 opposite hedge. No unlimited averaging. Levels are dynamic examples from current 5M ATR/structure.")
    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Entry #1", fmt(hedge["entry1"]))
    h2.metric("Entry #2 trigger", fmt(hedge["entry2"]))
    h3.metric("Hedge trigger", fmt(hedge["hedge"]))
    h4.metric("Invalidation", fmt(hedge["invalid"]))
    q1, q2 = st.columns(2)
    q1.metric("TP1", fmt(hedge["tp1"]))
    q2.metric("TP2", fmt(hedge["tp2"]))
    if final == "SELL":
        st.error("🔴 SELL #1 → price moves in favor → SELL #2 → confirmed reversal → BUY hedge. If invalidation is hit, stop the bearish idea; do not add another SELL.")
    else:
        st.success("🟢 BUY #1 → price moves in favor → BUY #2 → confirmed reversal → SELL hedge. If invalidation is hit, stop the bullish idea; do not add another BUY.")
else:
    st.info("🛡️ Hedge engine is OFF while the system is WAIT. No position should be opened from this module.")

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
    "Data: SiftingIO aggregated/reference XAUUSD. Big Move Detector is a conservative pre-expansion heuristic; Green/Red S/R levels are rule-based pivot estimates; they are not an exact copy of the TradingView indicator. Signal logic must be backtested/forward-tested before real-money use."
)

# Lightweight browser refresh; WebSocket supplies price without REST polling.
time.sleep(0.05)
st.markdown(
    f"<script>setTimeout(function(){{window.location.reload();}}, {int(refresh*1000)});</script>",
    unsafe_allow_html=True,
)
