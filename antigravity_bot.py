"""
================================================================================
ANTIGRAVITY BOT — DIRECT BINANCE BTC/USDT CANDLES + INR CAPITAL & RISK
================================================================================
Cloud Deployment for Render.com:
  - Chart Engine: Binance Public API (100% 1:1 Match with TradingView BINANCE:BTCUSDT)
  - Timeframes: 5m, 15m, 30m simultaneously monitored
  - Capital & Risk: ₹5,000 INR Paper Balance | Strict ₹200 INR Risk per trade
  - Trailing Stop: 50% TP -> Cost-to-Cost (BE), then +1R trailing milestones
  - Strategy: ICT FVG + IFVG + 200 EMA Trend Filter (Indicator V10.3 Logic)
  - Background Flask Keep-Alive on port 10000 (Ping '/' every 10 mins)
  - Persistent-History Engine (Zero phantom replays)
================================================================================
"""

import os
import sys
import time
import json
import signal
import logging
import threading
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any

import pytz
import requests
import numpy as np
import pandas as pd
from flask import Flask, jsonify

# ==============================================================================
# LOGGING SETUP (IST Timestamps)
# ==============================================================================
TIMEZONE_NAME = "Asia/Kolkata"
IST = pytz.timezone(TIMEZONE_NAME)

class ISTFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        dt = datetime.fromtimestamp(record.created, tz=pytz.utc).astimezone(IST)
        if datefmt:
            return dt.strftime(datefmt)
        return dt.strftime("%Y-%m-%d %H:%M:%S IST")

logger = logging.getLogger("antigravity_bot")
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(ISTFormatter("%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s"))
logger.handlers = [handler]
log = logger

# ==============================================================================
# CONFIGURATION
# ==============================================================================
DRY_RUN = True
SYMBOL = "BTCUSDT"                  # Official Binance BTC/USDT Pair
TIMEFRAMES = ["5m", "15m", "30m"]   # Multi-Timeframe
POLL_SECONDS = 20                   # Polling interval

PAPER_BALANCE_INR = 5000.0          # Starting Capital in INR
RISK_PER_TRADE_INR = 200.0         # Risk Per Trade in INR
USDT_INR_RATE = 90.0                # Live conversion for ₹200 risk calculation

PORT = int(os.environ.get("PORT", 10000))

# Strategy Parameters (ICT FVG + IFVG PRO V10.3)
ATR_LEN = 14
MIN_GAP_ATR = 0.15
MIN_BODY = 65.0
MIN_SCORE = 80
SWING_LEN = 10
SWEEP_LOOKBACK = 10
VOL_LEN = 20
VOL_MULT = 1.2
REQUIRE_SWEEP = False
REQUIRE_BOS = False
REQUIRE_TAP = True
RETEST_TOL = 0.25
MIN_RR = 1.5
SL_BUFFER = 1.0
SL_ZONE_MULT = 1.5
TARGET_LOOKBACK = 50
MAX_ZONE_AGE = 500
USE_TREND_FILTER = True
TREND_EMA_LEN = 200

# ==============================================================================
# DATA MODELS
# ==============================================================================
@dataclass
class Zone:
    top: float
    bottom: float
    direction: int   # 1 = Bullish, -1 = Bearish
    state: int       # 0 = FVG, 1 = IFVG, 3 = Entry Confirmed
    score: float
    tapped: bool
    retest: int
    born: int

@dataclass
class Position:
    direction: int   # 1 = LONG, -1 = SHORT
    entry: float     # In USD ($)
    stop: float      # In USD ($)
    target: float    # In USD ($)
    entry_bar: int
    rr: float
    initial_risk: float
    qty: float = 0.0
    sl_moved_to_be: bool = False
    locked_r: float = 0.0

BOT_STATE = {
    "start_time": time.time(),
    "paper_balance_inr": PAPER_BALANCE_INR,
    "last_poll_time": None,
    "last_candle_times": {tf: None for tf in TIMEFRAMES},
    "active_positions": {},
    "total_signals": 0,
    "total_trades_closed": 0,
    "recent_decisions": []
}
STATE_LOCK = threading.Lock()

# ==============================================================================
# BINANCE PUBLIC MARKET DATA CLIENT (Exact TradingView Candles)
# ==============================================================================
class BinanceDataClient:
    """Fetches exact candles directly from Binance Public API (Zero KYC, No API Key needed)."""
    BASE_URL = "https://api.binance.com/api/v3/klines"

    def __init__(self):
        self.session = requests.Session()

    def get_candles(self, symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        resp = self.session.get(self.BASE_URL, params=params, timeout=10)
        resp.raise_for_status()
        raw_data = resp.json()

        # Binance kline format:
        # [open_time, open, high, low, close, volume, close_time, ...]
        df = pd.DataFrame(raw_data)
        df = df.iloc[:, [0, 1, 2, 3, 4, 5]]
        df.columns = ["Time", "Open", "High", "Low", "Close", "Volume"]

        df["Open"] = df["Open"].astype(float)
        df["High"] = df["High"].astype(float)
        df["Low"] = df["Low"].astype(float)
        df["Close"] = df["Close"].astype(float)
        df["Volume"] = df["Volume"].astype(float)

        # Convert epoch ms to IST Datetime
        df["Time"] = pd.to_datetime(df["Time"], unit="ms").dt.tz_localize("UTC").dt.tz_convert(IST).dt.tz_localize(None)
        return df.sort_values("Time").reset_index(drop=True)

# ==============================================================================
# STRATEGY ENGINE (ICT FVG + IFVG + 200 EMA + TRAILING STOP)
# ==============================================================================
class FVGStrategy:
    MAX_HISTORY_BARS = 3000

    def __init__(self, tf_label: str):
        self.tf_label = tf_label
        self.zones: List[Zone] = []
        self.position: Optional[Position] = None
        self.history: Optional[pd.DataFrame] = None
        self.processed_count: int = 0

    def compute_signals(self, df: pd.DataFrame) -> dict:
        o = df["Open"].values; h = df["High"].values
        l = df["Low"].values; c = df["Close"].values
        v = df["Volume"].values
        n = len(df)

        tr = np.zeros(n); tr[0] = h[0] - l[0]
        for i in range(1, n):
            tr[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))

        atr = np.full(n, np.nan)
        if n > ATR_LEN:
            atr[ATR_LEN - 1] = tr[:ATR_LEN].mean()
            for i in range(ATR_LEN, n):
                atr[i] = (atr[i - 1] * (ATR_LEN - 1) + tr[i]) / ATR_LEN

        ema_trend = pd.Series(c).ewm(span=TREND_EMA_LEN, adjust=False).mean().values
        prev_low2 = pd.Series(l).shift(2)
        prev_high2 = pd.Series(h).shift(2)
        previous_low = prev_low2.rolling(SWING_LEN).min().values
        previous_high = prev_high2.rolling(SWING_LEN).max().values
        vol_avg = pd.Series(v).rolling(VOL_LEN).mean().values

        def barssince(flags: np.ndarray) -> np.ndarray:
            out = np.full(len(flags), np.inf); last = np.inf
            for idx in range(len(flags)):
                last = 0 if flags[idx] else (last + 1 if last != np.inf else np.inf)
                out[idx] = last
            return out

        bull_det = np.zeros(n, dtype=bool); bear_det = np.zeros(n, dtype=bool)
        bull_score = np.zeros(n); bear_score = np.zeros(n)
        ssl_h = np.zeros(n, dtype=bool); bsl_h = np.zeros(n, dtype=bool)
        bull_bos = np.zeros(n, dtype=bool); bear_bos = np.zeros(n, dtype=bool)

        for i in range(2, n):
            if np.isnan(atr[i]) or np.isnan(previous_low[i]) or np.isnan(previous_high[i]):
                continue
            body = abs(c[i - 1] - o[i - 1])
            rng = h[i - 1] - l[i - 1]
            body_pct = (body / rng * 100.0) if rng > 0 else 0.0

            bull_disp = (c[i - 1] > o[i - 1]) and (body_pct >= MIN_BODY)
            bear_disp = (c[i - 1] < o[i - 1]) and (body_pct >= MIN_BODY)
            bull_gap = l[i] - h[i - 2]
            bear_gap = l[i - 2] - h[i]

            bull = (h[i - 2] < l[i]) and bull_disp and (bull_gap >= atr[i] * MIN_GAP_ATR)
            bear = (l[i - 2] > h[i]) and bear_disp and (bear_gap >= atr[i] * MIN_GAP_ATR)
            ssl_h[i] = (l[i - 1] < previous_low[i]) and (c[i - 1] > previous_low[i])
            bsl_h[i] = (h[i - 1] > previous_high[i]) and (c[i - 1] < previous_high[i])
            bull_bos[i] = c[i - 1] > previous_high[i]
            bear_bos[i] = c[i - 1] < previous_low[i]

            vol_ok = (not np.isnan(vol_avg[i - 1])) and (vol_avg[i - 1] > 0) and (v[i - 1] >= vol_avg[i - 1] * VOL_MULT)
            bull_det[i] = bull; bear_det[i] = bear

            bull_score[i] = (25 if bull_disp else 0) + (15 if bull_gap >= atr[i] * MIN_GAP_ATR else 0) + (20 if bull_bos[i] else 0) + (15 if vol_ok else 0)
            bear_score[i] = (25 if bear_disp else 0) + (15 if bear_gap >= atr[i] * MIN_GAP_ATR else 0) + (20 if bear_bos[i] else 0) + (15 if vol_ok else 0)

        ssl_bars = barssince(ssl_h); bsl_bars = barssince(bsl_h)
        recent_ssl = ssl_bars <= SWEEP_LOOKBACK
        recent_bsl = bsl_bars <= SWEEP_LOOKBACK

        for i in range(n):
            if bull_det[i]: bull_score[i] += 25 if recent_ssl[i] else 0
            if bear_det[i]: bear_score[i] += 25 if recent_bsl[i] else 0

        valid_bull = bull_det & (bull_score >= MIN_SCORE)
        valid_bear = bear_det & (bear_score >= MIN_SCORE)
        if REQUIRE_SWEEP:
            valid_bull &= recent_ssl; valid_bear &= recent_bsl
        if REQUIRE_BOS:
            valid_bull &= bull_bos; valid_bear &= bear_bos

        return {
            "o": o, "h": h, "l": l, "c": c, "atr": atr, "ema_trend": ema_trend,
            "valid_bull": valid_bull, "valid_bear": valid_bear,
            "bull_score": bull_score, "bear_score": bear_score, "n": n
        }

    def step(self, df: pd.DataFrame) -> List[dict]:
        if self.history is None:
            self.history = df.copy().reset_index(drop=True)
        else:
            merged = pd.concat([self.history, df], ignore_index=True)
            merged = merged.drop_duplicates(subset="Time", keep="first").sort_values("Time").reset_index(drop=True)
            self.history = merged

        if len(self.history) > self.MAX_HISTORY_BARS:
            trim = len(self.history) - self.MAX_HISTORY_BARS
            self.history = self.history.iloc[trim:].reset_index(drop=True)
            self.processed_count = max(0, self.processed_count - trim)
            for z in self.zones:
                z.born -= trim
                if z.retest >= 0:
                    z.retest -= trim
            self.zones = [z for z in self.zones if z.born >= 0]

        sig = self.compute_signals(self.history)
        o, h, l, c = sig["o"], sig["h"], sig["l"], sig["c"]
        atr, ema_trend = sig["atr"], sig["ema_trend"]
        valid_bull, valid_bear = sig["valid_bull"], sig["valid_bear"]
        bull_score, bear_score = sig["bull_score"], sig["bear_score"]
        n = sig["n"]

        start = max(2, self.processed_count)
        decisions: List[dict] = []

        for i in range(start, n):
            candle_time = str(self.history["Time"].iloc[i])

            # 1. New Zones
            if valid_bull[i]:
                top, bottom = l[i], h[i - 2]
                overlap = any(z.direction == 1 and max(0.0, min(top, z.top) - max(bottom, z.bottom)) / max(top - bottom, 1e-9) >= 0.80 for z in self.zones)
                if not overlap:
                    self.zones.append(Zone(top, bottom, 1, 0, bull_score[i], False, -1, i))

            if valid_bear[i]:
                top, bottom = l[i - 2], h[i]
                overlap = any(z.direction == -1 and max(0.0, min(top, z.top) - max(bottom, z.bottom)) / max(top - bottom, 1e-9) >= 0.80 for z in self.zones)
                if not overlap:
                    self.zones.append(Zone(top, bottom, -1, 0, bear_score[i], False, -1, i))

            # 2. Zone Retest & Inversions
            selected_long = None; selected_short = None
            remove_idx = []
            for zi, z in enumerate(self.zones):
                if i - z.born > MAX_ZONE_AGE:
                    remove_idx.append(zi); continue
                if i <= z.born:
                    continue

                touched = (l[i] <= z.top) and (h[i] >= z.bottom)
                if z.state == 0:
                    if touched and not z.tapped:
                        z.tapped = True
                    broken_bull = (z.direction == 1) and (c[i] < z.bottom)
                    broken_bear = (z.direction == -1) and (c[i] > z.top)
                    if (broken_bull or broken_bear) and (z.tapped or not REQUIRE_TAP or touched):
                        z.direction = -z.direction; z.state = 1
                        z.tapped = False; z.retest = -1; z.born = i
                elif z.state == 1:
                    invalid_bull = (z.direction == 1) and (c[i] < z.bottom)
                    invalid_bear = (z.direction == -1) and (c[i] > z.top)
                    if invalid_bull or invalid_bear:
                        remove_idx.append(zi); continue

                    tol = atr[i] * RETEST_TOL
                    touched_edge = (l[i] <= z.bottom + tol) if z.direction == 1 else (h[i] >= z.top - tol)
                    if touched_edge and z.retest == -1:
                        z.retest = i

                    if z.retest >= 0 and i > z.retest:
                        bullish_confirm = (z.direction == 1) and (c[i] > z.top) and (c[i] > o[i])
                        bearish_confirm = (z.direction == -1) and (c[i] < z.bottom) and (c[i] < o[i])
                        if bullish_confirm or bearish_confirm:
                            z.state = 3
                            if bullish_confirm and (selected_long is None or z.score > selected_long[2]):
                                selected_long = (z.bottom, z.top, z.score, z.retest)
                            if bearish_confirm and (selected_short is None or z.score > selected_short[2]):
                                selected_short = (z.top, z.bottom, z.score, z.retest)

            for zi in sorted(set(remove_idx), reverse=True):
                self.zones.pop(zi)

            # 3. Trailing SL & Position Monitoring
            if self.position is not None and i > self.position.entry_bar:
                pos = self.position
                d = pos.direction
                hit_stop = (l[i] <= pos.stop) if d == 1 else (h[i] >= pos.stop)
                hit_target = (h[i] >= pos.target) if d == 1 else (l[i] <= pos.target)

                if hit_stop:
                    decisions.append({
                        "action": "close", "reason": "SL/Trailing Stop", "direction": "LONG" if d == 1 else "SHORT",
                        "price": pos.stop, "entry": pos.entry, "qty": pos.qty, "bar": i,
                        "tf": self.tf_label, "candle_time": candle_time
                    })
                    self.position = None
                    continue
                elif hit_target:
                    decisions.append({
                        "action": "close", "reason": "TP", "direction": "LONG" if d == 1 else "SHORT",
                        "price": pos.target, "entry": pos.entry, "qty": pos.qty, "bar": i,
                        "tf": self.tf_label, "candle_time": candle_time
                    })
                    self.position = None
                    continue

                # Trailing checks
                favorable = (h[i] - pos.entry) if d == 1 else (pos.entry - l[i])
                dist_to_tp = abs(pos.target - pos.entry)

                # Cost-to-Cost at 50% TP
                if not pos.sl_moved_to_be and favorable >= (dist_to_tp * 0.5):
                    pos.stop = pos.entry
                    pos.sl_moved_to_be = True
                    decisions.append({
                        "action": "trail", "reason": "50% TP Hit (Cost-to-Cost)",
                        "new_stop": pos.stop, "tf": self.tf_label, "candle_time": candle_time
                    })

                # +1R Milestone Trailing
                current_r = favorable / pos.initial_risk if pos.initial_risk > 0 else 0
                r_milestone = int(current_r)
                if r_milestone >= 2 and r_milestone > pos.locked_r + 1:
                    pos.locked_r = r_milestone - 1
                    new_stop = pos.entry + (pos.locked_r * pos.initial_risk * d)
                    is_tighter = (new_stop > pos.stop) if d == 1 else (new_stop < pos.stop)
                    if is_tighter:
                        pos.stop = new_stop
                        decisions.append({
                            "action": "trail", "reason": f"+{r_milestone}R Reached (Locked +{int(pos.locked_r)}R)",
                            "new_stop": pos.stop, "tf": self.tf_label, "candle_time": candle_time
                        })

            # 4. Entry Signals
            if self.position is None:
                recent_high = np.max(h[max(0, i - TARGET_LOOKBACK):i]) if i > 0 else np.nan
                recent_low = np.min(l[max(0, i - TARGET_LOOKBACK):i]) if i > 0 else np.nan
                trend_ok_long = (not USE_TREND_FILTER) or (c[i] > ema_trend[i])
                trend_ok_short = (not USE_TREND_FILTER) or (c[i] < ema_trend[i])

                if selected_long is not None and trend_ok_long:
                    bottom, top, score, rb = selected_long
                    entry = bottom
                    zh = top - bottom
                    sl_dist = max(zh * SL_ZONE_MULT, atr[i] * SL_BUFFER)
                    stop = entry - sl_dist
                    risk = entry - stop

                    if risk > 1e-4:
                        min_target = entry + risk * MIN_RR
                        target = recent_high if (not np.isnan(recent_high) and recent_high > min_target) else min_target
                        actual_rr = (target - entry) / risk
                        if actual_rr >= MIN_RR:
                            self.position = Position(1, entry, stop, target, rb, actual_rr, risk)
                            decisions.append({
                                "action": "open", "direction": "LONG", "entry": entry, "stop": stop,
                                "target": target, "rr": actual_rr, "risk": risk, "score": score,
                                "bar": i, "tf": self.tf_label, "candle_time": candle_time
                            })

                elif selected_short is not None and trend_ok_short:
                    top, bottom, score, rb = selected_short
                    entry = top
                    zh = top - bottom
                    sl_dist = max(zh * SL_ZONE_MULT, atr[i] * SL_BUFFER)
                    stop = entry + sl_dist
                    risk = stop - entry

                    if risk > 1e-4:
                        min_target = entry - risk * MIN_RR
                        target = recent_low if (not np.isnan(recent_low) and recent_low < min_target) else min_target
                        actual_rr = (entry - target) / risk
                        if actual_rr >= MIN_RR:
                            self.position = Position(-1, entry, stop, target, rb, actual_rr, risk)
                            decisions.append({
                                "action": "open", "direction": "SHORT", "entry": entry, "stop": stop,
                                "target": target, "rr": actual_rr, "risk": risk, "score": score,
                                "bar": i, "tf": self.tf_label, "candle_time": candle_time
                            })

        self.processed_count = n
        return decisions

# ============================================================
# FLASK KEEP-ALIVE SERVER (FOR RENDER.COM)
# ============================================================
app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)

@app.route("/", methods=["GET"])
def keep_alive():
    return "Antigravity Binance BTC/USDT Bot Online", 200

@app.route("/health", methods=["GET"])
def health():
    with STATE_LOCK:
        data = {
            "status": "online",
            "bot": "Antigravity Binance BTC/USDT Engine",
            "chart_feed": f"Binance {SYMBOL} (Exact TradingView Match)",
            "timeframes": TIMEFRAMES,
            "paper_balance_inr": round(BOT_STATE["paper_balance_inr"], 2),
            "risk_per_trade_inr": RISK_PER_TRADE_INR,
            "server_time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
            "active_positions": BOT_STATE["active_positions"],
            "total_signals": BOT_STATE["total_signals"],
            "total_trades_closed": BOT_STATE["total_trades_closed"],
            "uptime_hours": round((time.time() - BOT_STATE["start_time"]) / 3600.0, 2),
            "recent_decisions": BOT_STATE["recent_decisions"][-10:]
        }
    return jsonify(data), 200

def run_flask_app():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# ============================================================
# MAIN MULTI-TIMEFRAME ENGINE
# ============================================================
def run_trading_bot():
    client = BinanceDataClient()
    engines = {tf: FVGStrategy(tf) for tf in TIMEFRAMES}
    last_candle_times = {tf: None for tf in TIMEFRAMES}

    # Sizing conversion: ₹200 INR risk in USD terms
    risk_usd = RISK_PER_TRADE_INR / USDT_INR_RATE

    log.info("=" * 65)
    log.info(f"ANTIGRAVITY BOT RUNNING ON BINANCE {SYMBOL} @ {TIMEFRAMES}")
    log.info(f"Chart Feed: 100% TradingView BINANCE:BTCUSDT Match")
    log.info(f"Account: ₹{PAPER_BALANCE_INR:,.2f} INR | Fixed Risk: ₹{RISK_PER_TRADE_INR:,.2f} INR per trade")
    log.info("=" * 65)

    while True:
        for tf in TIMEFRAMES:
            try:
                df = client.get_candles(SYMBOL, tf, limit=500)
            except Exception as e:
                log.warning(f"Error fetching {tf} candles from Binance: {e}")
                continue

            if df is None or len(df) < 30:
                continue

            latest_closed_time = str(df["Time"].iloc[-2])
            if latest_closed_time == last_candle_times[tf]:
                continue
            last_candle_times[tf] = latest_closed_time

            with STATE_LOCK:
                BOT_STATE["last_candle_times"][tf] = latest_closed_time

            df_closed = df.iloc[:-1].reset_index(drop=True)
            strat = engines[tf]
            decisions = strat.step(df_closed)

            for d in decisions:
                action = d["action"]

                if action == "open":
                    risk_dist_usd = abs(d["entry"] - d["stop"])
                    qty = round(risk_usd / risk_dist_usd, 6)
                    strat.position.qty = qty

                    log.info(f"🚨 [{tf} | {d['candle_time']}] SIGNAL: {d['direction']} @ ${d['entry']:,.2f}")
                    log.info(f"    Stop: ${d['stop']:,.2f} | Target: ${d['target']:,.2f} | R:R: {d['rr']:.2f}")
                    log.info(f"    Size: {qty} BTC (Risk Capped at Exactly ₹{RISK_PER_TRADE_INR:.2f} INR)")

                    with STATE_LOCK:
                        BOT_STATE["total_signals"] += 1
                        BOT_STATE["active_positions"][tf] = {
                            "direction": d["direction"],
                            "entry_usd": f"${d['entry']:,.2f}",
                            "stop_usd": f"${d['stop']:,.2f}",
                            "target_usd": f"${d['target']:,.2f}",
                            "rr": d["rr"],
                            "time": d["candle_time"]
                        }
                        BOT_STATE["recent_decisions"].append(d)

                elif action == "trail":
                    log.info(f"🛡️ [{tf} | {d['candle_time']}] SL TRAILED: {d['reason']} | New SL: ${d['new_stop']:,.2f}")
                    with STATE_LOCK:
                        if tf in BOT_STATE["active_positions"] and BOT_STATE["active_positions"][tf]:
                            BOT_STATE["active_positions"][tf]["stop_usd"] = f"${d['new_stop']:,.2f}"
                        BOT_STATE["recent_decisions"].append(d)

                elif action == "close":
                    pnl_usd = d["qty"] * (d["price"] - d["entry"]) * (1 if d["direction"] == "LONG" else -1)
                    pnl_inr = pnl_usd * USDT_INR_RATE

                    with STATE_LOCK:
                        BOT_STATE["paper_balance_inr"] += pnl_inr
                        BOT_STATE["total_trades_closed"] += 1
                        BOT_STATE["active_positions"][tf] = None
                        BOT_STATE["recent_decisions"].append(d)

                    log.info(f"🛑 [{tf} | {d['candle_time']}] EXIT: {d['reason']} on {d['direction']} at ${d['price']:,.2f}")
                    log.info(f"    Trade PnL: ₹{pnl_inr:+.2f} INR | Updated Balance: ₹{BOT_STATE['paper_balance_inr']:,.2f} INR")

        time.sleep(POLL_SECONDS)

# ============================================================
# MAIN ENTRYPOINT
# ============================================================
def main():
    flask_thread = threading.Thread(target=run_flask_app, name="FlaskKeepAlive", daemon=True)
    flask_thread.start()

    def handle_shutdown(signum, frame):
        log.info(f"Shutdown signal received ({signum}). Exiting...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    run_trading_bot()

if __name__ == "__main__":
    main()
