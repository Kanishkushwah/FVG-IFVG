"""
================================================================================
ANTIGRAVITY BOT — DUAL PARALLEL MULTI-ASSET ENGINE (BTC/USDT + GOLD OANDA)
================================================================================
Cloud Deployment for Render.com:
  - Asset 1: BTCUSDT (Binance Public API — 100% TradingView Match)
    * Capital: ₹5,000 INR (Preserved & grown from previous trades)
    * Fee: 0.05% Taker + 18% GST (CoinDCX Futures)
  - Asset 2: XAUUSD / Gold (OANDA:XAUUSD via TradingView)
    * Capital: ₹5,000 INR (Independent initial balance)
    * Fee: 0.001 (0.1% Taker) + 18% GST
  - Timeframes: 5m, 15m, 30m monitored in parallel for BOTH pairs
  - Fixed Risk: Strictly ₹200 INR per trade per pair
  - Trailing Stop: 50% TP -> Cost-to-Cost (BE), then +1R trailing milestones
  - Strategy: ICT FVG + IFVG + 200 EMA Trend Filter (Indicator V10.3 Logic)
  - Full State Persistence: bot_state.json across server restarts & redeploys
  - Flask Keep-Alive on port 10000 (/ and /health endpoints)
================================================================================
"""

import os
import sys
import time
import json
import signal
import logging
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import Optional, List, Tuple, Dict, Any

# Ensure system timezone is IST for consistent datetime handling
os.environ["TZ"] = "Asia/Kolkata"
if hasattr(time, "tzset"):
    time.tzset()

import pytz
import requests
import numpy as np
import pandas as pd
from flask import Flask, jsonify, request

from tvDatafeed import TvDatafeed, Interval
logging.getLogger("tvDatafeed").setLevel(logging.CRITICAL)

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
# CONFIGURATION & MULTI-PAIR SETUP
# ==============================================================================
DRY_RUN = True
POLL_SECONDS = 20
USDT_INR_RATE = 90.0
GST_ON_FEE_PCT = 0.18
PORT = int(os.environ.get("PORT", 10000))

# ==============================================================================
# TELEGRAM ALERTS CONFIGURATION
# ==============================================================================
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "9057799504:AAFVoL2G3nBQ2odg9r8eULuIL2eo2PFZEkQ")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "1808628259")

def send_telegram_alert(text: str):
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "HTML"
    }
    try:
        requests.post(url, json=payload, timeout=5)
    except Exception as e:
        log.warning(f"Telegram alert delivery failed: {e}")

def notify_telegram_async(text: str):
    threading.Thread(target=send_telegram_alert, args=(text,), daemon=True).start()

PAIRS_CONFIG = {
    "BTCUSDT": {
        "name": "BTCUSDT",
        "label": "Bitcoin / TetherUS",
        "chart_feed": "Binance BTCUSDT (Exact TradingView Match)",
        "client_type": "binance",
        "starting_balance_inr": 5000.0,
        "risk_per_trade_inr": 200.0,
        "taker_fee_pct": 0.0005,       # 0.05% CoinDCX Futures standard rate
        "timeframes": ["5m", "15m", "30m", "1h"],
        "qty_precision": 6,
    },
    "XAUUSD": {
        "name": "XAUUSD",
        "label": "Gold / US Dollar",
        "chart_feed": "OANDA:XAUUSD (TradingView OANDA Chart)",
        "client_type": "oanda",
        "starting_balance_inr": 5000.0,
        "risk_per_trade_inr": 200.0,
        "taker_fee_pct": 0.001,        # 0.1% user-specified Gold fee rate
        "timeframes": ["3m", "5m", "15m", "30m", "1h"],
        "qty_precision": 4,
    }
}

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

# ==============================================================================
# STATE & PERSISTENCE
# ==============================================================================
BOT_STATE = {
    "start_time": time.time(),
    "last_poll_time": None,
    "last_daily_report_date": None,
    "closed_trades_history": [],
    "pairs": {
        sym: {
            "symbol": sym,
            "label": cfg["label"],
            "chart_feed": cfg["chart_feed"],
            "paper_balance_inr": cfg["starting_balance_inr"],
            "total_pnl_inr": 0.0,
            "total_gross_pnl_inr": 0.0,
            "total_fees_inr": 0.0,
            "risk_per_trade_inr": cfg["risk_per_trade_inr"],
            "taker_fee_pct": cfg["taker_fee_pct"],
            "last_candle_times": {tf: None for tf in cfg["timeframes"]},
            "active_zones": {tf: 0 for tf in cfg["timeframes"]},
            "active_positions": {tf: None for tf in cfg["timeframes"]},
            "total_signals": 0,
            "total_trades_closed": 0,
            "recent_decisions": []
        }
        for sym, cfg in PAIRS_CONFIG.items()
    }
}
STATE_LOCK = threading.Lock()
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bot_state.json")

def load_persisted_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
            with STATE_LOCK:
                if "start_time" in saved:
                    BOT_STATE["start_time"] = saved["start_time"]
                if "last_daily_report_date" in saved:
                    BOT_STATE["last_daily_report_date"] = saved["last_daily_report_date"]
                if "closed_trades_history" in saved and isinstance(saved["closed_trades_history"], list):
                    BOT_STATE["closed_trades_history"] = saved["closed_trades_history"]

                # Handle multi-pair format
                if "pairs" in saved and isinstance(saved["pairs"], dict):
                    for sym, p_saved in saved["pairs"].items():
                        if sym in BOT_STATE["pairs"]:
                            p_curr = BOT_STATE["pairs"][sym]
                            for key in ["paper_balance_inr", "total_pnl_inr", "total_gross_pnl_inr",
                                        "total_fees_inr", "total_signals", "total_trades_closed"]:
                                if key in p_saved:
                                    p_curr[key] = float(p_saved[key]) if "total_" in key or "balance" in key or "fees" in key else int(p_saved[key])
                            if "recent_decisions" in p_saved and isinstance(p_saved["recent_decisions"], list):
                                p_curr["recent_decisions"] = p_saved["recent_decisions"]
                else:
                    # Legacy single-pair format migration into BTCUSDT
                    p_btc = BOT_STATE["pairs"]["BTCUSDT"]
                    for key in ["paper_balance_inr", "total_pnl_inr", "total_gross_pnl_inr",
                                "total_fees_inr", "total_signals", "total_trades_closed"]:
                        if key in saved:
                            p_btc[key] = float(saved[key]) if "total_" in key or "balance" in key or "fees" in key else int(saved[key])
                    if "recent_decisions" in saved and isinstance(saved["recent_decisions"], list):
                        p_btc["recent_decisions"] = saved["recent_decisions"]

                # Auto-backfill closed_trades_history from recent_decisions if empty
                if not BOT_STATE.get("closed_trades_history"):
                    BOT_STATE["closed_trades_history"] = []
                    for sym, p in BOT_STATE["pairs"].items():
                        for d in p.get("recent_decisions", []):
                            if d.get("action") == "close":
                                c_time = d.get("candle_time", "")
                                d_date = c_time[:10] if len(c_time) >= 10 else datetime.now(IST).strftime("%Y-%m-%d")
                                BOT_STATE["closed_trades_history"].append({
                                    "symbol": sym,
                                    "tf": d.get("tf", "5m"),
                                    "direction": d.get("direction", "UNKNOWN"),
                                    "entry": d.get("entry", 0.0),
                                    "price": d.get("price", 0.0),
                                    "qty": d.get("qty", 0.0),
                                    "gross_pnl_inr": d.get("gross_pnl_inr", 0.0),
                                    "fee_inr": d.get("fee_inr", 0.0),
                                    "pnl_inr": d.get("pnl_inr", 0.0),
                                    "reason": d.get("reason", "EXIT"),
                                    "candle_time": c_time,
                                    "close_time": c_time,
                                    "date": d_date
                                })

            log.info(
                f"💾 [STATE RESTORED] State loaded from {STATE_FILE} | "
                f"BTC: ₹{BOT_STATE['pairs']['BTCUSDT']['paper_balance_inr']:,.2f} ({BOT_STATE['pairs']['BTCUSDT']['total_trades_closed']} trades) | "
                f"GOLD: ₹{BOT_STATE['pairs']['XAUUSD']['paper_balance_inr']:,.2f} ({BOT_STATE['pairs']['XAUUSD']['total_trades_closed']} trades) | "
                f"History: {len(BOT_STATE.get('closed_trades_history', []))} trades"
            )
        except Exception as e:
            log.warning(f"Could not load state from {STATE_FILE}: {e}")

def save_persisted_state():
    with STATE_LOCK:
        data = {
            "start_time": BOT_STATE["start_time"],
            "last_daily_report_date": BOT_STATE.get("last_daily_report_date"),
            "closed_trades_history": BOT_STATE.get("closed_trades_history", [])[-500:],
            "pairs": {
                sym: {
                    "paper_balance_inr": round(p["paper_balance_inr"], 2),
                    "total_pnl_inr": round(p["total_pnl_inr"], 2),
                    "total_gross_pnl_inr": round(p["total_gross_pnl_inr"], 2),
                    "total_fees_inr": round(p["total_fees_inr"], 2),
                    "total_signals": p["total_signals"],
                    "total_trades_closed": p["total_trades_closed"],
                    "recent_decisions": p["recent_decisions"][-50:]
                }
                for sym, p in BOT_STATE["pairs"].items()
            }
        }
    try:
        tmp_file = STATE_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp_file, STATE_FILE)
    except Exception as e:
        log.warning(f"Could not save state to {STATE_FILE}: {e}")

# ==============================================================================
# MARKET DATA CLIENT 1: BINANCE PUBLIC API (FOR BTCUSDT)
# ==============================================================================
class BinanceDataClient:
    """Fetches exact candles directly from Binance Public Market Data APIs."""
    ENDPOINTS = [
        "https://data-api.binance.vision/api/v3/klines",
        "https://api.binance.com/api/v3/klines",
        "https://api1.binance.com/api/v3/klines",
    ]

    def __init__(self):
        self.session = requests.Session()

    def get_candles(self, symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
        params = {"symbol": symbol.upper(), "interval": interval, "limit": limit}
        last_err = None
        raw_data = None

        for url in self.ENDPOINTS:
            try:
                resp = self.session.get(url, params=params, timeout=10)
                if resp.status_code == 451:
                    continue
                resp.raise_for_status()
                raw_data = resp.json()
                break
            except Exception as e:
                last_err = e

        if raw_data is None:
            raise RuntimeError(f"All Binance endpoints failed ({last_err})")

        df = pd.DataFrame(raw_data)
        df = df.iloc[:, [0, 1, 2, 3, 4, 5]]
        df.columns = ["Time", "Open", "High", "Low", "Close", "Volume"]

        for c in ["Open", "High", "Low", "Close", "Volume"]:
            df[c] = df[c].astype(float)

        df["Time"] = pd.to_datetime(df["Time"], unit="ms").dt.tz_localize("UTC").dt.tz_convert(IST).dt.tz_localize(None)
        return df.sort_values("Time").reset_index(drop=True)

# ==============================================================================
# MARKET DATA CLIENT 2: OANDA TRADINGVIEW FEED (FOR XAUUSD / GOLD)
# ==============================================================================
class OandaGoldClient:
    """Fetches exact candles for OANDA:XAUUSD directly from TradingView OANDA feed."""
    INTERVAL_MAP = {
        "3m": Interval.in_3_minute,
        "5m": Interval.in_5_minute,
        "15m": Interval.in_15_minute,
        "30m": Interval.in_30_minute,
        "1h": Interval.in_1_hour
    }

    def __init__(self):
        self.tv = None
        self._init_tv()
        self.last_cache: Dict[str, pd.DataFrame] = {}

    def _init_tv(self):
        try:
            self.tv = TvDatafeed()
        except Exception:
            self.tv = None

    def get_candles(self, symbol: str, interval: str, limit: int = 500) -> pd.DataFrame:
        tv_interval = self.INTERVAL_MAP.get(interval, Interval.in_5_minute)

        for attempt in range(3):
            try:
                if self.tv is None:
                    self._init_tv()
                raw_df = self.tv.get_hist(symbol="XAUUSD", exchange="OANDA", interval=tv_interval, n_bars=limit)
                if raw_df is not None and len(raw_df) >= 30:
                    df = raw_df.reset_index()[["datetime", "open", "high", "low", "close", "volume"]]
                    df.columns = ["Time", "Open", "High", "Low", "Close", "Volume"]
                    for col in ["Open", "High", "Low", "Close", "Volume"]:
                        df[col] = df[col].astype(float)
                    df["Time"] = pd.to_datetime(df["Time"])
                    df = df.sort_values("Time").reset_index(drop=True)
                    self.last_cache[interval] = df
                    time.sleep(1.2)
                    return df
            except Exception:
                pass
            time.sleep(1.5)
            self._init_tv()

        if interval in self.last_cache:
            return self.last_cache[interval]
        raise RuntimeError(f"Failed to fetch OANDA:XAUUSD {interval} candles after 3 attempts")

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

    def step(self, df: pd.DataFrame, risk_inr: float = 200.0, qty_prec: int = 6) -> List[dict]:
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
                        "price": float(pos.stop), "entry": float(pos.entry), "qty": float(pos.qty), "bar": int(i),
                        "tf": self.tf_label, "candle_time": candle_time
                    })
                    self.position = None
                    continue
                elif hit_target:
                    decisions.append({
                        "action": "close", "reason": "TP", "direction": "LONG" if d == 1 else "SHORT",
                        "price": float(pos.target), "entry": float(pos.entry), "qty": float(pos.qty), "bar": int(i),
                        "tf": self.tf_label, "candle_time": candle_time
                    })
                    self.position = None
                    continue

                # Trailing checks
                favorable = float(h[i] - pos.entry) if d == 1 else float(pos.entry - l[i])
                dist_to_tp = abs(pos.target - pos.entry)

                # Cost-to-Cost at 50% TP
                if not pos.sl_moved_to_be and favorable >= (dist_to_tp * 0.5):
                    pos.stop = float(pos.entry)
                    pos.sl_moved_to_be = True
                    decisions.append({
                        "action": "trail", "reason": "50% TP Hit (Cost-to-Cost)",
                        "new_stop": float(pos.stop), "tf": self.tf_label, "candle_time": candle_time
                    })

                # +1R Milestone Trailing
                current_r = favorable / pos.initial_risk if pos.initial_risk > 0 else 0
                r_milestone = int(current_r)
                if r_milestone >= 2 and r_milestone > pos.locked_r + 1:
                    pos.locked_r = r_milestone - 1
                    new_stop = float(pos.entry + (pos.locked_r * pos.initial_risk * d))
                    is_tighter = (new_stop > pos.stop) if d == 1 else (new_stop < pos.stop)
                    if is_tighter:
                        pos.stop = new_stop
                        decisions.append({
                            "action": "trail", "reason": f"+{r_milestone}R Reached (Locked +{int(pos.locked_r)}R)",
                            "new_stop": float(pos.stop), "tf": self.tf_label, "candle_time": candle_time
                        })

            # 4. Entry Signals
            if self.position is None:
                recent_high = np.max(h[max(0, i - TARGET_LOOKBACK):i]) if i > 0 else np.nan
                recent_low = np.min(l[max(0, i - TARGET_LOOKBACK):i]) if i > 0 else np.nan
                trend_ok_long = (not USE_TREND_FILTER) or (c[i] > ema_trend[i])
                trend_ok_short = (not USE_TREND_FILTER) or (c[i] < ema_trend[i])
                risk_usd = risk_inr / USDT_INR_RATE

                if selected_long is not None and trend_ok_long:
                    bottom, top, score, rb = selected_long
                    entry = float(bottom)
                    zh = float(top - bottom)
                    sl_dist = float(max(zh * SL_ZONE_MULT, atr[i] * SL_BUFFER))
                    stop = float(entry - sl_dist)
                    risk = float(entry - stop)

                    if risk > 1e-4:
                        min_target = entry + risk * MIN_RR
                        target = float(recent_high) if (not np.isnan(recent_high) and recent_high > min_target) else float(min_target)
                        actual_rr = float((target - entry) / risk)
                        # -1e-9 tolerance prevents IEEE-754 precision rejects
                        if actual_rr >= MIN_RR - 1e-9:
                            qty = round(risk_usd / risk, qty_prec)
                            self.position = Position(1, entry, stop, target, int(rb), actual_rr, risk, qty=qty)
                            decisions.append({
                                "action": "open", "direction": "LONG", "entry": entry, "stop": stop,
                                "target": target, "rr": actual_rr, "risk": risk, "qty": qty, "score": float(score),
                                "bar": int(i), "tf": self.tf_label, "candle_time": candle_time
                            })

                elif selected_short is not None and trend_ok_short:
                    top, bottom, score, rb = selected_short
                    entry = float(top)
                    zh = float(top - bottom)
                    sl_dist = float(max(zh * SL_ZONE_MULT, atr[i] * SL_BUFFER))
                    stop = float(entry + sl_dist)
                    risk = float(stop - entry)

                    if risk > 1e-4:
                        min_target = entry - risk * MIN_RR
                        target = float(recent_low) if (not np.isnan(recent_low) and recent_low < min_target) else float(min_target)
                        actual_rr = float((entry - target) / risk)
                        if actual_rr >= MIN_RR - 1e-9:
                            qty = round(risk_usd / risk, qty_prec)
                            self.position = Position(-1, entry, stop, target, int(rb), actual_rr, risk, qty=qty)
                            decisions.append({
                                "action": "open", "direction": "SHORT", "entry": entry, "stop": stop,
                                "target": target, "rr": actual_rr, "risk": risk, "qty": qty, "score": float(score),
                                "bar": int(i), "tf": self.tf_label, "candle_time": candle_time
                            })

        self.processed_count = n
        return decisions

# ============================================================
# DAILY MIDNIGHT TELEGRAM OVERVIEW SCHEDULER & FORMATTER
# ============================================================
def format_daily_summary(target_date: str) -> str:
    with STATE_LOCK:
        trades = [
            t for t in BOT_STATE.get("closed_trades_history", [])
            if t.get("date") == target_date or (t.get("candle_time") and t.get("candle_time")[:10] == target_date)
        ]
        btc_b = BOT_STATE["pairs"]["BTCUSDT"]["paper_balance_inr"]
        gold_b = BOT_STATE["pairs"]["XAUUSD"]["paper_balance_inr"]
        comb_b = round(btc_b + gold_b, 2)

        open_pos_list = []
        for s, p_data in BOT_STATE["pairs"].items():
            for tf_k, pos in p_data.get("active_positions", {}).items():
                if pos:
                    open_pos_list.append(f"{s} ({tf_k} {pos.get('direction', '')})")

    try:
        dt_obj = datetime.strptime(target_date, "%Y-%m-%d")
        date_display = dt_obj.strftime("%d %b %Y (%A)")
    except Exception:
        date_display = target_date

    total_trades = len(trades)
    if total_trades == 0:
        msg = (
            f"📊 <b>DAILY TRADING SUMMARY | ANTIGRAVITY BOT</b>\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"📅 <b>Date:</b> {date_display}\n"
            f"🎯 <b>Total Trades Taken Today:</b> 0\n"
            f"🟢 <b>Wins:</b> 0\n"
            f"🔴 <b>Losses:</b> 0\n"
            f"💰 <b>NET PROFIT COMBINED:</b> ₹0.00 INR\n"
            f"━━━━━━━━━━━━━━━━━━━━\n"
            f"💼 <b>Current Portfolio Balance:</b> ₹{comb_b:,.2f} INR\n"
            f"🚀 <b>Status:</b> Market scanned 24/7 across all timeframes. No setup triggers closed today."
        )
        return msg

    wins = [t for t in trades if t.get("pnl_inr", 0) > 0]
    losses = [t for t in trades if t.get("pnl_inr", 0) < 0]
    breakeven = [t for t in trades if t.get("pnl_inr", 0) == 0]

    win_count = len(wins)
    loss_count = len(losses)
    be_count = len(breakeven)
    win_rate = (win_count / total_trades) * 100.0

    total_gross = sum(t.get("gross_pnl_inr", 0.0) for t in trades)
    total_fees = sum(t.get("fee_inr", 0.0) for t in trades)
    total_net = sum(t.get("pnl_inr", 0.0) for t in trades)

    net_str = f"+₹{total_net:,.2f}" if total_net >= 0 else f"-₹{abs(total_net):,.2f}"
    gross_str = f"+₹{total_gross:,.2f}" if total_gross >= 0 else f"-₹{abs(total_gross):,.2f}"

    btc_trades = [t for t in trades if t.get("symbol") == "BTCUSDT"]
    gold_trades = [t for t in trades if t.get("symbol") == "XAUUSD"]

    btc_net = sum(t.get("pnl_inr", 0.0) for t in btc_trades)
    gold_net = sum(t.get("pnl_inr", 0.0) for t in gold_trades)

    btc_net_str = f"+₹{btc_net:,.2f}" if btc_net >= 0 else f"-₹{abs(btc_net):,.2f}"
    gold_net_str = f"+₹{gold_net:,.2f}" if gold_net >= 0 else f"-₹{abs(gold_net):,.2f}"

    btc_trade_label = f"{len(btc_trades)} Trade" if len(btc_trades) == 1 else f"{len(btc_trades)} Trades"
    gold_trade_label = f"{len(gold_trades)} Trade" if len(gold_trades) == 1 else f"{len(gold_trades)} Trades"

    open_pos_str = f"\n⚠️ <b>Active Open Positions:</b> {', '.join(open_pos_list)}\n" if open_pos_list else ""

    msg = (
        f"📊 <b>DAILY TRADING SUMMARY | ANTIGRAVITY BOT</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📅 <b>Date:</b> {date_display}\n"
        f"🎯 <b>Total Trades Taken Today:</b> {total_trades}\n"
        f"🟢 <b>Wins:</b> {win_count} ({win_rate:.1f}% Win Rate)\n"
        f"🔴 <b>Losses:</b> {loss_count}\n"
        f"⚪ <b>Breakeven:</b> {be_count}\n\n"
        f"💰 <b>NET PROFIT COMBINED:</b> <b>{net_str} INR</b>\n"
        f"💵 <b>Gross Profit:</b> {gross_str} INR\n"
        f"🧾 <b>Exchange Fees + GST:</b> ₹{total_fees:,.2f} INR\n"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"📌 <b>Pair Breakdown:</b>\n"
        f"• <b>BTC/USDT:</b> {btc_trade_label} | Net: <b>{btc_net_str} INR</b>\n"
        f"• <b>XAUUSD (Gold):</b> {gold_trade_label} | Net: <b>{gold_net_str} INR</b>\n"
        f"{open_pos_str}"
        f"━━━━━━━━━━━━━━━━━━━━\n"
        f"💼 <b>Total Portfolio Balance:</b> ₹{comb_b:,.2f} INR\n"
        f"🚀 <b>Engine:</b> Autonomous 24/7 Running"
    )
    return msg


def run_daily_report_scheduler():
    """
    Dedicated background scheduler running in its own thread.
    Checks IST clock every 10 seconds.
    At 12:00 AM midnight IST (00:00:00 to 00:05:00 window), it automatically
    compiles and sends the full daily trading summary for the completed day to Telegram.
    """
    log.info("⏰ [DAILY SCHEDULER] Daily Midnight (12:00 AM IST) Telegram overview daemon started.")
    while True:
        try:
            now = datetime.now(IST)
            # 12 midnight window (00:00:00 to 00:05:00 IST)
            if now.hour == 0 and now.minute < 5:
                # The calendar day to report is the day that just concluded
                target_date = (now - timedelta(days=1)).strftime("%Y-%m-%d")
                if BOT_STATE.get("last_daily_report_date") != target_date:
                    log.info(f"📅 [MIDNIGHT 12:00 AM IST] Dispatching Daily Summary for {target_date}...")
                    summary_text = format_daily_summary(target_date)
                    send_telegram_alert(summary_text)
                    with STATE_LOCK:
                        BOT_STATE["last_daily_report_date"] = target_date
                    save_persisted_state()
                    log.info(f"✅ Daily Summary for {target_date} sent to Telegram.")
        except Exception as e:
            log.warning(f"Error in daily report scheduler: {e}")
        time.sleep(10)

# ============================================================
# FLASK KEEP-ALIVE SERVER (FOR RENDER.COM)
# ============================================================
app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)

@app.route("/", methods=["GET"])
def keep_alive():
    return "Antigravity Multi-Asset (BTC + GOLD) Bot Online", 200

@app.route("/health", methods=["GET"])
def health():
    with STATE_LOCK:
        btc_p = BOT_STATE["pairs"]["BTCUSDT"]
        gold_p = BOT_STATE["pairs"]["XAUUSD"]

        combined_bal = round(btc_p["paper_balance_inr"] + gold_p["paper_balance_inr"], 2)
        combined_pnl = round(btc_p["total_pnl_inr"] + gold_p["total_pnl_inr"], 2)
        combined_gross = round(btc_p["total_gross_pnl_inr"] + gold_p["total_gross_pnl_inr"], 2)
        combined_fees = round(btc_p["total_fees_inr"] + gold_p["total_fees_inr"], 2)
        combined_signals = btc_p["total_signals"] + gold_p["total_signals"]
        combined_closed = btc_p["total_trades_closed"] + gold_p["total_trades_closed"]

        data = {
            "status": "online",
            "bot": "Antigravity Dual Multi-Asset Engine (BTC/USDT + GOLD OANDA)",
            "server_time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
            "uptime_hours": round((time.time() - BOT_STATE["start_time"]) / 3600.0, 2),
            "combined_balance_inr": combined_bal,
            "combined_net_pnl_inr": combined_pnl,
            "combined_gross_pnl_inr": combined_gross,
            "combined_fees_inr": combined_fees,
            "combined_signals": combined_signals,
            "combined_trades_closed": combined_closed,
            "pairs": {
                sym: {
                    "symbol": sym,
                    "label": p["label"],
                    "chart_feed": p["chart_feed"],
                    "paper_balance_inr": round(p["paper_balance_inr"], 2),
                    "total_pnl_inr": round(p["total_pnl_inr"], 2),
                    "total_gross_pnl_inr": round(p["total_gross_pnl_inr"], 2),
                    "total_fees_inr": round(p["total_fees_inr"], 2),
                    "fee_rate": f"{p['taker_fee_pct']*100:.3f}% Taker + 18% GST",
                    "risk_per_trade_inr": p["risk_per_trade_inr"],
                    "active_zones": p["active_zones"],
                    "active_positions": p["active_positions"],
                    "last_candle_times": p["last_candle_times"],
                    "total_signals": p["total_signals"],
                    "total_trades_closed": p["total_trades_closed"],
                    "recent_decisions": p["recent_decisions"][-10:]
                }
                for sym, p in BOT_STATE["pairs"].items()
            },
            # Backward-compatible fields mapped to primary pair (BTCUSDT)
            "paper_balance_inr": round(btc_p["paper_balance_inr"], 2),
            "total_pnl_inr": round(btc_p["total_pnl_inr"], 2),
            "total_gross_pnl_inr": round(btc_p["total_gross_pnl_inr"], 2),
            "total_fees_inr": round(btc_p["total_fees_inr"], 2),
            "risk_per_trade_inr": btc_p["risk_per_trade_inr"],
            "total_signals": btc_p["total_signals"],
            "total_trades_closed": btc_p["total_trades_closed"],
            "recent_decisions": btc_p["recent_decisions"][-10:]
        }
    return jsonify(data), 200

@app.route("/daily-summary", methods=["GET"])
def daily_summary():
    date_param = request.args.get("date")
    if not date_param:
        date_param = datetime.now(IST).strftime("%Y-%m-%d")
    summary = format_daily_summary(date_param)
    return summary, 200, {"Content-Type": "text/plain; charset=utf-8"}

@app.route("/send-daily-summary", methods=["GET", "POST"])
def trigger_send_daily_summary():
    date_param = request.args.get("date")
    if not date_param:
        date_param = datetime.now(IST).strftime("%Y-%m-%d")
    summary = format_daily_summary(date_param)
    notify_telegram_async(summary)
    return jsonify({"status": "sent", "date": date_param, "preview": summary}), 200

def run_flask_app():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# ============================================================
# MAIN PARALLEL MULTI-ASSET ENGINE
# ============================================================
def run_trading_bot():
    load_persisted_state()

    clients = {
        "BTCUSDT": BinanceDataClient(),
        "XAUUSD": OandaGoldClient()
    }

    engines = {
        sym: {tf: FVGStrategy(tf) for tf in cfg["timeframes"]}
        for sym, cfg in PAIRS_CONFIG.items()
    }

    last_candle_times = {
        sym: {tf: None for tf in cfg["timeframes"]}
        for sym, cfg in PAIRS_CONFIG.items()
    }

    log.info("=" * 70)
    log.info("ANTIGRAVITY BOT RUNNING DUAL ASSETS PARALLEL (BTCUSDT + GOLD XAUUSD)")
    for sym, cfg in PAIRS_CONFIG.items():
        p_state = BOT_STATE["pairs"][sym]
        log.info(f"  • {sym} ({cfg['label']}): Feed={cfg['chart_feed']}")
        log.info(f"    Balance: ₹{p_state['paper_balance_inr']:,.2f} INR | Risk: ₹{cfg['risk_per_trade_inr']:,.2f} | Fee: {cfg['taker_fee_pct']*100:.3f}%")
    log.info("=" * 70)

    while True:
        try:
            with STATE_LOCK:
                BOT_STATE["last_poll_time"] = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")

            for sym, cfg in PAIRS_CONFIG.items():
                client = clients[sym]
                sym_state = BOT_STATE["pairs"][sym]
                fee_pct = cfg["taker_fee_pct"]
                risk_inr = cfg["risk_per_trade_inr"]
                qty_prec = cfg["qty_precision"]

                for tf in cfg["timeframes"]:
                    try:
                        df = client.get_candles(sym, tf, limit=500)
                    except Exception as e:
                        log.warning(f"[{sym} | {tf}] Error fetching candles: {e}")
                        continue

                    if df is None or len(df) < 30:
                        continue

                    latest_closed_time = str(df["Time"].iloc[-2])
                    if latest_closed_time == last_candle_times[sym][tf]:
                        continue

                    is_warmup = (last_candle_times[sym][tf] is None)
                    last_candle_times[sym][tf] = latest_closed_time

                    df_closed = df.iloc[:-1].reset_index(drop=True)
                    strat = engines[sym][tf]
                    decisions = strat.step(df_closed, risk_inr=risk_inr, qty_prec=qty_prec)

                    with STATE_LOCK:
                        sym_state["last_candle_times"][tf] = latest_closed_time
                        sym_state["active_zones"][tf] = len(strat.zones)

                    if is_warmup:
                        strat.position = None
                        log.info(
                            f"[{sym} | {tf}] Warmed up {len(df_closed)} bars (latest: {latest_closed_time} IST). "
                            f"Active zones: {len(strat.zones)}. Monitoring live..."
                        )
                        continue

                    for d in decisions:
                        action = d["action"]

                        if action == "open":
                            qty = d["qty"]
                            log.info(f"🚨 [{sym} | {tf} | {d['candle_time']}] SIGNAL: {d['direction']} @ ${d['entry']:,.2f}")
                            log.info(f"    Stop: ${d['stop']:,.2f} | Target: ${d['target']:,.2f} | R:R: {d['rr']:.2f}")
                            log.info(f"    Size: {qty} ({sym}) | Risk Capped: ₹{risk_inr:.2f} INR")

                            with STATE_LOCK:
                                sym_state["total_signals"] += 1
                                sym_state["active_positions"][tf] = {
                                    "direction": d["direction"],
                                    "entry_usd": f"${d['entry']:,.2f}",
                                    "stop_usd": f"${d['stop']:,.2f}",
                                    "target_usd": f"${d['target']:,.2f}",
                                    "rr": d["rr"],
                                    "qty": qty,
                                    "time": d["candle_time"]
                                }
                                sym_state["recent_decisions"].append(d)
                                if len(sym_state["recent_decisions"]) > 50:
                                    sym_state["recent_decisions"] = sym_state["recent_decisions"][-50:]
                            save_persisted_state()

                            dir_emoji = "🟢 LONG" if d["direction"] == "LONG" else "🔴 SHORT"
                            trade_no = int(sym_state["total_signals"])
                            open_msg = (
                                f"🚨 <b>TRADE OPENED: {sym} (Trade #{trade_no})</b>\n\n"
                                f"⏱️ <b>Timeframe:</b> {tf}\n"
                                f"📈 <b>Direction:</b> {dir_emoji}\n"
                                f"💵 <b>Entry Price:</b> ${d['entry']:,.2f}\n"
                                f"🛑 <b>Stop Loss:</b> ${d['stop']:,.2f}\n"
                                f"🎯 <b>Target:</b> ${d['target']:,.2f}\n"
                                f"⚖️ <b>Target R:R:</b> {d['rr']:.2f} R\n"
                                f"🛡️ <b>Risk Capped:</b> ₹{risk_inr:,.2f} INR\n"
                                f"⏰ <b>Candle Time:</b> {d['candle_time']} IST"
                            )
                            notify_telegram_async(open_msg)

                        elif action == "trail":
                            log.info(f"🛡️ [{sym} | {tf} | {d['candle_time']}] SL TRAILED: {d['reason']} | New SL: ${d['new_stop']:,.2f}")
                            with STATE_LOCK:
                                if tf in sym_state["active_positions"] and sym_state["active_positions"][tf]:
                                    sym_state["active_positions"][tf]["stop_usd"] = f"${d['new_stop']:,.2f}"
                                sym_state["recent_decisions"].append(d)
                                if len(sym_state["recent_decisions"]) > 50:
                                    sym_state["recent_decisions"] = sym_state["recent_decisions"][-50:]
                            save_persisted_state()

                            trail_msg = (
                                f"🛡️ <b>SL TRAILED: {sym} ({tf})</b>\n\n"
                                f"📌 <b>Reason:</b> {d['reason']}\n"
                                f"🎯 <b>New Stop Loss:</b> ${d['new_stop']:,.2f}\n"
                                f"⏰ <b>Time:</b> {d['candle_time']} IST"
                            )
                            notify_telegram_async(trail_msg)

                        elif action == "close":
                            pnl_usd = d["qty"] * (d["price"] - d["entry"]) * (1 if d["direction"] == "LONG" else -1)
                            gross_pnl_inr = round(pnl_usd * USDT_INR_RATE, 2)

                            entry_notional_usd = d["qty"] * d["entry"]
                            exit_notional_usd = d["qty"] * d["price"]
                            fee_usd = (entry_notional_usd + exit_notional_usd) * fee_pct
                            fee_usd_with_gst = fee_usd * (1 + GST_ON_FEE_PCT)
                            fee_inr = round(fee_usd_with_gst * USDT_INR_RATE, 2)

                            net_pnl_inr = round(gross_pnl_inr - fee_inr, 2)

                            d["symbol"] = sym
                            d["gross_pnl_inr"] = gross_pnl_inr
                            d["fee_inr"] = fee_inr
                            d["pnl_inr"] = net_pnl_inr

                            close_time_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
                            c_date = d["candle_time"][:10] if len(d.get("candle_time", "")) >= 10 else datetime.now(IST).strftime("%Y-%m-%d")
                            trade_record = {
                                "symbol": sym,
                                "tf": tf,
                                "direction": d["direction"],
                                "entry": d["entry"],
                                "price": d["price"],
                                "qty": d["qty"],
                                "gross_pnl_inr": gross_pnl_inr,
                                "fee_inr": fee_inr,
                                "pnl_inr": net_pnl_inr,
                                "reason": d["reason"],
                                "candle_time": d["candle_time"],
                                "close_time": close_time_ist,
                                "date": c_date
                            }

                            with STATE_LOCK:
                                sym_state["paper_balance_inr"] += net_pnl_inr
                                sym_state["total_pnl_inr"] += net_pnl_inr
                                sym_state["total_gross_pnl_inr"] += gross_pnl_inr
                                sym_state["total_fees_inr"] += fee_inr
                                sym_state["total_trades_closed"] += 1
                                sym_state["active_positions"][tf] = None
                                sym_state["recent_decisions"].append(d)
                                if len(sym_state["recent_decisions"]) > 50:
                                    sym_state["recent_decisions"] = sym_state["recent_decisions"][-50:]

                                if "closed_trades_history" not in BOT_STATE:
                                    BOT_STATE["closed_trades_history"] = []
                                BOT_STATE["closed_trades_history"].append(trade_record)
                                if len(BOT_STATE["closed_trades_history"]) > 500:
                                    BOT_STATE["closed_trades_history"] = BOT_STATE["closed_trades_history"][-500:]
                            save_persisted_state()

                            log.info(f"🛑 [{sym} | {tf} | {d['candle_time']}] EXIT: {d['reason']} on {d['direction']} at ${d['price']:,.2f}")
                            log.info(f"    Gross PnL: ₹{gross_pnl_inr:+.2f} | Fee: ₹{fee_inr:.2f} | Net PnL: ₹{net_pnl_inr:+.2f} INR")
                            log.info(f"    Updated {sym} Balance: ₹{sym_state['paper_balance_inr']:,.2f} INR")

                            trade_no = int(sym_state["total_trades_closed"])
                            if d["reason"] == "TP":
                                outcome_emoji = "🎯 TAKE PROFIT HIT"
                            elif abs(gross_pnl_inr) < 1.0:
                                outcome_emoji = "⚪ BREAKEVEN HIT"
                            elif gross_pnl_inr > 0:
                                outcome_emoji = "🟢 TRAILING PROFIT HIT"
                            else:
                                outcome_emoji = "❌ STOP LOSS HIT"

                            rr_achieved = round(gross_pnl_inr / risk_inr, 2) if risk_inr > 0 else 0.0
                            sign = "+" if net_pnl_inr > 0 else ""

                            with STATE_LOCK:
                                btc_b = BOT_STATE["pairs"]["BTCUSDT"]["paper_balance_inr"]
                                gold_b = BOT_STATE["pairs"]["XAUUSD"]["paper_balance_inr"]
                                comb_b = round(btc_b + gold_b, 2)

                            close_msg = (
                                f"🏁 <b>TRADE CLOSED: {sym} (Trade #{trade_no})</b>\n\n"
                                f"📊 <b>Outcome:</b> {outcome_emoji}\n"
                                f"⏱️ <b>Timeframe:</b> {tf} ({d['direction']})\n"
                                f"💵 <b>Entry:</b> ${d['entry']:,.2f} ➡️ <b>Exit:</b> ${d['price']:,.2f}\n"
                                f"⚖️ <b>R:R Achieved:</b> {sign}{rr_achieved:.2f} R\n"
                                f"💰 <b>Gross PnL:</b> ₹{gross_pnl_inr:+,.2f} INR\n"
                                f"🧾 <b>Exchange Fees:</b> ₹{fee_inr:,.2f} INR\n"
                                f"💵 <b>Net PnL (In Hand):</b> <b>₹{net_pnl_inr:+,.2f} INR</b>\n\n"
                                f"💼 <b>Updated {sym} Balance:</b> ₹{sym_state['paper_balance_inr']:,.2f} INR\n"
                                f"💰 <b>Total Portfolio Balance:</b> ₹{comb_b:,.2f} INR\n"
                                f"⏰ <b>Close Time:</b> {d['candle_time']} IST"
                            )
                            notify_telegram_async(close_msg)

        except Exception as loop_err:
            log.exception(f"Unexpected error in trading loop: {loop_err}")

        time.sleep(POLL_SECONDS)

# ============================================================
# MAIN ENTRYPOINT
# ============================================================
def main():
    load_persisted_state()

    flask_thread = threading.Thread(target=run_flask_app, name="FlaskKeepAlive", daemon=True)
    flask_thread.start()

    daily_sched_thread = threading.Thread(target=run_daily_report_scheduler, name="DailyScheduler", daemon=True)
    daily_sched_thread.start()

    def handle_shutdown(signum, frame):
        log.info(f"Shutdown signal received ({signum}). Exiting...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    run_trading_bot()

if __name__ == "__main__":
    main()
