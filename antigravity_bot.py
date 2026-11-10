"""
================================================================================
ANTIGRAVITY BOT — COINDCX BTC/INR 24/7 MULTI-TIMEFRAME CLOUD ALGO
================================================================================
Production-ready cloud deployment script for Render.com free tier.

Core Active Features:
  1. TRAILING STOP-LOSS:
     - Cost-to-Cost (Breakeven): Moves SL to entry price when price hits 50% TP.
     - +1R Milestone Trailing: Locks +1R profit for every full 1R gained.
  2. DYNAMIC ₹200 RISK & ₹5,000 BALANCE (INR PAIR):
     - Market Pair: B-BTC_INR
     - Initial Paper Balance: ₹5,000
     - Exact Position Sizing: Qty calculated per trade so maximum loss is ₹200.
     - Real-time PnL & Balance updates on trade closure.
  3. CONCURRENT MULTI-TIMEFRAME (5m, 15m, 30m):
     - 3 independent strategy instances running simultaneously.
     - Independent zone tracking, retests, and position management per timeframe.
  4. RENDER.COM KEEP-ALIVE & IST TIMEZONE:
     - Flask web server running on background daemon thread.
     - Route '/' returns 'Antigravity Scanner Online' (200 OK) for 10-minute pings.
     - Telemetry & logging formatted strictly in IST (Asia/Kolkata).
================================================================================
"""

import os
import sys
import time
import hmac
import hashlib
import json
import signal
import logging
import threading
from datetime import datetime
from dataclasses import dataclass, asdict
from typing import Optional, List, Tuple, Dict, Any

import pytz
import requests
import numpy as np
import pandas as pd
from flask import Flask, jsonify

# ==============================================================================
# LOGGING SETUP (IST Timestamps, Structured Output, No print statements)
# ==============================================================================
TIMEZONE_NAME = "Asia/Kolkata"
IST = pytz.timezone(TIMEZONE_NAME)


class ISTFormatter(logging.Formatter):
    """Custom formatter to display all logs in IST (Asia/Kolkata)."""
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
# CONFIGURATION & ENVIRONMENT VARIABLES
# ==============================================================================
# Execution Mode
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"  # Set to "false" for live orders

# Market Pair & Capital Allocation (INR Pair)
PAIR = os.environ.get("PAIR", "B-BTC_INR").strip()
PAPER_BALANCE = float(os.environ.get("PAPER_BALANCE", 5000.0))     # Starting capital ₹5,000
RISK_PER_TRADE = float(os.environ.get("RISK_PER_TRADE", 200.0))    # Maximum risk ₹200 per trade

# Multi-Timeframe Configuration
TIMEFRAMES = ["5m", "15m", "30m"]
POLL_SECONDS = int(os.environ.get("POLL_SECONDS", 15))             # Frequent poll for 5m TF

# CoinDCX API Endpoints
API_BASE = "https://api.coindcx.com"
PUBLIC_BASE = "https://public.coindcx.com"
API_KEY = os.environ.get("COINDCX_API_KEY", "").strip()
API_SECRET = os.environ.get("COINDCX_API_SECRET", "").strip()

# Flask Keep-Alive Port (Render binds $PORT, default 10000)
PORT = int(os.environ.get("PORT", 10000))

# Strategy Parameters (Aligned with ICT FVG + IFVG PRO V10.3)
ATR_LEN = int(os.environ.get("ATR_LEN", 14))
MIN_GAP_ATR = float(os.environ.get("MIN_GAP_ATR", 0.15))
MIN_BODY = float(os.environ.get("MIN_BODY", 65.0))
MIN_SCORE = int(os.environ.get("MIN_SCORE", 80))
SWING_LEN = int(os.environ.get("SWING_LEN", 10))
SWEEP_LOOKBACK = int(os.environ.get("SWEEP_LOOKBACK", 10))
VOL_LEN = int(os.environ.get("VOL_LEN", 20))
VOL_MULT = float(os.environ.get("VOL_MULT", 1.2))
REQUIRE_SWEEP = os.environ.get("REQUIRE_SWEEP", "false").lower() == "true"
REQUIRE_BOS = os.environ.get("REQUIRE_BOS", "false").lower() == "true"
REQUIRE_TAP = os.environ.get("REQUIRE_TAP", "true").lower() == "true"
RETEST_TOL = float(os.environ.get("RETEST_TOL", 0.25))
MIN_RR = float(os.environ.get("MIN_RR", 1.5))
SL_BUFFER = float(os.environ.get("SL_BUFFER", 1.0))
SL_ZONE_MULT = float(os.environ.get("SL_ZONE_MULT", 1.5))
TARGET_LOOKBACK = int(os.environ.get("TARGET_LOOKBACK", 50))
MAX_ZONE_AGE = int(os.environ.get("MAX_ZONE_AGE", 500))
USE_TREND_FILTER = os.environ.get("USE_TREND_FILTER", "true").lower() == "true"
TREND_EMA_LEN = int(os.environ.get("TREND_EMA_LEN", 200))

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
    direction: int            # 1 = LONG, -1 = SHORT
    entry: float
    stop: float
    target: float
    entry_bar: int
    rr: float
    qty: float = 0.0          # Dynamically sized for ₹200 risk
    initial_risk: float = 0.0 # abs(entry - initial_stop)
    dist_to_tp: float = 0.0   # abs(target - entry)
    sl_moved_to_be: bool = False
    locked_r: int = 0
    order_id: Optional[str] = None
    tf: str = "15m"


# Thread-safe telemetry state for Flask monitoring
BOT_STATE = {
    "start_time": time.time(),
    "last_poll_time": None,
    "paper_balance": PAPER_BALANCE,
    "initial_balance": PAPER_BALANCE,
    "total_pnl": 0.0,
    "total_signals": 0,
    "total_trades_closed": 0,
    "positions": {tf: None for tf in TIMEFRAMES},
    "last_candle_times": {tf: None for tf in TIMEFRAMES},
    "recent_decisions": []
}
STATE_LOCK = threading.Lock()


# ==============================================================================
# COINDCX API CLIENT
# ==============================================================================
class CoinDCXClient:
    """
    Wrapper around CoinDCX REST APIs with HMAC-SHA256 signature generation,
    rate-limiting protection, and retry logic.
    """
    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret
        self.session = requests.Session()

    def _signed_headers(self, body: dict) -> Tuple[dict, str]:
        body_json = json.dumps(body, separators=(",", ":"))
        signature = hmac.new(
            self.api_secret.encode("utf-8"),
            body_json.encode("utf-8"),
            hashlib.sha256
        ).hexdigest()
        headers = {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }
        return headers, body_json

    def get_candles(self, pair: str, interval: str, limit: int = 500) -> pd.DataFrame:
        url = f"{PUBLIC_BASE}/market_data/candles"
        params = {"pair": pair, "interval": interval, "limit": limit}

        resp = self.session.get(url, params=params, timeout=12)
        if resp.status_code == 429:
            log.warning("CoinDCX rate limit reached (HTTP 429). Pausing 3 seconds...")
            time.sleep(3)
            resp = self.session.get(url, params=params, timeout=12)

        resp.raise_for_status()
        data = resp.json()

        df = pd.DataFrame(data)
        df = df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume", "time": "Time",
        })

        # Localize millisecond timestamp to IST datetime
        df["Time"] = pd.to_datetime(df["Time"], unit="ms").dt.tz_localize("UTC").dt.tz_convert(IST).dt.tz_localize(None)
        df = df.sort_values("Time").reset_index(drop=True)
        return df[["Time", "Open", "High", "Low", "Close", "Volume"]]

    def create_order(self, side: str, order_type: str, market: str,
                     total_quantity: float, price: Optional[float] = None) -> dict:
        body = {
            "side": side.lower(),
            "order_type": order_type,
            "market": market,
            "total_quantity": total_quantity,
            "timestamp": int(time.time() * 1000),
        }
        if order_type == "limit_order" and price is not None:
            body["price_per_unit"] = price

        headers, body_json = self._signed_headers(body)
        resp = self.session.post(
            f"{API_BASE}/exchange/v1/orders/create",
            data=body_json,
            headers=headers,
            timeout=10
        )
        resp.raise_for_status()
        return resp.json()


# ==============================================================================
# STRATEGY ENGINE (With Trailing SL & Multi-Timeframe Support)
# ==============================================================================
class FVGStrategy:
    """
    Independent Strategy Engine instance per timeframe.
    Incorporates:
      - ICT FVG + IFVG detection & scoring
      - Persistent history tracking
      - Trailing SL: 50% TP Breakeven & +1R Milestone Trailing
    """
    MAX_HISTORY_BARS = 3000

    def __init__(self, tf_label: str):
        self.tf_label = tf_label
        self.zones: List[Zone] = []
        self.position: Optional[Position] = None
        self.history: Optional[pd.DataFrame] = None
        self.processed_count: int = 0

    def compute_signals(self, df: pd.DataFrame) -> dict:
        o = df["Open"].values
        h = df["High"].values
        l = df["Low"].values
        c = df["Close"].values
        v = df["Volume"].values
        n = len(df)

        # 1. True Range & Smoothed ATR
        tr = np.zeros(n)
        tr[0] = h[0] - l[0]
        for i in range(1, n):
            tr[i] = max(h[i] - l[i], abs(h[i] - c[i - 1]), abs(l[i] - c[i - 1]))

        atr = np.full(n, np.nan)
        if n > ATR_LEN:
            atr[ATR_LEN - 1] = tr[:ATR_LEN].mean()
            for i in range(ATR_LEN, n):
                atr[i] = (atr[i - 1] * (ATR_LEN - 1) + tr[i]) / ATR_LEN

        # 2. Trend Filter: 200 EMA
        ema_trend = pd.Series(c).ewm(span=TREND_EMA_LEN, adjust=False).mean().values

        # 3. Rolling Highs / Lows for BOS & Sweeps
        prev_low2 = pd.Series(l).shift(2)
        prev_high2 = pd.Series(h).shift(2)
        previous_low = prev_low2.rolling(SWING_LEN).min().values
        previous_high = prev_high2.rolling(SWING_LEN).max().values
        vol_avg = pd.Series(v).rolling(VOL_LEN).mean().values

        def barssince(flags: np.ndarray) -> np.ndarray:
            out = np.full(len(flags), np.inf)
            last = np.inf
            for idx in range(len(flags)):
                last = 0 if flags[idx] else (last + 1 if last != np.inf else np.inf)
                out[idx] = last
            return out

        bull_det = np.zeros(n, dtype=bool)
        bear_det = np.zeros(n, dtype=bool)
        bull_score = np.zeros(n)
        bear_score = np.zeros(n)
        ssl_h = np.zeros(n, dtype=bool)
        bsl_h = np.zeros(n, dtype=bool)
        bull_bos = np.zeros(n, dtype=bool)
        bear_bos = np.zeros(n, dtype=bool)

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

            bull_det[i] = bull
            bear_det[i] = bear

            # Scoring Model (0-100)
            bull_score[i] = (
                (25 if bull_disp else 0) +
                (15 if bull_gap >= atr[i] * MIN_GAP_ATR else 0) +
                (20 if bull_bos[i] else 0) +
                (15 if vol_ok else 0)
            )
            bear_score[i] = (
                (25 if bear_disp else 0) +
                (15 if bear_gap >= atr[i] * MIN_GAP_ATR else 0) +
                (20 if bear_bos[i] else 0) +
                (15 if vol_ok else 0)
            )

        ssl_bars = barssince(ssl_h)
        bsl_bars = barssince(bsl_h)
        recent_ssl = ssl_bars <= SWEEP_LOOKBACK
        recent_bsl = bsl_bars <= SWEEP_LOOKBACK

        for i in range(n):
            if bull_det[i]:
                bull_score[i] += 25 if recent_ssl[i] else 0
            if bear_det[i]:
                bear_score[i] += 25 if recent_bsl[i] else 0

        valid_bull = bull_det & (bull_score >= MIN_SCORE)
        valid_bear = bear_det & (bear_score >= MIN_SCORE)

        if REQUIRE_SWEEP:
            valid_bull &= recent_ssl
            valid_bear &= recent_bsl
        if REQUIRE_BOS:
            valid_bull &= bull_bos
            valid_bear &= bear_bos

        return {
            "o": o, "h": h, "l": l, "c": c, "atr": atr, "ema_trend": ema_trend,
            "valid_bull": valid_bull, "valid_bear": valid_bear,
            "bull_score": bull_score, "bear_score": bear_score, "n": n
        }

    def step(self, df: pd.DataFrame) -> List[dict]:
        """
        Advances the strategy by processing newly closed candles incrementally.
        Returns decisions: 'open', 'close', and 'trail'.
        """
        if self.history is None:
            self.history = df.copy().reset_index(drop=True)
        else:
            merged = pd.concat([self.history, df], ignore_index=True)
            merged = merged.drop_duplicates(subset="Time", keep="first")
            merged = merged.sort_values("Time").reset_index(drop=True)
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

            # 1. Detect New FVG Zones
            if valid_bull[i]:
                top, bottom = l[i], h[i - 2]
                overlap = any(
                    z.direction == 1 and max(0.0, min(top, z.top) - max(bottom, z.bottom)) / max(top - bottom, 1e-9) >= 0.80
                    for z in self.zones
                )
                if not overlap:
                    self.zones.append(Zone(top, bottom, 1, 0, bull_score[i], False, -1, i))

            if valid_bear[i]:
                top, bottom = l[i - 2], h[i]
                overlap = any(
                    z.direction == -1 and max(0.0, min(top, z.top) - max(bottom, z.bottom)) / max(top - bottom, 1e-9) >= 0.80
                    for z in self.zones
                )
                if not overlap:
                    self.zones.append(Zone(top, bottom, -1, 0, bear_score[i], False, -1, i))

            # 2. Zone Lifecycle & Inversion (IFVG) Transitions
            selected_long = None
            selected_short = None
            remove_idx = []

            for zi, z in enumerate(self.zones):
                if i - z.born > MAX_ZONE_AGE:
                    remove_idx.append(zi)
                    continue
                if i <= z.born:
                    continue

                touched = (l[i] <= z.top) and (h[i] >= z.bottom)

                if z.state == 0:
                    if touched and not z.tapped:
                        z.tapped = True
                    broken_bull = (z.direction == 1) and (c[i] < z.bottom)
                    broken_bear = (z.direction == -1) and (c[i] > z.top)

                    if (broken_bull or broken_bear) and (z.tapped or not REQUIRE_TAP or touched):
                        z.direction = -z.direction
                        z.state = 1
                        z.tapped = False
                        z.retest = -1
                        z.born = i

                elif z.state == 1:
                    invalid_bull = (z.direction == 1) and (c[i] < z.bottom)
                    invalid_bear = (z.direction == -1) and (c[i] > z.top)
                    if invalid_bull or invalid_bear:
                        remove_idx.append(zi)
                        continue

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

            # 3. Position Monitoring & Trailing Stop-Loss
            if self.position is not None and i > self.position.entry_bar:
                pos = self.position
                d = pos.direction

                # Calculate favorable price move reached during candle
                favorable = (h[i] - pos.entry) if d == 1 else (pos.entry - l[i])

                # --------------------------------------------------------------
                # TRAILING SL RULE A: Move SL to Cost-to-Cost (BE) at 50% TP distance
                # --------------------------------------------------------------
                if not pos.sl_moved_to_be and favorable >= (pos.dist_to_tp * 0.5):
                    pos.stop = pos.entry
                    pos.sl_moved_to_be = True
                    decisions.append({
                        "action": "trail",
                        "reason": "50% TP Hit (Cost-to-Cost)",
                        "new_stop": pos.stop,
                        "tf": self.tf_label,
                        "candle_time": candle_time
                    })

                # --------------------------------------------------------------
                # TRAILING SL RULE B: Trail +1R for every full 1R gained
                # --------------------------------------------------------------
                current_r = favorable / pos.initial_risk if pos.initial_risk > 0 else 0
                r_milestone = int(current_r)
                if r_milestone >= 2 and r_milestone > pos.locked_r + 1:
                    pos.locked_r = r_milestone - 1
                    new_stop = pos.entry + (pos.locked_r * pos.initial_risk * d)
                    is_tighter = (new_stop > pos.stop) if d == 1 else (new_stop < pos.stop)
                    if is_tighter:
                        pos.stop = new_stop
                        decisions.append({
                            "action": "trail",
                            "reason": f"+{pos.locked_r}R Trailed",
                            "new_stop": pos.stop,
                            "tf": self.tf_label,
                            "candle_time": candle_time
                        })

                # Check SL / TP Hits
                hit_stop = (l[i] <= pos.stop) if d == 1 else (h[i] >= pos.stop)
                hit_target = (h[i] >= pos.target) if d == 1 else (l[i] <= pos.target)

                if hit_stop:
                    exit_reason = "BE" if (pos.stop == pos.entry) else ("TrailSL" if pos.locked_r > 0 else "SL")
                    decisions.append({
                        "action": "close",
                        "reason": exit_reason,
                        "direction": "LONG" if d == 1 else "SHORT",
                        "price": pos.stop,
                        "entry": pos.entry,
                        "qty": pos.qty,
                        "tf": self.tf_label,
                        "bar": i,
                        "candle_time": candle_time
                    })
                    self.position = None

                elif hit_target:
                    decisions.append({
                        "action": "close",
                        "reason": "TP",
                        "direction": "LONG" if d == 1 else "SHORT",
                        "price": pos.target,
                        "entry": pos.entry,
                        "qty": pos.qty,
                        "tf": self.tf_label,
                        "bar": i,
                        "candle_time": candle_time
                    })
                    self.position = None

            # 4. Entry Generation (When Flat on this Timeframe)
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
                            dist_to_tp = target - entry
                            self.position = Position(
                                direction=1,
                                entry=round(entry, 2),
                                stop=round(stop, 2),
                                target=round(target, 2),
                                entry_bar=rb,
                                rr=round(actual_rr, 2),
                                initial_risk=risk,
                                dist_to_tp=dist_to_tp,
                                tf=self.tf_label
                            )
                            decisions.append({
                                "action": "open",
                                "direction": "LONG",
                                "entry": round(entry, 2),
                                "stop": round(stop, 2),
                                "target": round(target, 2),
                                "rr": round(actual_rr, 2),
                                "score": score,
                                "pattern": "Bullish IFVG Retest",
                                "tf": self.tf_label,
                                "bar": i,
                                "candle_time": candle_time
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
                            dist_to_tp = entry - target
                            self.position = Position(
                                direction=-1,
                                entry=round(entry, 2),
                                stop=round(stop, 2),
                                target=round(target, 2),
                                entry_bar=rb,
                                rr=round(actual_rr, 2),
                                initial_risk=risk,
                                dist_to_tp=dist_to_tp,
                                tf=self.tf_label
                            )
                            decisions.append({
                                "action": "open",
                                "direction": "SHORT",
                                "entry": round(entry, 2),
                                "stop": round(stop, 2),
                                "target": round(target, 2),
                                "rr": round(actual_rr, 2),
                                "score": score,
                                "pattern": "Bearish IFVG Retest",
                                "tf": self.tf_label,
                                "bar": i,
                                "candle_time": candle_time
                            })

        self.processed_count = n
        return decisions


# ==============================================================================
# FLASK KEEP-ALIVE SERVER (Runs on background Thread)
# ==============================================================================
app = Flask(__name__)
logging.getLogger("werkzeug").setLevel(logging.ERROR)


@app.route("/", methods=["GET"])
def keep_alive():
    """Render.com Keep-Alive endpoint pinged every 10 minutes."""
    return "Antigravity Scanner Online", 200


@app.route("/health", methods=["GET"])
def health_check():
    """Returns remote telemetry, balance, PnL, and active positions."""
    with STATE_LOCK:
        uptime_h = round((time.time() - BOT_STATE["start_time"]) / 3600.0, 2)
        data = {
            "status": "online",
            "bot": "Antigravity CoinDCX BTC/INR Bot",
            "pair": PAIR,
            "timeframes": TIMEFRAMES,
            "dry_run": DRY_RUN,
            "paper_balance_inr": round(BOT_STATE["paper_balance"], 2),
            "total_pnl_inr": round(BOT_STATE["total_pnl"], 2),
            "risk_per_trade_inr": RISK_PER_TRADE,
            "server_time_ist": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
            "positions": BOT_STATE["positions"],
            "total_signals": BOT_STATE["total_signals"],
            "total_trades_closed": BOT_STATE["total_trades_closed"],
            "uptime_hours": uptime_h,
            "recent_decisions": BOT_STATE["recent_decisions"][-10:]
        }
    return jsonify(data), 200


def run_flask_app():
    log.info(f"Starting Flask Keep-Alive server on port {PORT} (0.0.0.0)...")
    try:
        app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)
    except Exception as e:
        log.exception(f"Flask server fatal error: {e}")


# ==============================================================================
# MAIN 24/7 MULTI-TIMEFRAME TRADING ENGINE
# ==============================================================================
def run_trading_bot():
    """
    Continuous 24/7 multi-timeframe engine for CoinDCX B-BTC_INR:
      - Iterates over 5m, 15m, 30m intervals concurrently.
      - Dynamic ₹200 risk position sizing.
      - Trailing SL: 50% TP Breakeven & +1R Trailing.
    """
    if not DRY_RUN and (not API_KEY or not API_SECRET):
        log.error("DRY_RUN is set to False, but COINDCX_API_KEY / COINDCX_API_SECRET are not set.")
        log.error("Please configure API credentials or keep DRY_RUN=true. Halting.")
        return

    client = CoinDCXClient(API_KEY, API_SECRET)
    engines: Dict[str, FVGStrategy] = {tf: FVGStrategy(tf) for tf in TIMEFRAMES}
    last_candle_times: Dict[str, Optional[str]] = {tf: None for tf in TIMEFRAMES}

    log.info("=" * 70)
    log.info(f"   ANTIGRAVITY BOT — COINDCX MULTI-TIMEFRAME ENGINE ({PAIR})")
    log.info(f"   Active Timeframes: {', '.join(TIMEFRAMES)} | Risk: ₹{RISK_PER_TRADE}/trade")
    log.info(f"   Paper Balance: ₹{PAPER_BALANCE:,.2f} | Execution: {'DRY RUN' if DRY_RUN else 'LIVE'}")
    log.info("=" * 70)

    while True:
        try:
            now_ist = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST")
            with STATE_LOCK:
                BOT_STATE["last_poll_time"] = now_ist

            # Poll each timeframe independently
            for tf in TIMEFRAMES:
                try:
                    df = client.get_candles(PAIR, tf, limit=500)
                except requests.exceptions.RequestException as req_err:
                    log.warning(f"[{tf.upper()}] Network warning fetching candles: {req_err}")
                    continue
                except Exception as e:
                    log.error(f"[{tf.upper()}] Error fetching candles: {e}")
                    continue

                if df is None or len(df) < 30:
                    continue

                latest_closed_time = str(df["Time"].iloc[-2])
                if latest_closed_time == last_candle_times[tf]:
                    continue  # Candle still forming for this TF

                last_candle_times[tf] = latest_closed_time
                with STATE_LOCK:
                    BOT_STATE["last_candle_times"][tf] = latest_closed_time

                df_closed = df.iloc[:-1].reset_index(drop=True)
                strat = engines[tf]
                decisions = strat.step(df_closed)

                # Process Decisions for this Timeframe
                for d in decisions:
                    action = d.get("action")

                    if action == "open":
                        direction = d["direction"]
                        entry = d["entry"]
                        stop = d["stop"]
                        target = d["target"]
                        rr = d["rr"]
                        score = d["score"]
                        pattern = d["pattern"]

                        # DYNAMIC ₹200 RISK SIZING:
                        risk_dist = abs(entry - stop)
                        qty = round(RISK_PER_TRADE / risk_dist, 6) if risk_dist > 1e-4 else 0.0001
                        strat.position.qty = qty
                        d["qty"] = qty

                        log.info(
                            f"🚨 [{tf.upper()} SIGNAL] {direction} | Qty: {qty:.6f} BTC (Risk: ₹{RISK_PER_TRADE}) | "
                            f"Entry: ₹{entry:,.2f} | Stop: ₹{stop:,.2f} | Target: ₹{target:,.2f} | R:R: {rr:.2f} | Score: {score}"
                        )

                        side = "buy" if direction == "LONG" else "sell"
                        if DRY_RUN:
                            log.info(f"[DRY RUN] Simulating {side.upper()} order for {qty:.6f} BTC on {PAIR}")
                        else:
                            try:
                                resp = client.create_order(side, "market_order", PAIR, qty)
                                log.info(f"Live order placed successfully: {resp}")
                            except Exception as order_err:
                                log.error(f"Failed to place live order on CoinDCX: {order_err}")

                        with STATE_LOCK:
                            BOT_STATE["total_signals"] += 1
                            BOT_STATE["positions"][tf] = {
                                "direction": direction,
                                "entry": entry,
                                "stop": stop,
                                "target": target,
                                "qty": qty,
                                "opened_at": latest_closed_time
                            }
                            BOT_STATE["recent_decisions"].append(d)

                    elif action == "trail":
                        new_stop = d["new_stop"]
                        reason = d["reason"]
                        log.info(f"🛡️ [{tf.upper()} TRAIL SL] Trailed Stop to ₹{new_stop:,.2f} ({reason})")

                        with STATE_LOCK:
                            if BOT_STATE["positions"][tf]:
                                BOT_STATE["positions"][tf]["stop"] = new_stop
                            BOT_STATE["recent_decisions"].append(d)

                    elif action == "close":
                        reason = d["reason"]
                        price = d["price"]
                        entry = d["entry"]
                        direction = d["direction"]
                        qty = d.get("qty", 0.0)

                        # REAL-TIME BALANCE & PnL CALCULATION
                        pnl = qty * (price - entry) * (1 if direction == "LONG" else -1)
                        d["pnl"] = round(pnl, 2)

                        exit_side = "sell" if direction == "LONG" else "buy"
                        log.info(
                            f"🛑 [{tf.upper()} EXIT] Closed {direction} on {reason} trigger at ₹{price:,.2f} | "
                            f"PnL: {'+₹' if pnl >= 0 else '-₹'}{abs(pnl):.2f}"
                        )

                        if DRY_RUN:
                            log.info(f"[DRY RUN] Simulating {exit_side.upper()} close order for {qty:.6f} BTC")
                        else:
                            try:
                                resp = client.create_order(exit_side, "market_order", PAIR, qty)
                                log.info(f"Live exit order placed: {resp}")
                            except Exception as exit_err:
                                log.error(f"Failed to close position on CoinDCX: {exit_err}")

                        with STATE_LOCK:
                            BOT_STATE["paper_balance"] += pnl
                            BOT_STATE["total_pnl"] += pnl
                            BOT_STATE["total_trades_closed"] += 1
                            BOT_STATE["positions"][tf] = None
                            BOT_STATE["recent_decisions"].append(d)

                        log.info(
                            f"💰 [BALANCE UPDATE] Current Balance: ₹{BOT_STATE['paper_balance']:,.2f} "
                            f"(Total PnL: {'+₹' if BOT_STATE['total_pnl'] >= 0 else '-₹'}{abs(BOT_STATE['total_pnl']):.2f})"
                        )

            time.sleep(POLL_SECONDS)

        except Exception as loop_err:
            log.exception(f"Unhandled exception in trading loop: {loop_err}")
            time.sleep(15)


# ==============================================================================
# MAIN ENTRYPOINT
# ==============================================================================
def main():
    log.info("=" * 70)
    log.info("   ANTIGRAVITY BOT — COINDCX BTC/INR 24/7 CLOUD ENGINE")
    log.info("              Render.com Multi-TF Production Build")
    log.info("=" * 70)

    # 1. Start Flask Keep-Alive Server on background thread
    flask_thread = threading.Thread(
        target=run_flask_app,
        name="FlaskKeepAliveThread",
        daemon=True
    )
    flask_thread.start()
    log.info(f"Flask Keep-Alive background thread active (Port {PORT}). Ping '/' every 10m.")

    # 2. Setup graceful container shutdown handles
    def handle_shutdown(signum, frame):
        log.info(f"Received termination signal ({signum}). Shutting down cleanly...")
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGINT, handle_shutdown)

    # 3. Start 24/7 Multi-Timeframe Trading Loop on Main Thread
    run_trading_bot()


if __name__ == "__main__":
    main()
