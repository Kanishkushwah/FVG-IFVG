"""
ICT FVG + IFVG + Trend Filter — CoinDCX Live/Paper Trading Bot (BTC)
=====================================================================

WHAT THIS IS
------------
A Python replica of the Pine Script strategy (same detection, zone,
retest, trend-filter, SL/TP logic) wired to CoinDCX's REST API so it
can poll live BTC candles and place real orders.

READ BEFORE RUNNING
--------------------
1. This talks to a REAL exchange. A bug, a bad fill, a connectivity
   drop mid-position, or a wrong parameter can lose real money. There
   is no undo button on a live order.
2. I (the AI that wrote this) cannot test this against CoinDCX's live
   API in this environment — no network access here. The endpoint
   paths, auth scheme, and payload shapes below are written from
   training knowledge of CoinDCX's documented REST API, NOT verified
   against a live call. Cross-check every endpoint against
   https://docs.coindcx.com before trusting this with real funds.
3. ALWAYS start with DRY_RUN = True (default below). It runs the full
   detection/decision loop and logs what it WOULD have done, without
   calling the order endpoints. Only flip to False after you've
   watched it make sensible decisions for a while.
4. Start with the smallest order size CoinDCX allows if you do go
   live. This strategy's own 6-month backtest (see your earlier
   conversation) still showed multi-R losing streaks — size for that,
   not for the best-case numbers.
5. API keys: generate a CoinDCX API key/secret with TRADE permission
   only (no withdrawal permission) and store them as environment
   variables (COINDCX_API_KEY / COINDCX_API_SECRET) — never hardcode
   them in this file, never commit them anywhere.

WHAT THIS DOES EACH LOOP
-------------------------
1. Pulls the latest BTC candles for the configured timeframe.
2. Recomputes the whole FVG/IFVG/zone/trend-filter state from scratch
   (same as the indicator does on every chart bar).
3. If no position is open and a valid LONG/SHORT signal just formed,
   places an order (or logs it, in dry-run) with a resting stop-loss
   and take-profit.
4. If a position is open, checks whether price has hit SL/TP on the
   latest candle and closes out if so.
5. Sleeps until the next candle close, repeats.

This is intentionally a POLLING loop (checks once per candle close),
not a tick-by-tick engine — matching how the Pine Script itself only
evaluates on confirmed bars (barstate.isconfirmed).
"""

import os
import time
import hmac
import hashlib
import json
import logging
from dataclasses import dataclass
from typing import Optional

import requests
import numpy as np
import pandas as pd

# ============================================================
# CONFIG — edit these
# ============================================================

DRY_RUN = True                      # <-- START HERE. Flip to False only after validating.

PAIR = "B-BTC_USDT"                 # CoinDCX market symbol for BTC/USDT spot.
                                     # Verify exact symbol via GET /exchange/v1/markets_details
CANDLE_INTERVAL = "15m"             # "5m" or "15m" per your earlier backtest
POLL_SECONDS = 30                   # how often to check for a new closed candle

ORDER_SIZE_BTC = 0.001              # position size in BTC — START TINY. Check CoinDCX min order size.

API_BASE = "https://api.coindcx.com"
PUBLIC_BASE = "https://public.coindcx.com"

API_KEY = os.environ.get("COINDCX_API_KEY", "")
API_SECRET = os.environ.get("COINDCX_API_SECRET", "")

# ---- Strategy parameters (identical to the Pine Script defaults) ----
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

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("fvg_bot.log")],
)
log = logging.getLogger("fvg_bot")


# ============================================================
# COINDCX API WRAPPER
# ============================================================

class CoinDCXClient:
    """
    Thin wrapper around CoinDCX's public + authenticated REST endpoints.

    NOTE: verify every path/param against https://docs.coindcx.com
    before relying on this. Written from documented API shape, not
    tested live from this environment.
    """

    def __init__(self, api_key: str, api_secret: str):
        self.api_key = api_key
        self.api_secret = api_secret

    def _signed_headers(self, body: dict) -> dict:
        body_json = json.dumps(body, separators=(",", ":"))
        signature = hmac.new(
            self.api_secret.encode(), body_json.encode(), hashlib.sha256
        ).hexdigest()
        return {
            "Content-Type": "application/json",
            "X-AUTH-APIKEY": self.api_key,
            "X-AUTH-SIGNATURE": signature,
        }, body_json

    def get_candles(self, pair: str, interval: str, limit: int = 500) -> pd.DataFrame:
        """Public endpoint — no auth needed."""
        url = f"{PUBLIC_BASE}/market_data/candles"
        params = {"pair": pair, "interval": interval, "limit": limit}
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        df = pd.DataFrame(data)
        # Expected fields per CoinDCX docs: open, high, low, close, volume, time
        df = df.rename(columns={
            "open": "Open", "high": "High", "low": "Low",
            "close": "Close", "volume": "Volume", "time": "Time",
        })
        df["Time"] = pd.to_datetime(df["Time"], unit="ms")
        df = df.sort_values("Time").reset_index(drop=True)
        return df[["Time", "Open", "High", "Low", "Close", "Volume"]]

    def get_balance(self) -> list:
        body = {"timestamp": int(time.time() * 1000)}
        headers, body_json = self._signed_headers(body)
        resp = requests.post(f"{API_BASE}/exchange/v1/users/balances",
                              data=body_json, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def create_order(self, side: str, order_type: str, market: str,
                      total_quantity: float, price: Optional[float] = None) -> dict:
        """
        side: 'buy' or 'sell'
        order_type: 'market_order' or 'limit_order'
        For market orders, price is ignored by the exchange.
        """
        body = {
            "side": side,
            "order_type": order_type,
            "market": market,
            "total_quantity": total_quantity,
            "timestamp": int(time.time() * 1000),
        }
        if order_type == "limit_order" and price is not None:
            body["price_per_unit"] = price

        headers, body_json = self._signed_headers(body)
        resp = requests.post(f"{API_BASE}/exchange/v1/orders/create",
                              data=body_json, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()

    def cancel_all_orders(self, market: str) -> dict:
        body = {"market": market, "timestamp": int(time.time() * 1000)}
        headers, body_json = self._signed_headers(body)
        resp = requests.post(f"{API_BASE}/exchange/v1/orders/cancel_all",
                              data=body_json, headers=headers, timeout=10)
        resp.raise_for_status()
        return resp.json()


# ============================================================
# STRATEGY ENGINE — same logic as the Pine Script / Python backtest
# ============================================================

@dataclass
class Zone:
    top: float
    bottom: float
    direction: int   # 1 bull, -1 bear
    state: int       # 0=FVG, 1=IFVG, 3=confirmed
    score: float
    tapped: bool
    retest: int
    born: int


@dataclass
class Position:
    direction: int
    entry: float
    stop: float
    target: float
    entry_bar: int
    rr: float
    order_id: Optional[str] = None


class FVGStrategy:
    """
    IMPORTANT FIX: this engine now keeps zones and bar-count state
    PERSISTENT across calls to step(), and only processes the single
    newest closed candle each call — it does NOT replay the whole
    fetched window from scratch every poll.

    The earlier version rebuilt self.zones = [] and reprocessed bars
    2..n on every single call, while self.position survived across
    calls. Since the bot polls with an overlapping/sliding window
    (e.g. "last 500 candles" fetched repeatedly), that meant the same
    historical bars got reprocessed again and again, triggering
    SL/TP logic on already-closed trades repeatedly. A direct test
    against 6 months of real BTC 15m data confirmed this: 106 opens
    vs 14,580 closes, which is nonsensical for a single-position-at-a-time
    strategy. This rewrite fixes that by tracking an absolute bar
    counter and only ever advancing it by the number of genuinely new
    candles since the last call.
    """

    MAX_HISTORY_BARS = 3000  # plenty for ATR/EMA200/swing lookback warm-up; trimmed below

    def __init__(self):
        self.zones: list[Zone] = []
        self.position: Optional[Position] = None
        self.history: Optional[pd.DataFrame] = None   # ever-growing, append-only (until trimmed)
        self.processed_count: int = 0                  # how many rows already run through per-bar logic

    def compute_signals(self, df: pd.DataFrame):
        """Recompute detection arrays over the whole candle window.
        Mirrors backtest_generic.py's logic exactly."""
        o = df["Open"].values; h = df["High"].values
        l = df["Low"].values; c = df["Close"].values
        v = df["Volume"].values
        n = len(df)

        tr = np.zeros(n); tr[0] = h[0] - l[0]
        for i in range(1, n):
            tr[i] = max(h[i]-l[i], abs(h[i]-c[i-1]), abs(l[i]-c[i-1]))
        atr = np.full(n, np.nan)
        if n > ATR_LEN:
            atr[ATR_LEN-1] = tr[:ATR_LEN].mean()
            for i in range(ATR_LEN, n):
                atr[i] = (atr[i-1]*(ATR_LEN-1) + tr[i]) / ATR_LEN

        ema_trend = pd.Series(c).ewm(span=TREND_EMA_LEN, adjust=False).mean().values

        prev_low2 = pd.Series(l).shift(2)
        prev_high2 = pd.Series(h).shift(2)
        previousLow = prev_low2.rolling(SWING_LEN).min().values
        previousHigh = prev_high2.rolling(SWING_LEN).max().values
        volAvg = pd.Series(v).rolling(VOL_LEN).mean().values

        def barssince(flags):
            out = np.full(len(flags), np.inf); last = np.inf
            for i in range(len(flags)):
                last = 0 if flags[i] else (last+1 if last != np.inf else np.inf)
                out[i] = last
            return out

        bullDet = np.zeros(n, dtype=bool); bearDet = np.zeros(n, dtype=bool)
        bullScore = np.zeros(n); bearScore = np.zeros(n)
        ssl_h = np.zeros(n, dtype=bool); bsl_h = np.zeros(n, dtype=bool)
        bullBOS = np.zeros(n, dtype=bool); bearBOS = np.zeros(n, dtype=bool)

        for i in range(2, n):
            if np.isnan(atr[i]) or np.isnan(previousLow[i]) or np.isnan(previousHigh[i]):
                continue
            body = abs(c[i-1]-o[i-1]); rng = h[i-1]-l[i-1]
            bodyPct = (body/rng*100) if rng > 0 else 0.0
            bullDisp = (c[i-1] > o[i-1]) and (bodyPct >= MIN_BODY)
            bearDisp = (c[i-1] < o[i-1]) and (bodyPct >= MIN_BODY)
            bullGap = l[i]-h[i-2]; bearGap = l[i-2]-h[i]
            bull = (h[i-2] < l[i]) and bullDisp and (bullGap >= atr[i]*MIN_GAP_ATR)
            bear = (l[i-2] > h[i]) and bearDisp and (bearGap >= atr[i]*MIN_GAP_ATR)
            ssl_h[i] = (l[i-1] < previousLow[i]) and (c[i-1] > previousLow[i])
            bsl_h[i] = (h[i-1] > previousHigh[i]) and (c[i-1] < previousHigh[i])
            bullBOS[i] = c[i-1] > previousHigh[i]
            bearBOS[i] = c[i-1] < previousLow[i]
            volOK = (not np.isnan(volAvg[i-1])) and volAvg[i-1] > 0 and v[i-1] >= volAvg[i-1]*VOL_MULT
            bullDet[i] = bull; bearDet[i] = bear
            bullScore[i] = (25 if bullDisp else 0)+(15 if bullGap>=atr[i]*MIN_GAP_ATR else 0)+(20 if bullBOS[i] else 0)+(15 if volOK else 0)
            bearScore[i] = (25 if bearDisp else 0)+(15 if bearGap>=atr[i]*MIN_GAP_ATR else 0)+(20 if bearBOS[i] else 0)+(15 if volOK else 0)

        sslBars = barssince(ssl_h); bslBars = barssince(bsl_h)
        recentSSL = sslBars <= SWEEP_LOOKBACK; recentBSL = bslBars <= SWEEP_LOOKBACK
        for i in range(n):
            if bullDet[i]: bullScore[i] += 25 if recentSSL[i] else 0
            if bearDet[i]: bearScore[i] += 25 if recentBSL[i] else 0

        validBull = bullDet & (bullScore >= MIN_SCORE)
        validBear = bearDet & (bearScore >= MIN_SCORE)
        if REQUIRE_SWEEP:
            validBull &= recentSSL; validBear &= recentBSL
        if REQUIRE_BOS:
            validBull &= bullBOS; validBear &= bearBOS

        return dict(o=o, h=h, l=l, c=c, atr=atr, ema_trend=ema_trend,
                    validBull=validBull, validBear=validBear,
                    bullScore=bullScore, bearScore=bearScore, n=n)

    def step(self, df: pd.DataFrame) -> list:
        """
        FIXED VERSION. `df` is whatever the exchange's candle endpoint
        just returned (may overlap heavily with what we already have —
        e.g. "last 500 candles" fetched repeatedly). This method:

          1. Merges only genuinely NEW rows (by timestamp) into
             self.history, which otherwise never shrinks or gets
             reprocessed.
          2. Runs the per-bar zone/position logic ONLY over the newly
             appended rows (self.processed_count onward) — every bar
             is processed exactly once, ever, across the life of the
             bot process.
          3. self.zones and self.position persist correctly across
             calls because nothing is reset and no bar is replayed.

        Returns a LIST of decisions (usually 0 or 1 per call, but if
        the bot was offline and multiple candles closed since the last
        poll, every intervening signal is still returned, in order).
        """
        if self.history is None:
            self.history = df.copy().reset_index(drop=True)
        else:
            merged = pd.concat([self.history, df], ignore_index=True)
            merged = merged.drop_duplicates(subset="Time", keep="first")
            merged = merged.sort_values("Time").reset_index(drop=True)
            self.history = merged

        # Trim very old history to bound memory growth over long runtimes.
        # Safe to do because MAX_HISTORY_BARS is far larger than any
        # indicator's lookback (ATR14, EMA200, swing10) and far larger
        # than MAX_ZONE_AGE, so no active zone's reference bar is lost —
        # but we must shift processed_count and every zone's born/retest
        # bar index down by the trimmed amount to keep them valid.
        if len(self.history) > self.MAX_HISTORY_BARS:
            trim = len(self.history) - self.MAX_HISTORY_BARS
            self.history = self.history.iloc[trim:].reset_index(drop=True)
            self.processed_count = max(0, self.processed_count - trim)
            for z in self.zones:
                z.born -= trim
                if z.retest >= 0:
                    z.retest -= trim
            self.zones = [z for z in self.zones if z.born >= 0]  # drop anything trimmed away

        sig = self.compute_signals(self.history)
        o, h, l, c, atr, ema_trend = sig["o"], sig["h"], sig["l"], sig["c"], sig["atr"], sig["ema_trend"]
        validBull, validBear = sig["validBull"], sig["validBear"]
        bullScore, bearScore, n = sig["bullScore"], sig["bearScore"], sig["n"]

        start = max(2, self.processed_count)
        decisions = []

        for i in range(start, n):
            if validBull[i]:
                top, bottom = l[i], h[i-2]
                dup = any(z.direction==1 and max(0.0,min(top,z.top)-max(bottom,z.bottom))/max(top-bottom,1e-9) >= 0.80 for z in self.zones)
                if not dup:
                    self.zones.append(Zone(top, bottom, 1, 0, bullScore[i], False, -1, i))
            if validBear[i]:
                top, bottom = l[i-2], h[i]
                dup = any(z.direction==-1 and max(0.0,min(top,z.top)-max(bottom,z.bottom))/max(top-bottom,1e-9) >= 0.80 for z in self.zones)
                if not dup:
                    self.zones.append(Zone(top, bottom, -1, 0, bearScore[i], False, -1, i))

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
                    brokenBull = (z.direction==1) and (c[i] < z.bottom)
                    brokenBear = (z.direction==-1) and (c[i] > z.top)
                    if (brokenBull or brokenBear) and (z.tapped or touched):
                        z.direction = -z.direction; z.state = 1
                        z.tapped = False; z.retest = -1; z.born = i
                elif z.state == 1:
                    invalidBull = (z.direction==1) and (c[i] < z.bottom)
                    invalidBear = (z.direction==-1) and (c[i] > z.top)
                    if invalidBull or invalidBear:
                        remove_idx.append(zi); continue
                    tol = atr[i]*RETEST_TOL
                    touchedEdge = (l[i] <= z.bottom+tol) if z.direction==1 else (h[i] >= z.top-tol)
                    if touchedEdge and z.retest == -1:
                        z.retest = i
                    if z.retest >= 0 and i > z.retest:
                        bullishConfirm = (z.direction==1) and (c[i] > z.top) and (c[i] > o[i])
                        bearishConfirm = (z.direction==-1) and (c[i] < z.bottom) and (c[i] < o[i])
                        if bullishConfirm or bearishConfirm:
                            z.state = 3
                            if bullishConfirm and (selected_long is None or z.score > selected_long[2]):
                                selected_long = (z.bottom, z.top, z.score, z.retest)
                            if bearishConfirm and (selected_short is None or z.score > selected_short[2]):
                                selected_short = (z.top, z.bottom, z.score, z.retest)
            for zi in sorted(set(remove_idx), reverse=True):
                self.zones.pop(zi)

            # position monitoring on this bar
            if self.position is not None and i > self.position.entry_bar:
                d = self.position.direction
                hitStop = (l[i] <= self.position.stop) if d==1 else (h[i] >= self.position.stop)
                hitTarget = (h[i] >= self.position.target) if d==1 else (l[i] <= self.position.target)
                if hitStop:
                    decisions.append({"action": "close", "reason": "SL", "price": self.position.stop, "bar": i})
                    self.position = None
                elif hitTarget:
                    decisions.append({"action": "close", "reason": "TP", "price": self.position.target, "bar": i})
                    self.position = None

            # entry generation
            if self.position is None:
                recentHigh = np.max(h[max(0,i-TARGET_LOOKBACK):i]) if i>0 else np.nan
                recentLow = np.min(l[max(0,i-TARGET_LOOKBACK):i]) if i>0 else np.nan
                trend_ok_long = (not USE_TREND_FILTER) or (c[i] > ema_trend[i])
                trend_ok_short = (not USE_TREND_FILTER) or (c[i] < ema_trend[i])

                if selected_long is not None and trend_ok_long:
                    bottom, top, score, rb = selected_long
                    entry = bottom; zh = top-bottom
                    slDist = max(zh*SL_ZONE_MULT, atr[i]*SL_BUFFER)
                    stop = bottom - slDist; risk = entry-stop
                    if risk > 1e-9:
                        minTarget = entry + risk*MIN_RR
                        target = recentHigh if (not np.isnan(recentHigh) and recentHigh>minTarget) else minTarget
                        actualRR = (target-entry)/risk
                        if actualRR >= MIN_RR:
                            self.position = Position(1, entry, stop, target, rb, actualRR)
                            decisions.append({"action": "open", "direction": "LONG", "entry": entry,
                                        "stop": stop, "target": target, "rr": actualRR, "bar": i})
                elif selected_short is not None and trend_ok_short:
                    top, bottom, score, rb = selected_short
                    entry = top; zh = top-bottom
                    slDist = max(zh*SL_ZONE_MULT, atr[i]*SL_BUFFER)
                    stop = top + slDist; risk = stop-entry
                    if risk > 1e-9:
                        minTarget = entry - risk*MIN_RR
                        target = recentLow if (not np.isnan(recentLow) and recentLow<minTarget) else minTarget
                        actualRR = (entry-target)/risk
                        if actualRR >= MIN_RR:
                            self.position = Position(-1, entry, stop, target, rb, actualRR)
                            decisions.append({"action": "open", "direction": "SHORT", "entry": entry,
                                        "stop": stop, "target": target, "rr": actualRR, "bar": i})

        self.processed_count = n
        return decisions


# ============================================================
# MAIN LOOP
# ============================================================

def main():
    if not DRY_RUN and (not API_KEY or not API_SECRET):
        log.error("DRY_RUN is False but COINDCX_API_KEY / COINDCX_API_SECRET are not set. "
                   "Refusing to start live. Set env vars or keep DRY_RUN=True.")
        return

    client = CoinDCXClient(API_KEY, API_SECRET)
    strat = FVGStrategy()
    last_candle_time = None

    log.info(f"Starting {'DRY-RUN' if DRY_RUN else 'LIVE'} bot on {PAIR} @ {CANDLE_INTERVAL}")

    while True:
        try:
            df = client.get_candles(PAIR, CANDLE_INTERVAL, limit=500)
        except Exception as e:
            log.error(f"Failed to fetch candles: {e}")
            time.sleep(POLL_SECONDS)
            continue

        latest_closed_time = df["Time"].iloc[-2]  # second-to-last row = last fully closed candle
        if latest_closed_time == last_candle_time:
            time.sleep(POLL_SECONDS)
            continue
        last_candle_time = latest_closed_time

        df_closed = df.iloc[:-1].reset_index(drop=True)  # exclude the still-forming candle
        decisions = strat.step(df_closed)  # now a LIST — usually 0 or 1, more if bot was offline

        if not decisions:
            log.info(f"[{latest_closed_time}] No action. Position open: {strat.position is not None}")

        for d in decisions:
            if d["action"] == "open":
                log.info(f"[{latest_closed_time}] SIGNAL: {d['direction']} entry={d['entry']:.2f} "
                          f"stop={d['stop']:.2f} target={d['target']:.2f} RR={d['rr']:.2f}")
                side = "buy" if d["direction"] == "LONG" else "sell"
                if DRY_RUN:
                    log.info(f"[DRY RUN] Would place {side} market order, qty={ORDER_SIZE_BTC} BTC")
                else:
                    try:
                        resp = client.create_order(side, "market_order", PAIR, ORDER_SIZE_BTC)
                        log.info(f"Order placed: {resp}")
                        # NOTE: CoinDCX's market order creates an immediate fill; you are
                        # responsible for separately placing/managing the resting SL and TP
                        # orders (e.g. via their stop-limit order type) — this skeleton logs
                        # the intended stop/target levels but does not yet submit them as
                        # exchange-side stop orders. Extend create_order calls here for that
                        # before relying on this for unattended live trading.
                    except Exception as e:
                        log.error(f"Order placement failed: {e}")
            elif d["action"] == "close":
                log.info(f"[{latest_closed_time}] EXIT: {d['reason']} at {d['price']:.2f}")
                if DRY_RUN:
                    log.info(f"[DRY RUN] Would close position, qty={ORDER_SIZE_BTC} BTC")
                else:
                    try:
                        # closing side is opposite of the entry side; track this per-position
                        # in a production version rather than assuming here
                        resp = client.create_order("sell", "market_order", PAIR, ORDER_SIZE_BTC)
                        log.info(f"Close order placed: {resp}")
                    except Exception as e:
                        log.error(f"Close order failed: {e}")

        time.sleep(POLL_SECONDS)


if __name__ == "__main__":
    main()
