# Project Handoff: ICT FVG + IFVG PRO Indicator + CoinDCX Bot

This document summarizes everything done in a long conversation with Claude,
so work can continue with another AI assistant without losing context.

---

## 1. WHAT THIS PROJECT IS

A Pine Script (TradingView) indicator called **"ICT FVG + IFVG PRO V10.3"**
that detects Fair Value Gaps (FVG), their inversions (IFVG), confirms
retest/entry signals, and plots LONG/SHORT trade setups with SL/TP — plus
a **Python backtest suite** that validated it on real historical data, and
a **CoinDCX trading bot** built from the same logic.

Two final deliverables exist as files:
- `ICT_FVG_IFVG_PRO_V10.3.pine` — the TradingView indicator (current final version)
- `fvg_coindcx_bot.py` — the CoinDCX live/paper trading bot (current final version, bug-fixed)

**If continuing with Gemini: upload both of these files along with this
summary**, since this document describes their history/reasoning but
Gemini will need the actual current code to keep editing it.

---

## 2. PINE SCRIPT INDICATOR — EVOLUTION & BUGS FIXED

Started as a working FVG/IFVG indicator but had many real bugs, found via
user testing on live TradingView charts (Gold, NQ futures, BTC) and fixed
iteratively:

1. **Zones drifting/stretching on zoom/pan** — boxes used a bad
   auto-extend logic that stretched them across the whole chart. Fixed by
   reverting to fixed-width zones (anchored via `xloc.bar_time`).

2. **Entry labels floating away from candles** — the retest-tap detection
   used "any overlap with zone" instead of requiring price to actually
   touch the specific edge that becomes the entry price. Fixed with an
   edge-specific touch check, with a tunable `Retest Tolerance (x ATR)`
   input (default 0.25) so it's not *too* strict (too strict = too few
   signals, too loose = disconnected-looking labels).

3. **Stop-loss shown as razor-thin on the chart** — SL was only
   `ATR × 0.10` from entry (entry itself was already the zone edge, so SL
   was almost on top of it). Fixed: SL distance = `max(zone height × SL
   Zone Multiplier, ATR × SL Buffer)`, with both as tunable inputs
   (`SL ATR Buffer` default 1.0, `SL Zone Height Multiplier` default 1.5).

4. **Multi-timeframe FVG support added** — originally single `fvgTF`
   input; refactored into **3 independent timeframe slots** (each with
   its own enable toggle), e.g. 15m + 30m + 1H FVGs simultaneously. Also
   replaced a hard `runtime.error()` crash (when selected TF < chart TF)
   with a silent per-slot "invalid" flag.

5. **Zone text not showing "FVG"/"IFVG" labels** — text visibility was
   incorrectly gated behind "Show FVG" toggle; fixed so "Show Zone Text"
   alone controls it. Text size also bumped from `small` to `normal`.

6. **Couldn't tell which zone caused which trade** — added logic so that
   ONLY the exact zone that produced an actual trade (not every confirmed
   zone) gets a thickened border + extended box, so it's visually obvious
   which FVG/IFVG is the source of a given LONG/SHORT label.

7. **Confirmed/traded zones getting deleted by "Maximum Zones" cleanup** —
   the cleanup used to always delete the oldest zone regardless of
   whether it was a trade's source. Fixed to prefer deleting unconfirmed
   zones first, protecting trade-source zones from being purged.

8. **Trend filter added** — an EMA (default length 200) gate: LONGs only
   allowed when price is above the EMA, SHORTs only when below. This was
   added after diagnosing a specific failed trade (a LONG taken in the
   middle of a strong bearish impulse) and backtesting confirmed it
   roughly **halves max drawdown and raises win rate** (see backtest
   results below). Toggle: `Use Trend Filter (EMA)`, default ON.

9. **Entries only working on "one particular chart timeframe"** — the
   3 timeframe slots would go fully "invalid" (and thus produce zero
   signals) if the user viewed a chart timeframe HIGHER than a configured
   slot (e.g. slots at 15m/30m, chart switched to 1H → both invalid).
   Fixed: each slot now has an "effective timeframe" that falls back to
   the chart's own timeframe when the configured one is invalid, so a
   slot is never fully dead regardless of what chart timeframe is open.

### Current indicator settings structure (as of final version)
- **FVG Detection**: 3 timeframe slots (enable + TF each), ATR Length,
  Min Gap ATR, Displacement Body %
- **Quality Filters**: Minimum Quality (score 0-100, default 80),
  Structure Lookback, Sweep Lookback, Volume Average, Relative Volume,
  Require Liquidity Sweep (off), Require BOS (off)
- **Zone Management**: Maximum Zones (12), Zone Width, Maximum Zone Age
  (500), Require FVG Tap Before IFVG, Retest Tolerance (0.25x ATR), show
  toggles
- **Entry & Risk**: Enable Entries, Use Trend Filter (ON, EMA len 200),
  Minimum RR (1.5), SL ATR Buffer (1.0), SL Zone Height Multiplier (1.5),
  Target Lookback (50), Position History, show toggles
- **Visual Colors**: bull/bear FVG/IFVG colors

### Scoring system (how a zone's quality score is computed, 0-100)
- +25 if displacement body % ≥ threshold (strong momentum candle)
- +15 if the gap size ≥ ATR × Min Gap ATR
- +25 if a recent liquidity sweep (SSL/BSL) happened within lookback
- +20 if a Break of Structure (BOS) occurred
- +15 if volume ≥ relative volume multiplier × average
- Minimum 80 required by default to even register as a valid FVG

---

## 3. PYTHON BACKTESTING — METHODOLOGY & RESULTS

**Important limitation disclosed throughout**: Claude cannot execute Pine
Script directly (no TradingView engine access) and has no live network
access in this environment. All backtesting was done by **hand-replicating
the Pine Script's exact logic in Python** (same detection math, same zone
state machine, same retest/confirmation/SL/TP rules) and running it
bar-by-bar over real historical OHLCV data that the user uploaded as CSV
files. This was repeatedly caveated as "a faithful replica, not a literal
Pine execution — cross-check against TradingView's own Strategy Tester
before trusting for real capital."

### Data used
- **Gold (XAUUSD) 15m**, ~6 months (user-uploaded CSV, columns: Date;Open;High;Low;Close;Volume)
- **BTC/USDT 5m and 15m**, full 6 months May–Oct 2026 (12 monthly ZIP files
  downloaded by the user from `data.binance.vision` — Binance's own free
  historical klines archive — then combined/deduplicated in Python)

### Gold 15m backtest results (first full run, before trend filter existed)
- 66 trades, 48.5% win rate, avg RR 3.59, profit factor 2.90, **+57.77R
  total, -34R max drawdown**

### Diagnosed the -34R drawdown's cause
User asked "why did this specific LONG fail" on a live chart screenshot —
it was a counter-trend entry taken mid-impulse during a strong sell-off.
This led to building and testing a trend filter.

### Gold variant comparison (same 6-month window, testing filter combos)
| Config | Trades | Win Rate | Total R | Max DD | PF |
|---|---|---|---|---|---|
| Baseline | 66 | 48.5% | 57.8R | -34.0R | 2.90 |
| + Trend filter (EMA200) | 34 | 61.8% | 41.4R | -13.0R | 4.29 |
| + Trend + requireBOS | 19 | 63.2% | 23.1R | -7.0R | 5.02 |
| + Trend + BOS + Sweep (all strict) | 19 | 63.2% | 23.1R | -7.0R | 5.02 |
| + Trend + BOS, minScore=90 | 14 | 64.3% | 11.4R | -5.0R | 2.37 |
| + Trend + BOS, RR target=1.2 | 21 | 71.4% | 27.6R | -6.0R | 6.23 |

Decision: added the EMA trend filter permanently to the indicator (as
item #8 above), kept BOS/Sweep as optional user toggles (not forced on).

### BTC backtest results (full 6 months, May–Oct 2026)
| Dataset | Trend Filter | Bars | Trades | Win Rate | Total R | Max DD | PF |
|---|---|---|---|---|---|---|---|
| BTC 15m | OFF | 17,664 | 141 | 63.6% | +227.6R | -51.0R | 5.80 |
| **BTC 15m** | **ON** | 17,664 | **96** | **66.3%** | **+138.3R** | **-32.0R** | **6.62** |
| BTC 5m | OFF | 52,992 | 392 | 48.2% | +445.2R | -203.0R | 2.80 |
| BTC 5m | ON | 52,992 | 270 | 55.6% | +309.1R | -120.0R | 3.22 |

**Conclusion given to user**: BTC 15m + Trend Filter ON was the strongest,
most robust combination found across everything tested (Gold included).
5m timeframe carries much deeper drawdowns (even filtered) — flagged as
risky, not recommended without very small position sizing.

A results spreadsheet was also generated: `BTC_FVG_Backtest_Results.xlsx`
(table + notes, no formulas, green-highlighted "Trend Filter ON" rows).

---

## 4. STRATEGY() CONVERSION FOR TRADINGVIEW STRATEGY TESTER

User asked for a `strategy()` version (vs `indicator()`) so they could
cross-check Python backtest numbers against TradingView's own Strategy
Tester. Work was started (base conversion with `initial_capital=10000`,
`commission_value=0.04%`, `slippage=1`) but **was not finished/delivered**
in this conversation — the user moved on to other requests before this
was completed. **This is open/incomplete work** if picked up again.

Key design note for whoever finishes it: entry price in the indicator is
the *historical* zone edge price (already passed by the time of
confirmation), which doesn't map cleanly to a `strategy.entry()` fill —
the plan was to enter at market on the confirmation bar's close instead,
using the same SL/TP distances, and to flag this as an intentional
simplification vs. the visual indicator's "retroactive ideal entry" style.

---

## 5. COINDCX TRADING BOT

Built a standalone Python bot (`fvg_coindcx_bot.py`) implementing the same
strategy logic, polling CoinDCX's REST API for BTC candles.

### Known/disclosed limitations (told to user explicitly)
- CoinDCX API endpoint paths/payload shapes (`/market_data/candles`,
  `/exchange/v1/orders/create`, HMAC-SHA256 auth scheme) were written from
  training knowledge, **not verified against a live call** (no network
  access in this environment). User was told to cross-check against
  `https://docs.coindcx.com` before trusting it.
- `DRY_RUN = True` by default — logs intended actions without placing
  real orders. User was told to run dry for days before flipping to live.
- SL/TP are NOT yet placed as real resting exchange-side stop orders —
  the bot tracks them internally and closes via market order when hit.
  This means if the bot process dies, the position has no exchange-side
  protection. Flagged as a to-do for production hardening.
- Needs `COINDCX_API_KEY` / `COINDCX_API_SECRET` as environment variables
  (trade-only permission, no withdrawal).
- User asked "do I need to keep my laptop on" — answered yes (it's a
  polling `while True` loop), with suggestions: `tmux`/`screen` on a
  laptop, or better, a cheap always-on VPS (DigitalOcean/AWS
  Lightsail/Oracle free tier) with a systemd service for auto-restart.

### CRITICAL BUG FOUND AND FIXED (important — read this carefully)
User's own instinct ("let's validate on historical data first") caught a
**severe bug** in the first version of the bot:

- **The bug**: `FVGStrategy.step()` reset `self.zones = []` and
  reprocessed the ENTIRE fetched candle window (e.g. last 500 candles)
  from scratch on every single poll, while `self.position` persisted
  across calls. Since the bot polls with an overlapping/sliding window,
  this caused the same historical bars to be reprocessed repeatedly,
  triggering phantom SL/TP closes over and over.
- **Proof it was broken**: tested against 6 months of real BTC 15m data
  using true incremental polling simulation → got **106 opens vs 14,580
  closes**, which is nonsensical for a one-position-at-a-time strategy.
- **The fix**: rewrote the engine to maintain an **ever-growing,
  append-only history** (`self.history`, deduplicated by timestamp,
  trimmed only far beyond any lookback/zone-age need with index
  correction on trim) and a `self.processed_count` cursor so **every bar
  is processed exactly once, ever**, never replayed. `step()` now returns
  a **list** of decisions (handles the case where multiple candles closed
  while the bot was offline) instead of a single dict.
- **Verification after fix**: re-ran the same true-incremental-polling
  test on 6 months of BTC 15m data → got **96 opens, 95 closes (63 wins,
  32 losses), 66.3% win rate** — this **exactly matches** the earlier
  verified Python backtest numbers for "BTC 15m + Trend Filter ON" (see
  table in section 3). This was treated as strong validation that the
  bot's logic is now consistent with the backtested logic.

**This fix is already applied in the current `fvg_coindcx_bot.py` file.**
If Gemini is asked to modify this file further, it should be aware this
persistent-history architecture is load-bearing — reintroducing a
"rebuild from scratch each call" pattern would reintroduce the bug.

---

## 6. OPEN / UNFINISHED ITEMS (for Gemini to potentially pick up)

1. **`strategy()` version for TradingView Strategy Tester** — started,
   not finished (see section 4).
2. **CoinDCX bot: real exchange-side SL/TP orders** — currently
   internally tracked only; a stop-limit order type should be added so
   positions are protected even if the bot process dies.
3. **CoinDCX API endpoints unverified** — should be checked against
   live docs/a real sandbox call before trusting with real funds.
4. User has not yet done a live TradingView Strategy Tester cross-check
   of the Python backtest numbers — this was recommended multiple times
   but not confirmed as done.
5. Multi-timeframe (30m+1H combined) BTC/Gold backtest was not separately
   tested in Python — only single-slot timeframes (15m, 5m, and Gold 15m)
   were backtested, even though the indicator itself supports 3
   simultaneous timeframe slots.

---

## 7. FILES THAT SHOULD BE HANDED OFF ALONGSIDE THIS DOCUMENT

- `ICT_FVG_IFVG_PRO_V10.3.pine` (final indicator)
- `fvg_coindcx_bot.py` (final, bug-fixed bot)
- `BTC_FVG_Backtest_Results.xlsx` (results spreadsheet, optional)
- The combined BTC CSVs (`BTC_5m_combined.csv`, `BTC_15m_combined.csv`)
  and Gold CSV, if further backtesting continuity is wanted — otherwise
  the user can re-derive them from the same Binance Vision source.

---

## 8. USER'S WORKING STYLE / PREFERENCES NOTED IN THIS CONVERSATION
- Communicates primarily in Hindi/Hinglish; responses in this
  conversation were given in Hinglish throughout.
- Prefers direct, concrete fixes over lengthy caveats, but does want
  honest disclosure of limitations (no network/no Pine execution here)
  rather than fabricated confidence.
- Validates claims by testing on actual charts/data and pushing back
  hard when something doesn't match what's shown — this caught multiple
  real bugs (zone drift, SL too tight, entry label disconnect, zone
  cleanup deleting trade sources, and the critical bot polling bug).
- Is working toward live/real-money deployment (CoinDCX bot), so
  correctness and honesty about unverified assumptions matters a lot
  more than usual.
