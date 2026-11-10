# FVG-IFVG

### Antigravity Bot — 24/7 Production Algorithmic Trading Bot for CoinDCX BTC/INR

Automated cloud deployment designed for **Render.com** free tier.

#### Features
- **Strategy:** ICT Fair Value Gaps (FVG) + Inversions (IFVG) + 200 EMA Trend Filter
- **Risk Management:** Dynamic ₹200 risk per trade on CoinDCX Indian Rupee pair (`B-BTC_INR`)
- **Starting Paper Balance:** ₹5,000 with real-time balance tracking & PnL logs
- **Trailing Stop-Loss:**
  - Moves to Cost-to-Cost (Breakeven) when price hits 50% TP distance
  - Trails +1R for every full 1R gained
- **Multi-Timeframe Engine:** Runs concurrent independent strategy engines across **5m, 15m, and 30m** intervals
- **Keep-Alive Server:** Built-in Flask web server on background thread (`/` route returns 200 OK for UptimeRobot / cron-job.org pings)
- **Timezone:** Native IST (`Asia/Kolkata`) telemetry and logging

#### Deployment on Render
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python antigravity_bot.py`
