#!/data/data/com.termux/files/usr/bin/bash
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

termux-wake-lock 2>/dev/null || true

# Kill any existing instances cleanly
pkill -f "watchdog.sh" 2>/dev/null || true
pkill -f "antigravity_bot.py" 2>/dev/null || true
sleep 1

# Start watchdog in background
nohup bash "$DIR/watchdog.sh" > /dev/null 2>&1 < /dev/null &

echo "===================================================="
echo "✔ Antigravity 24/7 Watchdog Started in Background!"
echo "Watchdog PID: $!"
echo "Auto-restarts automatically if Wi-Fi drops or bot exits."
echo "View live logs: tail -f bot.log"
echo "===================================================="
