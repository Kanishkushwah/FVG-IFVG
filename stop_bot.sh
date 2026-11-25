#!/data/data/com.termux/files/usr/bin/bash
pkill -f "watchdog.sh" 2>/dev/null || true
pkill -f "antigravity_bot.py" 2>/dev/null || true
echo "✔ Antigravity Bot and Watchdog stopped."
