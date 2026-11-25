#!/data/data/com.termux/files/usr/bin/bash
termux-wake-lock 2>/dev/null || true
pkill -f antigravity_bot.py 2>/dev/null || true
nohup python antigravity_bot.py > bot.log 2>&1 &
echo "===================================================="
echo "✔ Antigravity Bot is now running 24/7 in background!"
echo "PID: $!"
echo "View live logs: tail -f bot.log"
echo "===================================================="
