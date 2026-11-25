#!/data/data/com.termux/files/usr/bin/bash
# ==============================================================================
# ANTIGRAVITY 24/7 BULLETPROOF WATCHDOG FOR TERMUX
# Automatically restarts the bot if it crashes, loses connection, or exits.
# ==============================================================================

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$DIR"

# Ensure wake-lock is held
termux-wake-lock 2>/dev/null || true

echo "====================================================" >> bot.log
echo "$(date '+%Y-%m-%d %H:%M:%S IST') [WATCHDOG] Watchdog protection service started." >> bot.log
echo "====================================================" >> bot.log

while true; do
    echo "$(date '+%Y-%m-%d %H:%M:%S IST') [WATCHDOG] Launching antigravity_bot.py..." >> bot.log
    python antigravity_bot.py >> bot.log 2>&1
    EXIT_CODE=$?
    echo "$(date '+%Y-%m-%d %H:%M:%S IST') [WATCHDOG] Bot process exited with code $EXIT_CODE. Auto-restarting in 5 seconds..." >> bot.log
    sleep 5
done
