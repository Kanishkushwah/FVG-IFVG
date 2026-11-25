#!/data/data/com.termux/files/usr/bin/bash
set -e

echo "===================================================="
echo "  ANTIGRAVITY BOT - TERMUX ONEPLUS SETUP & RUNNER  "
echo "===================================================="

# 1. Acquire wake lock so Android doesn't suspend Termux CPU
termux-wake-lock 2>/dev/null || true
echo "✔ Wake lock acquired (Termux CPU will stay active 24/7)"

# 2. Update and install packages
echo "📦 Installing system packages (Python, NumPy, Pandas, Git, OpenSSH)..."
pkg update -y
pkg install -y python python-numpy python-pandas git openssh termux-api

# 3. Install Python dependencies
echo "📦 Installing Python packages..."
pip install --upgrade pip
pip install flask requests pytz websocket-client
pip install --no-cache-dir git+https://github.com/rongardF/tvdatafeed.git

# 4. Start SSH Server on Port 8022
sshd || true
USER_NAME=$(whoami 2>/dev/null || echo "u0_a...")
PHONE_IP=$(ip -4 addr show wlan0 2>/dev/null | grep -oP '(?<=inet\s)\d+(\.\d+){3}' || ifconfig 2>/dev/null | grep 'inet ' | grep -v '127.0.0.1' | awk '{print $2}' | head -n 1)

echo "===================================================="
echo "✔ Setup Completed Successfully!"
echo "📱 Termux User: $USER_NAME"
echo "🌐 Phone Local IP: $PHONE_IP"
echo "🔗 Health Check Dashboard: http://$PHONE_IP:10000/health"
echo "🔑 SSH Access from Mac: ssh -p 8022 $USER_NAME@$PHONE_IP"
echo "===================================================="
echo "🚀 Starting Antigravity Dual Bot (BTC + Gold)..."

python antigravity_bot.py
